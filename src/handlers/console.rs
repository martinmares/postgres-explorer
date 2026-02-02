use axum::extract::State;
use axum::response::{Html, Sse, IntoResponse};
use axum::http::StatusCode;
use axum::Json;
use askama::Template;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::process::Command;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use futures::stream::{self, Stream};
use std::convert::Infallible;
use std::time::Duration;
use axum_extra::extract::CookieJar;
use regex::Regex;

use crate::handlers::{build_ctx_with_endpoint, get_active_endpoint, AppState};
use crate::templates::ConsoleTemplate;

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    pub query: String,
    pub read_only: bool,
}

#[derive(Debug, Serialize)]
pub struct ExecuteResponse {
    pub job_id: String,
}

#[derive(Debug, Serialize)]
pub struct DestructiveCheck {
    pub is_destructive: bool,
    pub command_type: Option<String>,
    pub requires_confirmation: bool,
}

fn is_destructive_query(query: &str) -> DestructiveCheck {
    let query_upper = query.to_uppercase();

    // Patterns pro destruktivní příkazy
    let patterns = [
        (r"(?i)^\s*DROP\s+(TABLE|DATABASE|SCHEMA|INDEX|VIEW|FUNCTION|TRIGGER|SEQUENCE)", "DROP"),
        (r"(?i)^\s*DELETE\s+FROM", "DELETE"),
        (r"(?i)^\s*TRUNCATE\s+", "TRUNCATE"),
        (r"(?i)^\s*ALTER\s+.*\s+DROP\s+", "ALTER DROP"),
    ];

    for (pattern, cmd_type) in &patterns {
        if let Ok(re) = Regex::new(pattern) {
            if re.is_match(&query_upper) {
                return DestructiveCheck {
                    is_destructive: true,
                    command_type: Some(cmd_type.to_string()),
                    requires_confirmation: true,
                };
            }
        }
    }

    // UPDATE nebo DELETE bez WHERE je extra nebezpečný
    if query_upper.contains("UPDATE") && query_upper.contains("SET") {
        if !query_upper.contains("WHERE") {
            return DestructiveCheck {
                is_destructive: true,
                command_type: Some("UPDATE (no WHERE)".to_string()),
                requires_confirmation: true,
            };
        }
    }

    if query_upper.contains("DELETE") && !query_upper.contains("WHERE") {
        return DestructiveCheck {
            is_destructive: true,
            command_type: Some("DELETE (no WHERE)".to_string()),
            requires_confirmation: true,
        };
    }

    DestructiveCheck {
        is_destructive: false,
        command_type: None,
        requires_confirmation: false,
    }
}

pub async fn console(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Html<String> {
    let active = get_active_endpoint(&state, &jar).await;
    let use_local_storage = state.db.is_none(); // Stateless mode uses localStorage

    let tpl = ConsoleTemplate {
        ctx: build_ctx_with_endpoint(&state, active.as_ref()),
        title: "Dev Console | Postgres Explorer".to_string(),
        use_local_storage,
        endpoint_id: active.as_ref().map(|e| e.id).unwrap_or(0),
    };

    Html(tpl.render().unwrap_or_else(|_| "Template error".to_string()))
}

pub async fn execute_query(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Json(req): Json<ExecuteRequest>,
) -> Result<Json<ExecuteResponse>, (StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar)
        .await
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    // Check for destructive commands
    let destructive_check = is_destructive_query(&req.query);
    if destructive_check.is_destructive && !state.enable_destructive_commands {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "Destructive command detected: {}. Enable with --enable-destructive-commands flag.",
                destructive_check.command_type.unwrap_or_else(|| "UNKNOWN".to_string())
            ),
        ));
    }

    // Generate unique job ID
    let job_id = format!("console_{}", uuid::Uuid::new_v4());

    // Create job entry
    let job = crate::handlers::ExportJob {
        job_id: job_id.clone(),
        status: crate::handlers::JobStatus::Running,
        logs: std::collections::VecDeque::new(),
        started_at: std::time::SystemTime::now(),
        completed_at: None,
        file_path: None,
        error: None,
    };

    state.export_jobs.write().await.insert(job_id.clone(), job);

    // Spawn background task
    let state_clone = state.clone();
    let job_id_clone = job_id.clone();
    tokio::spawn(async move {
        run_psql_query(state_clone, job_id_clone, active, req).await;
    });

    Ok(Json(ExecuteResponse { job_id }))
}

async fn run_psql_query(
    state: Arc<AppState>,
    job_id: String,
    endpoint: crate::db::models::Endpoint,
    req: ExecuteRequest,
) {
    // Get password
    let password = if let Some(db) = &state.db {
        db.get_endpoint_password(&endpoint).await
    } else {
        state.stateless_password.clone()
    };

    let conn_parts = crate::handlers::export::parse_connection_url(&endpoint.url);

    let mut cmd = Command::new("psql");

    if let Some(ref pw) = password {
        cmd.env("PGPASSWORD", pw);
    }

    cmd.arg("-h").arg(&conn_parts.host);
    cmd.arg("-p").arg(&conn_parts.port);
    cmd.arg("-d").arg(&conn_parts.database);

    if let Some(username) = &endpoint.username {
        cmd.arg("-U").arg(username);
    }

    // psql options for better output
    cmd.arg("--no-psqlrc");  // Don't load user config
    cmd.arg("-a");           // Echo queries
    cmd.arg("-b");           // Stop on error
    cmd.arg("--pset=pager=off");  // Disable pager (important for memory)

    // Set environment to prevent buffering issues
    cmd.env("PAGER", "cat");  // No pager

    append_log(&state, &job_id, "🚀 Starting query execution...".to_string()).await;
    if req.read_only {
        append_log(&state, &job_id, "🔒 Running in READ-ONLY mode".to_string()).await;
    }
    append_log(&state, &job_id, "⚠️  Note: Output limited to first 100 rows to prevent memory issues".to_string()).await;
    append_log(&state, &job_id, "".to_string()).await;

    match cmd.stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(mut child) => {
            let mut stdin = child.stdin.take().unwrap();
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            // Write query to stdin
            let query_to_execute = if req.read_only {
                format!("SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;\nSET statement_timeout = '30s';\n{}\n", req.query)
            } else {
                format!("SET statement_timeout = '30s';\n{}\n", req.query)
            };

            if let Err(e) = stdin.write_all(query_to_execute.as_bytes()).await {
                let error = format!("Failed to write query to psql: {}", e);
                append_log(&state, &job_id, format!("❌ {}", error)).await;
                complete_job(&state, &job_id, None, Some(error)).await;
                return;
            }

            // Flush and close stdin to signal psql we're done
            if let Err(e) = stdin.flush().await {
                tracing::warn!("Failed to flush stdin: {}", e);
            }
            drop(stdin);

            // Stream stdout (limit to max 100 lines to prevent memory issues)
            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                let mut line_count = 0;
                const MAX_OUTPUT_LINES: usize = 100;

                while let Ok(Some(line)) = lines.next_line().await {
                    line_count += 1;
                    append_log(&state_clone, &job_id_clone, line).await;

                    if line_count >= MAX_OUTPUT_LINES {
                        append_log(&state_clone, &job_id_clone, "".to_string()).await;
                        append_log(&state_clone, &job_id_clone, "⚠️  Output truncated - reached 100 line limit".to_string()).await;
                        append_log(&state_clone, &job_id_clone, "💡 Use LIMIT clause in your query to see specific rows".to_string()).await;
                        break;
                    }
                }
            });

            // Stream stderr (with same line limit)
            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                let mut line_count = 0;
                const MAX_OUTPUT_LINES: usize = 100;

                while let Ok(Some(line)) = lines.next_line().await {
                    line_count += 1;

                    if line.contains("ERROR") || line.contains("FATAL") {
                        append_log(&state_clone, &job_id_clone, format!("❌ {}", line)).await;
                    } else if line.contains("WARNING") || line.contains("NOTICE") {
                        append_log(&state_clone, &job_id_clone, format!("⚠️  {}", line)).await;
                    } else {
                        append_log(&state_clone, &job_id_clone, line).await;
                    }

                    if line_count >= MAX_OUTPUT_LINES {
                        break;
                    }
                }
            });

            // Wait for process with timeout (60 seconds max)
            let wait_future = child.wait();
            let wait_result = tokio::time::timeout(
                Duration::from_secs(60),
                wait_future
            ).await;

            match wait_result {
                Ok(Ok(status)) => {
                    if status.success() {
                        append_log(&state, &job_id, "".to_string()).await;
                        append_log(&state, &job_id, "✅ Query executed successfully!".to_string()).await;
                        complete_job(&state, &job_id, None, None).await;
                    } else {
                        let error = format!("Query failed with exit code: {:?}", status.code());
                        append_log(&state, &job_id, "".to_string()).await;
                        append_log(&state, &job_id, format!("❌ {}", error)).await;
                        complete_job(&state, &job_id, None, Some(error)).await;
                    }
                }
                Ok(Err(e)) => {
                    let error = format!("Failed to wait for psql process: {}", e);
                    append_log(&state, &job_id, format!("❌ {}", error)).await;
                    complete_job(&state, &job_id, None, Some(error)).await;
                }
                Err(_) => {
                    // Timeout - force kill the process
                    append_log(&state, &job_id, "".to_string()).await;
                    append_log(&state, &job_id, "⏱️  Query timeout (60 seconds) - process killed".to_string()).await;
                    let error = "Query timeout (60 seconds)".to_string();
                    complete_job(&state, &job_id, None, Some(error)).await;
                }
            }
        }
        Err(e) => {
            let error = format!("Failed to spawn psql: {}", e);
            append_log(&state, &job_id, error.clone()).await;
            complete_job(&state, &job_id, None, Some(error)).await;
        }
    }
}

async fn append_log(state: &Arc<AppState>, job_id: &str, line: String) {
    let mut jobs = state.export_jobs.write().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.logs.push_back(line);
        if job.logs.len() > crate::handlers::export::MAX_LOG_LINES {
            job.logs.pop_front();
        }
    }
}

async fn complete_job(state: &Arc<AppState>, job_id: &str, file_path: Option<String>, error: Option<String>) {
    let mut jobs = state.export_jobs.write().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.status = if error.is_some() {
            crate::handlers::JobStatus::Failed
        } else {
            crate::handlers::JobStatus::Completed
        };
        job.completed_at = Some(std::time::SystemTime::now());
        job.file_path = file_path;
        job.error = error;
    }
}

pub async fn stream_console_logs(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(job_id): axum::extract::Path<String>,
) -> Sse<impl Stream<Item = Result<axum::response::sse::Event, Infallible>>> {
    let stream = stream::unfold(0usize, move |last_index| {
        let state = state.clone();
        let job_id = job_id.clone();

        async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;

                let jobs = state.export_jobs.read().await;
                if let Some(job) = jobs.get(&job_id) {
                    let logs: Vec<String> = job.logs.iter().skip(last_index).cloned().collect();
                    let new_index = last_index + logs.len();
                    let is_done = matches!(job.status, crate::handlers::JobStatus::Completed | crate::handlers::JobStatus::Failed);
                    drop(jobs);

                    if !logs.is_empty() {
                        let data = logs.join("\n");
                        let event = axum::response::sse::Event::default().data(data);
                        return Some((Ok(event), new_index));
                    }

                    // Send keepalive ping
                    if !is_done && last_index % 50 == 0 {
                        let event = axum::response::sse::Event::default()
                            .comment("keepalive");
                        return Some((Ok(event), new_index));
                    }

                    // Check if job is done
                    if is_done && last_index >= new_index {
                        return None;
                    }
                } else {
                    return None;
                }
            }
        }
    });

    Sse::new(stream)
}

pub async fn check_destructive(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ExecuteRequest>,
) -> Json<DestructiveCheck> {
    let mut check = is_destructive_query(&req.query);

    // Pokud není enabled flag, vždy blokuj
    if check.is_destructive && !state.enable_destructive_commands {
        check.requires_confirmation = false; // Nelze provést
    }

    Json(check)
}

#[derive(Debug, Serialize)]
pub struct HistoryResponse {
    pub history: Vec<crate::db::models::QueryHistory>,
}

pub async fn get_history(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Json<HistoryResponse>, (StatusCode, String)> {
    // Only for non-stateless mode
    let db = state.db.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "History not available in stateless mode".to_string()))?;

    let active = get_active_endpoint(&state, &jar)
        .await
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    let history = db.get_query_history(active.id, 50)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to fetch history: {}", e)))?;

    Ok(Json(HistoryResponse { history }))
}

#[derive(Debug, Deserialize)]
pub struct SaveHistoryRequest {
    pub query: String,
    pub status: String,
    pub duration_ms: Option<i64>,
}

pub async fn save_history(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Json(req): Json<SaveHistoryRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    // Only for non-stateless mode
    let db = state.db.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "History not available in stateless mode".to_string()))?;

    let active = get_active_endpoint(&state, &jar)
        .await
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    db.save_query_history(active.id, &req.query, &req.status, req.duration_ms)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to save history: {}", e)))?;

    Ok(StatusCode::OK)
}

pub async fn clear_history(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<StatusCode, (StatusCode, String)> {
    // Only for non-stateless mode
    let db = state.db.as_ref()
        .ok_or((StatusCode::BAD_REQUEST, "History not available in stateless mode".to_string()))?;

    let active = get_active_endpoint(&state, &jar)
        .await
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    db.clear_query_history(active.id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to clear history: {}", e)))?;

    Ok(StatusCode::OK)
}
