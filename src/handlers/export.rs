use axum::extract::{Path, State};
use axum::response::{Html, Sse, IntoResponse};
use axum::http::{StatusCode, header};
use axum::Json;
use axum::body::Body;
use axum_extra::extract::CookieJar;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::process::Command;
use tokio::io::{AsyncBufReadExt, BufReader};
use futures::stream::{self, Stream};
use std::convert::Infallible;
use std::time::Duration;

use crate::handlers::{build_ctx_with_endpoint, get_active_endpoint, AppState, ExportJob, JobStatus};
use crate::templates::ExportWizardTemplate;
use askama::Template;

#[derive(Debug, Deserialize)]
pub struct ExportRequest {
    pub scope: String,          // "full", "schema", "data", "tables"
    pub format: String,          // "custom", "plain", "directory", "tar"
    pub compress: bool,
    pub include_ownership: bool,
    pub include_drop: bool,
    pub include_create_db: bool,
    pub verbose: bool,
    pub exclude_patterns: Option<String>,
    pub pg_version: Option<String>, // "auto", "16", "17", "18"
    pub selected_tables: Option<Vec<String>>, // for scope="tables"
}

#[derive(Debug, Serialize)]
pub struct ExportResponse {
    pub job_id: String,
}

#[derive(Debug, Serialize)]
pub struct JobStatusResponse {
    pub status: String,
    pub started_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub file_path: Option<String>,
    pub error: Option<String>,
}

const MAX_LOG_LINES: usize = 100;

pub async fn export_wizard(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Html<String> {
    let active = get_active_endpoint(&state, &jar).await;
    let ctx = build_ctx_with_endpoint(&state, active.as_ref());

    let tmpl = ExportWizardTemplate { ctx };

    Html(tmpl.render().unwrap_or_else(|e| format!("Template error: {}", e)))
}

pub async fn start_export(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Json(req): Json<ExportRequest>,
) -> Result<Json<ExportResponse>, (StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar)
        .await
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    // Generate unique job ID
    let job_id = format!("export_{}", uuid::Uuid::new_v4());

    // Create job entry
    let job = ExportJob {
        job_id: job_id.clone(),
        status: JobStatus::Running,
        logs: VecDeque::new(),
        started_at: SystemTime::now(),
        completed_at: None,
        file_path: None,
        error: None,
    };

    state.export_jobs.write().await.insert(job_id.clone(), job);

    // Spawn background task
    let state_clone = state.clone();
    let job_id_clone = job_id.clone();
    tokio::spawn(async move {
        run_export_job(state_clone, job_id_clone, active, req).await;
    });

    Ok(Json(ExportResponse { job_id }))
}

async fn run_export_job(
    state: Arc<AppState>,
    job_id: String,
    endpoint: crate::db::models::Endpoint,
    req: ExportRequest,
) {
    let output_dir = "/tmp/postgres-explorer-exports";
    std::fs::create_dir_all(output_dir).ok();

    let file_name = format!("{}.dump", job_id);
    let file_path = format!("{}/{}", output_dir, file_name);

    // Build pg_dump command
    let mut cmd = build_pg_dump_command(&endpoint, &req, &file_path, &state).await;

    append_log(&state, &job_id, "🚀 Starting PostgreSQL export...".to_string()).await;
    append_log(&state, &job_id, format!("📝 Scope: {:?}", req.scope)).await;
    append_log(&state, &job_id, format!("📦 Format: {:?}", req.format)).await;
    append_log(&state, &job_id, "".to_string()).await;

    match cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(mut child) => {
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    append_log(&state_clone, &job_id_clone, line).await;
                }
            });

            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // pg_dump writes verbose output to stderr, not errors
                    // Only prefix actual errors (lines starting with "pg_dump: error:")
                    if line.contains("error:") || line.contains("FATAL") || line.contains("ERROR") {
                        append_log(&state_clone, &job_id_clone, format!("❌ {}", line)).await;
                    } else {
                        append_log(&state_clone, &job_id_clone, line).await;
                    }
                }
            });

            match child.wait().await {
                Ok(status) => {
                    if status.success() {
                        append_log(&state, &job_id, "".to_string()).await;
                        append_log(&state, &job_id, "✅ Export completed successfully!".to_string()).await;
                        append_log(&state, &job_id, format!("📦 File: {}", file_path)).await;
                        complete_job(&state, &job_id, Some(file_path), None).await;
                    } else {
                        let error = format!("Export failed with exit code: {:?}", status.code());
                        append_log(&state, &job_id, "".to_string()).await;
                        append_log(&state, &job_id, format!("❌ {}", error)).await;
                        complete_job(&state, &job_id, None, Some(error)).await;
                    }
                }
                Err(e) => {
                    let error = format!("Failed to wait for process: {}", e);
                    append_log(&state, &job_id, format!("❌ {}", error)).await;
                    complete_job(&state, &job_id, None, Some(error)).await;
                }
            }
        }
        Err(e) => {
            let error = format!("Failed to spawn pg_dump: {}", e);
            append_log(&state, &job_id, error.clone()).await;
            complete_job(&state, &job_id, None, Some(error)).await;
        }
    }
}

async fn build_pg_dump_command(
    endpoint: &crate::db::models::Endpoint,
    req: &ExportRequest,
    output_path: &str,
    state: &Arc<AppState>,
) -> Command {
    // Detect server version if auto
    let _pg_version = if let Some(v) = &req.pg_version {
        if v == "auto" {
            detect_server_version(state, endpoint).await.unwrap_or(18)
        } else {
            v.parse::<u32>().unwrap_or(18)
        }
    } else {
        18
    };

    // TODO: Use _pg_version to select binary path (e.g., pg_dump-17)
    let binary = format!("pg_dump");
    let mut cmd = Command::new(&binary);

    // Get password from DB or stateless config
    let password = if let Some(db) = &state.db {
        db.get_endpoint_password(endpoint).await
    } else {
        state.stateless_password.clone()
    };

    // Set PGPASSWORD environment variable if password exists
    if let Some(ref pw) = password {
        cmd.env("PGPASSWORD", pw);
    }

    // Build connection string with host/port/database format (no password in URL)
    let conn_parts = parse_connection_url(&endpoint.url);
    cmd.arg("-h").arg(&conn_parts.host);
    cmd.arg("-p").arg(&conn_parts.port);
    cmd.arg("-d").arg(&conn_parts.database);

    if let Some(username) = &endpoint.username {
        cmd.arg("-U").arg(username);
    }

    // Format
    match req.format.as_str() {
        "custom" => cmd.arg("-Fc"),
        "plain" => cmd.arg("-Fp"),
        "directory" => cmd.arg("-Fd"),
        "tar" => cmd.arg("-Ft"),
        _ => cmd.arg("-Fc"),
    };

    // Output file
    cmd.arg("-f").arg(output_path);

    // Scope
    match req.scope.as_str() {
        "schema" => { cmd.arg("--schema-only"); },
        "data" => { cmd.arg("--data-only"); },
        "tables" => {
            if let Some(tables) = &req.selected_tables {
                for table in tables {
                    cmd.arg("-t").arg(table);
                }
            }
        },
        _ => {}, // "full" - no extra flags
    }

    // Options
    if req.compress {
        cmd.arg("-Z6");
    }
    if !req.include_ownership {
        cmd.arg("--no-owner");
    }
    if req.include_drop {
        cmd.arg("--clean");
    }
    if req.include_create_db {
        cmd.arg("--create");
    }
    if req.verbose {
        cmd.arg("--verbose");
    }

    if let Some(patterns) = &req.exclude_patterns {
        for pattern in patterns.split(',') {
            cmd.arg("--exclude-table-data").arg(pattern.trim());
        }
    }

    cmd
}

async fn detect_server_version(state: &Arc<AppState>, endpoint: &crate::db::models::Endpoint) -> Option<u32> {
    let pg = crate::handlers::connect_pg(state, endpoint).await.ok()?;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pg)
        .await
        .ok()?;

    // Parse "17.2 (Ubuntu ...)" -> 17
    version.split('.').next()?.parse().ok()
}

struct ConnectionParts {
    host: String,
    port: String,
    database: String,
}

fn parse_connection_url(url: &str) -> ConnectionParts {
    // Parse postgres://[user:pass@]host[:port]/database
    let without_scheme = url.strip_prefix("postgres://")
        .or_else(|| url.strip_prefix("postgresql://"))
        .unwrap_or(url);

    // Remove user:pass@ if present
    let host_part = if let Some(at_pos) = without_scheme.find('@') {
        &without_scheme[at_pos + 1..]
    } else {
        without_scheme
    };

    // Split host:port/database
    let parts: Vec<&str> = host_part.splitn(2, '/').collect();
    let host_port = parts.get(0).unwrap_or(&"localhost:5432");
    let database = parts.get(1).unwrap_or(&"postgres").to_string();

    // Split host:port
    let hp: Vec<&str> = host_port.splitn(2, ':').collect();
    let host = hp.get(0).unwrap_or(&"localhost").to_string();
    let port = hp.get(1).unwrap_or(&"5432").to_string();

    ConnectionParts {
        host,
        port,
        database,
    }
}

async fn append_log(state: &Arc<AppState>, job_id: &str, line: String) {
    let mut jobs = state.export_jobs.write().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.logs.push_back(line);
        if job.logs.len() > MAX_LOG_LINES {
            job.logs.pop_front();
        }
    }
}

async fn complete_job(state: &Arc<AppState>, job_id: &str, file_path: Option<String>, error: Option<String>) {
    let mut jobs = state.export_jobs.write().await;
    if let Some(job) = jobs.get_mut(job_id) {
        job.status = if error.is_some() { JobStatus::Failed } else { JobStatus::Completed };
        job.completed_at = Some(SystemTime::now());
        job.file_path = file_path;
        job.error = error;
    }
}

pub async fn get_job_status(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobStatusResponse>, (StatusCode, String)> {
    let jobs = state.export_jobs.read().await;
    let job = jobs.get(&job_id)
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    Ok(Json(JobStatusResponse {
        status: format!("{:?}", job.status),
        started_at: job.started_at,
        completed_at: job.completed_at,
        file_path: job.file_path.clone(),
        error: job.error.clone(),
    }))
}

pub async fn stream_logs(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
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

                    if !logs.is_empty() {
                        let data = logs.join("\n");
                        let event = axum::response::sse::Event::default().data(data);
                        return Some((Ok(event), new_index));
                    }

                    // Check if job is done
                    if matches!(job.status, JobStatus::Completed | JobStatus::Failed) {
                        if last_index >= job.logs.len() {
                            return None; // Stream ends
                        }
                    }
                } else {
                    return None; // Job not found
                }
            }
        }
    });

    Sse::new(stream)
}

pub async fn download_export(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let jobs = state.export_jobs.read().await;
    let job = jobs.get(&job_id)
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    let file_path = job.file_path.clone()
        .ok_or((StatusCode::NOT_FOUND, "Export file not found".to_string()))?;

    // Read file
    let file_content = tokio::fs::read(&file_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read file: {}", e)))?;

    let file_name = format!("export_{}.dump", job_id);
    let content_disposition = format!("attachment; filename=\"{}\"", file_name);

    Ok((
        [
            (header::CONTENT_TYPE, "application/octet-stream".to_string()),
            (header::CONTENT_DISPOSITION, content_disposition),
        ],
        file_content,
    ))
}
