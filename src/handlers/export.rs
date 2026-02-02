use axum::extract::{Path, State, Multipart};
use axum::response::{Html, Sse, IntoResponse};
use axum::http::{StatusCode, header};
use axum::Json;
use axum_extra::extract::CookieJar;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
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

#[derive(Debug, Deserialize)]
pub struct ImportRequest {
    pub file_path: String,
    pub target_database: String,
    pub format: String, // "custom", "plain", "directory", "tar"
    pub clean: bool,
    pub create_db: bool,
    pub data_only: bool,
    pub schema_only: bool,
    pub disable_triggers: bool,
    pub single_transaction: bool,
    pub verbose: bool,
    pub pg_version: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct UploadResponse {
    pub file_path: String,
    pub file_size: u64,
    pub format: String, // "custom", "plain", "directory", "tar"
}

pub const MAX_LOG_LINES: usize = 10000; // Increased from 100 to support long-running exports
const MAX_UPLOAD_SIZE: u64 = 2 * 1024 * 1024 * 1024; // 2GB

pub async fn import_wizard(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Html<String> {
    let active = get_active_endpoint(&state, &jar).await;
    let ctx = build_ctx_with_endpoint(&state, active.as_ref());

    let tmpl = crate::templates::ImportWizardTemplate { ctx };

    Html(tmpl.render().unwrap_or_else(|e| format!("Template error: {}", e)))
}

pub async fn upload_import_file(
    State(_state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, StatusCode> {
    tracing::info!("Upload handler called");

    let upload_dir = "/tmp/postgres-explorer-imports";
    if let Err(e) = std::fs::create_dir_all(upload_dir) {
        tracing::error!("Failed to create upload dir: {}", e);
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }

    let mut file_path: Option<(String, String)> = None;
    let mut file_size = 0u64;

    tracing::info!("Starting to read multipart fields");
    while let Some(field) = multipart.next_field().await.map_err(|e| {
        tracing::error!("Failed to read multipart field: {} (type: {:?})", e, std::any::type_name_of_val(&e));
        StatusCode::BAD_REQUEST
    })? {
        let name = field.name().unwrap_or("").to_string();
        tracing::info!("Got field: {}", name);

        if name == "file" {
            let filename = field.file_name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("import_{}.dump", uuid::Uuid::new_v4()));

            let path = format!("{}/{}", upload_dir, filename);
            let mut file = tokio::fs::File::create(&path).await.map_err(|e| {
                tracing::error!("Failed to create file: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            // Read entire field data
            let data = field.bytes().await.map_err(|e| {
                tracing::error!("Failed to read field bytes: {}", e);
                StatusCode::BAD_REQUEST
            })?;

            file_size = data.len() as u64;
            if file_size > MAX_UPLOAD_SIZE {
                tracing::warn!("File too large: {} bytes", file_size);
                tokio::fs::remove_file(&path).await.ok();
                return Err(StatusCode::PAYLOAD_TOO_LARGE);
            }

            // Detect format before writing
            let format = detect_dump_format(&data);

            file.write_all(&data).await.map_err(|e| {
                tracing::error!("Failed to write file: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            file_path = Some((path, format));
        }
    }

    let (file_path, format) = file_path.ok_or_else(|| {
        tracing::error!("No file field found in multipart");
        StatusCode::BAD_REQUEST
    })?;

    tracing::info!("File uploaded successfully: {} ({} bytes, format: {})", file_path, file_size, format);

    Ok(Json(UploadResponse {
        file_path,
        file_size,
        format,
    }))
}

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

pub async fn start_import(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Json(req): Json<ImportRequest>,
) -> Result<Json<ExportResponse>, (StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar)
        .await
        .ok_or((StatusCode::BAD_REQUEST, "No active connection".to_string()))?;

    // Verify file exists
    if !tokio::fs::try_exists(&req.file_path).await.unwrap_or(false) {
        return Err((StatusCode::BAD_REQUEST, "Import file not found".to_string()));
    }

    // Generate unique job ID
    let job_id = format!("import_{}", uuid::Uuid::new_v4());

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
        run_import_job(state_clone, job_id_clone, active, req).await;
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
    let log_file_path = format!("{}/{}.log", output_dir, job_id);

    // Create log file
    let log_file = match tokio::fs::File::create(&log_file_path).await {
        Ok(f) => Arc::new(tokio::sync::Mutex::new(f)),
        Err(e) => {
            let error = format!("Failed to create log file: {}", e);
            append_log(&state, &job_id, error.clone()).await;
            complete_job(&state, &job_id, None, Some(error)).await;
            return;
        }
    };

    // Build pg_dump command
    let mut cmd = build_pg_dump_command(&endpoint, &req, &file_path, &state).await;

    append_log_with_file(&state, &job_id, &log_file, "🚀 Starting PostgreSQL export...".to_string()).await;
    append_log_with_file(&state, &job_id, &log_file, format!("📝 Scope: {:?}", req.scope)).await;
    append_log_with_file(&state, &job_id, &log_file, format!("📦 Format: {:?}", req.format)).await;
    append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;

    match cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(mut child) => {
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            let log_file_clone = log_file.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    append_log_with_file(&state_clone, &job_id_clone, &log_file_clone, line).await;
                }
            });

            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            let log_file_clone = log_file.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    // pg_dump writes verbose output to stderr, not errors
                    // Only prefix actual errors (lines starting with "pg_dump: error:")
                    let formatted_line = if line.contains("error:") || line.contains("FATAL") || line.contains("ERROR") {
                        format!("❌ {}", line)
                    } else {
                        line
                    };
                    append_log_with_file(&state_clone, &job_id_clone, &log_file_clone, formatted_line).await;
                }
            });

            match child.wait().await {
                Ok(status) => {
                    if status.success() {
                        append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;
                        append_log_with_file(&state, &job_id, &log_file, "✅ Export completed successfully!".to_string()).await;
                        append_log_with_file(&state, &job_id, &log_file, format!("📦 Dump file: {}", file_path)).await;
                        append_log_with_file(&state, &job_id, &log_file, format!("📋 Log file: {}", log_file_path)).await;
                        complete_job(&state, &job_id, Some(file_path), None).await;
                    } else {
                        let error = format!("Export failed with exit code: {:?}", status.code());
                        append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;
                        append_log_with_file(&state, &job_id, &log_file, format!("❌ {}", error)).await;
                        append_log_with_file(&state, &job_id, &log_file, format!("📋 Log file: {}", log_file_path)).await;
                        complete_job(&state, &job_id, None, Some(error)).await;
                    }
                }
                Err(e) => {
                    let error = format!("Failed to wait for process: {}", e);
                    append_log_with_file(&state, &job_id, &log_file, format!("❌ {}", error)).await;
                    append_log_with_file(&state, &job_id, &log_file, format!("📋 Log file: {}", log_file_path)).await;
                    complete_job(&state, &job_id, None, Some(error)).await;
                }
            }
        }
        Err(e) => {
            let error = format!("Failed to spawn pg_dump: {}", e);
            append_log_with_file(&state, &job_id, &log_file, error.clone()).await;
            complete_job(&state, &job_id, None, Some(error)).await;
        }
    }
}

async fn run_import_job(
    state: Arc<AppState>,
    job_id: String,
    endpoint: crate::db::models::Endpoint,
    req: ImportRequest,
) {
    let output_dir = "/tmp/postgres-explorer-exports";
    std::fs::create_dir_all(output_dir).ok();

    let log_file_path = format!("{}/{}.log", output_dir, job_id);

    // Create log file
    let log_file = match tokio::fs::File::create(&log_file_path).await {
        Ok(f) => Arc::new(tokio::sync::Mutex::new(f)),
        Err(e) => {
            let error = format!("Failed to create log file: {}", e);
            append_log(&state, &job_id, error.clone()).await;
            complete_job(&state, &job_id, None, Some(error)).await;
            return;
        }
    };

    append_log_with_file(&state, &job_id, &log_file, "🚀 Starting PostgreSQL import...".to_string()).await;
    append_log_with_file(&state, &job_id, &log_file, format!("📦 File: {}", req.file_path)).await;
    append_log_with_file(&state, &job_id, &log_file, format!("🎯 Target: {}", req.target_database)).await;
    append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;

    // Step 1: Create database if requested
    if req.create_db && !req.target_database.is_empty() {
        append_log_with_file(&state, &job_id, &log_file, format!("📝 Creating database '{}'...", req.target_database)).await;

        let password = if let Some(db) = &state.db {
            db.get_endpoint_password(&endpoint).await
        } else {
            state.stateless_password.clone()
        };

        let conn_parts = parse_connection_url(&endpoint.url);
        let mut create_cmd = Command::new("psql");

        if let Some(ref pw) = password {
            create_cmd.env("PGPASSWORD", pw);
        }

        create_cmd.arg("-h").arg(&conn_parts.host);
        create_cmd.arg("-p").arg(&conn_parts.port);
        create_cmd.arg("-d").arg(&conn_parts.database);

        if let Some(username) = &endpoint.username {
            create_cmd.arg("-U").arg(username);
        }

        create_cmd.arg("-c").arg(format!("CREATE DATABASE \"{}\"", req.target_database));

        match create_cmd.output().await {
            Ok(output) => {
                if output.status.success() {
                    append_log_with_file(&state, &job_id, &log_file, format!("✅ Database '{}' created successfully", req.target_database)).await;
                    append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;
                } else {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    let error = format!("Failed to create database: {}", stderr);
                    append_log_with_file(&state, &job_id, &log_file, format!("❌ {}", error)).await;
                    complete_job(&state, &job_id, None, Some(error)).await;
                    return;
                }
            }
            Err(e) => {
                let error = format!("Failed to execute CREATE DATABASE: {}", e);
                append_log_with_file(&state, &job_id, &log_file, format!("❌ {}", error)).await;
                complete_job(&state, &job_id, None, Some(error)).await;
                return;
            }
        }
    }

    // Step 2: Build pg_restore command (without --create now)
    let mut cmd = build_pg_restore_command(&endpoint, &req, &state).await;

    tracing::info!("Import command: {:?}", cmd);

    match cmd.stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
    {
        Ok(mut child) => {
            let stdout = child.stdout.take().unwrap();
            let stderr = child.stderr.take().unwrap();

            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            let log_file_clone = log_file.clone();
            tokio::spawn(async move {
                let reader = BufReader::new(stdout);
                let mut lines = reader.lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    append_log_with_file(&state_clone, &job_id_clone, &log_file_clone, line).await;
                }
            });

            let state_clone = state.clone();
            let job_id_clone = job_id.clone();
            let log_file_clone = log_file.clone();
            let stderr_handle = tokio::spawn(async move {
                let reader = BufReader::new(stderr);
                let mut lines = reader.lines();
                let mut error_lines = Vec::new();
                while let Ok(Some(line)) = lines.next_line().await {
                    let formatted_line = if line.contains("error:") || line.contains("FATAL") || line.contains("ERROR") {
                        // Check if this is a non-critical error
                        let is_critical = !is_non_critical_error(&line);
                        error_lines.push(line.clone());
                        if is_critical {
                            format!("❌ {}", line)
                        } else {
                            format!("⚠️  {}", line)
                        }
                    } else {
                        line
                    };
                    append_log_with_file(&state_clone, &job_id_clone, &log_file_clone, formatted_line).await;
                }
                error_lines
            });

            match child.wait().await {
                Ok(status) => {
                    // Wait for stderr processing to complete
                    let error_lines = stderr_handle.await.unwrap_or_default();

                    if status.success() {
                        append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;
                        append_log_with_file(&state, &job_id, &log_file, "✅ Import completed successfully!".to_string()).await;
                        append_log_with_file(&state, &job_id, &log_file, format!("📋 Log file: {}", log_file_path)).await;
                        complete_job(&state, &job_id, None, None).await;

                        // Cleanup import file
                        tokio::fs::remove_file(&req.file_path).await.ok();
                    } else {
                        // Check if errors are only non-critical
                        let all_non_critical = !error_lines.is_empty() &&
                            error_lines.iter().all(|line| is_non_critical_error(line));

                        if all_non_critical {
                            append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;
                            append_log_with_file(&state, &job_id, &log_file, "⚠️  Import completed with warnings (non-critical errors ignored)".to_string()).await;
                            append_log_with_file(&state, &job_id, &log_file, format!("📋 Log file: {}", log_file_path)).await;
                            complete_job(&state, &job_id, None, None).await;
                            tokio::fs::remove_file(&req.file_path).await.ok();
                        } else {
                            let error = format!("Import failed with exit code: {:?}", status.code());
                            append_log_with_file(&state, &job_id, &log_file, "".to_string()).await;
                            append_log_with_file(&state, &job_id, &log_file, format!("❌ {}", error)).await;
                            append_log_with_file(&state, &job_id, &log_file, format!("📋 Log file: {}", log_file_path)).await;
                            complete_job(&state, &job_id, None, Some(error)).await;
                        }
                    }
                }
                Err(e) => {
                    let error = format!("Failed to wait for process: {}", e);
                    append_log_with_file(&state, &job_id, &log_file, format!("❌ {}", error)).await;
                    complete_job(&state, &job_id, None, Some(error)).await;
                }
            }
        }
        Err(e) => {
            let error = format!("Failed to spawn pg_restore: {}", e);
            append_log_with_file(&state, &job_id, &log_file, error.clone()).await;
            complete_job(&state, &job_id, None, Some(error)).await;
        }
    }
}

async fn build_pg_restore_command(
    endpoint: &crate::db::models::Endpoint,
    req: &ImportRequest,
    state: &Arc<AppState>,
) -> Command {
    let _pg_version = if let Some(v) = &req.pg_version {
        if v == "auto" {
            detect_server_version(state, endpoint).await.unwrap_or(18)
        } else {
            v.parse::<u32>().unwrap_or(18)
        }
    } else {
        18
    };

    // Get password
    let password = if let Some(db) = &state.db {
        db.get_endpoint_password(endpoint).await
    } else {
        state.stateless_password.clone()
    };

    let conn_parts = parse_connection_url(&endpoint.url);
    let target_db = if !req.target_database.is_empty() {
        &req.target_database
    } else {
        &conn_parts.database
    };

    // Use psql for plain SQL, pg_restore for other formats
    if req.format == "plain" {
        // psql -h host -p port -U user -d database -f file.sql
        let mut cmd = Command::new("psql");

        if let Some(ref pw) = password {
            cmd.env("PGPASSWORD", pw);
        }

        cmd.arg("-h").arg(&conn_parts.host);
        cmd.arg("-p").arg(&conn_parts.port);
        cmd.arg("-d").arg(target_db);

        if let Some(username) = &endpoint.username {
            cmd.arg("-U").arg(username);
        }

        if req.single_transaction {
            cmd.arg("--single-transaction");
        }

        // Input file
        cmd.arg("-f").arg(&req.file_path);

        return cmd;
    }

    // pg_restore for custom/directory/tar formats
    let mut cmd = Command::new("pg_restore");

    if let Some(ref pw) = password {
        cmd.env("PGPASSWORD", pw);
    }

    cmd.arg("-h").arg(&conn_parts.host);
    cmd.arg("-p").arg(&conn_parts.port);

    if let Some(username) = &endpoint.username {
        cmd.arg("-U").arg(username);
    }

    // Always connect to target DB (created in step 1 if needed)
    cmd.arg("-d").arg(target_db);

    // Options
    if req.clean {
        cmd.arg("--clean");
    }
    if req.data_only {
        cmd.arg("--data-only");
    }
    if req.schema_only {
        cmd.arg("--schema-only");
    }
    if req.disable_triggers {
        cmd.arg("--disable-triggers");
    }
    if req.single_transaction {
        cmd.arg("--single-transaction");
    }
    if req.verbose {
        cmd.arg("--verbose");
    }

    // Always use --no-owner for safety
    cmd.arg("--no-owner");

    // Input file
    cmd.arg(&req.file_path);

    cmd
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

fn is_non_critical_error(error_line: &str) -> bool {
    // List of error patterns that are safe to ignore
    let non_critical_patterns = [
        "unrecognized configuration parameter",  // SET commands for newer PG versions
        "role .* does not exist",                 // When using --no-owner
        "already exists",                         // Re-running import or --clean issues
        "constraint .* already exists",           // Duplicate constraint warnings
        "relation .* already exists",             // Table already exists
        "must be owner of extension",             // COMMENT ON EXTENSION when not superuser
        "must be superuser to create extension",  // Extensions already exist (with --clean)
    ];

    non_critical_patterns.iter().any(|pattern| {
        error_line.contains(pattern) ||
        regex::Regex::new(pattern).map(|re| re.is_match(error_line)).unwrap_or(false)
    })
}

fn detect_dump_format(data: &[u8]) -> String {
    // Custom format: starts with "PGDMP"
    if data.len() >= 5 && &data[0..5] == b"PGDMP" {
        return "custom".to_string();
    }

    // Directory format: would be a directory, not a file
    // Tar format: starts with tar magic
    if data.len() >= 257 && &data[257..262] == b"ustar" {
        return "tar".to_string();
    }

    // Plain SQL format: text file with SQL commands
    // Check for common SQL keywords
    let text = String::from_utf8_lossy(&data[..data.len().min(1000)]);
    if text.contains("CREATE") || text.contains("INSERT") || text.contains("--") {
        return "plain".to_string();
    }

    // Default to custom
    "custom".to_string()
}

pub struct ConnectionParts {
    pub host: String,
    pub port: String,
    pub database: String,
}

pub fn parse_connection_url(url: &str) -> ConnectionParts {
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

async fn append_log_with_file(
    state: &Arc<AppState>,
    job_id: &str,
    log_file: &Arc<tokio::sync::Mutex<tokio::fs::File>>,
    line: String,
) {
    // Append to in-memory VecDeque (for UI streaming)
    append_log(state, job_id, line.clone()).await;

    // Append to log file on disk (for full log download)
    let mut file = log_file.lock().await;
    let line_with_newline = format!("{}\n", line);
    if let Err(e) = file.write_all(line_with_newline.as_bytes()).await {
        tracing::error!("Failed to write to log file: {}", e);
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
                    let is_done = matches!(job.status, JobStatus::Completed | JobStatus::Failed);
                    drop(jobs); // Release lock before potentially waiting

                    if !logs.is_empty() {
                        let data = logs.join("\n");
                        let event = axum::response::sse::Event::default().data(data);
                        return Some((Ok(event), new_index));
                    }

                    // Send keepalive ping even if no new logs (prevents browser timeout)
                    if !is_done && last_index % 50 == 0 {
                        let event = axum::response::sse::Event::default()
                            .comment("keepalive");
                        return Some((Ok(event), new_index));
                    }

                    // Check if job is done
                    if is_done && last_index >= new_index {
                        return None; // Stream ends
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

pub async fn download_log(
    State(state): State<Arc<AppState>>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let jobs = state.export_jobs.read().await;
    let _job = jobs.get(&job_id)
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;
    drop(jobs);

    // Log file path
    let log_file_path = format!("/tmp/postgres-explorer-exports/{}.log", job_id);

    // Check if log file exists
    if !tokio::fs::try_exists(&log_file_path).await.unwrap_or(false) {
        return Err((StatusCode::NOT_FOUND, "Log file not found".to_string()));
    }

    // Read log file
    let file_content = tokio::fs::read(&log_file_path)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to read log file: {}", e)))?;

    let file_name = format!("{}.log", job_id);
    let content_disposition = format!("attachment; filename=\"{}\"", file_name);

    Ok((
        [
            (header::CONTENT_TYPE, "text/plain; charset=utf-8".to_string()),
            (header::CONTENT_DISPOSITION, content_disposition),
        ],
        file_content,
    ))
}
