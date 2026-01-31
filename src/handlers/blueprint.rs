use std::sync::Arc;
use askama::Template;
use axum::extract::State;
use axum::response::Html;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use sqlx::Connection;

use crate::handlers::{build_ctx_with_endpoint, get_active_endpoint, connect_pg, build_pg_url, apply_connection_params, AppState};
use crate::templates::BlueprintWizardTemplate;

#[derive(Debug, Deserialize)]
pub struct BlueprintRequest {
    pub app_name: String,
    pub schema_name: Option<String>,
    pub encoding: Option<String>,
    pub lock_public_schema: bool,
    pub revoke_db_public: bool,
    pub set_search_path: bool,
}

#[derive(Debug, Serialize)]
pub struct BlueprintResponse {
    pub success: bool,
    pub passwords: Option<BlueprintPasswords>,
    pub error: Option<String>,
    pub sql_preview: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct BlueprintPasswords {
    pub admin_password: String,
    pub rw_password: String,
    pub ro_password: String,
}

pub async fn blueprint_wizard(
    State(state): State<Arc<AppState>>,
    jar: axum_extra::extract::CookieJar,
) -> Result<Html<String>, (StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if let Some(ref endpoint) = active {
        if !endpoint.enable_blueprint {
            return Err((
                StatusCode::FORBIDDEN,
                "Blueprint wizard is not enabled for this connection. Please enable it in connection settings.".to_string(),
            ));
        }
    } else {
        return Err((
            StatusCode::FORBIDDEN,
            "No active connection. Please select an endpoint first.".to_string(),
        ));
    }

    let ctx = build_ctx_with_endpoint(&state, active.as_ref());

    let tmpl = BlueprintWizardTemplate {
        ctx,
        title: "Blueprint Database Creator | Postgres Explorer".to_string(),
    };

    let html = tmpl.render()
        .map_err(|e: askama::Error| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Html(html))
}

pub async fn preview_blueprint(
    State(state): State<Arc<AppState>>,
    jar: axum_extra::extract::CookieJar,
    Json(req): Json<BlueprintRequest>,
) -> Result<Json<BlueprintResponse>, (StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if let Some(ref endpoint) = active {
        if !endpoint.enable_blueprint {
            return Err((
                StatusCode::FORBIDDEN,
                "Blueprint wizard is not enabled for this connection".to_string(),
            ));
        }
    } else {
        return Err((
            StatusCode::FORBIDDEN,
            "No active connection".to_string(),
        ));
    }

    // Validate app name
    if !is_valid_identifier(&req.app_name) {
        return Ok(Json(BlueprintResponse {
            success: false,
            passwords: None,
            error: Some("Invalid app name. Use only lowercase letters, numbers, and underscores.".to_string()),
            sql_preview: None,
        }));
    }

    // Get db_admin username from active endpoint
    let db_admin = if let Some(ref endpoint) = active {
        endpoint.username.as_deref().unwrap_or("postgres")
    } else {
        "postgres"
    };

    let schema = req.schema_name.as_ref().unwrap_or(&req.app_name);
    let encoding = req.encoding.as_ref().map(|s| s.as_str()).unwrap_or("UTF8");

    let sql = generate_blueprint_sql(
        &req.app_name,
        db_admin,
        schema,
        encoding,
        req.lock_public_schema,
        req.revoke_db_public,
        req.set_search_path,
        false, // no passwords in preview
    );

    Ok(Json(BlueprintResponse {
        success: true,
        passwords: None,
        error: None,
        sql_preview: Some(sql),
    }))
}

pub async fn execute_blueprint(
    State(state): State<Arc<AppState>>,
    jar: axum_extra::extract::CookieJar,
    Json(req): Json<BlueprintRequest>,
) -> Result<Json<BlueprintResponse>, (StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if let Some(ref endpoint) = active {
        if !endpoint.enable_blueprint {
            return Err((
                StatusCode::FORBIDDEN,
                "Blueprint wizard is not enabled for this connection".to_string(),
            ));
        }
    } else {
        return Err((
            StatusCode::FORBIDDEN,
            "No active connection".to_string(),
        ));
    }

    // Validate app name
    if !is_valid_identifier(&req.app_name) {
        return Ok(Json(BlueprintResponse {
            success: false,
            passwords: None,
            error: Some("Invalid app name. Use only lowercase letters, numbers, and underscores.".to_string()),
            sql_preview: None,
        }));
    }

    if active.is_none() {
        return Ok(Json(BlueprintResponse {
            success: false,
            passwords: None,
            error: Some("No active database connection. Please select an endpoint.".to_string()),
            sql_preview: None,
        }));
    }
    let active = active.unwrap();

    // Get db_admin username from endpoint
    let db_admin = active.username.as_ref().ok_or_else(|| {
        (StatusCode::BAD_REQUEST, "Connection must have a username configured".to_string())
    })?;

    let pg = connect_pg(&state, &active).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to connect: {}", e)))?;

    // Generate passwords
    let admin_password = generate_password();
    let rw_password = generate_password();
    let ro_password = generate_password();

    let schema = req.schema_name.as_ref().unwrap_or(&req.app_name);
    let encoding = req.encoding.as_ref().map(|s| s.as_str()).unwrap_or("UTF8");

    // Phase 1: Create roles and database on current connection
    let phase1_sql = generate_phase1_sql(
        &req.app_name,
        db_admin,
        encoding,
        &admin_password,
        &rw_password,
        &ro_password,
        req.set_search_path,
        schema,
    );

    for stmt in phase1_sql {
        if let Err(e) = sqlx::query(&stmt).execute(&pg).await {
            // Ignore "already exists" errors for idempotence
            let err_msg = e.to_string();
            if !err_msg.contains("already exists") && !err_msg.contains("is already a member") {
                return Ok(Json(BlueprintResponse {
                    success: false,
                    passwords: None,
                    error: Some(format!("Phase 1 error: {}", e)),
                    sql_preview: None,
                }));
            }
        }
    }

    // Phase 2: Connect to new database and set up schema/permissions
    // Get password (same logic as in connect_pg)
    let override_password = state
        .active_override_password
        .read()
        .ok()
        .and_then(|p| p.clone());

    let password = if override_password.is_some() {
        override_password
    } else if let Some(db) = &state.db {
        db.get_endpoint_password(&active).await
    } else {
        state.stateless_password.clone()
    };

    let base_url = build_pg_url(&active.url, active.username.as_deref(), password.as_deref());
    let app_db_url = build_database_url(&base_url, &req.app_name);

    // Apply connection parameters (SSL mode, insecure, etc.)
    let app_db_url = apply_connection_params(
        app_db_url,
        active.ssl_mode.as_deref(),
        None, // Don't set search_path in URL for Phase 2
        active.insecure,
    );

    let mut app_conn = sqlx::postgres::PgConnection::connect(&app_db_url)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to connect to new database: {}", e)))?;

    let phase2_sql = generate_phase2_sql(
        &req.app_name,
        schema,
        req.lock_public_schema,
        req.revoke_db_public,
    );

    for stmt in &phase2_sql {
        if let Err(e) = sqlx::query(stmt).execute(&mut app_conn).await {
            return Ok(Json(BlueprintResponse {
                success: false,
                passwords: None,
                error: Some(format!("Phase 2 error: {}", e)),
                sql_preview: None,
            }));
        }
    }

    Ok(Json(BlueprintResponse {
        success: true,
        passwords: Some(BlueprintPasswords {
            admin_password,
            rw_password,
            ro_password,
        }),
        error: None,
        sql_preview: None,
    }))
}

fn is_valid_identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && name.chars().next().unwrap().is_ascii_lowercase()
}

fn generate_password() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut rng = rand::thread_rng();
    (0..48)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

fn generate_blueprint_sql(
    app: &str,
    db_admin: &str,
    schema: &str,
    encoding: &str,
    lock_public: bool,
    revoke_db_public: bool,
    set_search_path: bool,
    show_passwords: bool,
) -> String {
    let pwd_placeholder = if show_passwords { "<generated>" } else { "********" };

    let mut sql = format!(
        "-- Phase 1: Cluster-level (roles + database)\n\
        -- Runs on current connection\n\
        CREATE ROLE {app}_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;\n\
        CREATE ROLE {app}_rw NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;\n\
        CREATE ROLE {app}_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT;\n\n\
        -- {db_admin} must be able to SET ROLE {app}_owner for Phase 2\n\
        GRANT {app}_owner TO {db_admin};\n\n\
        CREATE ROLE {app}_admin LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD '{pwd_placeholder}';\n\
        GRANT {app}_owner TO {app}_admin;\n\n\
        CREATE ROLE {app}_rw_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD '{pwd_placeholder}';\n\
        GRANT {app}_rw TO {app}_rw_user;\n\n\
        CREATE ROLE {app}_ro_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD '{pwd_placeholder}';\n\
        GRANT {app}_ro TO {app}_ro_user;\n\n\
        ALTER ROLE {app}_ro_user SET default_transaction_read_only = on;\n\n\
        CREATE DATABASE {app} OWNER {app}_owner ENCODING '{encoding}';\n\
        GRANT CONNECT ON DATABASE {app} TO {db_admin};\n\n"
    );

    if set_search_path {
        sql.push_str(&format!(
            "ALTER ROLE {app}_admin IN DATABASE {app} SET search_path = {schema};\n\
            ALTER ROLE {app}_rw_user IN DATABASE {app} SET search_path = {schema};\n\
            ALTER ROLE {app}_ro_user IN DATABASE {app} SET search_path = {schema};\n\n"
        ));
    }

    sql.push_str(&format!(
        "-- Phase 2: Application database setup\n\
        -- Runs in new database {app} (requires \\connect {app} or new connection)\n\
        SET ROLE {app}_owner;\n\n"
    ));

    if revoke_db_public {
        sql.push_str(&format!("REVOKE ALL ON DATABASE {app} FROM PUBLIC;\n"));
        sql.push_str(&format!("GRANT CONNECT, TEMPORARY ON DATABASE {app} TO {app}_owner, {app}_rw, {app}_ro;\n\n"));
    }

    if lock_public {
        sql.push_str("REVOKE CREATE ON SCHEMA public FROM PUBLIC;\n\n");
    }

    sql.push_str(&format!(
        "CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {app}_owner;\n\
        GRANT USAGE ON SCHEMA {schema} TO {app}_ro, {app}_rw;\n\n\
        GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {app}_ro;\n\
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {app}_rw;\n\
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {app}_ro;\n\
        GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {app}_rw;\n\
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {app}_ro, {app}_rw;\n\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT SELECT ON TABLES TO {app}_ro;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {app}_rw;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT USAGE, SELECT ON SEQUENCES TO {app}_ro;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {app}_rw;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {app}_ro, {app}_rw;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;\n\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT SELECT ON TABLES TO {app}_ro;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {app}_rw;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT USAGE, SELECT ON SEQUENCES TO {app}_ro;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {app}_rw;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {app}_ro, {app}_rw;\n\
        ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;\n\n\
        RESET ROLE;\n"
    ));

    sql
}

fn generate_phase1_sql(
    app: &str,
    db_admin: &str,
    encoding: &str,
    admin_pwd: &str,
    rw_pwd: &str,
    ro_pwd: &str,
    set_search_path: bool,
    schema: &str,
) -> Vec<String> {
    let mut sql = vec![
        format!("CREATE ROLE {app}_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT"),
        format!("CREATE ROLE {app}_rw NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT"),
        format!("CREATE ROLE {app}_ro NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT"),
        format!("GRANT {app}_owner TO {db_admin}"),
        format!("CREATE ROLE {app}_admin LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD '{admin_pwd}'"),
        format!("GRANT {app}_owner TO {app}_admin"),
        format!("CREATE ROLE {app}_rw_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD '{rw_pwd}'"),
        format!("GRANT {app}_rw TO {app}_rw_user"),
        format!("CREATE ROLE {app}_ro_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION INHERIT PASSWORD '{ro_pwd}'"),
        format!("GRANT {app}_ro TO {app}_ro_user"),
        format!("ALTER ROLE {app}_ro_user SET default_transaction_read_only = on"),
        format!("CREATE DATABASE {app} OWNER {app}_owner ENCODING '{encoding}'"),
        format!("GRANT CONNECT ON DATABASE {app} TO {db_admin}"),
    ];

    if set_search_path {
        sql.push(format!("ALTER ROLE {app}_admin IN DATABASE {app} SET search_path = {schema}"));
        sql.push(format!("ALTER ROLE {app}_rw_user IN DATABASE {app} SET search_path = {schema}"));
        sql.push(format!("ALTER ROLE {app}_ro_user IN DATABASE {app} SET search_path = {schema}"));
    }

    sql
}

fn generate_phase2_sql(
    app: &str,
    schema: &str,
    lock_public: bool,
    revoke_db_public: bool,
) -> Vec<String> {
    let mut sql = Vec::new();

    // Start as app_owner to have permissions
    sql.push(format!("SET ROLE {app}_owner"));

    if revoke_db_public {
        sql.push(format!("REVOKE ALL ON DATABASE {app} FROM PUBLIC"));
        sql.push(format!("GRANT CONNECT, TEMPORARY ON DATABASE {app} TO {app}_owner, {app}_rw, {app}_ro"));
    }

    if lock_public {
        sql.push("REVOKE CREATE ON SCHEMA public FROM PUBLIC".to_string());
    }

    sql.push(format!("CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {app}_owner"));
    sql.push(format!("GRANT USAGE ON SCHEMA {schema} TO {app}_ro, {app}_rw"));
    sql.push(format!("GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO {app}_ro"));
    sql.push(format!("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {schema} TO {app}_rw"));
    sql.push(format!("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {schema} TO {app}_ro"));
    sql.push(format!("GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA {schema} TO {app}_rw"));
    sql.push(format!("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA {schema} TO {app}_ro, {app}_rw"));

    // Default privileges for owner
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT SELECT ON TABLES TO {app}_ro"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {app}_rw"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT USAGE, SELECT ON SEQUENCES TO {app}_ro"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {app}_rw"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {app}_ro, {app}_rw"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_owner REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"));

    // Default privileges for admin
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT SELECT ON TABLES TO {app}_ro"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO {app}_rw"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT USAGE, SELECT ON SEQUENCES TO {app}_ro"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO {app}_rw"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin IN SCHEMA {schema} GRANT EXECUTE ON FUNCTIONS TO {app}_ro, {app}_rw"));
    sql.push(format!("ALTER DEFAULT PRIVILEGES FOR ROLE {app}_admin REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"));

    // Reset role
    sql.push("RESET ROLE".to_string());

    sql
}

fn build_database_url(base_url: &str, database: &str) -> String {
    // Parse URL and replace database name
    if let Ok(mut parsed) = url::Url::parse(base_url) {
        parsed.set_path(&format!("/{}", database));
        parsed.to_string()
    } else {
        // Fallback: simple string replacement
        base_url.rsplit_once('/').map(|(prefix, _)| format!("{}/{}", prefix, database)).unwrap_or_else(|| base_url.to_string())
    }
}
