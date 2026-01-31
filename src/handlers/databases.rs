use std::sync::Arc;

use axum::extract::State;
use axum::response::{Html, IntoResponse, Redirect, Response};
use axum::Json;
use axum_extra::extract::CookieJar;
use askama::Template;
use serde::Deserialize;
use sqlx::Row;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState};
use crate::templates::{DatabaseInfo, DatabasesTemplate};
use crate::utils::format::bytes_to_human;

#[derive(Deserialize)]
pub struct ActivateDbForm {
    pub db_name: String,
    pub active_id: Option<i64>,
    pub username: Option<String>,
    pub password: Option<String>,
}

pub async fn list_databases(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let mut databases: Vec<DatabaseInfo> = Vec::new();
    let mut menu_enabled = false;

    if let Ok(pg) = connect_pg(&state, &active).await {
        let query = r#"
            SELECT
                d.datname as name,
                pg_get_userbyid(d.datdba) as owner,
                pg_encoding_to_char(d.encoding) as encoding,
                d.datcollate as collate,
                d.datctype as ctype,
                d.datallowconn as allow_conn,
                d.datistemplate as is_template,
                d.datconnlimit as conn_limit,
                pg_database_size(d.datname) as size_bytes,
                COALESCE(s.numbackends, 0)::bigint as connections
            FROM pg_database d
            LEFT JOIN pg_stat_database s ON s.datname = d.datname
            ORDER BY d.datname
        "#;
        if let Ok(rows) = sqlx::query(query).fetch_all(&pg).await {
            menu_enabled = true;
            for row in rows {
                let size_bytes: i64 = row.get("size_bytes");
                databases.push(DatabaseInfo {
                    name: row.get("name"),
                    owner: row.get("owner"),
                    encoding: row.get("encoding"),
                    collate: row.get("collate"),
                    ctype: row.get("ctype"),
                    allow_conn: row.get("allow_conn"),
                    is_template: row.get("is_template"),
                    conn_limit: row.get("conn_limit"),
                    size: bytes_to_human(size_bytes),
                    connections: row.get::<i64, _>("connections"),
                });
            }
        }
    }

    if let Ok(mut guard) = state.databases_menu.write() {
        guard.insert(active.id, menu_enabled);
    }

    let tpl = DatabasesTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
        title: "Databases | Postgres Explorer".to_string(),
        active_endpoint_id: active.id,
        databases,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn activate_database(
    State(state): State<Arc<AppState>>,
    _jar: CookieJar,
    Json(form): Json<serde_json::Value>,
) -> Result<Redirect, (axum::http::StatusCode, String)> {
    let form: ActivateDbForm = serde_json::from_value(form)
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;

    let active = if let Some(endpoint) = &state.stateless_endpoint {
        endpoint.clone()
    } else {
        let id = form.active_id.ok_or_else(|| {
            (axum::http::StatusCode::BAD_REQUEST, "Missing active_id".to_string())
        })?;
        let db = state.db.as_ref().ok_or_else(|| {
            (axum::http::StatusCode::BAD_REQUEST, "No database".to_string())
        })?;
        db.get_endpoint(id)
            .await
            .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
            .ok_or_else(|| (axum::http::StatusCode::NOT_FOUND, "Endpoint not found".to_string()))?
    };

    let mut next = active.clone();
    next.url = replace_db_in_url(&active.url, &form.db_name);
    next.name = next.url.clone();

    if let Some(username) = form.username.as_ref().filter(|v| !v.trim().is_empty()) {
        next.username = Some(username.trim().to_string());
    }

    if let Ok(mut guard) = state.active_override.write() {
        *guard = Some(next);
    }

    let next_password = if let Some(password) = form.password.as_ref().filter(|v| !v.trim().is_empty()) {
        Some(password.trim().to_string())
    } else if let Some(db) = &state.db {
        db.get_endpoint_password(&active).await
    } else {
        state.stateless_password.clone()
    };

    if let Ok(mut guard) = state.active_override_password.write() {
        *guard = next_password;
    }

    let target = base_path_url(&state, "/");
    Ok(Redirect::to(&target))
}

pub async fn reset_database_override(
    State(state): State<Arc<AppState>>,
    _jar: CookieJar,
) -> Result<Redirect, (axum::http::StatusCode, String)> {
    if let Ok(mut guard) = state.active_override.write() {
        *guard = None;
    }
    if let Ok(mut guard) = state.active_override_password.write() {
        *guard = None;
    }
    let target = base_path_url(&state, "/");
    Ok(Redirect::to(&target))
}

fn replace_db_in_url(url: &str, db_name: &str) -> String {
    let mut parts = url.splitn(2, '?');
    let base = parts.next().unwrap_or(url);
    let query = parts.next();

    let host = if let Some(pos) = base.find("://") {
        let after = &base[pos + 3..];
        if let Some(slash) = after.find('/') {
            base[..pos + 3 + slash].to_string()
        } else {
            base.to_string()
        }
    } else if let Some(slash) = base.find('/') {
        base[..slash].to_string()
    } else {
        base.to_string()
    };

    let mut rebuilt = format!("{}/{}", host, db_name);
    if let Some(q) = query {
        rebuilt.push('?');
        rebuilt.push_str(q);
    }
    rebuilt
}
