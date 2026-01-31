pub mod blueprint;
pub mod console;
pub mod dashboard;
pub mod databases;
pub mod endpoints;
pub mod export;
pub mod indices;
pub mod maintenance;
pub mod patroni;
pub mod schemas;
pub mod table_detail;
pub mod tables;
pub mod tuning;

use std::sync::{Arc, RwLock as StdRwLock};
use std::time::{Duration, Instant, SystemTime};
use std::collections::{HashMap, VecDeque};
use tokio::sync::RwLock;
use crate::templates::AppContext;
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::Cookie;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: Option<crate::db::Database>,
    pub base_path: String,
    pub stateless_endpoint: Option<crate::db::models::Endpoint>,
    pub stateless_password: Option<String>,
    pub active_override: Arc<StdRwLock<Option<crate::db::models::Endpoint>>>,
    pub active_override_password: Arc<StdRwLock<Option<String>>>,
    pub databases_menu: Arc<StdRwLock<HashMap<i64, bool>>>,
    pub schemas_cache: Arc<RwLock<HashMap<i64, CacheEntry<crate::handlers::schemas::SchemaRowDb>>>>,
    pub tables_cache: Arc<RwLock<HashMap<i64, CacheEntry<crate::handlers::tables::TableRowDb>>>>,
    pub indices_cache: Arc<RwLock<HashMap<i64, CacheEntry<crate::handlers::indices::IndexRowDb>>>>,
    pub export_jobs: Arc<RwLock<HashMap<String, ExportJob>>>,
    pub patroni_urls: Option<Vec<String>>,
}

pub const CACHE_TTL: Duration = Duration::from_secs(15 * 60);
pub const JOB_CLEANUP_AGE: Duration = Duration::from_secs(60 * 60); // 1 hour

pub struct CacheEntry<T> {
    pub data: Vec<T>,
    pub fetched_at: Instant,
    pub fetching: bool,
}

#[derive(Debug, Clone)]
pub enum JobStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ExportJob {
    pub job_id: String,
    pub status: JobStatus,
    pub logs: VecDeque<String>,
    pub started_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub file_path: Option<String>,
    pub error: Option<String>,
}

pub fn build_ctx(state: &Arc<AppState>) -> AppContext {
    let in_memory_active = state
        .active_override
        .read()
        .ok()
        .and_then(|g| g.clone())
        .is_some();
    AppContext {
        base_path: state.base_path.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_endpoint_name: "No connection".to_string(),
        show_databases: false,
        in_memory_active,
        show_patroni: state.patroni_urls.is_some(),
        show_blueprint: false,
    }
}

pub fn build_ctx_with_endpoint(state: &Arc<AppState>, endpoint: Option<&crate::db::models::Endpoint>) -> AppContext {
    let in_memory_active = state
        .active_override
        .read()
        .ok()
        .and_then(|g| g.clone())
        .is_some();
    let show_databases = endpoint
        .and_then(|e| {
            state
                .databases_menu
                .read()
                .ok()
                .and_then(|map| map.get(&e.id).copied())
        })
        .unwrap_or(false);
    AppContext {
        base_path: state.base_path.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        active_endpoint_name: endpoint.map(|e| e.name.clone()).unwrap_or_else(|| "No connection".to_string()),
        show_databases,
        in_memory_active,
        show_patroni: state.patroni_urls.is_some(),
        show_blueprint: endpoint.map(|e| e.enable_blueprint).unwrap_or(false),
    }
}

pub fn base_path_url(state: &Arc<AppState>, path: &str) -> String {
    if state.base_path == "/" {
        path.to_string()
    } else {
        format!("{}{}", state.base_path, path)
    }
}

pub async fn get_active_endpoint(
    state: &Arc<AppState>,
    jar: &CookieJar,
) -> Option<crate::db::models::Endpoint> {
    if let Ok(guard) = state.active_override.read() {
        if let Some(endpoint) = guard.clone() {
            return Some(endpoint);
        }
    }
    if let Some(endpoint) = &state.stateless_endpoint {
        return Some(endpoint.clone());
    }
    let db = state.db.as_ref()?;
    let id = jar
        .get("pg_active_endpoint")
        .and_then(|c| c.value().parse::<i64>().ok());
    if let Some(id) = id {
        if let Ok(Some(endpoint)) = db.get_endpoint(id).await {
            return Some(endpoint);
        }
    }
    None
}

pub fn set_active_endpoint_cookie(id: i64) -> Cookie<'static> {
    Cookie::build(("pg_active_endpoint", id.to_string()))
        .path("/")
        .http_only(true)
        .build()
}

pub async fn connect_pg(
    state: &Arc<AppState>,
    endpoint: &crate::db::models::Endpoint,
) -> anyhow::Result<PgPool> {
    let override_password = state
        .active_override
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .and_then(|override_ep| {
            if override_ep.id == endpoint.id && override_ep.url == endpoint.url {
                state
                    .active_override_password
                    .read()
                    .ok()
                    .and_then(|p| p.clone())
            } else {
                None
            }
        });

    let password = if override_password.is_some() {
        override_password
    } else if let Some(db) = &state.db {
        db.get_endpoint_password(endpoint).await
    } else {
        state.stateless_password.clone()
    };
    let mut url = build_pg_url(&endpoint.url, endpoint.username.as_deref(), password.as_deref());

    // Aplikuj SSL mode a další parametry
    url = apply_connection_params(
        url,
        endpoint.ssl_mode.as_deref(),
        endpoint.search_path.as_deref(),
        endpoint.insecure,
    );

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(10))
        .connect(&url)
        .await?;

    // Nastav statement_timeout na 30 sekund
    sqlx::query("SET statement_timeout = '30s'")
        .execute(&pool)
        .await
        .ok();

    Ok(pool)
}

fn build_pg_url(base: &str, username: Option<&str>, password: Option<&str>) -> String {
    let mut url = base.to_string();
    if !url.contains("://") {
        url = format!("postgres://{}", url);
    }
    if url.contains('@') || username.is_none() {
        return url;
    }
    let user = username.unwrap_or("");
    let pass = password.unwrap_or("");
    if let Some(pos) = url.find("://") {
        let (scheme, rest) = url.split_at(pos + 3);
        if pass.is_empty() {
            format!("{}{}@{}", scheme, user, rest)
        } else {
            format!("{}{}:{}@{}", scheme, user, pass, rest)
        }
    } else {
        url
    }
}

fn apply_connection_params(
    mut url: String,
    ssl_mode: Option<&str>,
    search_path: Option<&str>,
    insecure: bool,
) -> String {
    let separator = if url.contains('?') { '&' } else { '?' };
    let mut params = Vec::new();

    // Aplikuj SSL mode
    let ssl_mode = ssl_mode.and_then(|m| {
        let trimmed = m.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    });
    if let Some(mode) = ssl_mode {
        params.push(format!("sslmode={}", urlencoding::encode(mode)));
    }

    // Pokud je insecure=true a není explicitní ssl mode, nastav require (bez verifikace)
    // Pokud je insecure=true a je verify-ca/verify-full, ponech (user ví co dělá)
    if insecure && ssl_mode.is_none() {
        params.push("sslmode=require".to_string());
    }

    // Aplikuj search_path
    if let Some(path) = search_path {
        let trimmed = path.trim();
        if !trimmed.is_empty() {
            params.push(format!("search_path={}", urlencoding::encode(trimmed)));
        }
    }

    if !params.is_empty() {
        url.push(separator);
        url.push_str(&params.join("&"));
    }

    url
}
