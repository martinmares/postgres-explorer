pub mod console;
pub mod dashboard;
pub mod endpoints;
pub mod indices;
pub mod schemas;
pub mod table_detail;
pub mod tables;

use std::sync::Arc;
use crate::templates::AppContext;
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::Cookie;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

#[derive(Clone)]
pub struct AppState {
    pub db: crate::db::Database,
    pub base_path: String,
}

pub fn build_ctx(state: &Arc<AppState>) -> AppContext {
    AppContext {
        base_path: state.base_path.clone(),
        version: env!("CARGO_PKG_VERSION").to_string(),
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
    let id = jar
        .get("pg_active_endpoint")
        .and_then(|c| c.value().parse::<i64>().ok());
    if let Some(id) = id {
        if let Ok(Some(endpoint)) = state.db.get_endpoint(id).await {
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
    let password = state.db.get_endpoint_password(endpoint).await;
    let url = build_pg_url(&endpoint.url, endpoint.username.as_deref(), password.as_deref());
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await?;
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
