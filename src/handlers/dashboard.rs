use axum::extract::State;
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use sqlx::Row;
use std::sync::Arc;
use axum_extra::extract::CookieJar;

use crate::handlers::{base_path_url, build_ctx, connect_pg, get_active_endpoint, AppState};
use crate::templates::DashboardTemplate;
use crate::utils::format::bytes_to_human;

pub async fn dashboard(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let mut server_version = "unknown".to_string();
    let mut schema_count = "-".to_string();
    let mut table_count = "-".to_string();
    let mut index_count = "-".to_string();
    let mut db_size = "-".to_string();
    let connections = "-".to_string();
    let cache_hit_ratio = "-".to_string();

    if let Ok(pg) = connect_pg(&state, &active).await {
        if let Ok(row) = sqlx::query(
            r#"
            SELECT version() as server_version,
                   pg_database_size(current_database()) as total_size,
                   (SELECT count(*) FROM pg_namespace WHERE nspname NOT IN ('pg_catalog','information_schema')) as schema_count,
                   (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema')) as table_count,
                   (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind='i' AND n.nspname NOT IN ('pg_catalog','information_schema')) as index_count
            "#,
        )
        .fetch_one(&pg)
        .await
        {
            server_version = row.get::<String, _>("server_version");
            let size: i64 = row.get("total_size");
            let schemas: i64 = row.get("schema_count");
            let tables: i64 = row.get("table_count");
            let indexes: i64 = row.get("index_count");
            db_size = bytes_to_human(size);
            schema_count = schemas.to_string();
            table_count = tables.to_string();
            index_count = indexes.to_string();
        }
    }

    let tpl = DashboardTemplate {
        ctx: build_ctx(&state),
        title: "Dashboard | Postgres Explorer".to_string(),
        server_name: active.name,
        server_version,
        connections,
        cache_hit_ratio,
        schema_count,
        table_count,
        index_count,
        db_size,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
