use axum::extract::State;
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use std::sync::Arc;
use axum_extra::extract::CookieJar;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState};
use crate::templates::{IndexRow, IndicesTemplate};
use crate::utils::format::bytes_to_human;

#[derive(sqlx::FromRow)]
struct IndexRowDb {
    schema: String,
    table_name: String,
    index_name: String,
    size_bytes: i64,
    scans: Option<i64>,
}

pub async fn list_indices(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let indices: Vec<IndexRow> = match connect_pg(&state, &active).await {
        Ok(pg) => match sqlx::query_as::<_, IndexRowDb>(
            r#"
            SELECT n.nspname as schema,
                   t.relname as table_name,
                   i.relname as index_name,
                   pg_relation_size(i.oid) as size_bytes,
                   s.idx_scan as scans
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY size_bytes DESC
            "#,
        )
        .fetch_all(&pg)
        .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|r| IndexRow {
                    schema: r.schema,
                    table: r.table_name,
                    name: r.index_name,
                    size: bytes_to_human(r.size_bytes),
                    scans: r.scans.unwrap_or(0).to_string(),
                })
                .collect(),
            Err(err) => {
                tracing::warn!("Failed to load indices: {}", err);
                Vec::new()
            }
        },
        Err(err) => {
            tracing::warn!("Failed to connect to Postgres: {}", err);
            Vec::new()
        }
    };

    let tpl = IndicesTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
        title: "Indexes | Postgres Explorer".to_string(),
        indices,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
