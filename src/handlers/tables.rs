use axum::extract::{Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use regex::Regex;
use serde::Deserialize;
use std::sync::Arc;

use crate::handlers::{base_path_url, build_ctx, connect_pg, get_active_endpoint, AppState};
use crate::templates::{TableModalTemplate, TableRow, TablesTemplate};
use crate::utils::format::bytes_to_human;
use sqlx::Row;
use axum_extra::extract::CookieJar;

#[derive(Deserialize)]
pub struct TableFilter {
    pub filter: Option<String>,
}

#[derive(sqlx::FromRow)]
struct TableRowDb {
    schema: String,
    name: String,
    size_bytes: i64,
    row_estimate: i64,
    index_count: i64,
}

pub async fn list_tables(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(params): Query<TableFilter>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let filter = params.filter.unwrap_or_default();

    let mut tables: Vec<TableRow> = match connect_pg(&state, &active).await {
        Ok(pg) => match sqlx::query_as::<_, TableRowDb>(
            r#"
            SELECT n.nspname as schema,
                   c.relname as name,
                   pg_total_relation_size(c.oid) as size_bytes,
                   c.reltuples::bigint as row_estimate,
                   (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY size_bytes DESC
            "#,
        )
        .fetch_all(&pg)
        .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|r| TableRow {
                    schema: r.schema,
                    name: r.name,
                    rows: r.row_estimate.to_string(),
                    size: bytes_to_human(r.size_bytes),
                    index_count: r.index_count.to_string(),
                })
                .collect(),
            Err(err) => {
                tracing::warn!("Failed to load tables: {}", err);
                Vec::new()
            }
        },
        Err(err) => {
            tracing::warn!("Failed to connect to Postgres: {}", err);
            Vec::new()
        }
    };

    if !filter.is_empty() {
        if let Ok(re) = Regex::new(&filter) {
            tables.retain(|t| re.is_match(&t.name) || re.is_match(&t.schema));
        }
    }

    let tpl = TablesTemplate {
        ctx: build_ctx(&state),
        title: "Tables | Postgres Explorer".to_string(),
        filter,
        table_count: tables.len(),
        tables,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(sqlx::FromRow)]
struct TableModalDb {
    size_bytes: i64,
    row_estimate: i64,
}

pub async fn table_modal(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, name)): Path<(String, String)>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let mut rows = "-".to_string();
    let mut size = "-".to_string();
    let mut authorized = true;
    let mut fragmentation = "unknown".to_string();

    match connect_pg(&state, &active).await {
        Ok(pg) => {
            match sqlx::query_as::<_, TableModalDb>(
                r#"
                SELECT pg_total_relation_size(c.oid) as size_bytes,
                       c.reltuples::bigint as row_estimate
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = $1 AND c.relname = $2
                "#,
            )
            .bind(&schema)
            .bind(&name)
            .fetch_optional(&pg)
            .await
            {
                Ok(Some(r)) => {
                    rows = r.row_estimate.to_string();
                    size = bytes_to_human(r.size_bytes);
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!("Failed to load table modal: {}", err);
                    authorized = false;
                }
            }

            if authorized {
                match sqlx::query(
                    r#"
                    SELECT n_dead_tup, n_live_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = $1 AND relname = $2
                    "#,
                )
                .bind(&schema)
                .bind(&name)
                .fetch_optional(&pg)
                .await
                {
                    Ok(Some(row)) => {
                        let dead: i64 = row.get("n_dead_tup");
                        let live: i64 = row.get("n_live_tup");
                        let total = dead + live;
                        if total > 0 {
                            let pct = (dead as f64 / total as f64) * 100.0;
                            fragmentation = format!("{:.1}% dead tuples", pct);
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!("Failed to load table stats: {}", err);
                        authorized = false;
                    }
                }
            }
        }
        Err(err) => {
            tracing::warn!("Failed to connect to Postgres: {}", err);
            authorized = false;
        }
    }

    let tpl = TableModalTemplate {
        ctx: build_ctx(&state),
        schema,
        name,
        rows,
        size,
        fragmentation,
        authorized,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
