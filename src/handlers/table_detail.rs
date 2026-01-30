use axum::extract::{Path, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use sqlx::Row;
use std::sync::Arc;
use axum_extra::extract::CookieJar;

use crate::handlers::{base_path_url, build_ctx, connect_pg, get_active_endpoint, AppState};
use crate::templates::TableDetailTemplate;
use crate::utils::format::bytes_to_human;

#[derive(sqlx::FromRow)]
struct TableDetailDb {
    size_bytes: i64,
    row_estimate: i64,
    index_count: i64,
}

pub async fn table_detail(
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
    let mut schema = schema;

    let mut rows = "-".to_string();
    let mut size = "-".to_string();
    let mut index_count = "-".to_string();
    let mut authorized = true;
    let mut fragmentation = "unknown".to_string();
    let mut vacuum_hint = "No data".to_string();

    match connect_pg(&state, &active).await {
        Ok(pg) => {
            if schema == "*" {
                let schemas = sqlx::query_scalar::<_, String>(
                    r#"
                    SELECT n.nspname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relname = $1 AND c.relkind IN ('r','p')
                      AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY n.nspname
                    "#,
                )
                .bind(&name)
                .fetch_all(&pg)
                .await
                .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                if schemas.len() == 1 {
                    schema = schemas[0].clone();
                } else {
                    return Err((axum::http::StatusCode::BAD_REQUEST, "Table name is ambiguous without schema".to_string()));
                }
            }

            match sqlx::query_as::<_, TableDetailDb>(
                r#"
                SELECT pg_total_relation_size(c.oid) as size_bytes,
                       c.reltuples::bigint as row_estimate,
                       (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count
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
                    index_count = r.index_count.to_string();
                }
                Ok(None) => {}
                Err(err) => {
                    tracing::warn!("Failed to load table detail: {}", err);
                    authorized = false;
                }
            }

            if authorized {
                match sqlx::query(
                    r#"
                    SELECT n_dead_tup, n_live_tup
                    FROM pg_stat_all_tables
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
                            if pct > 20.0 {
                                vacuum_hint = "Consider VACUUM (high dead tuples)".to_string();
                            } else {
                                vacuum_hint = "Vacuum not urgent".to_string();
                            }
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

    let tpl = TableDetailTemplate {
        ctx: build_ctx(&state),
        title: format!("{}.{} | Postgres Explorer", schema, name),
        schema,
        name,
        rows,
        size,
        fragmentation,
        vacuum_hint,
        index_count,
        authorized,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
