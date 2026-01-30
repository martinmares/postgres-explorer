use axum::extract::{Path, State};
use axum::response::{Html, IntoResponse, Json, Redirect, Response};
use askama::Template;
use serde::Serialize;
use sqlx::Row;
use std::sync::Arc;
use axum_extra::extract::CookieJar;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState};
use crate::templates::{DashboardTemplate, TopTable};
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

    // Metriky s progress bary
    let mut cache_hit_ratio = 0.0;
    let mut cache_hit_ratio_text = "-".to_string();
    let mut active_connections = 0;
    let mut max_connections = 100;
    let mut connections_text = "-".to_string();
    let mut connections_percent = 0.0;

    // Top tables
    let mut top_tables: Vec<TopTable> = Vec::new();

    if let Ok(pg) = connect_pg(&state, &active).await {
        // Základní info
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

        // Cache hit ratio
        if let Ok(row) = sqlx::query(
            r#"
            SELECT
                sum(blks_hit)::float / NULLIF(sum(blks_hit + blks_read), 0)::float * 100 as cache_hit_ratio
            FROM pg_stat_database
            WHERE datname = current_database()
            "#,
        )
        .fetch_one(&pg)
        .await
        {
            if let Ok(ratio) = row.try_get::<f64, _>("cache_hit_ratio") {
                cache_hit_ratio = ratio;
                cache_hit_ratio_text = format!("{:.1}%", ratio);
            }
        }

        // Active connections
        if let Ok(row) = sqlx::query(
            r#"
            SELECT
                (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active_connections,
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
            "#,
        )
        .fetch_one(&pg)
        .await
        {
            active_connections = row.get::<i64, _>("active_connections") as i32;
            max_connections = row.get::<i32, _>("max_connections");
            connections_text = format!("{} / {}", active_connections, max_connections);
            connections_percent = (active_connections as f64 / max_connections as f64) * 100.0;
        }

        // Top 10 tables by size (grouping partitions with their parent table)
        if let Ok(rows) = sqlx::query(
            r#"
            WITH table_hierarchy AS (
                SELECT
                    c.oid,
                    n.nspname as schema,
                    c.relname as name,
                    COALESCE(parent.relname, c.relname) as parent_name,
                    COALESCE(parent_ns.nspname, n.nspname) as parent_schema,
                    pg_total_relation_size(c.oid) as size_bytes,
                    COALESCE(NULLIF(s.n_live_tup, 0), NULLIF(c.reltuples, 0), 0)::bigint as row_estimate,
                    CASE WHEN parent.oid IS NOT NULL THEN true ELSE false END as is_partition
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
                LEFT JOIN pg_class parent ON parent.oid = i.inhparent
                LEFT JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
                LEFT JOIN pg_stat_all_tables s ON s.relid = c.oid
                WHERE c.relkind IN ('r', 'p')
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
            )
            SELECT
                parent_schema as schema,
                parent_name as name,
                SUM(size_bytes)::bigint as size_bytes,
                SUM(row_estimate)::bigint as row_estimate,
                array_agg(th.name ORDER BY th.name) FILTER (WHERE is_partition) as partitions
            FROM table_hierarchy th
            GROUP BY parent_schema, parent_name
            ORDER BY size_bytes DESC
            LIMIT 10
            "#,
        )
        .fetch_all(&pg)
        .await
        {
            let total_size: i64 = rows.iter().map(|r| r.get::<i64, _>("size_bytes")).sum();

            for row in rows {
                let schema: String = row.get("schema");
                let name: String = row.get("name");
                let size_bytes: i64 = row.get("size_bytes");
                let row_estimate: i64 = row.get("row_estimate");
                let partitions: Option<Vec<String>> = row.try_get("partitions").ok();
                let mut relative_percent = if total_size > 0 {
                    (size_bytes as f64 / total_size as f64) * 100.0
                } else {
                    0.0
                };
                if size_bytes > 0 && relative_percent < 1.0 {
                    relative_percent = 1.0;
                }
                let relative_percent = relative_percent.round().min(100.0).max(0.0) as i64;

                let stats_stale = row_estimate == 0 && size_bytes > 0;

                top_tables.push(TopTable {
                    schema: schema.clone(),
                    name: name.clone(),
                    size: bytes_to_human(size_bytes),
                    size_bytes,
                    rows: format_number(row_estimate),
                    partitions: partitions.unwrap_or_default(),
                    relative_percent,
                    stats_stale,
                    schema_filter_url: format!("/tables/{}/{}", urlencoding::encode(&schema), urlencoding::encode("*")),
                    table_filter_url: format!("/tables/{}/{}/detail", urlencoding::encode(&schema), urlencoding::encode(&name)),
                });
            }
        }
    }

    let tpl = DashboardTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
        title: "Dashboard | Postgres Explorer".to_string(),
        server_name: active.name,
        server_version,
        schema_count,
        table_count,
        index_count,
        db_size,
        cache_hit_ratio,
        cache_hit_ratio_text,
        active_connections,
        max_connections,
        connections_text,
        connections_percent,
        top_tables,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

#[derive(Serialize)]
pub struct AnalyzeResponse {
    success: bool,
    error: Option<String>,
}

pub async fn analyze_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, table)): Path<(String, String)>,
) -> Json<AnalyzeResponse> {
    let active = match get_active_endpoint(&state, &jar).await {
        Some(a) => a,
        None => {
            return Json(AnalyzeResponse {
                success: false,
                error: Some("No active connection".to_string()),
            });
        }
    };

    let pg = match connect_pg(&state, &active).await {
        Ok(p) => p,
        Err(e) => {
            return Json(AnalyzeResponse {
                success: false,
                error: Some(format!("Connection failed: {}", e)),
            });
        }
    };

    // Escapni schema a table jména pomocí quote_ident
    let analyze_sql = format!(
        "ANALYZE {}.{}",
        sqlx::query_scalar::<_, String>("SELECT quote_ident($1)")
            .bind(&schema)
            .fetch_one(&pg)
            .await
            .unwrap_or_else(|_| schema.clone()),
        sqlx::query_scalar::<_, String>("SELECT quote_ident($1)")
            .bind(&table)
            .fetch_one(&pg)
            .await
            .unwrap_or_else(|_| table.clone())
    );

    match sqlx::query(&analyze_sql).execute(&pg).await {
        Ok(_) => {
            tracing::info!("Successfully ran ANALYZE on {}.{}", schema, table);
            Json(AnalyzeResponse {
                success: true,
                error: None,
            })
        }
        Err(e) => {
            tracing::error!("Failed to run ANALYZE on {}.{}: {}", schema, table, e);
            Json(AnalyzeResponse {
                success: false,
                error: Some(format!("ANALYZE failed: {}", e)),
            })
        }
    }
}

fn format_number(n: i64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(' ');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}
