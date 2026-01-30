use axum::extract::State;
use axum::response::{IntoResponse, Redirect, Response};
use askama::Template;
use axum_extra::extract::CookieJar;
use sqlx::Row;
use std::sync::Arc;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState};
use crate::templates::TuningTemplate;
use crate::utils::format::bytes_to_human;

#[derive(Debug, Clone)]
pub struct FullScanQuery {
    pub query: String,
    pub calls: i64,
    pub total_time_ms: f64,
    pub rows: i64,
    pub seq_scans: i64,
}

#[derive(Debug, Clone)]
pub struct OverIndexedTable {
    pub schema: String,
    pub table: String,
    pub index_count: i64,
    pub total_index_size: String,
    pub table_size: String,
}

#[derive(Debug, Clone)]
pub struct FragmentedTable {
    pub schema: String,
    pub table: String,
    pub size: String,
    pub bloat_pct: f64,
    pub wasted_space: String,
}

#[derive(Debug, Clone)]
pub struct FragmentedIndex {
    pub schema: String,
    pub table: String,
    pub index: String,
    pub size: String,
    pub bloat_pct: f64,
}

pub async fn tuning_page(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let mut full_scan_queries = Vec::new();
    let mut over_indexed_tables = Vec::new();
    let mut fragmented_tables = Vec::new();
    let mut fragmented_indexes = Vec::new();
    let mut pg_stat_statements_enabled = false;

    match connect_pg(&state, &active).await {
        Ok(pg) => {
            // Check if pg_stat_statements is enabled
            if let Ok(Some(_)) = sqlx::query("SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'")
                .fetch_optional(&pg)
                .await
            {
                pg_stat_statements_enabled = true;

                // Get top full table scan queries
                if let Ok(rows) = sqlx::query(
                    r#"
                    SELECT
                        query,
                        calls,
                        total_exec_time as total_time,
                        rows,
                        (SELECT COALESCE(SUM(seq_scan), 0) FROM pg_stat_user_tables) as seq_scans
                    FROM pg_stat_statements
                    WHERE query NOT LIKE '%pg_stat%'
                      AND query NOT LIKE '%pg_class%'
                      AND calls > 10
                    ORDER BY total_exec_time DESC
                    LIMIT 20
                    "#,
                )
                .fetch_all(&pg)
                .await
                {
                    for row in rows {
                        let query: String = row.get("query");
                        if query.to_uppercase().contains("SELECT") && !query.to_uppercase().contains("WHERE") {
                            full_scan_queries.push(FullScanQuery {
                                query: if query.len() > 200 { format!("{}...", &query[..200]) } else { query },
                                calls: row.get("calls"),
                                total_time_ms: row.get("total_time"),
                                rows: row.get("rows"),
                                seq_scans: row.try_get("seq_scans").unwrap_or(0),
                            });
                        }
                    }
                }
            }

            // Get over-indexed tables (5+ indexes)
            if let Ok(rows) = sqlx::query(
                r#"
                SELECT
                    n.nspname as schema,
                    c.relname as table,
                    count(i.indexrelid) as index_count,
                    COALESCE(SUM(pg_relation_size(i.indexrelid))::bigint, 0) as total_index_size,
                    pg_total_relation_size(c.oid) as table_size
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_index i ON i.indrelid = c.oid
                WHERE c.relkind = 'r'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                GROUP BY n.nspname, c.relname, c.oid
                HAVING count(i.indexrelid) >= 5
                ORDER BY count(i.indexrelid) DESC
                LIMIT 20
                "#,
            )
            .fetch_all(&pg)
            .await
            {
                for row in rows {
                    over_indexed_tables.push(OverIndexedTable {
                        schema: row.get("schema"),
                        table: row.get("table"),
                        index_count: row.get("index_count"),
                        total_index_size: bytes_to_human(row.get("total_index_size")),
                        table_size: bytes_to_human(row.get("table_size")),
                    });
                }
            }

            // Get fragmented tables (bloat > 20%)
            if let Ok(rows) = sqlx::query(
                r#"
                SELECT
                    schemaname as schema,
                    tablename as table,
                    pg_total_relation_size((schemaname||'.'||tablename)::regclass) as size_bytes,
                    CASE
                        WHEN pg_total_relation_size((schemaname||'.'||tablename)::regclass) > 0
                        THEN (n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)::float * 100)
                        ELSE 0
                    END as bloat_pct,
                    pg_size_pretty(pg_total_relation_size((schemaname||'.'||tablename)::regclass) *
                        (n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)::float)) as wasted
                FROM pg_stat_user_tables
                WHERE n_dead_tup > 1000
                  AND (n_dead_tup::float / GREATEST(n_live_tup + n_dead_tup, 1)::float) > 0.2
                ORDER BY n_dead_tup DESC
                LIMIT 20
                "#,
            )
            .fetch_all(&pg)
            .await
            {
                for row in rows {
                    fragmented_tables.push(FragmentedTable {
                        schema: row.get("schema"),
                        table: row.get("table"),
                        size: bytes_to_human(row.get("size_bytes")),
                        bloat_pct: row.get::<f64, _>("bloat_pct"),
                        wasted_space: row.get("wasted"),
                    });
                }
            }

            // Get fragmented indexes (size > 10MB and low usage)
            if let Ok(rows) = sqlx::query(
                r#"
                SELECT
                    n.nspname as schema,
                    t.relname as table,
                    i.relname as index,
                    pg_relation_size(i.oid) as size_bytes,
                    (CASE
                        WHEN s.idx_scan = 0 THEN 100.0
                        WHEN s.idx_scan < 100 THEN 80.0
                        ELSE 50.0
                    END)::float8 as bloat_pct
                FROM pg_class i
                JOIN pg_index ix ON ix.indexrelid = i.oid
                JOIN pg_class t ON t.oid = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
                WHERE i.relkind = 'i'
                  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
                  AND pg_relation_size(i.oid) > 10485760
                ORDER BY pg_relation_size(i.oid) DESC
                LIMIT 20
                "#,
            )
            .fetch_all(&pg)
            .await
            {
                for row in rows {
                    fragmented_indexes.push(FragmentedIndex {
                        schema: row.get("schema"),
                        table: row.get("table"),
                        index: row.get("index"),
                        size: bytes_to_human(row.get("size_bytes")),
                        bloat_pct: row.get("bloat_pct"),
                    });
                }
            }
        }
        Err(_) => {}
    }

    let ctx = build_ctx_with_endpoint(&state, Some(&active));
    let tmpl = TuningTemplate {
        ctx,
        pg_stat_statements_enabled,
        full_scan_queries,
        over_indexed_tables,
        fragmented_tables,
        fragmented_indexes,
    };

    match tmpl.render() {
        Ok(html) => Ok(axum::response::Html(html).into_response()),
        Err(e) => Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
