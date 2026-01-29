use axum::extract::{Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use serde::Deserialize;
use std::sync::Arc;

use crate::handlers::{base_path_url, build_ctx, connect_pg, get_active_endpoint, AppState};
use crate::templates::{TableModalTemplate, TableRow, TablesTemplate, TablesTableTemplate};
use crate::utils::filter::parse_pattern_expression;
use crate::utils::format::bytes_to_human;
use sqlx::Row;
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::Cookie;

#[derive(Deserialize)]
pub struct TablesQuery {
    #[serde(default = "default_filter")]
    pub filter: String,
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_per_page")]
    pub per_page: usize,
    #[serde(default = "default_sort_by")]
    pub sort_by: String,
    #[serde(default = "default_sort_order")]
    pub sort_order: String,
}

fn default_filter() -> String {
    "*".to_string()
}

fn default_page() -> usize {
    1
}

fn default_per_page() -> usize {
    50
}

fn default_sort_by() -> String {
    "size".to_string()
}

fn default_sort_order() -> String {
    "desc".to_string()
}

#[derive(sqlx::FromRow, Clone)]
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
    Query(mut query): Query<TablesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    // Načti filtr z cookie, pokud není v query
    let filter_cookie_name = format!("tables_filter_{}", active.id);
    let per_page_cookie_name = format!("tables_per_page_{}", active.id);

    if query.filter == "*" {
        if let Some(cookie) = jar.get(&filter_cookie_name) {
            query.filter = cookie.value().to_string();
        }
    }

    if query.per_page == 50 {
        if let Some(cookie) = jar.get(&per_page_cookie_name) {
            if let Ok(pp) = cookie.value().parse::<usize>() {
                query.per_page = pp;
            }
        }
    }

    let mut all_tables: Vec<TableRowDb> = match connect_pg(&state, &active).await {
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
            Ok(rows) => rows,
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

    // Získej seznam unikátních schémat pro dropdown
    let mut schemas: Vec<String> = all_tables.iter()
        .map(|t| t.schema.clone())
        .collect();
    schemas.sort();
    schemas.dedup();

    // Aplikuj filtr pomocí parse_pattern_expression
    let total_count = all_tables.len();
    let (includes, excludes) = parse_pattern_expression(&query.filter);

    all_tables.retain(|t| {
        let full_name = format!("{}.{}", t.schema, t.name);

        // Testuj schema i table name
        let matches_include = includes.iter().any(|pattern| {
            crate::utils::filter::matches_pattern(&t.name, pattern)
                || crate::utils::filter::matches_pattern(&t.schema, pattern)
                || crate::utils::filter::matches_pattern(&full_name, pattern)
        });

        if !matches_include {
            return false;
        }

        let matches_exclude = excludes.iter().any(|pattern| {
            crate::utils::filter::matches_pattern(&t.name, pattern)
                || crate::utils::filter::matches_pattern(&t.schema, pattern)
                || crate::utils::filter::matches_pattern(&full_name, pattern)
        });

        !matches_exclude
    });

    let filtered_count = all_tables.len();

    // Aplikuj sortování
    match query.sort_by.as_str() {
        "schema" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.schema.cmp(&b.schema)
            } else {
                b.schema.cmp(&a.schema)
            }
        }),
        "name" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.name.cmp(&b.name)
            } else {
                b.name.cmp(&a.name)
            }
        }),
        "rows" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.row_estimate.cmp(&b.row_estimate)
            } else {
                b.row_estimate.cmp(&a.row_estimate)
            }
        }),
        "size" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.size_bytes.cmp(&b.size_bytes)
            } else {
                b.size_bytes.cmp(&a.size_bytes)
            }
        }),
        "indexes" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.index_count.cmp(&b.index_count)
            } else {
                b.index_count.cmp(&a.index_count)
            }
        }),
        _ => {}
    }

    // Paginace
    let page = if query.page == 0 { 1 } else { query.page };
    let per_page = query.per_page;
    let total_pages = (filtered_count as f64 / per_page as f64).ceil() as usize;
    let start = (page - 1) * per_page;
    let end = std::cmp::min(start + per_page, filtered_count);

    let paginated_tables: Vec<TableRow> = all_tables
        .into_iter()
        .skip(start)
        .take(per_page)
        .enumerate()
        .map(|(idx, r)| TableRow {
            num: start + idx + 1,
            schema: r.schema,
            name: r.name,
            rows: format_number(r.row_estimate),
            size: bytes_to_human(r.size_bytes),
            index_count: r.index_count.to_string(),
        })
        .collect();

    // Detekuj, zda je filtr ve formátu "schema.*"
    let (selected_schema, display_filter) = parse_schema_filter(&query.filter, &schemas);

    let tpl = TablesTemplate {
        ctx: build_ctx(&state),
        title: "Tables | Postgres Explorer".to_string(),
        filter: query.filter.clone(),
        display_filter,
        selected_schema,
        sort_by: query.sort_by.clone(),
        sort_order: query.sort_order.clone(),
        page,
        per_page,
        total_count,
        filtered_count,
        total_pages,
        showing_start: if filtered_count == 0 { 0 } else { start + 1 },
        showing_end: end,
        tables: paginated_tables,
        schemas,
    };

    // Ulož filtr a per_page do cookies
    let mut jar = jar;
    let filter_cookie = Cookie::build((filter_cookie_name.clone(), query.filter.clone()))
        .path("/")
        .http_only(true)
        .build();
    let per_page_cookie = Cookie::build((per_page_cookie_name.clone(), query.per_page.to_string()))
        .path("/")
        .http_only(true)
        .build();
    jar = jar.add(filter_cookie);
    jar = jar.add(per_page_cookie);

    tpl.render()
        .map(Html)
        .map(|h| (jar, h).into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

/// GET /tables/table - HTMX endpoint pro live reload tabulky
pub async fn tables_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<TablesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "No active endpoint".to_string()));
    }
    let active = active.unwrap();

    let mut all_tables: Vec<TableRowDb> = match connect_pg(&state, &active).await {
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
            Ok(rows) => rows,
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

    let total_count = all_tables.len();
    let (includes, excludes) = parse_pattern_expression(&query.filter);

    all_tables.retain(|t| {
        let full_name = format!("{}.{}", t.schema, t.name);
        let matches_include = includes.iter().any(|pattern| {
            crate::utils::filter::matches_pattern(&t.name, pattern)
                || crate::utils::filter::matches_pattern(&t.schema, pattern)
                || crate::utils::filter::matches_pattern(&full_name, pattern)
        });
        if !matches_include {
            return false;
        }
        let matches_exclude = excludes.iter().any(|pattern| {
            crate::utils::filter::matches_pattern(&t.name, pattern)
                || crate::utils::filter::matches_pattern(&t.schema, pattern)
                || crate::utils::filter::matches_pattern(&full_name, pattern)
        });
        !matches_exclude
    });

    let filtered_count = all_tables.len();

    match query.sort_by.as_str() {
        "schema" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.schema.cmp(&b.schema)
            } else {
                b.schema.cmp(&a.schema)
            }
        }),
        "name" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.name.cmp(&b.name)
            } else {
                b.name.cmp(&a.name)
            }
        }),
        "rows" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.row_estimate.cmp(&b.row_estimate)
            } else {
                b.row_estimate.cmp(&a.row_estimate)
            }
        }),
        "size" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.size_bytes.cmp(&b.size_bytes)
            } else {
                b.size_bytes.cmp(&a.size_bytes)
            }
        }),
        "indexes" => all_tables.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.index_count.cmp(&b.index_count)
            } else {
                b.index_count.cmp(&a.index_count)
            }
        }),
        _ => {}
    }

    let page = if query.page == 0 { 1 } else { query.page };
    let per_page = query.per_page;
    let total_pages = (filtered_count as f64 / per_page as f64).ceil() as usize;
    let start = (page - 1) * per_page;
    let end = std::cmp::min(start + per_page, filtered_count);

    let paginated_tables: Vec<TableRow> = all_tables
        .into_iter()
        .skip(start)
        .take(per_page)
        .enumerate()
        .map(|(idx, r)| TableRow {
            num: start + idx + 1,
            schema: r.schema,
            name: r.name,
            rows: format_number(r.row_estimate),
            size: bytes_to_human(r.size_bytes),
            index_count: r.index_count.to_string(),
        })
        .collect();

    let tpl = TablesTableTemplate {
        filter: query.filter.clone(),
        sort_by: query.sort_by.clone(),
        sort_order: query.sort_order.clone(),
        page,
        per_page,
        total_count,
        filtered_count,
        total_pages,
        showing_start: if filtered_count == 0 { 0 } else { start + 1 },
        showing_end: end,
        tables: paginated_tables,
    };

    // Ulož filtr a per_page do cookies
    let filter_cookie_name = format!("tables_filter_{}", active.id);
    let per_page_cookie_name = format!("tables_per_page_{}", active.id);

    let mut jar = jar;
    let filter_cookie = Cookie::build((filter_cookie_name, query.filter.clone()))
        .path("/")
        .http_only(true)
        .build();
    let per_page_cookie = Cookie::build((per_page_cookie_name, query.per_page.to_string()))
        .path("/")
        .http_only(true)
        .build();
    jar = jar.add(filter_cookie);
    jar = jar.add(per_page_cookie);

    tpl.render()
        .map(Html)
        .map(|h| (jar, h).into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
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

/// Parsuje filtr a detekuje, zda je ve formátu "schema.*"
/// Vrací (selected_schema, display_filter)
fn parse_schema_filter(filter: &str, schemas: &[String]) -> (String, String) {
    let trimmed = filter.trim();

    // Kontrola formátu "schema.*" (bez dalších pattern)
    if trimmed.ends_with(".*") && !trimmed.contains(',') && !trimmed.contains('-')
        && !trimmed.contains("OR") && !trimmed.contains("AND") {
        let schema_part = &trimmed[..trimmed.len() - 2]; // Odstraň ".*"

        // Ověř, že je to validní schema v seznamu
        if schemas.iter().any(|s| s == schema_part) {
            return (schema_part.to_string(), "*".to_string());
        }
    }

    // Pokud není simple schema filtr, vrať prázdný schema a celý filtr
    ("".to_string(), filter.to_string())
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
                    rows = format_number(r.row_estimate);
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
