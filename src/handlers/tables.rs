use axum::extract::{Path, Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Instant;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState, CACHE_TTL, CacheEntry};
use crate::templates::{TableModalTemplate, TableRow, TablesTemplate, TablesTableTemplate};
use crate::utils::filter::{matches_simple_terms, parse_simple_terms};
use crate::utils::format::bytes_to_human;
use sqlx::Row;
use axum_extra::extract::CookieJar;
use axum_extra::extract::cookie::Cookie;

#[derive(Deserialize)]
pub struct TablesQuery {
    #[serde(default = "default_schema")]
    pub schema: String,
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

fn default_schema() -> String {
    "*".to_string()
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

fn cookie_schema_key(schema: &str) -> String {
    let normalized = if schema.is_empty() { "*" } else { schema };
    let encoded = urlencoding::encode(normalized);
    // Cookie names are restrictive; replace '%' to keep it safe and deterministic.
    encoded.replace('%', "_")
}

fn has_pattern_ops(value: &str) -> bool {
    let v = value;
    v.contains('*')
        || v.contains('?')
        || v.contains(',')
        || v.contains(" OR ")
        || v.contains(" AND ")
        || v.contains(" - ")
        || v.contains(" NOT ")
}

#[derive(sqlx::FromRow, Clone)]
pub struct TableRowDb {
    schema: String,
    name: String,
    size_bytes: i64,
    row_estimate: i64,
    index_count: i64,
    partitions: Option<Vec<String>>,
}

async fn fetch_tables_from_db(
    state: &Arc<AppState>,
    active: &crate::db::models::Endpoint,
) -> Result<Vec<TableRowDb>, String> {
    let tables_sql = r#"
            WITH table_hierarchy AS (
                SELECT
                    c.oid,
                    n.nspname as schema,
                    c.relname as name,
                    COALESCE(parent.relname, c.relname) as parent_name,
                    COALESCE(parent_ns.nspname, n.nspname) as parent_schema,
                    pg_total_relation_size(c.oid) as size_bytes,
                    COALESCE(NULLIF(s.n_live_tup, 0), NULLIF(c.reltuples, 0), 0)::bigint as row_estimate,
                    (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count,
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
                SUM(index_count)::bigint as index_count,
                array_agg(name ORDER BY name) FILTER (WHERE is_partition) as partitions
            FROM table_hierarchy
            GROUP BY parent_schema, parent_name
            ORDER BY size_bytes DESC
            "#;

    match connect_pg(state, active).await {
        Ok(pg) => match sqlx::query_as::<_, TableRowDb>(tables_sql).fetch_all(&pg).await {
            Ok(rows) => Ok(rows),
            Err(err) => Err(format!("Failed to load tables: {}", err)),
        },
        Err(err) => Err(format!("Failed to connect to Postgres: {}", err)),
    }
}

async fn get_cached_tables(
    state: &Arc<AppState>,
    active: &crate::db::models::Endpoint,
) -> (Vec<TableRowDb>, bool) {
    let now = Instant::now();
    let mut should_refresh = false;
    let (data, fetching) = {
        let mut cache = state.tables_cache.write().await;
        match cache.get_mut(&active.id) {
            Some(entry) => {
                let stale = now.duration_since(entry.fetched_at) > CACHE_TTL;
                if stale && !entry.fetching {
                    entry.fetching = true;
                    should_refresh = true;
                }
                tracing::debug!(
                    "tables cache hit id={} stale={} fetching={}",
                    active.id,
                    stale,
                    entry.fetching
                );
                (entry.data.clone(), entry.fetching)
            }
            None => {
                cache.insert(
                    active.id,
                    CacheEntry {
                        data: Vec::new(),
                        fetched_at: now,
                        fetching: true,
                    },
                );
                should_refresh = true;
                tracing::debug!("tables cache miss id={}, scheduling refresh", active.id);
                (Vec::new(), true)
            }
        }
    };

    if should_refresh {
        let state = state.clone();
        let active = active.clone();
        tokio::spawn(async move {
            let result = fetch_tables_from_db(&state, &active).await;
            let mut cache = state.tables_cache.write().await;
            if let Some(entry) = cache.get_mut(&active.id) {
                if let Ok(rows) = result {
                    entry.data = rows;
                    entry.fetched_at = Instant::now();
                }
                entry.fetching = false;
            }
        });
    }

    (data, fetching)
}

pub async fn list_tables(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, filter)): Path<(String, String)>,
    Query(mut query): Query<TablesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let overall_start = Instant::now();
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    // Rozlišuj, jestli filter přišel z URL (explicitní) nebo použij cookie
    let schema_from_url = !schema.is_empty();
    let filter_from_url = !filter.is_empty() && filter != "*";

    query.schema = if schema_from_url { schema } else { "*".to_string() };
    query.filter = if filter_from_url { filter } else { "*".to_string() };

    // Načti filtr z cookie, pokud není v query
    let schema_key = cookie_schema_key(&query.schema);
    let filter_cookie_name = format!("tables_filter_{}_{}", schema_key, active.id);
    let per_page_cookie_name = format!("tables_per_page_{}_{}", schema_key, active.id);

    if query.filter.ends_with(".map") {
        return Err((axum::http::StatusCode::NOT_FOUND, "Not found".to_string()));
    }

    // Použij cookie JEN pokud nebyl filter explicitně v URL
    if !filter_from_url && query.filter == "*" {
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

    let cache_start = Instant::now();
    let (mut all_tables, is_fetching) = get_cached_tables(&state, &active).await;

    // Získej seznam unikátních schémat pro dropdown (z full cache)
    let mut schemas: Vec<String> = all_tables.iter()
        .map(|t| t.schema.clone())
        .collect();
    schemas.sort();
    schemas.dedup();
    let cache_ms = cache_start.elapsed().as_millis();

    let schema_list_start = Instant::now();
    // Získej seznam unikátních schémat pro dropdown
    let mut schemas: Vec<String> = all_tables.iter()
        .map(|t| t.schema.clone())
        .collect();
    schemas.sort();
    schemas.dedup();
    let schema_list_ms = schema_list_start.elapsed().as_millis();

    // Získej seznam unikátních schémat pro dropdown (z full cache)
    let mut schemas: Vec<String> = all_tables.iter()
        .map(|t| t.schema.clone())
        .collect();
    schemas.sort();
    schemas.dedup();

    // Schema filter - podporuje wildcard matching
    let schema_filter_start = Instant::now();
    if query.schema != "*" && !query.schema.is_empty() {
        if has_pattern_ops(&query.schema) {
            all_tables.retain(|t| {
                crate::utils::filter::matches_pattern(&t.schema, &query.schema)
            });
        } else {
            let schema_exact = query.schema.clone();
            all_tables.retain(|t| t.schema == schema_exact);
        }
    }
    let schema_filter_ms = schema_filter_start.elapsed().as_millis();

    // Aplikuj jednoduchý substring filtr (OR přes čárku)
    let filter_start = Instant::now();
    let total_count = all_tables.len();
    if query.filter != "*" && !query.filter.trim().is_empty() {
        let terms = parse_simple_terms(&query.filter);
        if !terms.is_empty() {
            all_tables.retain(|t| matches_simple_terms(&t.name, &terms));
        }
    }
    let filter_ms = filter_start.elapsed().as_millis();

    let filtered_count = all_tables.len();

    // Aplikuj sortování
    let sort_start = Instant::now();
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
    let sort_ms = sort_start.elapsed().as_millis();

    // Paginace
    let page_start = Instant::now();
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
            partitions: r.partitions.unwrap_or_default(),
        })
        .collect();
    let page_ms = page_start.elapsed().as_millis();

    let selected_schema = if query.schema.is_empty() { "*".to_string() } else { query.schema.clone() };
    let display_filter = query.filter.clone();

    // Vyrenderuj initial table HTML
    let render_start = Instant::now();
    let schemas_json = serde_json::to_string(&schemas).unwrap_or_else(|_| "[]".to_string());
    let table_tpl = TablesTableTemplate {
        base_path: state.base_path.clone(),
        schema: query.schema.clone(),
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
        tables: paginated_tables.clone(),
        is_fetching,
        schemas_json,
    };
    let initial_table_html = table_tpl.render().unwrap_or_else(|_| String::from("<div>Error rendering table</div>"));
    let render_ms = render_start.elapsed().as_millis();

    let tpl = TablesTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
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
        initial_table_html,
        is_fetching,
    };
    let overall_ms = overall_start.elapsed().as_millis();
    tracing::debug!(
        schema = %query.schema,
        filter = %query.filter,
        cache_ms,
        schema_list_ms,
        schema_filter_ms,
        filter_ms,
        sort_ms,
        page_ms,
        render_ms,
        overall_ms,
        "tables timings"
    );

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

    match tpl.render() {
        Ok(html) => {
            tracing::debug!(
                schema = %query.schema,
                filter = %query.filter,
                rendered_len = html.len(),
                "tables_table rendered"
            );
            Ok((jar, Html(html)).into_response())
        }
        Err(e) => {
            tracing::error!(schema = %query.schema, filter = %query.filter, error = %e, "tables_table render failed");
            Err((axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
        }
    }
}

/// GET /tables/table - HTMX endpoint pro live reload tabulky
pub async fn tables_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, filter)): Path<(String, String)>,
    Query(mut query): Query<TablesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let overall_start = Instant::now();
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "No active endpoint".to_string()));
    }
    let active = active.unwrap();
    query.schema = if schema.is_empty() { "*".to_string() } else { schema };
    query.filter = if filter.is_empty() { "*".to_string() } else { filter };
    if query.filter.ends_with(".map") {
        return Err((axum::http::StatusCode::NOT_FOUND, "Not found".to_string()));
    }

    let cache_start = Instant::now();
    let (mut all_tables, is_fetching) = get_cached_tables(&state, &active).await;
    let cache_ms = cache_start.elapsed().as_millis();

    // Získej seznam unikátních schémat pro dropdown (z full cache)
    let mut schemas: Vec<String> = all_tables.iter()
        .map(|t| t.schema.clone())
        .collect();
    schemas.sort();
    schemas.dedup();

    // Schema filter - podporuje wildcard matching
    let schema_filter_start = Instant::now();
    if query.schema != "*" && !query.schema.is_empty() {
        if has_pattern_ops(&query.schema) {
            all_tables.retain(|t| {
                crate::utils::filter::matches_pattern(&t.schema, &query.schema)
            });
        } else {
            let schema_exact = query.schema.clone();
            all_tables.retain(|t| t.schema == schema_exact);
        }
    }
    let schema_filter_ms = schema_filter_start.elapsed().as_millis();

    let filter_start = Instant::now();
    let total_count = all_tables.len();
    let sample_all: Vec<String> = all_tables.iter().take(5).map(|t| format!("{}.{}", t.schema, t.name)).collect();
    tracing::debug!(
        schema = %query.schema,
        filter = %query.filter,
        total = total_count,
        sample = ?sample_all,
        "tables_table before filter"
    );
    if query.filter != "*" && !query.filter.trim().is_empty() {
        let terms = parse_simple_terms(&query.filter);
        if !terms.is_empty() {
            all_tables.retain(|t| matches_simple_terms(&t.name, &terms));
        }
    }
    let filter_ms = filter_start.elapsed().as_millis();

    let filtered_count = all_tables.len();
    let sample: Vec<String> = all_tables.iter().take(5).map(|t| t.name.clone()).collect();
    tracing::debug!(schema = %query.schema, filter = %query.filter, filtered = filtered_count, sample = ?sample, "tables_table filtered result");

    let sort_start = Instant::now();
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
    let sort_ms = sort_start.elapsed().as_millis();

    let page_start = Instant::now();
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
            partitions: r.partitions.unwrap_or_default(),
        })
        .collect();
    let page_ms = page_start.elapsed().as_millis();

    let render_start = Instant::now();
    let schemas_json = serde_json::to_string(&schemas).unwrap_or_else(|_| "[]".to_string());
    let tpl = TablesTableTemplate {
        base_path: state.base_path.clone(),
        schema: query.schema.clone(),
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
        is_fetching,
        schemas_json,
    };
    let render_ms = render_start.elapsed().as_millis();
    let overall_ms = overall_start.elapsed().as_millis();
    tracing::debug!(
        schema = %query.schema,
        filter = %query.filter,
        cache_ms,
        schema_filter_ms,
        filter_ms,
        sort_ms,
        page_ms,
        render_ms,
        overall_ms,
        "tables_table timings"
    );

    // Ulož filtr a per_page do cookies
    let schema_key = cookie_schema_key(&query.schema);
    let filter_cookie_name = format!("tables_filter_{}_{}", schema_key, active.id);
    let per_page_cookie_name = format!("tables_per_page_{}_{}", schema_key, active.id);

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

    match tpl.render() {
        Ok(html) => {
            tracing::debug!(
                schema = %query.schema,
                filter = %query.filter,
                rendered_len = html.len(),
                "tables_table rendered"
            );
            Ok((jar, Html(html)).into_response())
        }
        Err(e) => {
            tracing::error!(
                schema = %query.schema,
                filter = %query.filter,
                error = %e,
                "tables_table render failed"
            );
            let fallback = format!("<div class=\"text-danger\">Render error: {}</div>", e);
            Ok((jar, Html(fallback)).into_response())
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
    let mut schema = schema;

    let mut rows = "-".to_string();
    let mut size = "-".to_string();
    let mut authorized = true;
    let mut fragmentation = "unknown".to_string();
    let mut columns = Vec::new();
    let mut indexes = Vec::new();
    let mut constraints = Vec::new();
    let mut stats = crate::templates::TableStats {
        live_rows: "-".to_string(),
        dead_rows: "-".to_string(),
        ins: "-".to_string(),
        upd: "-".to_string(),
        del: "-".to_string(),
        vacuum_count: "-".to_string(),
        autovacuum_count: "-".to_string(),
        analyze_count: "-".to_string(),
        autoanalyze_count: "-".to_string(),
        last_vacuum: "-".to_string(),
        last_autovacuum: "-".to_string(),
        last_analyze: "-".to_string(),
        last_autoanalyze: "-".to_string(),
    };
    let mut storage = crate::templates::TableStorage {
        table: "-".to_string(),
        indexes: "-".to_string(),
        toast: "-".to_string(),
        total: "-".to_string(),
    };

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
                        }
                    }
                    Ok(None) => {}
                    Err(err) => {
                        tracing::warn!("Failed to load table stats: {}", err);
                        authorized = false;
                    }
                }
            }

            if authorized {
                if let Ok(cols) = sqlx::query(
                    r#"
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_schema = $1 AND table_name = $2
                    ORDER BY ordinal_position
                    "#,
                )
                .bind(&schema)
                .bind(&name)
                .fetch_all(&pg)
                .await
                {
                    for row in cols {
                        columns.push(crate::templates::ColumnInfo {
                            name: row.get::<String, _>("column_name"),
                            data_type: row.get::<String, _>("data_type"),
                            nullable: row.get::<String, _>("is_nullable"),
                            default_value: row.try_get::<String, _>("column_default").unwrap_or_else(|_| "".to_string()),
                        });
                    }
                }

                if let Ok(rows_idx) = sqlx::query(
                    r#"
                    SELECT i.relname as index_name,
                           pg_get_indexdef(ix.indexrelid) as definition,
                           pg_relation_size(i.oid) as size_bytes,
                           s.idx_scan as scans,
                           ix.indisunique as is_unique,
                           ix.indisprimary as is_primary
                    FROM pg_class t
                    JOIN pg_index ix ON t.oid = ix.indrelid
                    JOIN pg_class i ON i.oid = ix.indexrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    LEFT JOIN pg_stat_all_indexes s ON s.indexrelid = i.oid
                    WHERE n.nspname = $1 AND t.relname = $2
                    ORDER BY pg_relation_size(i.oid) DESC
                    "#,
                )
                .bind(&schema)
                .bind(&name)
                .fetch_all(&pg)
                .await
                {
                    for row in rows_idx {
                        let size_bytes: i64 = row.get("size_bytes");
                        let scans: Option<i64> = row.try_get("scans").ok();
                        indexes.push(crate::templates::IndexInfo {
                            name: row.get::<String, _>("index_name"),
                            definition: row.get::<String, _>("definition"),
                            size: bytes_to_human(size_bytes),
                            scans: scans.unwrap_or(0).to_string(),
                            unique: row.get::<bool, _>("is_unique"),
                            primary: row.get::<bool, _>("is_primary"),
                        });
                    }
                }

                if let Ok(rows_cons) = sqlx::query(
                    r#"
                    SELECT conname, contype::text as contype, pg_get_constraintdef(c.oid) as definition
                    FROM pg_constraint c
                    JOIN pg_class t ON t.oid = c.conrelid
                    JOIN pg_namespace n ON n.oid = t.relnamespace
                    WHERE n.nspname = $1 AND t.relname = $2
                    ORDER BY contype, conname
                    "#,
                )
                .bind(&schema)
                .bind(&name)
                .fetch_all(&pg)
                .await
                {
                    for row in rows_cons {
                        let contype: String = row.get("contype");
                        let ctype = match contype.as_str() {
                            "p" => "PRIMARY KEY",
                            "u" => "UNIQUE",
                            "f" => "FOREIGN KEY",
                            "c" => "CHECK",
                            "x" => "EXCLUDE",
                            other => other,
                        };
                        constraints.push(crate::templates::ConstraintInfo {
                            name: row.get::<String, _>("conname"),
                            ctype: ctype.to_string(),
                            definition: row.get::<String, _>("definition"),
                        });
                    }
                }

                if let Some(row) = sqlx::query(
                    r#"
                    SELECT n_live_tup, n_dead_tup, n_tup_ins, n_tup_upd, n_tup_del,
                           vacuum_count, autovacuum_count, analyze_count, autoanalyze_count,
                           last_vacuum, last_autovacuum, last_analyze, last_autoanalyze
                    FROM pg_stat_all_tables
                    WHERE schemaname = $1 AND relname = $2
                    "#,
                )
                .bind(&schema)
                .bind(&name)
                .fetch_optional(&pg)
                .await
                .ok()
                .flatten()
                {
                    stats.live_rows = row.get::<i64, _>("n_live_tup").to_string();
                    stats.dead_rows = row.get::<i64, _>("n_dead_tup").to_string();
                    stats.ins = row.get::<i64, _>("n_tup_ins").to_string();
                    stats.upd = row.get::<i64, _>("n_tup_upd").to_string();
                    stats.del = row.get::<i64, _>("n_tup_del").to_string();
                    stats.vacuum_count = row.get::<i64, _>("vacuum_count").to_string();
                    stats.autovacuum_count = row.get::<i64, _>("autovacuum_count").to_string();
                    stats.analyze_count = row.get::<i64, _>("analyze_count").to_string();
                    stats.autoanalyze_count = row.get::<i64, _>("autoanalyze_count").to_string();
                    stats.last_vacuum = row.try_get::<String, _>("last_vacuum").unwrap_or_else(|_| "-".to_string());
                    stats.last_autovacuum = row.try_get::<String, _>("last_autovacuum").unwrap_or_else(|_| "-".to_string());
                    stats.last_analyze = row.try_get::<String, _>("last_analyze").unwrap_or_else(|_| "-".to_string());
                    stats.last_autoanalyze = row.try_get::<String, _>("last_autoanalyze").unwrap_or_else(|_| "-".to_string());
                }

                if let Some(row) = sqlx::query(
                    r#"
                    SELECT pg_relation_size(c.oid) as table_bytes,
                           pg_indexes_size(c.oid) as index_bytes,
                           pg_total_relation_size(c.oid) as total_bytes,
                           pg_table_size(c.oid) as table_only_bytes
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = $1 AND c.relname = $2
                    "#,
                )
                .bind(&schema)
                .bind(&name)
                .fetch_optional(&pg)
                .await
                .ok()
                .flatten()
                {
                    let table_bytes: i64 = row.get("table_bytes");
                    let index_bytes: i64 = row.get("index_bytes");
                    let total_bytes: i64 = row.get("total_bytes");
                    let table_only_bytes: i64 = row.get("table_only_bytes");
                    let toast_bytes = (total_bytes - table_only_bytes - index_bytes).max(0);
                    storage.table = bytes_to_human(table_bytes);
                    storage.indexes = bytes_to_human(index_bytes);
                    storage.toast = bytes_to_human(toast_bytes);
                    storage.total = bytes_to_human(total_bytes);
                }
            }
        }
        Err(err) => {
            tracing::warn!("Failed to connect to Postgres: {}", err);
            authorized = false;
        }
    }

    let tpl = TableModalTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
        schema,
        name,
        rows,
        size,
        fragmentation,
        authorized,
        columns,
        indexes,
        constraints,
        stats,
        storage,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
