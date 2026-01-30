use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use std::sync::Arc;
use axum_extra::extract::CookieJar;
use serde::Deserialize;
use sqlx::Row;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState, CACHE_TTL, CacheEntry};
use crate::templates::{IndexRow, IndicesTemplate, IndicesTableTemplate};
use crate::utils::format::bytes_to_human;
use crate::utils::filter::parse_pattern_expression;
use std::time::Instant;

#[derive(sqlx::FromRow, Clone)]
pub struct IndexRowDb {
    schema: String,
    table_name: String,
    index_name: String,
    size_bytes: i64,
    scans: Option<i64>,
    idx_tup_read: Option<i64>,
    idx_tup_fetch: Option<i64>,
}

#[derive(Deserialize)]
pub struct IndicesQuery {
    #[serde(default = "default_filter")]
    pub filter: String,
    #[serde(default = "default_schema")]
    pub schema: String,
    #[serde(default = "default_table")]
    pub table: String,
    #[serde(default = "default_page")]
    pub page: usize,
    #[serde(default = "default_per_page")]
    pub per_page: usize,
    #[serde(default = "default_sort_by")]
    pub sort_by: String,
    #[serde(default = "default_sort_order")]
    pub sort_order: String,
}

fn default_filter() -> String { "*".to_string() }
fn default_schema() -> String { "*".to_string() }
fn default_table() -> String { "*".to_string() }
fn default_page() -> usize { 1 }
fn default_per_page() -> usize { 50 }
fn default_sort_by() -> String { "size".to_string() }
fn default_sort_order() -> String { "desc".to_string() }

async fn fetch_indices_from_db(
    state: &Arc<AppState>,
    active: &crate::db::models::Endpoint,
) -> Result<Vec<IndexRowDb>, String> {
    match connect_pg(state, active).await {
        Ok(pg) => match sqlx::query_as::<_, IndexRowDb>(
            r#"
            SELECT n.nspname as schema,
                   t.relname as table_name,
                   i.relname as index_name,
                   pg_relation_size(i.oid) as size_bytes,
                   s.idx_scan as scans,
                   s.idx_tup_read as idx_tup_read,
                   s.idx_tup_fetch as idx_tup_fetch
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
            Ok(rows) => Ok(rows),
            Err(err) => Err(format!("Failed to load indices: {}", err)),
        },
        Err(err) => Err(format!("Failed to connect to Postgres: {}", err)),
    }
}

async fn get_cached_indices(
    state: &Arc<AppState>,
    active: &crate::db::models::Endpoint,
) -> (Vec<IndexRowDb>, bool) {
    let now = Instant::now();
    let mut should_refresh = false;
    let (data, fetching) = {
        let mut cache = state.indices_cache.write().await;
        match cache.get_mut(&active.id) {
            Some(entry) => {
                let stale = now.duration_since(entry.fetched_at) > CACHE_TTL;
                if stale && !entry.fetching {
                    entry.fetching = true;
                    should_refresh = true;
                }
                tracing::debug!(
                    "indices cache hit id={} stale={} fetching={}",
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
                tracing::debug!("indices cache miss id={}, scheduling refresh", active.id);
                (Vec::new(), true)
            }
        }
    };

    if should_refresh {
        let state = state.clone();
        let active = active.clone();
        tokio::spawn(async move {
            let result = fetch_indices_from_db(&state, &active).await;
            let mut cache = state.indices_cache.write().await;
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

fn format_number(n: i64) -> String {
    let s = n.abs().to_string();
    let mut out = String::new();
    let mut count = 0;
    for ch in s.chars().rev() {
        if count == 3 {
            out.push(' ');
            count = 0;
        }
        out.push(ch);
        count += 1;
    }
    let mut out: String = out.chars().rev().collect();
    if n < 0 {
        out.insert(0, '-');
    }
    out
}

pub async fn list_indices(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<IndicesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let overall_start = Instant::now();
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let cache_start = Instant::now();
    let (index_rows, is_fetching) = get_cached_indices(&state, &active).await;
    let cache_ms = cache_start.elapsed().as_millis();
    let mut indices: Vec<IndexRow> = index_rows
        .into_iter()
        .map(|r| {
            let scans = r.scans.unwrap_or(0);
            let idx_tup_read = r.idx_tup_read.unwrap_or(0);
            let idx_tup_fetch = r.idx_tup_fetch.unwrap_or(0);
            IndexRow {
                schema: r.schema,
                table: r.table_name,
                name: r.index_name,
                size: bytes_to_human(r.size_bytes),
                size_bytes: r.size_bytes,
                scans: format_number(scans),
                scans_count: scans,
                idx_tup_read: format_number(idx_tup_read),
                idx_tup_read_count: idx_tup_read,
                idx_tup_fetch: format_number(idx_tup_fetch),
                idx_tup_fetch_count: idx_tup_fetch,
            }
        })
        .collect();

    let schema_list_start = Instant::now();
    // Build schema list
    let mut schemas: Vec<String> = indices.iter().map(|i| i.schema.clone()).collect();
    schemas.sort();
    schemas.dedup();
    let schema_list_ms = schema_list_start.elapsed().as_millis();
    let schemas_json = serde_json::to_string(&schemas).unwrap_or_else(|_| "[]".to_string());

    // Apply schema/table filters
    let schema_filter_start = Instant::now();
    if query.schema != "*" {
        indices.retain(|i| i.schema == query.schema);
    }
    let schema_filter_ms = schema_filter_start.elapsed().as_millis();

    let table_list_start = Instant::now();
    let mut tables: Vec<String> = if query.schema != "*" {
        indices.iter().map(|i| i.table.clone()).collect()
    } else {
        Vec::new()
    };
    tables.sort();
    tables.dedup();
    let table_list_ms = table_list_start.elapsed().as_millis();

    let table_filter_start = Instant::now();
    if query.table != "*" {
        indices.retain(|i| i.table == query.table);
    }
    let table_filter_ms = table_filter_start.elapsed().as_millis();

    // Apply name filter
    let filter_start = Instant::now();
    let total_count = indices.len();
    if query.filter != "*" && !query.filter.trim().is_empty() {
        let (includes, excludes) = parse_pattern_expression(&query.filter);
        if !(includes.len() == 1 && includes[0] == "*" && excludes.is_empty()) {
            indices.retain(|i| {
                let matches_include = includes.iter().any(|p| crate::utils::filter::matches_pattern(&i.name, p));
                if !matches_include { return false; }
                let matches_exclude = excludes.iter().any(|p| crate::utils::filter::matches_pattern(&i.name, p));
                !matches_exclude
            });
        }
    }
    let filter_ms = filter_start.elapsed().as_millis();
    let filtered_count = indices.len();

    // Sorting
    let sort_start = Instant::now();
    match query.sort_by.as_str() {
        "schema" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.schema.cmp(&b.schema) } else { b.schema.cmp(&a.schema) }),
        "table" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.table.cmp(&b.table) } else { b.table.cmp(&a.table) }),
        "name" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.name.cmp(&b.name) } else { b.name.cmp(&a.name) }),
        "scans" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.scans_count.cmp(&b.scans_count) } else { b.scans_count.cmp(&a.scans_count) }),
        "size" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.size_bytes.cmp(&b.size_bytes) } else { b.size_bytes.cmp(&a.size_bytes) }),
        _ => {}
    }
    let sort_ms = sort_start.elapsed().as_millis();

    // Pagination
    let page_start = Instant::now();
    let page = if query.page == 0 { 1 } else { query.page };
    let per_page = query.per_page;
    let total_pages = (filtered_count as f64 / per_page as f64).ceil() as usize;
    let start = (page - 1) * per_page;
    let end = std::cmp::min(start + per_page, filtered_count);
    let paginated = indices.into_iter().skip(start).take(per_page).collect::<Vec<_>>();
    let page_ms = page_start.elapsed().as_millis();

    let render_start = Instant::now();
    let table_tpl = IndicesTableTemplate {
        indices: paginated.clone(),
        base_path: state.base_path.clone(),
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
        schema: query.schema.clone(),
        table: query.table.clone(),
        is_fetching,
        schemas_json: schemas_json.clone(),
    };
    let initial_table_html = table_tpl.render().unwrap_or_else(|_| "<div>Error rendering table</div>".to_string());
    let render_ms = render_start.elapsed().as_millis();

    let selected_table = if query.schema == "*" {
        "*".to_string()
    } else if query.table == "*" || tables.contains(&query.table) {
        query.table.clone()
    } else {
        "*".to_string()
    };

    let tpl = IndicesTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
        title: "Indexes | Postgres Explorer".to_string(),
        indices: paginated,
        schemas,
        tables,
        selected_schema: query.schema.clone(),
        selected_table,
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
        initial_table_html,
    };
    let overall_ms = overall_start.elapsed().as_millis();
    tracing::debug!(
        schema = %query.schema,
        table = %query.table,
        filter = %query.filter,
        cache_ms,
        schema_list_ms,
        schema_filter_ms,
        table_list_ms,
        table_filter_ms,
        filter_ms,
        sort_ms,
        page_ms,
        render_ms,
        overall_ms,
        "indices timings"
    );

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn indices_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<IndicesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let overall_start = Instant::now();
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "No active endpoint".to_string()));
    }
    let active = active.unwrap();

    let cache_start = Instant::now();
    let (index_rows, is_fetching) = get_cached_indices(&state, &active).await;
    let cache_ms = cache_start.elapsed().as_millis();
    let mut indices: Vec<IndexRow> = index_rows
        .into_iter()
        .map(|r| {
            let scans = r.scans.unwrap_or(0);
            let idx_tup_read = r.idx_tup_read.unwrap_or(0);
            let idx_tup_fetch = r.idx_tup_fetch.unwrap_or(0);
            IndexRow {
                schema: r.schema,
                table: r.table_name,
                name: r.index_name,
                size: bytes_to_human(r.size_bytes),
                size_bytes: r.size_bytes,
                scans: format_number(scans),
                scans_count: scans,
                idx_tup_read: format_number(idx_tup_read),
                idx_tup_read_count: idx_tup_read,
                idx_tup_fetch: format_number(idx_tup_fetch),
                idx_tup_fetch_count: idx_tup_fetch,
            }
        })
        .collect();

    let schema_list_start = Instant::now();
    let mut schemas: Vec<String> = indices.iter().map(|i| i.schema.clone()).collect();
    schemas.sort();
    schemas.dedup();
    let schema_list_ms = schema_list_start.elapsed().as_millis();
    let schemas_json = serde_json::to_string(&schemas).unwrap_or_else(|_| "[]".to_string());

    let schema_filter_start = Instant::now();
    if query.schema != "*" {
        indices.retain(|i| i.schema == query.schema);
    }
    let schema_filter_ms = schema_filter_start.elapsed().as_millis();

    let table_filter_start = Instant::now();
    if query.table != "*" {
        indices.retain(|i| i.table == query.table);
    }
    let table_filter_ms = table_filter_start.elapsed().as_millis();

    let total_count = indices.len();
    let filter_start = Instant::now();
    if query.filter != "*" && !query.filter.trim().is_empty() {
        let (includes, excludes) = parse_pattern_expression(&query.filter);
        if !(includes.len() == 1 && includes[0] == "*" && excludes.is_empty()) {
            indices.retain(|i| {
                let matches_include = includes.iter().any(|p| crate::utils::filter::matches_pattern(&i.name, p));
                if !matches_include { return false; }
                let matches_exclude = excludes.iter().any(|p| crate::utils::filter::matches_pattern(&i.name, p));
                !matches_exclude
            });
        }
    }
    let filter_ms = filter_start.elapsed().as_millis();
    let filtered_count = indices.len();

    let sort_start = Instant::now();
    match query.sort_by.as_str() {
        "schema" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.schema.cmp(&b.schema) } else { b.schema.cmp(&a.schema) }),
        "table" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.table.cmp(&b.table) } else { b.table.cmp(&a.table) }),
        "name" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.name.cmp(&b.name) } else { b.name.cmp(&a.name) }),
        "scans" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.scans_count.cmp(&b.scans_count) } else { b.scans_count.cmp(&a.scans_count) }),
        "size" => indices.sort_by(|a,b| if query.sort_order == "asc" { a.size_bytes.cmp(&b.size_bytes) } else { b.size_bytes.cmp(&a.size_bytes) }),
        _ => {}
    }
    let sort_ms = sort_start.elapsed().as_millis();

    let page_start = Instant::now();
    let page = if query.page == 0 { 1 } else { query.page };
    let per_page = query.per_page;
    let total_pages = (filtered_count as f64 / per_page as f64).ceil() as usize;
    let start = (page - 1) * per_page;
    let end = std::cmp::min(start + per_page, filtered_count);
    let paginated = indices.into_iter().skip(start).take(per_page).collect::<Vec<_>>();
    let page_ms = page_start.elapsed().as_millis();

    let render_start = Instant::now();
    let tpl = IndicesTableTemplate {
        indices: paginated,
        base_path: state.base_path.clone(),
        schema: query.schema.clone(),
        table: query.table.clone(),
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
        is_fetching,
        schemas_json,
    };
    let render_ms = render_start.elapsed().as_millis();
    let overall_ms = overall_start.elapsed().as_millis();
    tracing::debug!(
        schema = %query.schema,
        table = %query.table,
        filter = %query.filter,
        cache_ms,
        schema_list_ms,
        schema_filter_ms,
        table_filter_ms,
        filter_ms,
        sort_ms,
        page_ms,
        render_ms,
        overall_ms,
        "indices_table timings"
    );

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

pub async fn indices_tables(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<IndicesQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "No active endpoint".to_string()));
    }
    let active = active.unwrap();

    let mut tables: Vec<String> = Vec::new();
    if query.schema != "*" {
        let (rows, _) = get_cached_indices(&state, &active).await;
        for r in rows {
            if r.schema == query.schema {
                tables.push(r.table_name);
            }
        }
        tables.sort();
        tables.dedup();
    }

    let payload = serde_json::json!({ "tables": tables });
    Ok(axum::Json(payload).into_response())
}

pub async fn index_info(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    axum::extract::Path((schema, index)): axum::extract::Path<(String, String)>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "No active endpoint".to_string()));
    }
    let active = active.unwrap();

    let mut html = String::new();
    html.push_str("<div class='card'><div class='card-body'>");

    match connect_pg(&state, &active).await {
        Ok(pg) => {
            if let Ok(row_opt) = sqlx::query(
                r#"
                SELECT
                    n.nspname as schema,
                    t.relname as table_name,
                    i.relname as index_name,
                    pg_relation_size(i.oid) as size_bytes,
                    s.idx_scan as scans,
                    s.idx_tup_read as idx_tup_read,
                    s.idx_tup_fetch as idx_tup_fetch,
                    ix.indisunique as is_unique,
                    ix.indisprimary as is_primary,
                    pg_get_indexdef(i.oid) as definition
                FROM pg_class i
                JOIN pg_index ix ON ix.indexrelid = i.oid
                JOIN pg_class t ON t.oid = ix.indrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
                WHERE n.nspname = $1 AND i.relname = $2
                "#,
            )
            .bind(&schema)
            .bind(&index)
            .fetch_optional(&pg)
            .await
            {
                if let Some(r) = row_opt {
                    let size = bytes_to_human(r.get::<i64, _>("size_bytes"));
                    let scans: i64 = r.get::<Option<i64>, _>("scans").unwrap_or(0);
                    let read: i64 = r.get::<Option<i64>, _>("idx_tup_read").unwrap_or(0);
                    let fetch: i64 = r.get::<Option<i64>, _>("idx_tup_fetch").unwrap_or(0);
                    let def: String = r.get("definition");
                    let is_unique: bool = r.get("is_unique");
                    let is_primary: bool = r.get("is_primary");

                    html.push_str(&format!("<div class='mb-2'><strong>{}.{} ({})</strong></div>", schema, r.get::<String, _>("table_name"), r.get::<String, _>("index_name")));
                    html.push_str("<div class='row g-2'>");
                    html.push_str(&format!("<div class='col-md-4'><div class='text-muted'>Size</div><div class='fw-semibold'>{}</div></div>", size));
                    html.push_str(&format!("<div class='col-md-4'><div class='text-muted'>Scans</div><div class='fw-semibold'>{}</div></div>", scans));
                    html.push_str(&format!("<div class='col-md-4'><div class='text-muted'>Tuples read/fetch</div><div class='fw-semibold'>{} / {}</div></div>", read, fetch));
                    html.push_str("</div>");
                    html.push_str("<div class='mt-3'><div class='text-muted'>Definition</div>");
                    html.push_str(&format!("<code class='small'>{}</code></div>", def));
                    html.push_str("<div class='mt-2'>");
                    if is_primary { html.push_str("<span class='badge bg-green-lt me-1'>PRIMARY</span>"); }
                    if is_unique { html.push_str("<span class='badge bg-blue-lt me-1'>UNIQUE</span>"); }
                    html.push_str("</div>");
                } else {
                    html.push_str("<div class='text-muted'>Index not found.</div>");
                }
            }
        }
        Err(e) => {
            html.push_str(&format!("<div class='text-danger'>Failed to connect: {}</div>", e));
        }
    }

    html.push_str("</div></div>");
    Ok(Html(html).into_response())
}
