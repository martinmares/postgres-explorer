use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use serde::Deserialize;
use std::sync::Arc;
use axum_extra::extract::CookieJar;

use crate::handlers::{base_path_url, build_ctx_with_endpoint, connect_pg, get_active_endpoint, AppState, CACHE_TTL, CacheEntry};
use crate::templates::{SchemaRow, SchemasTemplate, SchemasTableTemplate};
use crate::utils::filter::{matches_simple_terms, parse_simple_terms};
use std::time::Instant;

#[derive(Deserialize)]
pub struct SchemasQuery {
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
    "name".to_string()
}

fn default_sort_order() -> String {
    "asc".to_string()
}

#[derive(sqlx::FromRow, Clone)]
pub struct SchemaRowDb {
    name: String,
    table_count: i64,
    index_count: i64,
    total_size: i64,
}

async fn fetch_schemas_from_db(
    state: &Arc<AppState>,
    active: &crate::db::models::Endpoint,
) -> Result<Vec<SchemaRowDb>, String> {
    match connect_pg(state, active).await {
        Ok(pg) => match sqlx::query_as::<_, SchemaRowDb>(
            r#"
            SELECT n.nspname as name,
                   (
                       SELECT count(DISTINCT COALESCE(parent.oid, c.oid))
                       FROM pg_class c
                       LEFT JOIN pg_inherits i ON i.inhrelid = c.oid
                       LEFT JOIN pg_class parent ON parent.oid = i.inhparent
                       WHERE c.relnamespace = n.oid AND c.relkind IN ('r','p')
                   ) as table_count,
                   (SELECT count(*) FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'i') as index_count,
                   COALESCE((SELECT SUM(pg_total_relation_size(c.oid))::bigint FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'r'), 0) as total_size
            FROM pg_namespace n
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname
            "#,
        )
        .fetch_all(&pg)
        .await
        {
            Ok(rows) => Ok(rows),
            Err(err) => Err(format!("Failed to load schemas: {}", err)),
        },
        Err(err) => Err(format!("Failed to connect to Postgres: {}", err)),
    }
}

async fn get_cached_schemas(
    state: &Arc<AppState>,
    active: &crate::db::models::Endpoint,
) -> (Vec<SchemaRowDb>, bool) {
    let now = Instant::now();
    let mut should_refresh = false;
    let (data, fetching) = {
        let mut cache = state.schemas_cache.write().await;
        match cache.get_mut(&active.id) {
            Some(entry) => {
                let stale = now.duration_since(entry.fetched_at) > CACHE_TTL;
                if stale && !entry.fetching {
                    entry.fetching = true;
                    should_refresh = true;
                }
                tracing::debug!(
                    "schemas cache hit id={} stale={} fetching={}",
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
                tracing::debug!("schemas cache miss id={}, scheduling refresh", active.id);
                (Vec::new(), true)
            }
        }
    };

    if should_refresh {
        let state = state.clone();
        let active = active.clone();
        tokio::spawn(async move {
            let result = fetch_schemas_from_db(&state, &active).await;
            let mut cache = state.schemas_cache.write().await;
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

pub async fn list_schemas(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(mut query): Query<SchemasQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let filter_cookie_name = format!("schemas_filter_{}", active.id);
    let per_page_cookie_name = format!("schemas_per_page_{}", active.id);

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

    let (mut all_schemas, is_fetching) = get_cached_schemas(&state, &active).await;

    // Filtr (substring OR přes čárku)
    let total_count = all_schemas.len();
    if query.filter != "*" && !query.filter.trim().is_empty() {
        let terms = parse_simple_terms(&query.filter);
        if !terms.is_empty() {
            all_schemas.retain(|s| matches_simple_terms(&s.name, &terms));
        }
    }

    let filtered_count = all_schemas.len();

    // Sortování
    match query.sort_by.as_str() {
        "name" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.name.cmp(&b.name)
            } else {
                b.name.cmp(&a.name)
            }
        }),
        "tables" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.table_count.cmp(&b.table_count)
            } else {
                b.table_count.cmp(&a.table_count)
            }
        }),
        "indexes" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.index_count.cmp(&b.index_count)
            } else {
                b.index_count.cmp(&a.index_count)
            }
        }),
        "size" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.total_size.cmp(&b.total_size)
            } else {
                b.total_size.cmp(&a.total_size)
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

    let paginated_schemas: Vec<SchemaRow> = all_schemas
        .into_iter()
        .skip(start)
        .take(per_page)
        .enumerate()
        .map(|(idx, r)| SchemaRow {
            num: start + idx + 1,
            name: r.name,
            table_count: format_number(r.table_count),
            index_count: format_number(r.index_count),
            total_size: crate::utils::format::bytes_to_human(r.total_size),
        })
        .collect();

    let tpl = SchemasTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
        title: "Schemas | Postgres Explorer".to_string(),
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
        schemas: paginated_schemas,
    };

    let mut jar = jar;
    let filter_cookie = axum_extra::extract::cookie::Cookie::build((filter_cookie_name, query.filter.clone()))
        .path("/")
        .http_only(true)
        .build();
    let per_page_cookie = axum_extra::extract::cookie::Cookie::build((per_page_cookie_name, query.per_page.to_string()))
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

/// GET /schemas/table - HTMX endpoint
pub async fn schemas_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<SchemasQuery>,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        return Err((axum::http::StatusCode::BAD_REQUEST, "No active endpoint".to_string()));
    }
    let active = active.unwrap();

    let filter_cookie_name = format!("schemas_filter_{}", active.id);
    let per_page_cookie_name = format!("schemas_per_page_{}", active.id);

    let (mut all_schemas, is_fetching) = get_cached_schemas(&state, &active).await;

    let total_count = all_schemas.len();
    if query.filter != "*" && !query.filter.trim().is_empty() {
        let terms = parse_simple_terms(&query.filter);
        if !terms.is_empty() {
            all_schemas.retain(|s| matches_simple_terms(&s.name, &terms));
        }
    }

    let filtered_count = all_schemas.len();

    match query.sort_by.as_str() {
        "name" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.name.cmp(&b.name)
            } else {
                b.name.cmp(&a.name)
            }
        }),
        "tables" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.table_count.cmp(&b.table_count)
            } else {
                b.table_count.cmp(&a.table_count)
            }
        }),
        "indexes" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.index_count.cmp(&b.index_count)
            } else {
                b.index_count.cmp(&a.index_count)
            }
        }),
        "size" => all_schemas.sort_by(|a, b| {
            if query.sort_order == "asc" {
                a.total_size.cmp(&b.total_size)
            } else {
                b.total_size.cmp(&a.total_size)
            }
        }),
        _ => {}
    }

    let page = if query.page == 0 { 1 } else { query.page };
    let per_page = query.per_page;
    let total_pages = (filtered_count as f64 / per_page as f64).ceil() as usize;
    let start = (page - 1) * per_page;
    let end = std::cmp::min(start + per_page, filtered_count);

    let paginated_schemas: Vec<SchemaRow> = all_schemas
        .into_iter()
        .skip(start)
        .take(per_page)
        .enumerate()
        .map(|(idx, r)| SchemaRow {
            num: start + idx + 1,
            name: r.name,
            table_count: format_number(r.table_count),
            index_count: format_number(r.index_count),
            total_size: crate::utils::format::bytes_to_human(r.total_size),
        })
        .collect();

    let tpl = SchemasTableTemplate {
        ctx: build_ctx_with_endpoint(&state, Some(&active)),
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
        schemas: paginated_schemas,
    };

    let mut jar = jar;
    let filter_cookie = axum_extra::extract::cookie::Cookie::build((filter_cookie_name, query.filter.clone()))
        .path("/")
        .http_only(true)
        .build();
    let per_page_cookie = axum_extra::extract::cookie::Cookie::build((per_page_cookie_name, query.per_page.to_string()))
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
