use axum::extract::{Path, State};
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use sqlx::Row;
use std::sync::Arc;
use axum_extra::extract::CookieJar;
use chrono::{DateTime, Utc};

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
    let mut owner = "-".to_string();
    let mut table_type = "table".to_string();
    let mut last_vacuum = "-".to_string();
    let mut last_analyze = "-".to_string();
    let mut comment = "".to_string();

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

            // Načti table type a owner + detekce partitioned table
            let is_partitioned = match sqlx::query(
                r#"SELECT pg_catalog.pg_get_userbyid(c.relowner) as owner, c.relkind::text as relkind,
                   CASE c.relkind WHEN 'r' THEN 'table' WHEN 'p' THEN 'partitioned table' WHEN 'f' THEN 'foreign table'
                   WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' ELSE 'other' END as table_type,
                   obj_description(c.oid, 'pg_class') as comment, c.oid::bigint as table_oid
                   FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2"#,
            )
            .bind(&schema).bind(&name).fetch_optional(&pg).await {
                Ok(Some(r)) => {
                    owner = r.get::<String, _>("owner");
                    table_type = r.get::<String, _>("table_type");
                    comment = r.get::<Option<String>, _>("comment").unwrap_or_default();
                    let relkind: String = r.get("relkind");
                    let table_oid: i64 = r.get("table_oid");
                    if relkind == "p" {
                        if let Ok(Some(pr)) = sqlx::query("SELECT COALESCE(SUM(c.reltuples::bigint), 0)::bigint as total_rows, COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint as total_size FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1").bind(table_oid).fetch_optional(&pg).await {
                            rows = pr.get::<i64, _>("total_rows").to_string();
                            size = bytes_to_human(pr.get("total_size"));
                        }
                        if let Ok(idx) = sqlx::query_scalar::<_, i64>("SELECT COALESCE(SUM((SELECT count(*) FROM pg_index idx WHERE idx.indrelid = c.oid)::bigint), 0)::bigint FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid WHERE i.inhparent = $1").bind(table_oid).fetch_one(&pg).await {
                            index_count = idx.to_string();
                        }
                        true
                    } else {
                        if let Ok(Some(nr)) = sqlx::query("SELECT pg_total_relation_size($1::regclass) as size_bytes, c.reltuples::bigint as row_estimate, (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid) as index_count FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $2 AND c.relname = $3").bind(format!("{}.{}", schema, name)).bind(&schema).bind(&name).fetch_optional(&pg).await {
                            rows = nr.get::<i64, _>("row_estimate").to_string();
                            size = bytes_to_human(nr.get("size_bytes"));
                            index_count = nr.get::<i64, _>("index_count").to_string();
                        }
                        false
                    }
                }
                Ok(None) => false,
                Err(err) => { tracing::warn!("Failed to load table detail: {}", err); authorized = false; false }
            };

            if authorized {
                let sq = if is_partitioned { "SELECT COALESCE(SUM(s.n_dead_tup), 0)::bigint as n_dead_tup, COALESCE(SUM(s.n_live_tup), 0)::bigint as n_live_tup, MAX(s.last_vacuum) as last_vacuum, MAX(s.last_autovacuum) as last_autovacuum, MAX(s.last_analyze) as last_analyze, MAX(s.last_autoanalyze) as last_autoanalyze FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid JOIN pg_namespace n ON n.oid = c.relnamespace JOIN pg_stat_all_tables s ON s.schemaname = n.nspname AND s.relname = c.relname WHERE i.inhparent = (SELECT pc.oid FROM pg_class pc JOIN pg_namespace pn ON pn.oid = pc.relnamespace WHERE pn.nspname = $1 AND pc.relname = $2)" } else { "SELECT n_dead_tup, n_live_tup, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze FROM pg_stat_all_tables WHERE schemaname = $1 AND relname = $2" };
                if let Ok(Some(row)) = sqlx::query(sq).bind(&schema).bind(&name).fetch_optional(&pg).await {
                    let dead: i64 = row.get("n_dead_tup");
                    let live: i64 = row.get("n_live_tup");
                    let total = dead + live;
                    if total > 0 {
                        let pct = (dead as f64 / total as f64) * 100.0;
                        fragmentation = format!("{:.1}% dead tuples", pct);
                        vacuum_hint = if pct > 20.0 { "Consider VACUUM (high dead tuples)".to_string() } else { "Vacuum not urgent".to_string() };
                    }
                    let lv: Option<DateTime<Utc>> = row.get("last_vacuum");
                    let lav: Option<DateTime<Utc>> = row.get("last_autovacuum");
                    last_vacuum = if let Some(m) = lv {
                        if let Some(a) = lav {
                            if m > a { format!("{} (manual)", m.format("%Y-%m-%d %H:%M:%S")) }
                            else { format!("{} (auto)", a.format("%Y-%m-%d %H:%M:%S")) }
                        } else { format!("{} (manual)", m.format("%Y-%m-%d %H:%M:%S")) }
                    } else if let Some(a) = lav {
                        format!("{} (auto)", a.format("%Y-%m-%d %H:%M:%S"))
                    } else { "never".to_string() };
                    let la: Option<DateTime<Utc>> = row.get("last_analyze");
                    let laa: Option<DateTime<Utc>> = row.get("last_autoanalyze");
                    last_analyze = if let Some(m) = la {
                        if let Some(a) = laa {
                            if m > a { format!("{} (manual)", m.format("%Y-%m-%d %H:%M:%S")) }
                            else { format!("{} (auto)", a.format("%Y-%m-%d %H:%M:%S")) }
                        } else { format!("{} (manual)", m.format("%Y-%m-%d %H:%M:%S")) }
                    } else if let Some(a) = laa {
                        format!("{} (auto)", a.format("%Y-%m-%d %H:%M:%S"))
                    } else { "never".to_string() };
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
        owner,
        table_type,
        last_vacuum,
        last_analyze,
        comment,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}

// Lazy load columns
pub async fn table_columns(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, name)): Path<(String, String)>,
) -> Html<String> {
    let active = match get_active_endpoint(&state, &jar).await {
        Some(a) => a,
        None => return Html("<div class='text-muted'>No active connection</div>".to_string()),
    };

    let pg = match connect_pg(&state, &active).await {
        Ok(p) => p,
        Err(_) => return Html("<div class='text-muted'>Connection failed</div>".to_string()),
    };

    let cols = sqlx::query(
        r#"
        SELECT DISTINCT ON (a.attnum)
            a.attname as column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as data_type,
            CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END as is_nullable,
            pg_get_expr(ad.adbin, ad.adrelid) as column_default,
            col_description(c.oid, a.attnum) as description,
            COALESCE(pk.is_pk, false) as is_primary_key,
            COALESCE(MIN(fk.foreign_table), '') as fk_table,
            COALESCE(MIN(fk.foreign_column), '') as fk_column,
            COALESCE(bool_or(unq.is_unique), false) as is_unique,
            COALESCE(bool_or(idx.is_indexed), false) as is_indexed
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
        LEFT JOIN (
            SELECT a.attnum, true as is_pk
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass AND i.indisprimary
        ) pk ON pk.attnum = a.attnum
        LEFT JOIN (
            SELECT a.attnum, nf.nspname || '.' || cf.relname as foreign_table, af.attname as foreign_column
            FROM pg_constraint con
            JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
            JOIN pg_class cf ON cf.oid = con.confrelid
            JOIN pg_namespace nf ON nf.oid = cf.relnamespace
            JOIN pg_attribute af ON af.attrelid = con.confrelid AND af.attnum = ANY(con.confkey)
            WHERE con.conrelid = $1::regclass AND con.contype = 'f'
        ) fk ON fk.attnum = a.attnum
        LEFT JOIN (
            SELECT a.attnum, true as is_unique
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass AND i.indisunique AND NOT i.indisprimary
        ) unq ON unq.attnum = a.attnum
        LEFT JOIN (
            SELECT a.attnum, true as is_indexed
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = $1::regclass
        ) idx ON idx.attnum = a.attnum
        WHERE n.nspname = $2 AND c.relname = $3 AND a.attnum > 0 AND NOT a.attisdropped
        GROUP BY c.oid, a.attnum, a.attname, a.atttypid, a.atttypmod, a.attnotnull, ad.adbin, ad.adrelid, pk.is_pk
        ORDER BY a.attnum
        "#,
    )
    .bind(format!("{}.{}", schema, name))
    .bind(&schema)
    .bind(&name)
    .fetch_all(&pg)
    .await;

    match cols {
        Ok(rows) if rows.is_empty() => Html("<div class='text-center text-muted py-5'>No columns found</div>".to_string()),
        Ok(rows) => {
            let mut html = String::from("<div class='table-responsive'><table class='table table-vcenter'><thead><tr><th>Name</th><th>Type</th><th>Nullable</th><th>Default</th><th>Description</th><th>Constraints</th></tr></thead><tbody>");
            
            for row in rows {
                let col_name: String = row.get("column_name");
                let data_type: String = row.get("data_type");
                let nullable: String = row.get("is_nullable");
                let default_val: String = row.try_get("column_default").unwrap_or_default();
                let description: String = row.try_get("description").unwrap_or_default();
                let is_pk: bool = row.get("is_primary_key");
                let fk_table: String = row.get("fk_table");
                let fk_column: String = row.get("fk_column");
                let is_unique: bool = row.get("is_unique");
                let is_indexed: bool = row.get("is_indexed");
                
                html.push_str(&format!("<tr><td><strong>{}</strong></td>", col_name));
                html.push_str(&format!("<td><code>{}</code></td>", data_type));
                
                if nullable == "YES" {
                    html.push_str("<td><span class='badge bg-yellow-lt text-yellow-fg'>NULL</span></td>");
                } else {
                    html.push_str("<td><span class='badge bg-green-lt text-green-fg'>NOT NULL</span></td>");
                }
                
                if default_val.is_empty() {
                    html.push_str("<td><span class='text-muted'>-</span></td>");
                } else {
                    html.push_str(&format!("<td><code class='text-muted small'>{}</code></td>", default_val));
                }
                
                if description.is_empty() {
                    html.push_str("<td><span class='text-muted'>-</span></td>");
                } else {
                    html.push_str(&format!("<td>{}</td>", description));
                }
                
                html.push_str("<td>");
                if is_pk {
                    html.push_str("<span class='badge bg-blue text-blue-fg me-1'><i class='ti ti-key'></i> PK</span>");
                }
                if !fk_table.is_empty() {
                    html.push_str(&format!("<span class='badge bg-purple text-purple-fg me-1' title='FK to {}.{}'><i class='ti ti-arrow-right'></i> FK → {}</span>", fk_table, fk_column, fk_table));
                }
                if is_unique {
                    html.push_str("<span class='badge bg-cyan text-cyan-fg me-1'><i class='ti ti-check'></i> UNIQUE</span>");
                }
                if is_indexed {
                    html.push_str("<span class='badge bg-gray-lt text-gray-fg me-1'><i class='ti ti-sort-ascending'></i> IDX</span>");
                }
                html.push_str("</td></tr>");
            }
            
            html.push_str("</tbody></table></div>");
            Html(html)
        },
        Err(_) => Html("<div class='alert alert-danger'>Failed to load columns</div>".to_string()),
    }
}

// Lazy load indexes
pub async fn table_indexes(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, name)): Path<(String, String)>,
) -> Html<String> {
    let active = match get_active_endpoint(&state, &jar).await {
        Some(a) => a,
        None => return Html("<div class='text-muted'>No active connection</div>".to_string()),
    };

    let pg = match connect_pg(&state, &active).await {
        Ok(p) => p,
        Err(_) => return Html("<div class='text-muted'>Connection failed</div>".to_string()),
    };

    let idxs = sqlx::query(
        r#"
        SELECT
            i.relname as index_name,
            string_agg(a.attname, ', ' ORDER BY array_position(ix.indkey, a.attnum)) as columns,
            am.amname as index_type,
            pg_relation_size(i.oid) as size_bytes,
            ix.indisunique as is_unique,
            ix.indisprimary as is_primary,
            pg_get_indexdef(ix.indexrelid) as definition,
            COALESCE(s.idx_scan, 0) as scans
        FROM pg_index ix
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_class t ON t.oid = ix.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = i.relam
        LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.oid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        WHERE n.nspname = $1 AND t.relname = $2
        GROUP BY i.relname, i.oid, am.amname, ix.indisunique, ix.indisprimary, ix.indexrelid, s.idx_scan
        ORDER BY ix.indisprimary DESC, ix.indisunique DESC, i.relname
        "#,
    )
    .bind(&schema)
    .bind(&name)
    .fetch_all(&pg)
    .await;

    match idxs {
        Ok(rows) if rows.is_empty() => Html("<div class='text-center text-muted py-5'>No indexes found</div>".to_string()),
        Ok(rows) => {
            let mut html = String::from("<div class='table-responsive'><table class='table table-vcenter'><thead><tr><th>Name</th><th>Columns</th><th>Type</th><th class='text-end'>Size</th><th class='text-end'>Scans</th><th>Attributes</th><th>Actions</th></tr></thead><tbody>");
            
            for row in rows {
                let idx_name: String = row.get("index_name");
                let columns: String = row.get("columns");
                let idx_type: String = row.get("index_type");
                let size_bytes: i64 = row.get("size_bytes");
                let is_unique: bool = row.get("is_unique");
                let is_primary: bool = row.get("is_primary");
                let definition: String = row.get("definition");
                let scans: i64 = row.get("scans");
                
                html.push_str(&format!("<tr><td><strong>{}</strong></td>", idx_name));
                html.push_str(&format!("<td><code class='text-muted'>{}</code></td>", columns));
                html.push_str(&format!("<td><span class='badge bg-gray-lt text-gray-fg'>{}</span></td>", idx_type));
                html.push_str(&format!("<td class='text-end'>{}</td>", bytes_to_human(size_bytes)));
                html.push_str(&format!("<td class='text-end'>{}</td>", scans));
                
                html.push_str("<td>");
                if is_primary {
                    html.push_str("<span class='badge bg-blue text-blue-fg me-1'><i class='ti ti-key'></i> PRIMARY</span>");
                }
                if is_unique {
                    html.push_str("<span class='badge bg-cyan text-cyan-fg me-1'><i class='ti ti-check'></i> UNIQUE</span>");
                }
                html.push_str("</td>");
                
                let def_escaped = definition.replace('`', "\\`").replace('\'', "\\'");
                html.push_str(&format!("<td><button class='btn btn-sm btn-ghost-secondary' onclick='showIndexDDL(\"{}\", `{}`)' title='Show DDL'><i class='ti ti-code'></i></button></td></tr>", idx_name, def_escaped));
            }
            
            html.push_str("</tbody></table></div>");
            Html(html)
        },
        Err(_) => Html("<div class='alert alert-danger'>Failed to load indexes</div>".to_string()),
    }
}
