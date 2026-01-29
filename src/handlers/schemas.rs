use axum::extract::State;
use axum::response::{Html, IntoResponse, Redirect, Response};
use askama::Template;
use std::sync::Arc;
use axum_extra::extract::CookieJar;

use crate::handlers::{base_path_url, build_ctx, connect_pg, get_active_endpoint, AppState};
use crate::templates::{SchemaRow, SchemasTemplate};

#[derive(sqlx::FromRow)]
struct SchemaRowDb {
    name: String,
    table_count: i64,
    index_count: i64,
}

pub async fn list_schemas(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
) -> Result<Response, (axum::http::StatusCode, String)> {
    let active = get_active_endpoint(&state, &jar).await;
    if active.is_none() {
        let target = base_path_url(&state, "/endpoints");
        return Ok(Redirect::to(&target).into_response());
    }
    let active = active.unwrap();

    let schemas: Vec<SchemaRow> = match connect_pg(&state, &active).await {
        Ok(pg) => match sqlx::query_as::<_, SchemaRowDb>(
            r#"
            SELECT n.nspname as name,
                   (SELECT count(*) FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'r') as table_count,
                   (SELECT count(*) FROM pg_class c WHERE c.relnamespace = n.oid AND c.relkind = 'i') as index_count
            FROM pg_namespace n
            WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
            ORDER BY n.nspname
            "#,
        )
        .fetch_all(&pg)
        .await
        {
            Ok(rows) => rows
                .into_iter()
                .map(|r| SchemaRow {
                    name: r.name,
                    table_count: r.table_count.to_string(),
                    index_count: r.index_count.to_string(),
                })
                .collect(),
            Err(err) => {
                tracing::warn!("Failed to load schemas: {}", err);
                Vec::new()
            }
        },
        Err(err) => {
            tracing::warn!("Failed to connect to Postgres: {}", err);
            Vec::new()
        }
    };

    let tpl = SchemasTemplate {
        ctx: build_ctx(&state),
        title: "Schemas | Postgres Explorer".to_string(),
        schemas,
    };

    tpl.render()
        .map(Html)
        .map(|h| h.into_response())
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
}
