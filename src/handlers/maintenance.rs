use axum::extract::{Path, State};
use axum::response::Json;
use axum_extra::extract::CookieJar;
use serde::Serialize;
use std::sync::Arc;

use crate::handlers::{connect_pg, get_active_endpoint, AppState};

#[derive(Serialize)]
pub struct MaintenanceResponse {
    pub success: bool,
    pub error: Option<String>,
}

// REINDEX single index
pub async fn reindex_index(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, index_name)): Path<(String, String)>,
) -> Json<MaintenanceResponse> {
    execute_maintenance(&state, &jar, &format!(
        "REINDEX INDEX {}.{}",
        quote_ident(&schema),
        quote_ident(&index_name)
    ), "REINDEX INDEX").await
}

// REINDEX all indexes on table
pub async fn reindex_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, table)): Path<(String, String)>,
) -> Json<MaintenanceResponse> {
    execute_maintenance(&state, &jar, &format!(
        "REINDEX TABLE {}.{}",
        quote_ident(&schema),
        quote_ident(&table)
    ), "REINDEX TABLE").await
}

// VACUUM
pub async fn vacuum_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, table)): Path<(String, String)>,
) -> Json<MaintenanceResponse> {
    execute_maintenance(&state, &jar, &format!(
        "VACUUM {}.{}",
        quote_ident(&schema),
        quote_ident(&table)
    ), "VACUUM").await
}

// VACUUM FULL
pub async fn vacuum_full_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, table)): Path<(String, String)>,
) -> Json<MaintenanceResponse> {
    execute_maintenance(&state, &jar, &format!(
        "VACUUM FULL {}.{}",
        quote_ident(&schema),
        quote_ident(&table)
    ), "VACUUM FULL").await
}

// ANALYZE
pub async fn analyze_table(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((schema, table)): Path<(String, String)>,
) -> Json<MaintenanceResponse> {
    execute_maintenance(&state, &jar, &format!(
        "ANALYZE {}.{}",
        quote_ident(&schema),
        quote_ident(&table)
    ), "ANALYZE").await
}

async fn execute_maintenance(
    state: &Arc<AppState>,
    jar: &CookieJar,
    sql: &str,
    operation: &str,
) -> Json<MaintenanceResponse> {
    let active = match get_active_endpoint(state, jar).await {
        Some(a) => a,
        None => {
            return Json(MaintenanceResponse {
                success: false,
                error: Some("No active connection".to_string()),
            });
        }
    };

    let pg = match connect_pg(state, &active).await {
        Ok(p) => p,
        Err(e) => {
            return Json(MaintenanceResponse {
                success: false,
                error: Some(format!("Connection failed: {}", e)),
            });
        }
    };

    match sqlx::query(sql).execute(&pg).await {
        Ok(_) => {
            tracing::info!("Successfully executed: {}", sql);
            Json(MaintenanceResponse {
                success: true,
                error: None,
            })
        }
        Err(e) => {
            tracing::error!("Failed to execute {}: {}", operation, e);
            Json(MaintenanceResponse {
                success: false,
                error: Some(format!("{} failed: {}", operation, e)),
            })
        }
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}
