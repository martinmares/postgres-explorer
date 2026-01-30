mod config;
mod db;
mod handlers;
mod templates;
mod utils;

use anyhow::Result;
use axum::routing::{get, put};
use axum::Router;
use clap::Parser;
use std::sync::Arc;
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "postgres-explorer")]
#[command(about = "Postgres cluster explorer", long_about = None)]
struct Args {
    /// Host for HTTP server
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Port for HTTP server
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Do not open browser automatically
    #[arg(long = "no-open")]
    no_open: bool,

    /// Base path when running behind reverse proxy (e.g. /postgres-explorer)
    #[arg(long, default_value = "/")]
    base_path: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();

    config::init_directories()?;
    let db = db::Database::new().await?;
    let base_path = normalize_base_path(&args.base_path);
    let state = Arc::new(handlers::AppState { db, base_path: base_path.clone() });

    let router = Router::new()
        .route("/", get(handlers::dashboard::dashboard))
        .route("/analyze/{schema}/{table}", axum::routing::post(handlers::dashboard::analyze_table))
        .route("/endpoints", get(handlers::endpoints::list_endpoints).post(handlers::endpoints::create_endpoint))
        .route("/endpoints/{id}", put(handlers::endpoints::update_endpoint).delete(handlers::endpoints::delete_endpoint))
        .route("/endpoints/{id}/select", axum::routing::post(handlers::endpoints::select_endpoint))
        .route("/endpoints/{id}/test", axum::routing::post(handlers::endpoints::test_endpoint))
        .route("/schemas", get(handlers::schemas::list_schemas))
        .route("/schemas/table", get(handlers::schemas::schemas_table))
        .route("/tables/{schema}/{filter}", get(handlers::tables::list_tables))
        .route("/tables/{schema}/{filter}/table", get(handlers::tables::tables_table))
        .route("/tables/indices", get(handlers::indices::list_indices))
        .route(
            "/tables/{schema}/{table}/detail",
            get(handlers::table_detail::table_detail),
        )
        .route(
            "/tables/{schema}/{table}/columns",
            get(handlers::table_detail::table_columns),
        )
        .route(
            "/tables/{schema}/{table}/indexes",
            get(handlers::table_detail::table_indexes),
        )
        .route(
            "/tables/{schema}/{table}/data",
            get(handlers::table_detail::table_data),
        )
        .route(
            "/tables/{schema}/{table}/data/preview",
            get(handlers::table_detail::table_data_preview),
        )
        .route(
            "/tables/{schema}/{table}/partitions",
            get(handlers::table_detail::table_partitions),
        )
        .route(
            "/tables/{schema}/{table}/triggers",
            get(handlers::table_detail::table_triggers),
        )
        .route(
            "/tables/{schema}/{table}/relationships",
            get(handlers::table_detail::table_relationships),
        )
        .route("/maintenance/reindex-index/{schema}/{index}", axum::routing::post(handlers::maintenance::reindex_index))
        .route("/maintenance/reindex-table/{schema}/{table}", axum::routing::post(handlers::maintenance::reindex_table))
        .route("/maintenance/vacuum/{schema}/{table}", axum::routing::post(handlers::maintenance::vacuum_table))
        .route("/maintenance/vacuum-full/{schema}/{table}", axum::routing::post(handlers::maintenance::vacuum_full_table))
        .route("/maintenance/analyze/{schema}/{table}", axum::routing::post(handlers::maintenance::analyze_table))
        .route("/maintenance/autovacuum/{schema}/{table}", axum::routing::post(handlers::maintenance::set_autovacuum))
        .route("/maintenance/autovacuum-reset/{schema}/{table}", axum::routing::post(handlers::maintenance::reset_autovacuum))
        .route(
            "/tables/{schema}/{table}/modal",
            get(handlers::tables::table_modal),
        )
        .route("/tuning", get(handlers::tuning::tuning_page))
        .route("/dev", get(handlers::console::console))
        .nest_service("/static", axum::routing::get_service(ServeDir::new("static")))
        .with_state(state.clone());
    let app = if base_path == "/" {
        router
    } else {
        Router::new().nest(&base_path, router)
    };

    let addr = format!("{}:{}", args.host, args.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Listening on http://{}{}", addr, base_path);

    if !args.no_open {
        let url = format!("http://{}{}", addr, base_path);
        if let Err(e) = utils::browser::open_browser(&url) {
            tracing::warn!("Failed to open browser: {}", e);
            tracing::info!("Please open {} manually", url);
        }
    }

    axum::serve(listener, app).await?;

    Ok(())
}

fn normalize_base_path(input: &str) -> String {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed == "/" {
        return "/".to_string();
    }
    let mut path = trimmed.to_string();
    if !path.starts_with('/') {
        path.insert(0, '/');
    }
    while path.ends_with('/') {
        path.pop();
    }
    path
}
