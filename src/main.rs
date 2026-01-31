mod config;
mod db;
mod handlers;
mod templates;
mod utils;

use anyhow::Result;
use axum::routing::{get, put};
use axum::Router;
use axum::extract::DefaultBodyLimit;
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::services::ServeDir;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "postgres-explorer")]
#[command(about = "Postgres cluster explorer", long_about = None)]
#[command(version = env!("CARGO_PKG_VERSION"))]
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

    /// Stateless mode: no local storage, use single connection from CLI/.env
    #[arg(long, default_value_t = false)]
    stateless: bool,

    /// Connection name (shown in UI)
    #[arg(long, env = "CONF_NAME")]
    conf_name: Option<String>,

    /// Postgres URL (e.g. postgres://host:5432/db)
    #[arg(long, env = "CONF_DB_URL")]
    conf_db_url: Option<String>,

    /// Postgres username
    #[arg(long, env = "CONF_DB_USERNAME")]
    conf_db_username: Option<String>,

    /// Postgres password
    #[arg(long, env = "CONF_DB_PASSWORD")]
    conf_db_password: Option<String>,

    /// SSL mode (e.g. require, disable)
    #[arg(long, env = "CONF_DB_SSL_MODE")]
    conf_db_ssl_mode: Option<String>,

    /// Allow insecure TLS
    #[arg(long, env = "CONF_DB_INSECURE", default_value_t = false)]
    conf_db_insecure: bool,

    /// Search path override (comma-separated)
    #[arg(long, env = "CONF_DB_SEARCH_PATH")]
    conf_db_search_path: Option<String>,

    /// Enable Patroni cluster monitoring
    #[arg(long, env = "ENABLE_PATRONI", default_value_t = false)]
    enable_patroni: bool,

    /// Patroni REST API URLs (comma-separated, e.g. http://node1:8008,http://node2:8008)
    #[arg(long, env = "PATRONI_URLS")]
    patroni_urls: Option<String>,

    /// Enable Blueprint database creator wizard
    #[arg(long, env = "ENABLE_BLUEPRINT", default_value_t = false)]
    enable_blueprint: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let args = Args::parse();

    let db = if args.stateless {
        None
    } else {
        config::init_directories()?;
        Some(db::Database::new().await?)
    };
    let base_path = normalize_base_path(&args.base_path);
    let stateless_endpoint = if args.stateless {
        let url = args.conf_db_url.clone().ok_or_else(|| anyhow::anyhow!("--conf-db-url is required in --stateless mode"))?;
        let name = args.conf_name.clone().unwrap_or_else(|| url.clone());
        Some(db::models::Endpoint {
            id: 0,
            name,
            url,
            insecure: args.conf_db_insecure,
            username: args.conf_db_username.clone(),
            password_encrypted: None,
            ssl_mode: args.conf_db_ssl_mode.clone(),
            search_path: args.conf_db_search_path.clone(),
            created_at: String::new(),
            updated_at: String::new(),
        })
    } else {
        None
    };

    let patroni_urls = if args.enable_patroni {
        args.patroni_urls.as_ref().map(|urls| {
            urls.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    } else {
        None
    };

    let state = Arc::new(handlers::AppState {
        db,
        base_path: base_path.clone(),
        stateless_endpoint,
        stateless_password: if args.stateless { args.conf_db_password.clone() } else { None },
        active_override: Arc::new(std::sync::RwLock::new(None)),
        active_override_password: Arc::new(std::sync::RwLock::new(None)),
        databases_menu: Arc::new(std::sync::RwLock::new(HashMap::new())),
        schemas_cache: Arc::new(RwLock::new(HashMap::new())),
        tables_cache: Arc::new(RwLock::new(HashMap::new())),
        indices_cache: Arc::new(RwLock::new(HashMap::new())),
        export_jobs: Arc::new(RwLock::new(HashMap::new())),
        patroni_urls,
        blueprint_enabled: args.enable_blueprint,
    });

    let router = Router::new()
        .route("/", get(handlers::dashboard::dashboard))
        .route("/analyze/{schema}/{table}", axum::routing::post(handlers::dashboard::analyze_table))
        .route("/databases", get(handlers::databases::list_databases))
        .route("/databases/activate", axum::routing::post(handlers::databases::activate_database))
        .route("/databases/reset", axum::routing::post(handlers::databases::reset_database_override))
        .route("/endpoints", get(handlers::endpoints::list_endpoints).post(handlers::endpoints::create_endpoint))
        .route("/endpoints/{id}", put(handlers::endpoints::update_endpoint).delete(handlers::endpoints::delete_endpoint))
        .route("/endpoints/{id}/select", axum::routing::post(handlers::endpoints::select_endpoint))
        .route("/endpoints/{id}/test", axum::routing::post(handlers::endpoints::test_endpoint))
        .route("/schemas", get(handlers::schemas::list_schemas))
        .route("/schemas/table", get(handlers::schemas::schemas_table))
        .route("/tables/{schema}/{filter}", get(handlers::tables::list_tables))
        .route("/tables/{schema}/{filter}/table", get(handlers::tables::tables_table))
        .route("/tables/indices", get(handlers::indices::list_indices))
        .route("/tables/indices/table", get(handlers::indices::indices_table))
        .route("/tables/indices/tables", get(handlers::indices::indices_tables))
        .route("/tables/indices/{schema}/{index}/info", get(handlers::indices::index_info))
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
        .route("/export", get(handlers::export::export_wizard))
        .route("/maintenance/export", axum::routing::post(handlers::export::start_export))
        .route("/maintenance/export/{job_id}/status", get(handlers::export::get_job_status))
        .route("/maintenance/export/{job_id}/logs", get(handlers::export::stream_logs))
        .route("/maintenance/export/{job_id}/download", get(handlers::export::download_export))
        .route("/import", get(handlers::export::import_wizard))
        .route("/maintenance/import/upload", axum::routing::post(handlers::export::upload_import_file))
        .route("/maintenance/import", axum::routing::post(handlers::export::start_import))
        .route("/maintenance/import/{job_id}/status", get(handlers::export::get_job_status))
        .route("/maintenance/import/{job_id}/logs", get(handlers::export::stream_logs))
        .route(
            "/tables/{schema}/{table}/modal",
            get(handlers::tables::table_modal),
        )
        .route("/tuning", get(handlers::tuning::tuning_page))
        .route("/dev", get(handlers::console::console))
        .route("/patroni", get(handlers::patroni::patroni_view))
        .route("/patroni/status", get(handlers::patroni::patroni_status))
        .route("/blueprint", get(handlers::blueprint::blueprint_wizard))
        .route("/blueprint/preview", axum::routing::post(handlers::blueprint::preview_blueprint))
        .route("/blueprint/execute", axum::routing::post(handlers::blueprint::execute_blueprint))
        .nest_service("/static", axum::routing::get_service(ServeDir::new("static")))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024 * 1024)) // 2GB limit
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
