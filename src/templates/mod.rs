use askama::Template;

#[derive(Clone)]
pub struct AppContext {
    pub base_path: String,
    pub version: String,
    pub active_endpoint_name: String,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub server_name: String,
    pub server_version: String,
    pub schema_count: String,
    pub table_count: String,
    pub index_count: String,
    pub db_size: String,
    pub cache_hit_ratio: f64,
    pub cache_hit_ratio_text: String,
    pub active_connections: i32,
    pub max_connections: i32,
    pub connections_text: String,
    pub connections_percent: f64,
    pub top_tables: Vec<TopTable>,
}

#[derive(Template)]
#[template(path = "tables.html")]
pub struct TablesTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub filter: String,
    pub display_filter: String,
    pub selected_schema: String,
    pub sort_by: String,
    pub sort_order: String,
    pub page: usize,
    pub per_page: usize,
    pub total_count: usize,
    pub filtered_count: usize,
    pub total_pages: usize,
    pub showing_start: usize,
    pub showing_end: usize,
    pub tables: Vec<TableRow>,
    pub schemas: Vec<String>,
    pub initial_table_html: String,
    pub is_fetching: bool,
}

#[derive(Template)]
#[template(path = "tables_table.html")]
pub struct TablesTableTemplate {
    pub base_path: String,
    pub schema: String,
    pub filter: String,
    pub sort_by: String,
    pub sort_order: String,
    pub page: usize,
    pub per_page: usize,
    pub total_count: usize,
    pub filtered_count: usize,
    pub total_pages: usize,
    pub showing_start: usize,
    pub showing_end: usize,
    pub tables: Vec<TableRow>,
    pub is_fetching: bool,
    pub schemas_json: String,
}

#[derive(Template)]
#[template(path = "table_detail.html")]
pub struct TableDetailTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub schema: String,
    pub name: String,
    pub rows: String,
    pub size: String,
    pub fragmentation: String,
    pub vacuum_hint: String,
    pub index_count: String,
    pub authorized: bool,
    pub owner: String,
    pub table_type: String,
    pub last_vacuum: String,
    pub last_analyze: String,
    pub comment: String,
}

#[derive(Template)]
#[template(path = "table_modal.html")]
pub struct TableModalTemplate {
    pub ctx: AppContext,
    pub schema: String,
    pub name: String,
    pub rows: String,
    pub size: String,
    pub fragmentation: String,
    pub authorized: bool,
    pub columns: Vec<ColumnInfo>,
    pub indexes: Vec<IndexInfo>,
    pub constraints: Vec<ConstraintInfo>,
    pub stats: TableStats,
    pub storage: TableStorage,
}

#[derive(Template)]
#[template(path = "table_data.html")]
pub struct TableDataTemplate {
    pub ctx: AppContext,
    pub schema: String,
    pub name: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
    pub col_meta: Vec<ColumnMeta>,
    pub page: usize,
    pub per_page: usize,
    pub has_prev: bool,
    pub has_next: bool,
}

#[derive(Clone)]
pub struct FkMeta {
    pub schema: String,
    pub table: String,
    pub column: String,
}

#[derive(Clone)]
pub struct ColumnMeta {
    pub is_json: bool,
    pub fk: Option<FkMeta>,
}

#[derive(Template)]
#[template(path = "indices.html")]
pub struct IndicesTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub indices: Vec<IndexRow>,
    pub schemas: Vec<String>,
    pub tables: Vec<String>,
    pub selected_schema: String,
    pub selected_table: String,
    pub filter: String,
    pub sort_by: String,
    pub sort_order: String,
    pub page: usize,
    pub per_page: usize,
    pub total_count: usize,
    pub filtered_count: usize,
    pub total_pages: usize,
    pub showing_start: usize,
    pub showing_end: usize,
    pub initial_table_html: String,
}

#[derive(Template)]
#[template(path = "indices_table.html")]
pub struct IndicesTableTemplate {
    pub indices: Vec<IndexRow>,
    pub base_path: String,
    pub schema: String,
    pub table: String,
    pub filter: String,
    pub sort_by: String,
    pub sort_order: String,
    pub page: usize,
    pub per_page: usize,
    pub total_count: usize,
    pub filtered_count: usize,
    pub total_pages: usize,
    pub showing_start: usize,
    pub showing_end: usize,
    pub is_fetching: bool,
    pub schemas_json: String,
}

#[derive(Template)]
#[template(path = "console.html")]
pub struct ConsoleTemplate {
    pub ctx: AppContext,
    pub title: String,
}

#[derive(Template)]
#[template(path = "schemas.html")]
pub struct SchemasTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub filter: String,
    pub sort_by: String,
    pub sort_order: String,
    pub page: usize,
    pub per_page: usize,
    pub total_count: usize,
    pub filtered_count: usize,
    pub total_pages: usize,
    pub showing_start: usize,
    pub showing_end: usize,
    pub is_fetching: bool,
    pub schemas: Vec<SchemaRow>,
}

#[derive(Template)]
#[template(path = "schemas_table.html")]
pub struct SchemasTableTemplate {
    pub ctx: AppContext,
    pub filter: String,
    pub sort_by: String,
    pub sort_order: String,
    pub page: usize,
    pub per_page: usize,
    pub total_count: usize,
    pub filtered_count: usize,
    pub total_pages: usize,
    pub showing_start: usize,
    pub showing_end: usize,
    pub is_fetching: bool,
    pub schemas: Vec<SchemaRow>,
}

#[derive(Template)]
#[template(path = "endpoints.html")]
pub struct EndpointsTemplate {
    pub ctx: AppContext,
    pub endpoints: Vec<crate::db::models::Endpoint>,
    pub active_id: i64,
}

#[derive(Template)]
#[template(path = "endpoints_list.html")]
pub struct EndpointsListTemplate {
    pub endpoints: Vec<crate::db::models::Endpoint>,
    pub active_id: i64,
}

#[derive(Clone)]
pub struct TableRow {
    pub num: usize,
    pub schema: String,
    pub name: String,
    pub rows: String,
    pub size: String,
    pub index_count: String,
    pub partitions: Vec<String>,
}

#[derive(Clone)]
pub struct IndexRow {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub size: String,
    pub size_bytes: i64,
    pub scans: String,
    pub scans_count: i64,
    pub idx_tup_read: String,
    pub idx_tup_read_count: i64,
    pub idx_tup_fetch: String,
    pub idx_tup_fetch_count: i64,
}

#[derive(Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: String,
    pub default_value: String,
}

#[derive(Clone)]
pub struct IndexInfo {
    pub name: String,
    pub size: String,
    pub scans: String,
    pub unique: bool,
    pub primary: bool,
    pub definition: String,
}

#[derive(Clone)]
pub struct ConstraintInfo {
    pub name: String,
    pub ctype: String,
    pub definition: String,
}

#[derive(Clone)]
pub struct TableStats {
    pub live_rows: String,
    pub dead_rows: String,
    pub ins: String,
    pub upd: String,
    pub del: String,
    pub vacuum_count: String,
    pub autovacuum_count: String,
    pub analyze_count: String,
    pub autoanalyze_count: String,
    pub last_vacuum: String,
    pub last_autovacuum: String,
    pub last_analyze: String,
    pub last_autoanalyze: String,
}

#[derive(Clone)]
pub struct TableStorage {
    pub table: String,
    pub indexes: String,
    pub toast: String,
    pub total: String,
}

#[derive(Clone)]
pub struct SchemaRow {
    pub num: usize,
    pub name: String,
    pub table_count: String,
    pub index_count: String,
    pub total_size: String,
}

#[derive(Clone)]
pub struct TopTable {
    pub schema: String,
    pub name: String,
    pub size: String,
    pub size_bytes: i64,
    pub rows: String,
    pub relative_percent: i64,
    pub partitions: Vec<String>,
    pub stats_stale: bool,
    pub schema_filter_url: String,
    pub table_filter_url: String,
}

#[derive(Template)]
#[template(path = "tuning.html")]
pub struct TuningTemplate {
    pub ctx: AppContext,
    pub pg_stat_statements_enabled: bool,
    pub full_scan_queries: Vec<crate::handlers::tuning::FullScanQuery>,
    pub over_indexed_tables: Vec<crate::handlers::tuning::OverIndexedTable>,
    pub fragmented_tables: Vec<crate::handlers::tuning::FragmentedTable>,
    pub fragmented_indexes: Vec<crate::handlers::tuning::FragmentedIndex>,
    pub health_score: i64,
    pub health_summary: Vec<crate::handlers::tuning::HealthIssue>,
    pub base_path: String,
}

#[derive(Template)]
#[template(path = "export_wizard.html")]
pub struct ExportWizardTemplate {
    pub ctx: AppContext,
}
