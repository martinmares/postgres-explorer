use askama::Template;

#[derive(Clone)]
pub struct AppContext {
    pub base_path: String,
    pub version: String,
}

#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub server_name: String,
    pub server_version: String,
    pub connections: String,
    pub cache_hit_ratio: String,
    pub schema_count: String,
    pub table_count: String,
    pub index_count: String,
    pub db_size: String,
}

#[derive(Template)]
#[template(path = "tables.html")]
pub struct TablesTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub filter: String,
    pub table_count: usize,
    pub tables: Vec<TableRow>,
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
}

#[derive(Template)]
#[template(path = "indices.html")]
pub struct IndicesTemplate {
    pub ctx: AppContext,
    pub title: String,
    pub indices: Vec<IndexRow>,
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
    pub schema: String,
    pub name: String,
    pub rows: String,
    pub size: String,
    pub index_count: String,
}

#[derive(Clone)]
pub struct IndexRow {
    pub schema: String,
    pub table: String,
    pub name: String,
    pub size: String,
    pub scans: String,
}

#[derive(Clone)]
pub struct SchemaRow {
    pub name: String,
    pub table_count: String,
    pub index_count: String,
}
