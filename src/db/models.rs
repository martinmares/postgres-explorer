use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct Endpoint {
    pub id: i64,
    pub name: String,
    pub url: String,
    pub insecure: bool,
    pub username: Option<String>,
    pub password_encrypted: Option<String>,
    pub ssl_mode: Option<String>,
    pub search_path: Option<String>,
    #[serde(default)]
    pub enable_blueprint: bool,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateEndpoint {
    pub name: String,
    pub url: String,
    pub insecure: bool,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl_mode: Option<String>,
    pub search_path: Option<String>,
    #[serde(default)]
    pub enable_blueprint: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEndpoint {
    pub name: Option<String>,
    pub url: Option<String>,
    pub insecure: Option<bool>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub ssl_mode: Option<String>,
    pub search_path: Option<String>,
    pub enable_blueprint: Option<bool>,
}

#[derive(Debug, Clone, FromRow, Serialize, Deserialize)]
pub struct QueryHistory {
    pub id: i64,
    pub endpoint_id: i64,
    pub query_text: String,
    pub executed_at: String,
    pub status: String,
    pub duration_ms: Option<i64>,
}
