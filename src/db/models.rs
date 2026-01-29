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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateEndpoint {
    pub name: Option<String>,
    pub url: Option<String>,
    pub insecure: Option<bool>,
    pub username: Option<String>,
    pub password: Option<String>,
}
