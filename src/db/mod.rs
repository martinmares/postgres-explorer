pub mod models;

use anyhow::{Context, Result};
use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use rand::TryRngCore;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::str::FromStr;
use base64::Engine;

use crate::config;
use crate::db::models::{CreateEndpoint, Endpoint, UpdateEndpoint};

#[derive(Clone)]
pub struct Database {
    pool: SqlitePool,
    encryption_key: [u8; 32],
}

impl Database {
    pub async fn new() -> Result<Self> {
        let db_path = config::get_db_path()?;
        let db_url = format!("sqlite://{}", db_path.display());

        tracing::info!("Connecting to database: {}", db_url);

        let options = SqliteConnectOptions::from_str(&db_url)?
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await
            .context("Failed to connect to database")?;

        Self::run_migrations(&pool).await?;

        let encryption_key = config::load_or_create_key()?
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid encryption key length"))?;

        Ok(Self { pool, encryption_key })
    }

    async fn run_migrations(pool: &SqlitePool) -> Result<()> {
        tracing::info!("Running database migrations...");

        let migration_001 = include_str!("../../migrations/001_init.sql");
        sqlx::raw_sql(migration_001)
            .execute(pool)
            .await
            .context("Failed to run migration 001")?;

        let columns = sqlx::query("PRAGMA table_info(endpoints)")
            .fetch_all(pool)
            .await
            .context("Failed to inspect endpoints schema")?;
        let mut has_encrypted_column = false;
        for row in &columns {
            let name: String = row.get("name");
            if name == "password_encrypted" {
                has_encrypted_column = true;
            }
        }

        if !has_encrypted_column {
            let migration_002 = include_str!("../../migrations/002_add_password_encrypted.sql");
            sqlx::raw_sql(migration_002)
                .execute(pool)
                .await
                .context("Failed to run migration 002")?;
        }

        // Check if ssl_mode and search_path columns exist
        let columns = sqlx::query("PRAGMA table_info(endpoints)")
            .fetch_all(pool)
            .await
            .context("Failed to inspect endpoints schema")?;
        let mut has_ssl_mode = false;
        let mut has_search_path = false;
        for row in &columns {
            let name: String = row.get("name");
            if name == "ssl_mode" {
                has_ssl_mode = true;
            }
            if name == "search_path" {
                has_search_path = true;
            }
        }

        if !has_ssl_mode || !has_search_path {
            let migration_003 = include_str!("../../migrations/003_add_ssl_and_search_path.sql");
            sqlx::raw_sql(migration_003)
                .execute(pool)
                .await
                .context("Failed to run migration 003")?;
        }

        // Check if enable_blueprint column exists
        let columns = sqlx::query("PRAGMA table_info(endpoints)")
            .fetch_all(pool)
            .await
            .context("Failed to inspect endpoints schema")?;
        let mut has_enable_blueprint = false;
        for row in &columns {
            let name: String = row.get("name");
            if name == "enable_blueprint" {
                has_enable_blueprint = true;
            }
        }

        if !has_enable_blueprint {
            let migration_004 = include_str!("../../migrations/004_add_enable_blueprint.sql");
            sqlx::raw_sql(migration_004)
                .execute(pool)
                .await
                .context("Failed to run migration 004")?;
        }

        tracing::info!("Migrations completed successfully");
        Ok(())
    }

    pub async fn get_endpoints(&self) -> Result<Vec<Endpoint>> {
        let endpoints = sqlx::query_as::<_, Endpoint>(
            "SELECT * FROM endpoints ORDER BY name"
        )
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch endpoints")?;

        Ok(endpoints)
    }

    pub async fn get_endpoint(&self, id: i64) -> Result<Option<Endpoint>> {
        let endpoint = sqlx::query_as::<_, Endpoint>(
            "SELECT * FROM endpoints WHERE id = ?"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .context("Failed to fetch endpoint")?;

        Ok(endpoint)
    }

    pub async fn create_endpoint(&self, endpoint: CreateEndpoint) -> Result<i64> {
        let mut tx = self.pool.begin().await?;

        let result = sqlx::query(
            "INSERT INTO endpoints (name, url, insecure, username, password_encrypted, ssl_mode, search_path, enable_blueprint)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&endpoint.name)
        .bind(&endpoint.url)
        .bind(endpoint.insecure)
        .bind(&endpoint.username)
        .bind::<Option<String>>(None)
        .bind(&endpoint.ssl_mode)
        .bind(&endpoint.search_path)
        .bind(endpoint.enable_blueprint)
        .execute(&mut *tx)
        .await
        .context("Failed to insert endpoint")?;

        let endpoint_id = result.last_insert_rowid();

        if let Some(password) = endpoint.password {
            let encrypted = self.encrypt_password(&password)?;
            sqlx::query("UPDATE endpoints SET password_encrypted = ? WHERE id = ?")
                .bind(&encrypted)
                .bind(endpoint_id)
                .execute(&mut *tx)
                .await?;
        }

        tx.commit().await?;

        tracing::info!("Created endpoint: {} (id: {})", endpoint.name, endpoint_id);
        Ok(endpoint_id)
    }

    pub async fn delete_endpoint(&self, id: i64) -> Result<()> {
        sqlx::query("DELETE FROM endpoints WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await
            .context("Failed to delete endpoint")?;

        tracing::info!("Deleted endpoint: {}", id);
        Ok(())
    }

    pub async fn update_endpoint(&self, id: i64, endpoint: UpdateEndpoint) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let name = endpoint.name.context("Missing endpoint name")?;
        let url = endpoint.url.context("Missing endpoint url")?;
        let insecure = endpoint.insecure.context("Missing endpoint insecure flag")?;

        sqlx::query(
            "UPDATE endpoints
             SET name = ?, url = ?, insecure = ?, username = ?, ssl_mode = ?, search_path = ?, enable_blueprint = COALESCE(?, enable_blueprint), updated_at = CURRENT_TIMESTAMP
             WHERE id = ?"
        )
        .bind(name)
        .bind(url)
        .bind(insecure)
        .bind(endpoint.username)
        .bind(endpoint.ssl_mode)
        .bind(endpoint.search_path)
        .bind(endpoint.enable_blueprint)
        .bind(id)
        .execute(&mut *tx)
        .await
        .context("Failed to update endpoint")?;

        if let Some(password) = endpoint.password {
            let encrypted = self.encrypt_password(&password)?;
            sqlx::query("UPDATE endpoints SET password_encrypted = ? WHERE id = ?")
                .bind(&encrypted)
                .bind(id)
                .execute(&mut *tx)
                .await
                .context("Failed to update endpoint password")?;
        }

        tx.commit().await?;
        tracing::info!("Updated endpoint: {}", id);
        Ok(())
    }

    pub async fn get_endpoint_password(&self, endpoint: &Endpoint) -> Option<String> {
        if let Some(ref encrypted) = endpoint.password_encrypted {
            match self.decrypt_password(encrypted) {
                Ok(password) => return Some(password),
                Err(e) => {
                    tracing::warn!(
                        "Failed to decrypt password for endpoint {}: {}",
                        endpoint.id,
                        e
                    );
                }
            }
        }
        None
    }

    fn encrypt_password(&self, password: &str) -> Result<String> {
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.encryption_key));
        let mut nonce_bytes = [0u8; 12];
        let mut rng = rand::rngs::OsRng;
        rng.try_fill_bytes(&mut nonce_bytes)
            .context("Failed to generate encryption nonce")?;
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, password.as_bytes())
            .map_err(|_| anyhow::anyhow!("Failed to encrypt password"))?;

        let mut payload = Vec::with_capacity(nonce_bytes.len() + ciphertext.len());
        payload.extend_from_slice(&nonce_bytes);
        payload.extend_from_slice(&ciphertext);
        Ok(base64::prelude::BASE64_STANDARD.encode(payload))
    }

    fn decrypt_password(&self, encrypted: &str) -> Result<String> {
        let payload = base64::prelude::BASE64_STANDARD
            .decode(encrypted)
            .context("Failed to decode encrypted password")?;
        if payload.len() < 12 {
            return Err(anyhow::anyhow!("Encrypted password payload is too short"));
        }
        let (nonce_bytes, ciphertext) = payload.split_at(12);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&self.encryption_key));
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| anyhow::anyhow!("Failed to decrypt password"))?;
        let password = String::from_utf8(plaintext)
            .context("Decrypted password is not valid UTF-8")?;
        Ok(password)
    }
}
