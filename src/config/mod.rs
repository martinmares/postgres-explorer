use anyhow::{Context, Result};
use rand::RngCore;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

pub fn get_app_dir() -> Result<PathBuf> {
    let base_dir = dirs::config_dir()
        .context("Failed to resolve config directory")?
        .join("postgres-explorer");

    Ok(base_dir)
}

pub fn get_data_dir() -> Result<PathBuf> {
    get_app_dir()
}

pub fn get_db_path() -> Result<PathBuf> {
    let data_dir = get_data_dir()?;
    let db_path = data_dir.join("postgres-explorer.db");
    Ok(db_path)
}

pub fn get_key_path() -> Result<PathBuf> {
    let key_path = get_app_dir()?.join("db.key");
    Ok(key_path)
}

pub fn load_or_create_key() -> Result<Vec<u8>> {
    let key_path = get_key_path()?;

    if key_path.exists() {
        let key_hex = fs::read_to_string(&key_path)
            .context("Failed to read encryption key")?;
        let key_bytes = hex::decode(key_hex.trim())
            .context("Failed to decode encryption key")?;
        if key_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Encryption key must be 32 bytes"));
        }
        return Ok(key_bytes);
    }

    let mut key = [0u8; 32];
    let mut rng = rand::rngs::OsRng;
    rng.try_fill_bytes(&mut key)
        .context("Failed to generate encryption key")?;
    let key_hex = hex::encode(key);

    let mut file = fs::File::create(&key_path)
        .context("Failed to create encryption key file")?;
    file.write_all(key_hex.as_bytes())
        .context("Failed to write encryption key")?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = fs::Permissions::from_mode(0o600);
        fs::set_permissions(&key_path, perms)
            .context("Failed to set encryption key permissions")?;
    }

    tracing::info!("Created encryption key: {}", key_path.display());
    Ok(key.to_vec())
}

pub fn init_directories() -> Result<()> {
    let data_dir = get_data_dir()?;

    if !data_dir.exists() {
        std::fs::create_dir_all(&data_dir)
            .context("Failed to create data directory")?;
        tracing::info!("Created data directory: {}", data_dir.display());
    }

    let _ = load_or_create_key()?;
    Ok(())
}
