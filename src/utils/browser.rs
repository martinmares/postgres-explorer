use anyhow::{anyhow, Context, Result};
use std::process::Command;

pub fn open_browser(url: &str) -> Result<()> {
    if cfg!(target_os = "windows") {
        Command::new("cmd")
            .args(["/C", "start", "", url])
            .spawn()
            .context("Failed to open browser on Windows")?;
        return Ok(());
    }

    if cfg!(target_os = "macos") {
        Command::new("open")
            .arg(url)
            .spawn()
            .context("Failed to open browser on macOS")?;
        return Ok(());
    }

    if cfg!(target_os = "linux") {
        Command::new("xdg-open")
            .arg(url)
            .spawn()
            .context("Failed to open browser on Linux")?;
        return Ok(());
    }

    Err(anyhow!("Unsupported OS for opening browser"))
}
