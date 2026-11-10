#![allow(dead_code)]

// Child arguments and IPC wiring are identical in both supervised examples.
#[path = "../example2/common.rs"]
mod local;

#[allow(unused_imports)] // The HTTP server needs configuration but no IPC helpers.
pub use local::*;

use monsterft::error::{Error, Result};
use std::path::PathBuf;

pub fn data_directory() -> PathBuf {
    std::env::var_os("MONSTERFT_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("example3/data"))
}

pub fn fact_url() -> Result<String> {
    std::env::var("MONSTERFT_FACT_URL")
        .map_err(|_| Error::application("MONSTERFT_FACT_URL is required; use example3/entry.sh"))
}

pub fn request_id() -> Result<String> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes).map_err(|e| Error::application(e.to_string()))?;

    let id = bytes.iter().map(|byte| format!("{byte:02x}")).collect();

    Ok(id)
}
