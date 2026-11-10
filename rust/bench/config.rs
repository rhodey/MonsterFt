use monsterft::error::{Code, Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    path::{Path, PathBuf},
    rc::Rc,
};

pub const NODES: [&str; 3] = ["1", "2", "3"];

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub run_dir: PathBuf,
    pub base_port: u16,
    #[serde(default)]
    pub hosts: Option<[String; 3]>,
    pub rtt_us: u64,
    #[serde(default)]
    pub one_way_delay_us: Option<f64>,
    pub window: usize,
    #[serde(default = "batch_size")]
    pub batch_size: usize,
    pub warmup_ms: u64,
    pub measure_ms: u64,
    #[serde(default = "tick_ms")]
    pub tick_ms: u64,
    #[serde(default = "audit_batches")]
    pub audit_batches: usize,
}
fn batch_size() -> usize {
    1
}
fn tick_ms() -> u64 {
    10
}
fn audit_batches() -> usize {
    64
}
impl Config {
    pub fn read(path: &Path) -> Result<Self> {
        let config: Self =
            serde_json::from_slice(&std::fs::read(path)?).map_err(|e| error(e.to_string()))?;
        if !config.run_dir.is_absolute()
            || config.base_port > 65513
            || config
                .hosts
                .as_ref()
                .is_some_and(|hosts| hosts.iter().any(String::is_empty))
            || config.window == 0
            || config.window > 256
            || config.batch_size == 0
            || config.batch_size > 1024
            || config.measure_ms == 0
            || config.warmup_ms > 3_600_000
            || config.measure_ms > 3_600_000
            || config
                .one_way_delay_us
                .is_some_and(|v| !v.is_finite() || v < 0.0)
            || config.tick_ms == 0
            || config.tick_ms > 1000
            || config.audit_batches < 2
        {
            return Err(error("invalid benchmark configuration"));
        }
        Ok(config)
    }
}

#[derive(Default)]
pub struct Shared {
    pub errors: RefCell<Vec<String>>,
    pub attempts: Cell<u64>,
    pub active: Cell<bool>,
    pub measuring: Cell<bool>,
    pub audit: Cell<bool>,
    pub delay: RefCell<Option<crate::delay::DelaySender>>,
    pub data_appends: Cell<u64>,
    pub paired_appends: Cell<u64>,
    pub sent_frames: Cell<u64>,
    pub sent_bytes: Cell<u64>,
    pub received_frames: Cell<u64>,
    pub received_bytes: Cell<u64>,
    pub dropped: Cell<u64>,
    pub last_sync_cmd: Cell<i64>,
    pub expected_leader: RefCell<Option<(String, i64)>>,
}
impl Shared {
    pub fn new() -> Rc<Self> {
        Rc::new(Self {
            last_sync_cmd: Cell::new(-1),
            ..Self::default()
        })
    }
    pub fn fail(&self, message: impl Into<String>) {
        self.errors.borrow_mut().push(message.into());
    }
    pub fn check(&self) -> Result<()> {
        if let Some(message) = self.errors.borrow().first() {
            return Err(error(message.clone()));
        }
        if self.attempts.get() > 1 {
            return Err(error("benchmark restarted a local owner group"));
        }
        Ok(())
    }
}

pub fn error(message: impl Into<String>) -> Error {
    Error::new(Code::MonsterIllegal, message)
}
pub fn io_error(error: std::io::Error) -> Error {
    Error::new(Code::SendError, error.to_string())
}
pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| error(e.to_string()))?;
    let temporary = path.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::write(&temporary, bytes)?;
    std::fs::rename(temporary, path)?;
    Ok(())
}
pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match std::fs::read(path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| error(e.to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}
