use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Error categories retain the JS protocol's numeric codes. Application codes
/// outside that set remain intact when an error is sent to another component.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Code {
    ArgumentIllegal,
    LogNotOpen,
    LogCorrupt,
    LogOpen,
    SqliteError,
    FsError,
    NodeNotOpen,
    NoLeader,
    NotLeader,
    TermDiff,
    NotCommit,
    HeartbeatTimeout,
    AppendTimeout,
    RpcIllegal,
    RaftIllegal,
    SendError,
    ApplyError,
    ReplBacktrack,
    ReplForgot,
    RepairNoQuorum,
    RepairOutside,
    Draining,
    MonsterIllegal,
    MonsterCorrupt,
    LocalTimeout,
    ApplyTimeout,
    LocalDivergence,
    FrameCorrupt,
    GenerationDiff,
    SupervisorError,
    Other(i64),
}

impl Code {
    pub fn from_number(number: i64) -> Self {
        Self::from(number)
    }

    pub const fn number(self) -> i64 {
        match self {
            Self::ArgumentIllegal => 10_000,
            Self::LogNotOpen => 10_001,
            Self::LogCorrupt => 10_002,
            Self::LogOpen => 10_003,
            Self::SqliteError => 10_004,
            Self::FsError => 10_005,
            Self::NodeNotOpen => 10_006,
            Self::NoLeader => 10_007,
            Self::NotLeader => 10_008,
            Self::TermDiff => 10_009,
            Self::NotCommit => 10_010,
            Self::HeartbeatTimeout => 10_011,
            Self::AppendTimeout => 10_012,
            Self::RpcIllegal => 10_013,
            Self::RaftIllegal => 10_014,
            Self::SendError => 10_015,
            Self::ApplyError => 10_016,
            Self::ReplBacktrack => 10_017,
            Self::ReplForgot => 10_018,
            Self::RepairNoQuorum => 10_019,
            Self::RepairOutside => 10_020,
            Self::Draining => 10_021,
            Self::MonsterIllegal => 10_022,
            Self::MonsterCorrupt => 10_023,
            Self::LocalTimeout => 10_024,
            Self::ApplyTimeout => 10_025,
            Self::LocalDivergence => 10_026,
            Self::FrameCorrupt => 10_027,
            Self::GenerationDiff => 10_028,
            Self::SupervisorError => 10_029,
            Self::Other(number) => number,
        }
    }
}

impl From<i64> for Code {
    fn from(number: i64) -> Self {
        match number {
            10_000 => Self::ArgumentIllegal,
            10_001 => Self::LogNotOpen,
            10_002 => Self::LogCorrupt,
            10_003 => Self::LogOpen,
            10_004 => Self::SqliteError,
            10_005 => Self::FsError,
            10_006 => Self::NodeNotOpen,
            10_007 => Self::NoLeader,
            10_008 => Self::NotLeader,
            10_009 => Self::TermDiff,
            10_010 => Self::NotCommit,
            10_011 => Self::HeartbeatTimeout,
            10_012 => Self::AppendTimeout,
            10_013 => Self::RpcIllegal,
            10_014 => Self::RaftIllegal,
            10_015 => Self::SendError,
            10_016 => Self::ApplyError,
            10_017 => Self::ReplBacktrack,
            10_018 => Self::ReplForgot,
            10_019 => Self::RepairNoQuorum,
            10_020 => Self::RepairOutside,
            10_021 => Self::Draining,
            10_022 => Self::MonsterIllegal,
            10_023 => Self::MonsterCorrupt,
            10_024 => Self::LocalTimeout,
            10_025 => Self::ApplyTimeout,
            10_026 => Self::LocalDivergence,
            10_027 => Self::FrameCorrupt,
            10_028 => Self::GenerationDiff,
            10_029 => Self::SupervisorError,
            number => Self::Other(number),
        }
    }
}

impl Serialize for Code {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_i64(self.number())
    }
}

impl<'de> Deserialize<'de> for Code {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        Ok(Self::from(i64::deserialize(deserializer)?))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Error {
    pub message: String,
    pub code: Option<Code>,
    pub sql_code: Option<i32>,
    /// Only explicitly shared stacks belong on the wire. Locally captured Rust
    /// backtraces must not enter three-Proc output comparisons or CMD digests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: Some(code),
            sql_code: None,
            stack: None,
        }
    }

    pub fn application(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code: None,
            sql_code: None,
            stack: None,
        }
    }

    pub fn with_code(mut self, code: Code) -> Self {
        self.code = Some(code);
        self
    }

    pub fn context(mut self, prefix: impl fmt::Display) -> Self {
        self.message = format!("{prefix}{}", self.message);
        self
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::new(Code::FsError, error.to_string())
    }
}

impl From<rusqlite::Error> for Error {
    fn from(error: rusqlite::Error) -> Self {
        let sql_code = match &error {
            rusqlite::Error::SqliteFailure(error, _)
            | rusqlite::Error::SqlInputError { error, .. } => Some(error.extended_code),
            _ => None,
        };
        Self {
            message: error.to_string(),
            code: Some(Code::SqliteError),
            sql_code,
            stack: None,
        }
    }
}
