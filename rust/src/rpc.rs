//! Cluster messages and committed output envelopes. Request-specific ACK fields
//! are checked only after correlating their sender and call ID.
use crate::{
    Value,
    error::{Code, Error, Result},
    log::LogEntry,
    protocol::{CommandResult, Event, Execute, Hash, Outcome},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Rpc {
    VoteRequest {
        term: i64,
        #[serde(rename = "termP")]
        term_p: i64,
        #[serde(rename = "seqP")]
        seq_p: i64,
    },
    Vote {
        term: i64,
        #[serde(rename = "voteGranted")]
        vote_granted: bool,
    },
    Append {
        cid: String,
        term: i64,
        #[serde(rename = "termP")]
        term_p: i64,
        #[serde(rename = "seqP")]
        seq_p: i64,
        #[serde(rename = "commitSeq")]
        commit_seq: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<Vec<LogEntry>>,
    },
    Ack {
        cid: String,
        term: i64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        seq: Option<i64>,
        #[serde(rename = "cmdSeq", default, skip_serializing_if = "Option::is_none")]
        cmd_seq: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        results: Option<Vec<Outcome>>,
    },
    Err {
        cid: String,
        term: i64,
        #[serde(flatten)]
        error: Error,
        #[serde(
            rename = "logSeq",
            default,
            deserialize_with = "backtrack_hint",
            skip_serializing_if = "Option::is_none"
        )]
        log_seq: Option<i64>,
    },
    FwdCmd {
        cid: String,
        term: i64,
        items: Vec<Bytes>,
    },
    MonsterOutcome {
        #[serde(rename = "cmdSeq")]
        cmd_seq: i64,
        #[serde(with = "serde_bytes")]
        digest: Hash,
    },
    MonsterOutcomeRequest {
        #[serde(rename = "cmdSeq")]
        cmd_seq: i64,
    },
}

impl Rpc {
    pub fn parse(value: Value) -> Result<Self> {
        let binary = |v: &Value| matches!(v, Value::Binary(_));
        let valid_binary = match field(&value, "type").and_then(Value::as_str) {
            Some("append") => field(&value, "data").is_none_or(|data| {
                data.as_array().is_some_and(|rows| {
                    rows.iter()
                        .all(|row| field(row, "entry").is_some_and(binary))
                })
            }),
            Some("fwd_cmd") => field(&value, "items").is_some_and(|items| {
                items
                    .as_array()
                    .is_some_and(|items| items.iter().all(binary))
            }),
            Some("monster_outcome") => field(&value, "digest").is_some_and(binary),
            _ => true,
        };
        if !valid_binary {
            return Err(Error::new(
                Code::RpcIllegal,
                "RPC byte fields must be binary",
            ));
        }
        let rpc: Self = rmpv::ext::from_value(value)
            .map_err(|e| Error::new(Code::RpcIllegal, format!("RPC fields are illegal: {e}")))?;
        let valid = match &rpc {
            Self::VoteRequest {
                term,
                term_p,
                seq_p,
            } => *term >= 0 && *term_p >= -1 && *seq_p >= -1,
            Self::Vote { term, .. } => *term >= 0,
            Self::Append {
                cid,
                term,
                term_p,
                seq_p,
                commit_seq,
                data,
            } => {
                !cid.is_empty()
                    && *term >= 0
                    && *term_p >= -1
                    && *seq_p >= -1
                    && *commit_seq >= -1
                    && data.as_ref().is_none_or(|rows| {
                        !rows.is_empty()
                            && rows.iter().all(|r| r.term >= 0)
                            && i64::try_from(rows.len())
                                .ok()
                                .and_then(|n| seq_p.checked_add(n))
                                .is_some()
                    })
            }
            Self::Ack { cid, term, .. } | Self::Err { cid, term, .. } => {
                !cid.is_empty() && *term >= 0
            }
            Self::FwdCmd { cid, term, items } => !cid.is_empty() && *term >= 0 && !items.is_empty(),
            Self::MonsterOutcome { cmd_seq, .. } | Self::MonsterOutcomeRequest { cmd_seq } => {
                *cmd_seq >= 0
            }
        };
        if valid {
            Ok(rpc)
        } else {
            Err(Error::new(Code::RpcIllegal, "RPC fields are illegal"))
        }
    }

    pub fn cid(&self) -> Option<&str> {
        match self {
            Self::Append { cid, .. }
            | Self::Ack { cid, .. }
            | Self::Err { cid, .. }
            | Self::FwdCmd { cid, .. } => Some(cid),
            _ => None,
        }
    }
    pub fn term(&self) -> Option<i64> {
        match self {
            Self::VoteRequest { term, .. }
            | Self::Vote { term, .. }
            | Self::Append { term, .. }
            | Self::Ack { term, .. }
            | Self::Err { term, .. }
            | Self::FwdCmd { term, .. } => Some(*term),
            _ => None,
        }
    }
    pub fn error(&self) -> Option<&Error> {
        if let Self::Err { error, .. } = self {
            Some(error)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RpcEnvelope {
    pub from: String,
    #[serde(flatten)]
    pub rpc: Rpc,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientReply {
    Ack {
        #[serde(rename = "clientId")]
        client_id: String,
        #[serde(flatten, default, skip_serializing_if = "Option::is_none")]
        result: Option<CommandResult>,
    },
    Err {
        #[serde(rename = "clientId")]
        client_id: String,
        #[serde(flatten)]
        error: Error,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ApplyRequest {
    Execute {
        #[serde(flatten)]
        execute: Execute,
    },
}

#[derive(Clone, Debug, Serialize)]
#[serde(untagged)]
pub enum OutputMessage {
    Cluster(RpcEnvelope),
    Apply(ApplyRequest),
    Event(Event),
    Client(ClientReply),
}

#[derive(Clone, Debug, Serialize)]
pub struct Output {
    pub to: String,
    pub message: OutputMessage,
}

#[derive(Clone, Debug)]
pub enum Diagnostic {
    Warn(Error),
    Error(Error),
}

pub fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value
        .as_map()?
        .iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
}

// An optional malformed backtracking hint must not invalidate an otherwise
// usable ERR; the replication continuation falls back to one-step backtracking.
fn backtrack_hint<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<i64>, D::Error> {
    Ok(Value::deserialize(d)?.as_i64().filter(|seq| *seq >= -1))
}
