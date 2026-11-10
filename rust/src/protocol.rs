//! Typed messages. Semantic checks that depend on current state stay with owners.
use crate::{
    Value,
    error::{Code, Error, Result},
    wire,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub type Hash = [u8; 16];
pub type Seed = [u8; 16];
pub type Seq = i64;
pub type Term = i64;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    Follower,
    Candidate,
    Leader,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub state: State,
    pub leader: Option<String>,
    pub term: Term,
    pub seq: Seq,
    pub commit_seq: Seq,
    pub commit_term: Term,
    pub followers: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Change {
        status: Status,
    },
    Commit {
        status: Status,
        seq: Seq,
    },
    Cmd {
        status: Status,
        #[serde(rename = "cmdSeq")]
        cmd_seq: Seq,
        #[serde(rename = "cmdCount")]
        cmd_count: usize,
    },
    Sync {
        status: Status,
        #[serde(rename = "cmdSeq")]
        cmd_seq: Seq,
        #[serde(rename = "syncSeq")]
        sync_seq: Seq,
        quorum: bool,
        agree: Vec<String>,
        disagree: Vec<String>,
    },
}
impl Event {
    pub fn status(&self) -> &Status {
        match self {
            Self::Change { status }
            | Self::Commit { status, .. }
            | Self::Cmd { status, .. }
            | Self::Sync { status, .. } => status,
        }
    }
    pub fn validate(&self, nodes: &[String], id: &str) -> Result<()> {
        let s = self.status();
        if s.term < 0
            || s.seq < -1
            || s.commit_seq < -1
            || s.commit_term < -1
            || s.leader.as_ref().is_some_and(|id| !nodes.contains(id))
        {
            return Err(Error::new(Code::RpcIllegal, "event status is illegal"));
        }
        validate_members(&s.followers, nodes, Some(id), Code::RpcIllegal)?;
        match self {
            Self::Change { .. } => {}
            Self::Commit { seq, .. } if *seq >= 0 => {}
            Self::Cmd {
                cmd_seq, cmd_count, ..
            } if *cmd_seq >= 0 && *cmd_count > 0 => {}
            Self::Sync {
                cmd_seq,
                sync_seq,
                agree,
                disagree,
                ..
            } if *cmd_seq >= 0 && *sync_seq >= 0 => {
                validate_members(agree, nodes, None, Code::RpcIllegal)?;
                validate_members(disagree, nodes, None, Code::RpcIllegal)?;
            }
            _ => return Err(Error::new(Code::RpcIllegal, "event fields are illegal")),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Outcome {
    Fulfilled(Value),
    Rejected(Error),
}
impl Outcome {
    pub fn to_value(&self, include_stack: bool) -> Value {
        match self {
            Self::Fulfilled(value) => Value::Array(vec![0.into(), value.clone()]),
            Self::Rejected(err) => {
                let mut values = vec![
                    1.into(),
                    err.message.clone().into(),
                    err.code.map(|c| c.number().into()).unwrap_or(Value::Nil),
                    err.sql_code.map(|c| c.into()).unwrap_or(Value::Nil),
                ];
                if include_stack && let Some(stack) = &err.stack {
                    values.push(stack.clone().into());
                }
                Value::Array(values)
            }
        }
    }
    fn from_value(value: Value) -> std::result::Result<Self, String> {
        let Value::Array(mut values) = value else {
            return Err("outcome must be tuple".into());
        };
        match values.first().and_then(Value::as_i64) {
            Some(0) if values.len() == 2 => Ok(Self::Fulfilled(values.remove(1))),
            Some(1) if values.len() == 4 || values.len() == 5 => {
                let message = values[1]
                    .as_str()
                    .ok_or("outcome message is illegal")?
                    .to_owned();
                let optional_int = |v: &Value| -> std::result::Result<Option<i64>, String> {
                    if v.is_nil() {
                        Ok(None)
                    } else {
                        v.as_i64().map(Some).ok_or("outcome code is illegal".into())
                    }
                };
                let code = optional_int(&values[2])?.map(Code::from_number);
                let sql_code = optional_int(&values[3])?
                    .map(i32::try_from)
                    .transpose()
                    .map_err(|_| "outcome sql code is illegal")?;
                let stack = if values.len() == 5 {
                    Some(
                        values[4]
                            .as_str()
                            .ok_or("outcome stack is illegal")?
                            .to_owned(),
                    )
                } else {
                    None
                };
                Ok(Self::Rejected(Error {
                    message,
                    code,
                    sql_code,
                    stack,
                }))
            }
            _ => Err("outcome tuple is illegal".into()),
        }
    }
}
// Serialize borrowed fields directly. In particular, opaque application results
// can be large and must not be cloned just to encode them or compute a digest.
struct EncodedOutcome<'a> {
    outcome: &'a Outcome,
    include_stack: bool,
}
impl Serialize for EncodedOutcome<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        match self.outcome {
            Outcome::Fulfilled(value) => (0, value).serialize(s),
            Outcome::Rejected(error) => {
                let code = error.code.map(|code| code.number());
                if self.include_stack
                    && let Some(stack) = &error.stack
                {
                    (1, &error.message, code, error.sql_code, stack).serialize(s)
                } else {
                    (1, &error.message, code, error.sql_code).serialize(s)
                }
            }
        }
    }
}
impl Serialize for Outcome {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        EncodedOutcome {
            outcome: self,
            include_stack: true,
        }
        .serialize(s)
    }
}
impl<'de> Deserialize<'de> for Outcome {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::from_value(Value::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandResult {
    pub cmd_seq: Seq,
    pub results: Vec<Outcome>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Entry {
    Cmd {
        #[serde(rename = "matchIndex")]
        match_index: Vec<Seq>,
        #[serde(deserialize_with = "crate::binary::many")]
        items: Vec<Bytes>,
    },
    Sync {
        #[serde(rename = "cmdSeq")]
        cmd_seq: Seq,
        quorum: bool,
        agree: Vec<String>,
        disagree: Vec<String>,
        #[serde(default, with = "optional_hash")]
        digest: Option<Hash>,
    },
}
impl Entry {
    pub fn validate(&self, nodes: &[String], quorum_size: usize, seq: Seq) -> Result<()> {
        let illegal = |s| Error::new(Code::MonsterCorrupt, s);
        match self {
            Self::Cmd { match_index, items } => {
                if items.is_empty()
                    || match_index.len() != nodes.len()
                    || match_index.iter().any(|p| *p < -1 || *p >= seq)
                {
                    return Err(illegal("CMD entry items or matchIndex is corrupt"));
                }
            }
            Self::Sync {
                cmd_seq,
                quorum,
                agree,
                disagree,
                digest,
            } => {
                if *cmd_seq < 0
                    || (*quorum && (agree.len() < quorum_size || digest.is_none()))
                    || (!quorum && !agree.is_empty())
                {
                    return Err(illegal("SYNC entry decision is corrupt"));
                }
                validate_members(agree, nodes, None, Code::MonsterCorrupt)?;
                validate_members(disagree, nodes, None, Code::MonsterCorrupt)?;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Execute {
    pub cid: String,
    pub tick_count: i64,
    pub term: Term,
    pub seq: Seq,
    #[serde(
        serialize_with = "serde_bytes::serialize",
        deserialize_with = "crate::binary::hash"
    )]
    pub entry_hash: Hash,
    #[serde(deserialize_with = "crate::binary::optional_many")]
    pub items: Option<Vec<Bytes>>,
    pub match_index: Option<Vec<Seq>>,
}
impl Execute {
    pub fn validate(&self) -> Result<()> {
        let valid = !self.cid.is_empty() && self.tick_count >= 0 && self.term >= 0 && self.seq >= 0;
        let fields = if self.seq == 0 {
            self.items.is_none() && self.match_index.is_none()
        } else {
            self.items.as_ref().is_some_and(|items| !items.is_empty())
                && self
                    .match_index
                    .as_ref()
                    .is_some_and(|matches| matches.iter().all(|seq| *seq >= -1 && *seq < self.seq))
        };
        if valid && fields {
            Ok(())
        } else {
            Err(Error::new(
                Code::RpcIllegal,
                "Apply execute fields are illegal",
            ))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyCompletion {
    pub cid: String,
    pub tick_count: i64,
    pub receipt: crate::storage::Receipt,
    pub outcomes: Vec<Outcome>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub to: String,
    pub message: T,
}

pub fn digest_cmd(term: Term, seq: Seq, patchset: &[u8], outcomes: &[Outcome]) -> Result<Hash> {
    struct Outcomes<'a>(&'a [Outcome]);
    impl Serialize for Outcomes<'_> {
        fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
            s.collect_seq(self.0.iter().map(|outcome| EncodedOutcome {
                outcome,
                include_stack: false,
            }))
        }
    }
    Ok(wire::checksum(&wire::encode_payload(&(
        term,
        seq,
        serde_bytes::Bytes::new(patchset),
        Outcomes(outcomes),
    ))?))
}

pub fn validate_members(
    ids: &[String],
    nodes: &[String],
    excluded: Option<&str>,
    code: Code,
) -> Result<()> {
    if ids
        .iter()
        .any(|id| Some(id.as_str()) == excluded || !nodes.contains(id))
        || ids.windows(2).any(|pair| pair[0] >= pair[1])
    {
        Err(Error::new(code, "membership ids are illegal"))
    } else {
        Ok(())
    }
}

pub fn membership(
    id: &str,
    nodes: Vec<String>,
    quorum: Option<usize>,
) -> Result<(Vec<String>, usize)> {
    if id.is_empty() {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "id must be non-empty string",
        ));
    }
    if nodes.iter().any(String::is_empty) {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "node ids must be non-empty",
        ));
    }
    let nodes: Vec<_> = nodes
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    if nodes.len() < 3 || !nodes.iter().any(|n| n == id) {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "nodes must contain at least 3 ids including its id",
        ));
    }
    if nodes
        .iter()
        .any(|n| ["gate", "apply", "procA", "procB", "procC"].contains(&n.as_str()))
    {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "node ids must not name local destinations",
        ));
    }
    let minimum = nodes.len() / 2 + 1;
    let quorum = quorum.unwrap_or(minimum);
    if quorum < minimum || quorum > nodes.len() {
        return Err(Error::new(
            Code::ArgumentIllegal,
            "quorum is outside membership majority",
        ));
    }
    Ok((nodes, quorum))
}

mod optional_hash {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        hash: &Option<Hash>,
        s: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        hash.as_ref()
            .map(|h| serde_bytes::Bytes::new(h))
            .serialize(s)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        d: D,
    ) -> std::result::Result<Option<Hash>, D::Error> {
        crate::binary::optional_hash(d)
    }
}

/// Leave space for completion, output, and command-result envelopes above a value.
/// rmp-serde's encoder depth setter is currently unused, so inspect opaque values
/// before encoding them or accepting them into a committed Apply receipt.
pub fn validate_result_value(value: &Value) -> Result<()> {
    fn visit(value: &Value, depth: usize) -> bool {
        if depth >= wire::MAX_DEPTH - 16 {
            return false;
        }
        match value {
            Value::Array(values) => values.iter().all(|v| visit(v, depth + 1)),
            Value::Map(values) => values
                .iter()
                .all(|(k, v)| visit(k, depth + 1) && visit(v, depth + 1)),
            _ => true,
        }
    }
    if visit(value, 0) {
        Ok(())
    } else {
        Err(Error::new(
            Code::ApplyError,
            "apply result nesting exceeds protocol limit",
        ))
    }
}
