//! Local RPCs carry the same frames in process and through shared memory.
use crate::{
    error::{Code, Error, Result},
    protocol::{ApplyCompletion, Seed},
    storage::{Capture, Receipt},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub const PROCS: [&str; 3] = ["procA", "procB", "procC"];
pub const COMPONENTS: [&str; 4] = ["procA", "procB", "procC", "apply"];
pub type SendFrame = Box<dyn FnMut(&str, Bytes) -> Result<()>>;

#[derive(Clone, Debug)]
pub struct Received {
    pub from: String,
    pub frame: Bytes,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StartupRequest {
    Hello {
        cid: String,
        #[serde(
            serialize_with = "serde_bytes::serialize",
            deserialize_with = "crate::binary::hash"
        )]
        seed: Seed,
    },
    Open {
        cid: String,
    },
    Capture {
        cid: String,
    },
    Adopt {
        cid: String,
    },
    Reconcile {
        cid: String,
    },
    Ready {
        cid: String,
    },
    Close {
        cid: String,
    },
}
impl StartupRequest {
    pub fn cid(&self) -> &str {
        match self {
            Self::Hello { cid, .. }
            | Self::Open { cid }
            | Self::Capture { cid }
            | Self::Adopt { cid }
            | Self::Reconcile { cid }
            | Self::Ready { cid }
            | Self::Close { cid } => cid,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Input {
    Start,
    Tick,
    ClusterMessage {
        from: String,
        #[serde(deserialize_with = "crate::binary::bytes")]
        frame: Bytes,
    },
    Append {
        #[serde(rename = "clientId")]
        client_id: String,
        #[serde(deserialize_with = "crate::binary::many")]
        items: Vec<Bytes>,
    },
    Drain {
        #[serde(rename = "clientId")]
        client_id: String,
    },
    ApplyResult {
        reply: ApplyCompletion,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderedInput {
    pub cid: String,
    pub tick_count: i64,
    pub timems: u64,
    #[serde(flatten)]
    pub input: Input,
}
impl OrderedInput {
    pub fn validate(&self) -> Result<()> {
        if self.cid.is_empty() || self.tick_count < 0 || self.timems > i64::MAX as u64 {
            return Err(Error::new(
                Code::RpcIllegal,
                "Proc input fields are illegal",
            ));
        }
        match &self.input {
            Input::Append { client_id, items } if client_id.is_empty() || items.is_empty() => Err(
                Error::new(Code::RpcIllegal, "Proc append fields are illegal"),
            ),
            Input::Drain { client_id } if client_id.is_empty() => Err(Error::new(
                Code::RpcIllegal,
                "Proc drain clientId is illegal",
            )),
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LocalReply {
    Ack {
        cid: String,
        #[serde(rename = "tickCount", default, skip_serializing_if = "Option::is_none")]
        tick_count: Option<i64>,
        #[serde(
            default,
            skip_serializing_if = "Option::is_none",
            deserialize_with = "crate::binary::optional_many"
        )]
        outputs: Option<Vec<Bytes>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selection: Option<Capture>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        receipt: Option<Receipt>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        outcomes: Option<Vec<crate::protocol::Outcome>>,
    },
    Err {
        cid: String,
        #[serde(flatten)]
        error: Error,
    },
}
impl LocalReply {
    pub fn ack(cid: impl Into<String>) -> Self {
        Self::Ack {
            cid: cid.into(),
            tick_count: None,
            outputs: None,
            selection: None,
            receipt: None,
            outcomes: None,
        }
    }
    pub fn cid(&self) -> &str {
        match self {
            Self::Ack { cid, .. } | Self::Err { cid, .. } => cid,
        }
    }
    pub fn completion(completion: ApplyCompletion) -> Self {
        Self::Ack {
            cid: completion.cid,
            tick_count: Some(completion.tick_count),
            outputs: None,
            selection: None,
            receipt: Some(completion.receipt),
            outcomes: Some(completion.outcomes),
        }
    }
}

pub(crate) fn field<'a>(value: &'a rmpv::Value, name: &str) -> Option<&'a rmpv::Value> {
    value
        .as_map()?
        .iter()
        .find(|(k, _)| k.as_str() == Some(name))
        .map(|(_, v)| v)
}
pub(crate) fn kind(value: &rmpv::Value) -> Result<&str> {
    field(value, "type")
        .and_then(rmpv::Value::as_str)
        .ok_or_else(|| Error::new(Code::RpcIllegal, "message type is illegal"))
}
pub(crate) fn decode_value<T: serde::de::DeserializeOwned>(value: rmpv::Value) -> Result<T> {
    rmpv::ext::from_value(value).map_err(|e| Error::new(Code::RpcIllegal, e.to_string()))
}
