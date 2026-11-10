//! Untimed inspection of replication requests, including their commit frontier.
use monsterft::{
    error::{Code, Error, Result},
    protocol::Entry,
    rpc::{Rpc, RpcEnvelope},
    wire,
};
use serde::Deserialize;

#[derive(Debug, Default, PartialEq)]
pub struct AppendAudit {
    pub has_data: bool,
    pub uncommitted_pair: bool,
}
#[derive(Deserialize)]
struct Envelope {
    message: RpcEnvelope,
}

pub fn inspect(frame: &[u8]) -> Result<AppendAudit> {
    let envelope: Envelope = wire::decode_frame(frame)?;
    let Rpc::Append {
        seq_p,
        commit_seq,
        data: Some(entries),
        ..
    } = envelope.message.rpc
    else {
        return Ok(AppendAudit::default());
    };
    let kinds: Vec<_> = entries
        .iter()
        .map(|entry| {
            // Raft no-ops have an empty payload, rather than a Monster entry type.
            if entry.entry.is_empty() {
                Ok(None)
            } else {
                wire::decode_payload::<Entry>(&entry.entry).map(Some)
            }
        })
        .collect::<Result<_>>()?;
    let mut paired = false;
    for (index, pair) in kinds.windows(2).enumerate() {
        if matches!(
            (&pair[0], &pair[1]),
            (Some(Entry::Sync { .. }), Some(Entry::Cmd { .. }))
        ) {
            let sync_seq = i64::try_from(index)
                .ok()
                .and_then(|index| seq_p.checked_add(index))
                .and_then(|seq| seq.checked_add(1))
                .ok_or_else(|| Error::new(Code::RpcIllegal, "audit sequence overflow"))?;
            // A slow follower may receive a committed SYNC beside a newer CMD.
            // Only a pair sent before that SYNC commits proves the optimization.
            paired |= commit_seq < sync_seq;
        }
    }
    Ok(AppendAudit {
        has_data: !entries.is_empty(),
        uncommitted_pair: paired,
    })
}
