#[path = "../bench/audit.rs"]
mod audit;
use audit::{AppendAudit, inspect};
use bytes::Bytes;
use monsterft::{
    error::Result,
    log::LogEntry,
    protocol::Entry,
    rpc::{Output, OutputMessage, Rpc, RpcEnvelope},
    wire,
};

fn cmd() -> Vec<u8> {
    wire::encode_payload(&Entry::Cmd {
        match_index: vec![0, 0, 0],
        items: vec![Bytes::from_static(b"increment")],
    })
    .unwrap()
}
fn sync() -> Vec<u8> {
    wire::encode_payload(&Entry::Sync {
        cmd_seq: 1,
        quorum: true,
        agree: vec!["1".into(), "2".into()],
        disagree: vec![],
        digest: Some([1; 16]),
    })
    .unwrap()
}
fn append(seq_p: i64, commit_seq: i64, entries: Option<Vec<Vec<u8>>>) -> Bytes {
    wire::encode_frame(&Output {
        to: "2".into(),
        message: OutputMessage::Cluster(RpcEnvelope {
            from: "1".into(),
            rpc: Rpc::Append {
                cid: "audit".into(),
                term: 1,
                term_p: 1,
                seq_p,
                commit_seq,
                data: entries.map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| LogEntry { term: 1, entry })
                        .collect()
                }),
            },
        }),
    })
    .unwrap()
}
#[test]
fn uncommitted_sync_with_next_cmd_is_a_pair() -> Result<()> {
    assert_eq!(
        inspect(&append(5, 5, Some(vec![sync(), cmd()])))?,
        AppendAudit {
            has_data: true,
            uncommitted_pair: true
        }
    );
    Ok(())
}
#[test]
fn committed_sync_with_new_cmd_is_catchup_not_pairing() -> Result<()> {
    for committed in [6, 7, 15] {
        assert_eq!(
            inspect(&append(5, committed, Some(vec![sync(), cmd()])))?,
            AppendAudit {
                has_data: true,
                uncommitted_pair: false
            }
        );
    }
    Ok(())
}
#[test]
fn sync_position_uses_the_entry_offset_and_skips_noops() -> Result<()> {
    // Entries 40=noop,41=CMD,42=SYNC,43=CMD. Frontier41 leaves42 uncommitted.
    let entries = || Some(vec![vec![], cmd(), sync(), cmd()]);
    assert!(inspect(&append(39, 41, entries()))?.uncommitted_pair);
    assert!(!inspect(&append(39, 42, entries()))?.uncommitted_pair);
    Ok(())
}
#[test]
fn heartbeat_single_cmd_and_reversed_entries_are_not_pairs() -> Result<()> {
    assert_eq!(inspect(&append(5, 5, None))?, AppendAudit::default());
    for entries in [vec![cmd()], vec![sync()], vec![cmd(), sync()], vec![vec![]]] {
        assert_eq!(
            inspect(&append(5, 5, Some(entries)))?,
            AppendAudit {
                has_data: true,
                uncommitted_pair: false
            }
        );
    }
    Ok(())
}
