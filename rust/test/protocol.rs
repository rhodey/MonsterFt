//! Ports of JS raft-safety, replication-liveness, paired-recovery, retention,
//! and certify regressions. See README.md for scope and remaining priorities.
#[path = "support/protocol.rs"]
mod support;

use bytes::Bytes;
use monsterft::{
    certify::certify,
    engine::{Options, Retention},
    error::{Code, Error},
    local::Input,
    protocol::{Event, State},
    rpc::{ClientReply, Rpc, RpcEnvelope},
    storage::{self, ProcRole},
};
use std::collections::VecDeque;
use support::*;

#[test]
fn votes_are_durable_before_publication_and_cannot_change_after_restart() {
    let mut f = Protocol::new(Options::default());
    let vote = Rpc::VoteRequest {
        term: 1,
        term_p: -1,
        seq_p: -1,
    };
    for (from, granted) in [("b", true), ("c", false), ("b", true)] {
        let out = f.rpc(from, vote.clone());
        assert!(
            matches!(rpcs(&out, from)[0], Rpc::Vote { vote_granted, .. } if vote_granted == granted)
        );
    }
    assert_eq!(*f.vote_publications.borrow(), 2);
    f.reopen();
    let out = f.rpc("c", vote);
    assert!(matches!(
        rpcs(&out, "c")[0],
        Rpc::Vote {
            vote_granted: false,
            ..
        }
    ));
    f.elect();
    assert_eq!(
        *f.vote_publications.borrow(),
        4,
        "self vote observed durably before both election requests"
    );
    assert!(
        f.snapshot()
            .iter()
            .all(|(s, _)| s.election.voted_for.as_deref() == Some("a"))
    );
}

#[test]
fn log_freshness_and_unique_votes_govern_configured_election_quorum() {
    for (term_p, seq_p, granted) in [(1, 9, false), (2, 0, false), (2, 1, true), (3, 0, true)] {
        let mut f = Protocol::new(Options::default());
        f.rpc("b", append(2, -1, -1, -1, Some(vec![noop(2); 2])));
        let out = f.rpc(
            "c",
            Rpc::VoteRequest {
                term: 4,
                term_p,
                seq_p,
            },
        );
        assert!(
            matches!(rpcs(&out, "c")[0], Rpc::Vote { vote_granted, .. } if vote_granted == granted)
        );
    }
    let mut f = Protocol::with_members(
        Options {
            quorum: Some(4),
            ..Default::default()
        },
        "a",
        &["a", "b", "c", "d", "e"],
    );
    f.tick(10_000);
    for from in ["b", "b", "c"] {
        f.rpc(
            from,
            Rpc::Vote {
                term: 1,
                vote_granted: true,
            },
        );
        assert_eq!(f.status.as_ref().unwrap().state, State::Candidate);
        assert!(f.status.as_ref().unwrap().followers.is_empty());
    }
    f.rpc(
        "d",
        Rpc::Vote {
            term: 1,
            vote_granted: true,
        },
    );
    assert_eq!(f.status.as_ref().unwrap().state, State::Leader);
    assert_eq!(f.status.as_ref().unwrap().followers, ["b", "c", "d"]);
}

#[test]
fn election_sql_failures_publish_nothing_and_preserve_durable_state() {
    for action in ["self", "grant", "higher append"] {
        let mut f = Protocol::new(Options::default());
        let before = f.snapshot();
        f.sql("CREATE TRIGGER reject_vote BEFORE UPDATE ON raft_election BEGIN SELECT RAISE(ABORT,'vote write failed'); END;");
        let input = match action {
            "self" => {
                f.clock = 10_000;
                Input::Tick
            }
            "grant" => f.rpc_input(
                "b",
                Rpc::VoteRequest {
                    term: 5,
                    term_p: -1,
                    seq_p: -1,
                },
            ),
            _ => f.rpc_input("b", append(5, -1, -1, -1, Some(vec![noop(5)]))),
        };
        assert!(f.try_input(input).is_none(), "{action}");
        for proc in &f.procs {
            let error = proc.fatal_error().unwrap();
            assert_eq!(error.code, Some(Code::SqliteError));
            assert!(error.message.contains("vote write failed"));
        }
        assert_eq!(f.snapshot(), before);
        f.sql("DROP TRIGGER reject_vote");
        f.reopen();
        assert_eq!(f.snapshot(), before);
    }
}

#[test]
fn old_term_majority_cannot_commit_until_current_term_entry_is_replicated() {
    let mut f = Protocol::new(Options {
        rpc_max: 1,
        ..Default::default()
    });
    f.rpc("b", append(1, -1, -1, -1, Some(vec![noop(1); 3])));
    let elected = f.elect();
    let Rpc::Append { cid, term, .. } = page(&elected, "b", true) else {
        unreachable!()
    };
    let mut out = f.rpc(
        "b",
        Rpc::Err {
            cid,
            term,
            error: Error::new(Code::ReplBacktrack, "empty"),
            log_seq: Some(-1),
        },
    );
    f.input(Input::Append {
        client_id: "queued".into(),
        items: vec![Bytes::from_static(b"later")],
    });
    for _ in 0..3 {
        out = f.rpc("b", ack(&page(&out, "b", true)));
        assert_eq!(f.status.as_ref().unwrap().commit_seq, -1);
        assert_eq!(
            f.status.as_ref().unwrap().seq,
            3,
            "queued CMD waits for election commitment"
        );
        assert!(executes(&out).is_empty());
    }
    out = f.rpc("b", ack(&page(&out, "b", true)));
    assert_eq!(f.status.as_ref().unwrap().commit_seq, 3);
    let execute = executes(&out).remove(0);
    assert_eq!(execute.seq, 0);
    assert!(execute.items.is_none());
    let out = f.complete(execute);
    assert_eq!(f.status.as_ref().unwrap().seq, 4);
    page(&out, "b", true);
}

#[test]
fn duplicate_replication_preserves_suffix_and_commits_only_verified_history() {
    let mut f = Protocol::new(Options::default());
    f.setup();
    let original = vec![noop(1), noop(2), noop(2)];
    f.rpc("b", append(3, 0, 1, -1, Some(original.clone())));
    let before = f.snapshot();
    let out = f.rpc("b", append(3, 0, 1, -1, Some(original.clone())));
    assert!(matches!(rpcs(&out, "b")[0], Rpc::Ack { .. }));
    assert_eq!(f.snapshot(), before);
    f.rpc("b", append(3, 0, 1, 3, Some(original[..1].to_vec())));
    assert_eq!(f.snapshot()[0].1, before[0].1);
    assert_eq!(f.status.as_ref().unwrap().commit_seq, 1);
    f.rpc("b", append(3, 1, 1, 3, None));
    assert_eq!(f.status.as_ref().unwrap().commit_seq, 1);
    f.rpc("b", append(3, 1, 1, 3, Some(vec![noop(3); 2])));
    assert!(f.snapshot().iter().all(|(s, _)| s.meta.applied_seq == 3));
    f.rpc("b", append(3, 3, 3, 100, None));
    assert_eq!(f.status.as_ref().unwrap().commit_seq, 3);
}

#[test]
fn failed_replacement_rolls_back_trim_and_new_election_term_together() {
    let mut f = Protocol::new(Options::default());
    f.setup();
    f.rpc("b", append(1, 0, 1, -1, Some(vec![noop(1); 2])));
    let before = f.snapshot();
    f.sql("CREATE TRIGGER reject_replace BEFORE INSERT ON raft_log WHEN NEW.term=2 BEGIN SELECT RAISE(ABORT,'replacement failed'); END;");
    let input = f.rpc_input("b", append(2, 0, 1, -1, Some(vec![noop(2)])));
    assert!(f.try_input(input).is_none());
    for proc in &f.procs {
        assert_eq!(proc.fatal_error().unwrap().code, Some(Code::SqliteError));
        assert!(
            proc.fatal_error()
                .unwrap()
                .message
                .contains("replacement failed")
        );
    }
    assert_eq!(f.snapshot(), before);
    f.sql("DROP TRIGGER reject_replace");
    f.reopen();
    assert_eq!(f.snapshot(), before);
}

fn liveness_options() -> Options {
    Options {
        election_timeout: 150,
        append_timeout: 1000,
        ..Default::default()
    }
}

#[test]
fn heartbeat_restores_live_quorum_and_commits_an_already_acknowledged_command() {
    let mut f = Protocol::with_members(liveness_options(), "a", &["a", "b", "c", "d", "e"]);
    let election = f.elect();
    f.rpc("b", ack(&page(&election, "b", true)));
    let ready = f.rpc("c", ack(&page(&election, "c", true)));
    f.complete(executes(&ready).remove(0));
    for to in ["b", "c"] {
        for heartbeat in rpcs(&ready, to)
            .iter()
            .filter(|r| matches!(r, Rpc::Append { data: None, .. }))
        {
            f.rpc(to, ack(heartbeat));
        }
    }
    f.clock = 10_001;
    let command = f.input(Input::Append {
        client_id: "user-command".into(),
        items: vec![Bytes::from_static(b"insert")],
    });
    f.clock = 10_002;
    f.rpc("b", ack(&page(&command, "b", true)));
    let heartbeat = page(&f.tick(10_045), "b", false);
    f.tick(10_090);
    f.tick(10_135);
    f.clock = 10_155;
    f.rpc("c", ack(&page(&command, "c", true)));
    assert_eq!(
        f.status.as_ref().unwrap().commit_seq,
        0,
        "B expired before C acknowledged"
    );
    f.clock = 10_160;
    let out = f.rpc("b", ack(&heartbeat));
    let status = f.status.as_ref().unwrap();
    assert_eq!(status.followers, ["b", "c"]);
    assert_eq!(status.state, State::Leader);
    assert_eq!(status.commit_seq, 1);
    assert_eq!(
        executes(&out)[0].seq,
        1,
        "same-input commitment starts Apply"
    );
}

#[test]
fn missing_data_and_heartbeat_replies_do_not_block_liveness_or_invent_commitment() {
    let mut f = Protocol::new(liveness_options());
    let data = page(&f.elect(), "b", true);
    let first = page(&f.tick(10_045), "b", false);
    let second = page(&f.tick(10_090), "b", false);
    assert_ne!(first.cid(), second.cid());
    f.clock = 10_091;
    f.rpc("b", ack(&second));
    for time in [10_135, 10_180, 10_225] {
        let heartbeat = page(&f.tick(time), "b", false);
        f.clock = time + 1;
        f.rpc("b", ack(&heartbeat));
        assert_eq!(f.status.as_ref().unwrap().state, State::Leader);
        assert_eq!(
            f.status.as_ref().unwrap().commit_seq,
            -1,
            "heartbeat is not a data acknowledgment"
        );
    }
    let out = f.rpc("b", ack(&data));
    assert_eq!(
        f.status.as_ref().unwrap().commit_seq,
        0,
        "original data request is still pending"
    );
    assert_eq!(executes(&out)[0].seq, 0);
}

#[test]
fn heartbeat_does_not_change_data_retry_deadline_and_new_term_cancels_old_work() {
    let mut f = Protocol::new(liveness_options());
    let data = page(&f.elect(), "b", true);
    f.clock = 10_040;
    f.rpc(
        "b",
        Rpc::Err {
            cid: data.cid().unwrap().into(),
            term: 1,
            error: Error::new(Code::RpcIllegal, "temporary rejection"),
            log_seq: None,
        },
    );
    let out = f.tick(10_045);
    assert!(
        !rpcs(&out, "b")
            .iter()
            .any(|r| matches!(r, Rpc::Append { data: Some(_), .. }))
    );
    let heartbeat = page(&out, "b", false);
    f.clock = 10_046;
    f.rpc("b", ack(&heartbeat));
    let retried = page(&f.tick(10_055), "b", true);
    assert_ne!(retried.cid(), data.cid());
    if let (Rpc::Append { data: a, .. }, Rpc::Append { data: b, .. }) = (&retried, &data) {
        assert_eq!(a, b);
    }
    let heartbeat = page(&f.tick(10_090), "b", false);
    let mut higher = ack(&heartbeat);
    if let Rpc::Ack { term, .. } = &mut higher {
        *term = 2;
    }
    f.clock = 10_091;
    f.rpc("b", higher);
    assert_eq!(f.status.as_ref().unwrap().state, State::Follower);
    assert!(f.snapshot().iter().all(|(s, _)| s.election.term == 2));
    f.clock = 10_092;
    let out = f.rpc("b", ack(&retried));
    assert_eq!(f.status.as_ref().unwrap().commit_seq, -1);
    assert!(rpcs(&out, "b").is_empty());
    let out = f.tick(10_140);
    assert!(
        rpcs(&out, "b").is_empty(),
        "old periodic heartbeat was cancelled"
    );
}

#[test]
fn backtracking_replaces_empty_equal_and_longer_divergent_logs_with_delayed_delivery() {
    for delay in [0, 4] {
        for length in [0, 20, 30] {
            let mut a = Protocol::new(liveness_options());
            let mut b = Protocol::with_members(liveness_options(), "b", &["a", "b", "c"]);
            a.rpc("c", append(3, -1, -1, -1, Some(vec![noop(2); 20])));
            if length > 0 {
                b.rpc("c", append(3, -1, -1, -1, Some(vec![noop(1); length])));
            }
            let mut queue = VecDeque::new();
            let collect = |from: &str, out: Outputs, now: u64| {
                out.into_iter()
                    .filter(|o| o.to == "a" || o.to == "b")
                    .map(|o| (from.to_owned(), o, now + delay))
                    .collect::<Vec<_>>()
            };
            queue.extend(collect("a", a.elect(), a.clock));
            let mut backtracks = 0;
            let mut deliveries = 0;
            let mut now = a.clock;
            while let Some((from, output, due)) = queue.pop_front() {
                deliveries += 1;
                assert!(deliveries < 1000, "replication must quiesce");
                now = due;
                let envelope: RpcEnvelope = rmpv::ext::from_value(output.message).unwrap();
                let rejection = matches!(&envelope.rpc, Rpc::Err { error, .. } if error.code == Some(Code::ReplBacktrack));
                let node = if output.to == "a" { &mut a } else { &mut b };
                node.clock = now;
                let out = node.rpc(&from, envelope.rpc);
                if rejection {
                    backtracks += 1;
                    assert_eq!(
                        node.status.as_ref().unwrap().commit_seq,
                        -1,
                        "a rejection never proves a matching entry"
                    );
                }
                queue.extend(collect(&output.to, out, now));
            }
            assert_eq!(backtracks, if length == 0 { 1 } else { 20 });
            assert_eq!(a.status.as_ref().unwrap().state, State::Leader);
            assert_eq!(a.status.as_ref().unwrap().term, 4);
            assert_eq!(a.status.as_ref().unwrap().commit_seq, 20);
            assert_eq!(a.snapshot()[0].1, b.snapshot()[0].1);
            if delay > 0 && length > 0 {
                assert!(now - 10_000 > 150);
            }
        }
    }
}

#[test]
fn drain_between_paired_entries_leaves_certifiable_sync_and_resumable_command() {
    let mut f = Protocol::new(Options::default());
    f.setup();
    f.pending();
    f.input(Input::Drain {
        client_id: "drain".into(),
    });
    let out = f.rpc("b", append(1, 1, 1, 3, Some(vec![sync(1), command()])));
    assert!(executes(&out).is_empty());
    let replies: Vec<ClientReply> = out
        .iter()
        .filter(|o| o.to == "gate")
        .filter_map(|o| rmpv::ext::from_value(o.message.clone()).ok())
        .collect();
    assert!(
        matches!(&replies[..], [ClientReply::Ack { client_id, result: None }] if client_id == "drain")
    );
    assert!(
        f.snapshot()
            .iter()
            .all(|(s, _)| s.meta.applied_seq == 2 && s.meta.pending_cmd_seq.is_none())
    );
    assert_eq!(f.receipt().seq, 1);
    f.stop();
    assert_eq!(certify(&f.base, false).unwrap(), 2);
    f.reopen();
    let out = f.rpc("b", append(1, 3, 1, 3, None));
    let execute = executes(&out).remove(0);
    assert_eq!(execute.seq, 3);
    assert_eq!(
        execute.items.as_ref().unwrap(),
        &[Bytes::from_static(b"next")]
    );
    f.complete(execute);
    assert_eq!(f.receipt().seq, 3);
}

#[test]
fn retention_and_sync_checkpoint_failures_rollback_input_without_repeating_apply() {
    for fault in ["delete", "checkpoint"] {
        let mut f = Protocol::new(Options {
            keep: Some(Retention {
                target: 3,
                trigger: 6,
            }),
            ..Default::default()
        });
        f.setup();
        f.pending();
        f.rpc("b", append(1, 1, 1, 5, Some(vec![noop(1); 4])));
        let before = f.snapshot();
        let receipt = f.receipt();
        f.sql(if fault == "delete" {
            "CREATE TRIGGER retention_fault BEFORE DELETE ON raft_log BEGIN SELECT RAISE(ABORT,'retention delete failed'); END;"
        } else {
            "CREATE TRIGGER retention_fault BEFORE UPDATE ON monsterft_meta BEGIN SELECT RAISE(ABORT,'SYNC checkpoint failed'); END;"
        });
        let input = f.rpc_input("b", append(1, 5, 1, 6, Some(vec![sync(1)])));
        assert!(f.try_input(input).is_none(), "{fault}");
        assert!(
            f.procs
                .iter()
                .all(|p| p.fatal_error().unwrap().code == Some(Code::SqliteError))
        );
        assert_eq!(
            f.snapshot(),
            before,
            "SYNC insert, pruning and checkpoint rollback together"
        );
        assert_eq!(f.receipt(), receipt);
        f.sql("DROP TRIGGER retention_fault");
        f.reopen();
        let out = f.rpc("b", append(1, 5, 1, 6, Some(vec![sync(1)])));
        assert!(executes(&out).is_empty());
        for (state, rows) in f.snapshot() {
            assert_eq!(state.meta.applied_seq, 6);
            assert_eq!(state.meta.pending_cmd_seq, None);
            assert_eq!(rows[0].seq, 4);
        }
    }
}

#[test]
fn repeated_paired_entries_prune_history_and_restart_without_reexecuting_receipt() {
    let mut f = Protocol::new(Options {
        keep: Some(Retention {
            target: 3,
            trigger: 6,
        }),
        ..Default::default()
    });
    f.setup();
    f.pending();
    for _ in 0..12 {
        let head = f.status.as_ref().unwrap().seq;
        let out = f.rpc(
            "b",
            append(1, head, 1, head + 2, Some(vec![sync(head), command()])),
        );
        let execute = executes(&out).remove(0);
        assert_eq!(execute.seq, head + 2);
        assert!(
            f.snapshot()
                .iter()
                .all(|(s, _)| s.meta.applied_seq == head + 1)
        );
        f.complete(execute);
        assert!(f.snapshot().iter().all(|(_, rows)| rows.len() <= 6));
    }
    assert!(f.snapshot()[0].1[0].seq > 0);
    let receipt = f.receipt();
    f.reopen();
    assert_eq!(f.receipt(), receipt);
    assert!(
        f.snapshot()
            .iter()
            .all(|(s, _)| s.meta.pending_cmd_seq == Some(receipt.seq))
    );
    let out = f.rpc(
        "b",
        append(
            1,
            receipt.seq,
            1,
            receipt.seq + 1,
            Some(vec![sync(receipt.seq)]),
        ),
    );
    assert!(executes(&out).is_empty());
    let events: Vec<Event> = out
        .iter()
        .filter(|o| o.to == "gate")
        .filter_map(|o| rmpv::ext::from_value(o.message.clone()).ok())
        .collect();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Event::Sync { cmd_seq, .. } if *cmd_seq == receipt.seq))
    );
    assert!(
        f.snapshot()
            .iter()
            .all(|(s, _)| s.meta.pending_cmd_seq.is_none())
    );
}

#[test]
fn certification_checks_every_matching_copy_before_adopting_or_clearing_fences() {
    for (corrupt, outlier) in [
        (ProcRole::B, None),
        (ProcRole::C, None),
        (ProcRole::C, Some(ProcRole::A)),
    ] {
        let mut f = Protocol::new(Options::default());
        f.setup();
        f.pending();
        f.rpc("b", append(1, 1, 1, 2, Some(vec![sync(1)])));
        f.rpc("b", append(1, 2, 1, 2, Some(vec![noop(1)])));
        f.sql(&format!(
            "UPDATE monsterft_meta SET repair_state={} WHERE id=1",
            storage::REPAIR_NO_QUORUM
        ));
        f.db(corrupt)
            .execute(
                "UPDATE raft_log SET entry=? WHERE seq=2",
                [b"damaged".as_slice()],
            )
            .unwrap();
        if let Some(role) = outlier {
            f.db(role)
                .execute_batch("INSERT INTO raft_log(seq,term,entry) VALUES(4,2,x'01')")
                .unwrap();
        }
        let receipt = f.receipt();
        f.stop();
        let before = f.snapshot();
        let donor = if outlier == Some(ProcRole::A) { 1 } else { 0 };
        assert_eq!(
            storage::state_digest(&before[donor].0).unwrap(),
            storage::state_digest(&before[corrupt.index()].0).unwrap(),
            "corrupt checkpoint is below the matching captured head"
        );
        let error = certify(&f.base, false).unwrap_err();
        assert_eq!(error.code, Some(Code::MonsterCorrupt));
        assert!(error.message.contains("applied entry hash"));
        assert_eq!(
            f.snapshot(),
            before,
            "neither fence clearance nor adoption may commit"
        );
        let db2 =
            rusqlite::Connection::open(storage::database_paths(&f.base).unwrap().apply).unwrap();
        assert_eq!(storage::read_apply(&db2).unwrap(), receipt);
    }
}
