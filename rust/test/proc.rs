use bytes::Bytes;
use monsterft::{
    Value,
    error::Code,
    local::{Input, LocalReply, OrderedInput, StartupRequest},
    proc::{Options, Proc},
    storage::{self, ProcRole},
    wire,
};
use std::{cell::RefCell, rc::Rc, time::Duration};

type Replies = Rc<RefCell<Vec<LocalReply>>>;

struct LocalProcs {
    _temp: tempfile::TempDir,
    procs: Vec<Proc>,
    replies: Vec<Replies>,
    _apply: rusqlite::Connection,
}

impl LocalProcs {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let base = temp.path().join("db");
        let mut procs = Vec::new();
        let mut replies = Vec::new();
        for role in ProcRole::ALL {
            let captured = Rc::new(RefCell::new(Vec::new()));
            let sent = captured.clone();
            let mut proc = Proc::new(
                role,
                "a",
                vec!["a".into(), "b".into(), "c".into()],
                Box::new(move |to, frame| {
                    assert_eq!(to, "gate");
                    sent.borrow_mut().push(wire::decode_frame(&frame)?);
                    Ok(())
                }),
                &base,
                Options::default(),
            )
            .unwrap();
            proc.on_receive(
                "gate",
                wire::encode_frame(&StartupRequest::Hello {
                    cid: "hello".into(),
                    seed: [1; 16],
                })
                .unwrap(),
            );
            proc.on_receive(
                "gate",
                wire::encode_frame(&StartupRequest::Open { cid: "open".into() }).unwrap(),
            );
            assert!(!proc.is_stopped());
            procs.push(proc);
            replies.push(captured);
        }
        let apply = storage::open_apply_database(&base, &[1; 16]).unwrap();
        for request in [
            StartupRequest::Capture {
                cid: "capture".into(),
            },
            StartupRequest::Adopt {
                cid: "adopt".into(),
            },
            StartupRequest::Reconcile {
                cid: "reconcile".into(),
            },
        ] {
            for proc in &mut procs {
                proc.on_receive("gate", wire::encode_frame(&request).unwrap());
                assert!(!proc.is_stopped());
            }
        }
        let input = OrderedInput {
            cid: "start".into(),
            tick_count: 1,
            timems: 1,
            input: Input::Start,
        };
        for proc in &mut procs {
            proc.on_receive("gate", wire::encode_frame(&input).unwrap());
            assert!(proc.is_open(), "{:?}", proc.fatal_error());
        }
        Self {
            _temp: temp,
            procs,
            replies,
            _apply: apply,
        }
    }
}

#[test]
fn three_independent_startups_commit_matching_halves() {
    let locals = LocalProcs::new();
    let outputs = locals
        .replies
        .iter()
        .map(|replies| match replies.borrow().last().unwrap() {
            LocalReply::Ack {
                tick_count: Some(1),
                outputs: Some(outputs),
                ..
            } => outputs.clone(),
            _ => panic!("completion missing"),
        })
        .collect::<Vec<_>>();
    assert_eq!(outputs[0].len(), 1);
    assert_eq!(outputs[1].len(), 1);
    assert_eq!(outputs[2].len(), 1);
    let hash = wire::checksum(&outputs[0][0]);
    assert_eq!(outputs[1][0].as_ref(), &hash[..8]);
    assert_eq!(outputs[2][0].as_ref(), &hash[8..]);
    for proc in &locals.procs {
        assert!(proc.store.db().unwrap().is_autocommit());
    }
}

#[test]
fn remote_bad_decode_warns_after_committed_completion() {
    let mut locals = LocalProcs::new();
    let reply_count = locals.replies[0].clone();
    let count = reply_count.borrow().len();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let observed = seen.clone();
    locals.procs[0].diagnostics.on_warn(move |error| {
        assert_eq!(reply_count.borrow().len(), count + 1);
        observed.borrow_mut().push(error.code);
    });
    let corrupt = wire::frame_payload(&[0xc1], &wire::checksum(&[0xc1])).unwrap();
    let input = OrderedInput {
        cid: "bad_remote".into(),
        tick_count: 2,
        timems: 2,
        input: Input::ClusterMessage {
            from: "b".into(),
            frame: corrupt,
        },
    };
    locals.procs[0].on_receive("gate", wire::encode_frame(&input).unwrap());
    assert!(locals.procs[0].is_open());
    assert_eq!(*seen.borrow(), vec![Some(Code::FrameCorrupt)]);
    assert_eq!(locals.procs[0].tick_count, 2);
}

#[test]
fn local_checksum_failure_stops_before_error_then_close() {
    let mut locals = LocalProcs::new();
    let proc = &mut locals.procs[0];
    let stop = proc.stop_handle();
    let events = Rc::new(RefCell::new(Vec::new()));
    let errors = events.clone();
    let closes = events.clone();
    proc.diagnostics.on_error(move |error| {
        assert!(stop.is_stopped());
        assert_eq!(error.code, Some(Code::FrameCorrupt));
        errors.borrow_mut().push("error");
    });
    proc.diagnostics
        .on_close(move || closes.borrow_mut().push("close"));
    let before = locals.replies[0].borrow().len();
    proc.on_receive("gate", Bytes::from_static(b"invalid"));
    assert!(proc.is_stopped());
    assert_eq!(*events.borrow(), vec!["error", "close"]);
    assert_eq!(locals.replies[0].borrow().len(), before);
}

#[test]
fn nested_checksum_failure_is_local_after_gates_acceptance() {
    let mut locals = LocalProcs::new();
    let before = locals.replies[0].borrow().len();
    let input = OrderedInput {
        cid: "nested".into(),
        tick_count: 2,
        timems: 2,
        input: Input::ClusterMessage {
            from: "b".into(),
            frame: Bytes::from_static(b"bad nested frame"),
        },
    };
    locals.procs[0].on_receive("gate", wire::encode_frame(&input).unwrap());
    assert_eq!(
        locals.procs[0].fatal_error().unwrap().code,
        Some(Code::FrameCorrupt)
    );
    assert_eq!(locals.replies[0].borrow().len(), before);
}

#[test]
fn stale_generation_overrides_pending_remote_warning() {
    let mut locals = LocalProcs::new();
    let seen = Rc::new(RefCell::new(0));
    let count = seen.clone();
    locals.procs[0]
        .diagnostics
        .on_warn(move |_| *count.borrow_mut() += 1);
    locals.procs[0]
        .store
        .db()
        .unwrap()
        .execute(
            "UPDATE monsterft_meta SET generation=? WHERE id=1",
            [vec![2u8; 16]],
        )
        .unwrap();
    let malformed = wire::frame_payload(&[0xc1], &wire::checksum(&[0xc1])).unwrap();
    let input = OrderedInput {
        cid: "stale".into(),
        tick_count: 2,
        timems: 2,
        input: Input::ClusterMessage {
            from: "b".into(),
            frame: malformed,
        },
    };
    locals.procs[0].on_receive("gate", wire::encode_frame(&input).unwrap());
    assert_eq!(
        locals.procs[0].fatal_error().unwrap().code,
        Some(Code::GenerationDiff)
    );
    assert_eq!(*seen.borrow(), 0);
}

#[test]
fn duplicated_tick_and_late_hello_are_terminal_state_errors() {
    let mut locals = LocalProcs::new();
    let tick = OrderedInput {
        cid: "duplicate".into(),
        tick_count: 1,
        timems: 1,
        input: Input::Tick,
    };
    locals.procs[0].on_receive("gate", wire::encode_frame(&tick).unwrap());
    assert_eq!(
        locals.procs[0].fatal_error().unwrap().code,
        Some(Code::MonsterIllegal)
    );
    locals.procs[1].on_receive(
        "gate",
        wire::encode_frame(&StartupRequest::Hello {
            cid: "late".into(),
            seed: [1; 16],
        })
        .unwrap(),
    );
    assert_eq!(
        locals.procs[1].fatal_error().unwrap().code,
        Some(Code::MonsterIllegal)
    );
}

#[test]
fn watchdog_is_native_and_runs_without_protocol_clock_advance() {
    let mut locals = LocalProcs::new();
    let proc = &mut locals.procs[0];
    let deadline = proc.watchdog_deadline().unwrap();
    proc.check_watchdog(deadline - Duration::from_millis(1));
    assert!(proc.is_open());
    proc.check_watchdog(deadline);
    assert!(proc.is_stopped());
    assert_eq!(proc.fatal_error().unwrap().code, Some(Code::LocalTimeout));
    assert_eq!(proc.tick_count, 1);
}

#[test]
fn malformed_remote_envelope_is_discarded_without_an_output() {
    let mut locals = LocalProcs::new();
    let value = Value::Map(vec![
        ("to".into(), "other".into()),
        (
            "message".into(),
            Value::Map(vec![
                ("type".into(), "vote".into()),
                ("from".into(), "b".into()),
            ]),
        ),
    ]);
    let input = OrderedInput {
        cid: "bad_envelope".into(),
        tick_count: 2,
        timems: 2,
        input: Input::ClusterMessage {
            from: "b".into(),
            frame: wire::encode_frame(&value).unwrap(),
        },
    };
    locals.procs[0].on_receive("gate", wire::encode_frame(&input).unwrap());
    assert!(locals.procs[0].is_open());
    match locals.replies[0].borrow().last().unwrap() {
        LocalReply::Ack {
            outputs: Some(outputs),
            tick_count: Some(2),
            ..
        } => assert!(outputs.is_empty()),
        _ => panic!("missing completion"),
    }
}

fn deliver(locals: &mut LocalProcs, tick: i64, input: Input) {
    let ordered = OrderedInput {
        cid: format!("input-{tick}"),
        tick_count: tick,
        timems: tick as u64,
        input,
    };
    locals.procs[0].on_receive("gate", wire::encode_frame(&ordered).unwrap());
}

fn append_input(previous: i64, commit: i64, entries: Vec<monsterft::log::LogEntry>) -> Input {
    use monsterft::{
        protocol::Envelope,
        rpc::{Rpc, RpcEnvelope},
    };
    Input::ClusterMessage {
        from: "b".into(),
        frame: wire::encode_frame(&Envelope {
            to: "a".into(),
            message: RpcEnvelope {
                from: "b".into(),
                rpc: Rpc::Append {
                    cid: format!("append-{previous}"),
                    term: 1,
                    term_p: if previous < 0 { -1 } else { 1 },
                    seq_p: previous,
                    commit_seq: commit,
                    data: Some(entries),
                },
            },
        })
        .unwrap(),
    }
}

fn command_entry() -> monsterft::log::LogEntry {
    monsterft::log::LogEntry {
        term: 1,
        entry: wire::encode_payload(&monsterft::protocol::Entry::Cmd {
            match_index: vec![-1; 3],
            items: vec![Bytes::from_static(b"command")],
        })
        .unwrap(),
    }
}

fn latest_execute(locals: &LocalProcs) -> monsterft::protocol::Execute {
    use monsterft::{protocol::Envelope, rpc::ApplyRequest};
    let replies = locals.replies[0].borrow();
    let LocalReply::Ack {
        outputs: Some(outputs),
        ..
    } = replies.last().unwrap()
    else {
        panic!("expected completion, found {:?}", replies.last());
    };
    outputs
        .iter()
        .find_map(|payload| {
            let envelope: Envelope<Value> = wire::decode_payload(payload).unwrap();
            if envelope.to != "apply" {
                return None;
            }
            let ApplyRequest::Execute { execute } =
                rmpv::ext::from_value(envelope.message).unwrap();
            Some(execute)
        })
        .expect("input did not execute Apply")
}

fn finish_execute(locals: &mut LocalProcs, tick: i64, request: monsterft::protocol::Execute) {
    use monsterft::protocol::{ApplyCompletion, Outcome};
    let receipt = storage::Receipt {
        seq: request.seq,
        entry_hash: Some(request.entry_hash),
        local_digest: Some(if request.seq == 0 { [0; 16] } else { [1; 16] }),
    };
    locals._apply.execute_batch("BEGIN IMMEDIATE").unwrap();
    storage::write_receipt(&locals._apply, &receipt).unwrap();
    locals._apply.execute_batch("COMMIT").unwrap();
    deliver(
        locals,
        tick,
        Input::ApplyResult {
            reply: ApplyCompletion {
                cid: request.cid,
                tick_count: request.tick_count,
                receipt,
                outcomes: request.items.map_or_else(Vec::new, |items| {
                    items.iter().map(|_| Outcome::Fulfilled(1.into())).collect()
                }),
            },
        },
    );
}

fn append_repair_decision(locals: &mut LocalProcs, quorum: bool) {
    use monsterft::{log::LogEntry, protocol::Entry};
    let sync = Entry::Sync {
        cmd_seq: 1,
        quorum,
        agree: if quorum {
            vec!["b".into(), "c".into()]
        } else {
            Vec::new()
        },
        disagree: if quorum {
            vec!["a".into()]
        } else {
            vec!["a".into(), "b".into(), "c".into()]
        },
        digest: quorum.then_some([9; 16]),
    };
    deliver(
        locals,
        2,
        append_input(
            -1,
            2,
            vec![
                LogEntry {
                    term: 1,
                    entry: Vec::new(),
                },
                command_entry(),
                LogEntry {
                    term: 1,
                    entry: wire::encode_payload(&sync).unwrap(),
                },
            ],
        ),
    );
    assert!(
        locals.procs[0].is_open(),
        "{:?}",
        locals.procs[0].fatal_error()
    );
    let setup = latest_execute(locals);
    assert_eq!(setup.seq, 0);
    assert_eq!(setup.tick_count, 2);
    finish_execute(locals, 3, setup);
    assert!(
        locals.procs[0].is_open(),
        "{:?}",
        locals.procs[0].fatal_error()
    );
    let command = latest_execute(locals);
    assert_eq!(command.seq, 1);
    assert_eq!(command.tick_count, 3);
    finish_execute(locals, 4, command);
}

#[test]
fn dissenting_follower_commits_repair_before_error_and_publishes_no_final_output() {
    let mut locals = LocalProcs::new();
    let replies = locals.replies[0].clone();
    let path = locals.procs[0].store.log.path.clone();
    let stop = locals.procs[0].stop_handle();
    let observed = Rc::new(RefCell::new(Vec::new()));
    let errors = observed.clone();
    locals.procs[0].diagnostics.on_error(move |error| {
        assert!(stop.is_stopped());
        assert_eq!(error.code, Some(Code::RepairOutside));
        let db = rusqlite::Connection::open(&path).unwrap();
        let meta = storage::read_meta(&db).unwrap();
        assert_eq!(meta.repair_state, storage::REPAIR_OUTSIDE);
        assert_eq!(meta.applied_seq, 2);
        assert_eq!(meta.pending_cmd_seq, None);
        assert!(!replies.borrow().iter().any(|r| matches!(
            r,
            LocalReply::Ack {
                tick_count: Some(4),
                ..
            }
        )));
        errors.borrow_mut().push("error");
    });
    let closes = observed.clone();
    locals.procs[0]
        .diagnostics
        .on_close(move || closes.borrow_mut().push("close"));
    append_repair_decision(&mut locals, true);
    assert!(locals.procs[0].is_stopped());
    assert_eq!(*observed.borrow(), vec!["error", "close"]);
    assert_eq!(storage::read_apply(&locals._apply).unwrap().seq, 1);
}

#[test]
fn quorum_impossible_commits_live_fence_then_stops_before_the_next_command() {
    let mut locals = LocalProcs::new();
    let stop = locals.procs[0].stop_handle();
    let observed = Rc::new(RefCell::new(Vec::new()));
    let errors = observed.clone();
    locals.procs[0].diagnostics.on_error(move |error| {
        assert_eq!(error.code, Some(Code::RepairNoQuorum));
        errors.borrow_mut().push(stop.is_stopped());
    });
    append_repair_decision(&mut locals, false);
    assert!(locals.procs[0].is_open());
    assert_eq!(*observed.borrow(), vec![false]);
    let meta = storage::read_meta(locals.procs[0].store.db().unwrap()).unwrap();
    assert_eq!(meta.repair_state, storage::REPAIR_NO_QUORUM);
    assert_eq!(meta.applied_seq, 2);
    match locals.replies[0].borrow().last().unwrap() {
        LocalReply::Ack {
            tick_count: Some(4),
            outputs: Some(outputs),
            ..
        } => {
            let events = outputs
                .iter()
                .filter_map(|bytes| {
                    let envelope: monsterft::protocol::Envelope<Value> =
                        wire::decode_payload(bytes).unwrap();
                    if envelope.to == "gate" {
                        envelope
                            .message
                            .as_map()
                            .and_then(|map| {
                                map.iter().find(|(key, _)| key.as_str() == Some("type"))
                            })
                            .and_then(|(_, value)| value.as_str())
                            .map(str::to_owned)
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            assert_eq!(events, vec!["cmd", "sync"]);
        }
        other => panic!("expected committed repair completion, found {other:?}"),
    }
    let before = locals.replies[0].borrow().len();
    let path = locals.procs[0].store.log.path.clone();
    deliver(&mut locals, 5, append_input(2, 3, vec![command_entry()]));
    assert!(locals.procs[0].is_stopped());
    assert_eq!(*observed.borrow(), vec![false, true]);
    assert_eq!(locals.replies[0].borrow().len(), before);
    assert_eq!(storage::read_apply(&locals._apply).unwrap().seq, 1);
    let db = rusqlite::Connection::open(path).unwrap();
    let meta = storage::read_meta(&db).unwrap();
    assert_eq!(meta.repair_state, storage::REPAIR_NO_QUORUM);
    assert_eq!(meta.applied_seq, 2);
}

#[test]
fn warning_listener_failure_stops_before_later_listeners() {
    let mut locals = LocalProcs::new();
    let stop = locals.procs[0].stop_handle();
    let events = Rc::new(RefCell::new(Vec::new()));
    locals.procs[0]
        .diagnostics
        .on_warn(|_| panic!("warning listener failed"));
    let observed = events.clone();
    locals.procs[0].diagnostics.on_warn(move |_| {
        assert!(stop.is_stopped());
        observed.borrow_mut().push("later warning");
    });
    locals.procs[0]
        .diagnostics
        .on_error(|_| panic!("error listener failed"));
    let observed = events.clone();
    locals.procs[0].diagnostics.on_error(move |error| {
        assert!(error.message.contains("warning listener failed"));
        observed.borrow_mut().push("error");
    });
    let observed = events.clone();
    locals.procs[0]
        .diagnostics
        .on_close(move || observed.borrow_mut().push("close"));
    locals.procs[0].on_receive("not-gate", Bytes::new());
    assert!(locals.procs[0].is_stopped());
    assert_eq!(*events.borrow(), vec!["error", "close", "later warning"]);
}
