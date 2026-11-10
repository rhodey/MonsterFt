use bytes::Bytes;
use monsterft::{
    Value,
    engine::{Context, Engine, Options, Retention, Task},
    error::{Code, Result},
    executor::Executor,
    protocol::{ApplyCompletion, Entry, Event, Outcome, State},
    rpc::{ApplyRequest, ClientReply, Diagnostic, Output, OutputMessage, Rpc, RpcEnvelope},
    storage::{self, ProcRole, ProtocolStore, Receipt},
    wire,
};
use std::collections::{BTreeMap, VecDeque};
use tempfile::TempDir;

struct Node {
    engine: Engine,
    store: ProtocolStore,
    executor: Executor<Task>,
    tick: i64,
    clock: u64,
    events: Vec<Event>,
    results: Vec<ClientReply>,
    diagnostics: Vec<Diagnostic>,
    executed: Vec<i64>,
}
impl Node {
    fn new(base: &std::path::Path, id: &str, opts: Options) -> Result<Self> {
        let mut store = ProtocolStore::new(ProcRole::A, base, 128)?;
        store.open(&[1; 16])?;
        let engine = Engine::new(
            id.into(),
            vec!["1".into(), "2".into(), "3".into()],
            [1; 16],
            opts,
            &store,
        )?;
        Ok(Self {
            engine,
            store,
            executor: Executor::new(),
            tick: 0,
            clock: 0,
            events: vec![],
            results: vec![],
            diagnostics: vec![],
            executed: vec![],
        })
    }
    fn input(&mut self, task: Task) -> Result<Vec<Output>> {
        self.tick += 1;
        self.store.db()?.execute_batch("BEGIN IMMEDIATE")?;
        self.store.check_generation()?;
        let mut outputs = Vec::new();
        let mut context = (
            &mut self.engine,
            &mut self.store,
            &mut outputs,
            &mut self.diagnostics,
            self.tick,
        );
        let result = self.executor.run(
            self.clock,
            task,
            &mut context,
            |(engine, store, outputs, diagnostics, tick), executor, task| {
                engine.dispatch(
                    task,
                    &mut Context {
                        store,
                        executor,
                        outputs,
                        diagnostics,
                        tick_count: *tick,
                    },
                )
            },
            |(engine, store, outputs, diagnostics, tick), executor| {
                engine.change(&mut Context {
                    store,
                    executor,
                    outputs,
                    diagnostics,
                    tick_count: *tick,
                })
            },
        );
        if let Err(error) = result {
            self.store.db()?.execute_batch("ROLLBACK")?;
            return Err(error);
        }
        self.store.db()?.execute_batch("COMMIT")?;
        if self.engine.fatal_error().is_some() {
            return Ok(Vec::new());
        }
        Ok(outputs)
    }
}
struct Cluster {
    _temp: TempDir,
    nodes: BTreeMap<String, Node>,
    queue: VecDeque<(String, Output)>,
    history: Vec<(String, Output)>,
    digest_mode: u8,
}
impl Cluster {
    fn new(opts: Options) -> Result<Self> {
        let temp = tempfile::tempdir()?;
        let mut nodes = BTreeMap::new();
        for id in ["1", "2", "3"] {
            nodes.insert(
                id.into(),
                Node::new(&temp.path().join(id), id, opts.clone())?,
            );
        }
        Ok(Self {
            _temp: temp,
            nodes,
            queue: VecDeque::new(),
            history: vec![],
            digest_mode: 0,
        })
    }
    fn input(&mut self, id: &str, task: Task) -> Result<()> {
        let outputs = self.nodes.get_mut(id).unwrap().input(task)?;
        self.queue
            .extend(outputs.into_iter().map(|o| (id.into(), o)));
        Ok(())
    }
    fn start(&mut self) -> Result<()> {
        for id in ["1", "2", "3"] {
            self.input(id, Task::Start)?;
        }
        self.input("1", Task::Election)?;
        self.pump()?;
        assert_eq!(self.nodes["1"].engine.state, State::Leader);
        assert_eq!(self.nodes["1"].engine.applied_seq, 0);
        Ok(())
    }
    fn submit(&mut self, id: &str, client: &str, values: &[&[u8]]) -> Result<()> {
        self.input(
            id,
            Task::Submit {
                client_id: client.into(),
                items: values.iter().map(|x| Bytes::copy_from_slice(x)).collect(),
            },
        )
    }
    fn pump(&mut self) -> Result<()> {
        let mut steps = 0;
        while let Some((from, output)) = self.queue.pop_front() {
            steps += 1;
            assert!(steps < 10000, "protocol queue must quiesce");
            self.deliver(from, output)?;
        }
        Ok(())
    }
    fn pump_without_outcomes_from(&mut self, senders: &[&str]) -> Result<usize> {
        let mut dropped = 0;
        let mut steps = 0;
        while let Some((from, output)) = self.queue.pop_front() {
            steps += 1;
            assert!(steps < 10000);
            if senders.contains(&from.as_str())
                && matches!(
                    &output.message,
                    OutputMessage::Cluster(RpcEnvelope {
                        rpc: Rpc::MonsterOutcome { .. },
                        ..
                    })
                )
            {
                self.history.push((from, output));
                dropped += 1;
            } else {
                self.deliver(from, output)?;
            }
        }
        Ok(dropped)
    }
    fn deliver(&mut self, from: String, output: Output) -> Result<()> {
        self.history.push((from.clone(), output.clone()));
        match output.message {
            OutputMessage::Cluster(envelope) => {
                self.input(
                    &output.to,
                    Task::Receive {
                        from: envelope.from.clone(),
                        message: wire::decode_payload(&wire::encode_payload(&envelope)?)?,
                    },
                )?;
            }
            OutputMessage::Apply(ApplyRequest::Execute { execute }) => {
                let node = self.nodes.get_mut(&from).unwrap();
                assert!(!node.executed.contains(&execute.seq));
                node.executed.push(execute.seq);
                let outcomes = execute
                    .items
                    .as_ref()
                    .map(|items| {
                        items
                            .iter()
                            .map(|item| Outcome::Fulfilled(Value::Binary(item.to_vec())))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                let digest = if execute.seq == 0 {
                    [0; 16]
                } else if self.digest_mode == 1 && from == "3" {
                    [3; 16]
                } else if self.digest_mode == 2 {
                    [from.as_bytes()[0]; 16]
                } else if self.digest_mode == 3 && from == "1" {
                    [1; 16]
                } else {
                    wire::checksum(&execute.seq.to_le_bytes())
                };
                self.input(
                    &from,
                    Task::Apply(ApplyCompletion {
                        cid: execute.cid,
                        tick_count: execute.tick_count,
                        receipt: Receipt {
                            seq: execute.seq,
                            entry_hash: Some(execute.entry_hash),
                            local_digest: Some(digest),
                        },
                        outcomes,
                    }),
                )?;
            }
            OutputMessage::Event(event) => self.nodes.get_mut(&from).unwrap().events.push(event),
            OutputMessage::Client(reply) => self.nodes.get_mut(&from).unwrap().results.push(reply),
        }
        Ok(())
    }
    fn entries(&self, id: &str) -> Result<Vec<Entry>> {
        self.nodes[id]
            .store
            .log
            .iter(0)?
            .filter_map(|r| match r {
                Ok(r) if !r.entry.is_empty() => Some(wire::decode_payload(&r.entry)),
                Ok(_) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }
}

#[test]
fn queued_fifo_batches_pair_sync_and_cmd_and_keep_events_ordered() -> Result<()> {
    let mut c = Cluster::new(Options {
        election_timeout: 100,
        ..Default::default()
    })?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    c.submit("1", "b", &[b"b", b"c"])?;
    c.submit("1", "c", &[b"d"])?;
    c.pump()?;
    for id in ["1", "2", "3"] {
        assert_eq!(c.nodes[id].executed, [0, 1, 3, 5]);
        assert_eq!(c.nodes[id].engine.applied_seq, 6);
    }
    let seqs: Vec<_> = c.nodes["1"]
        .results
        .iter()
        .map(|r| match r {
            ClientReply::Ack {
                result: Some(r), ..
            } => r.cmd_seq,
            _ => panic!("expected CMD result"),
        })
        .collect();
    assert_eq!(seqs, [1, 3, 5]);
    assert!(c.history.iter().any(|(_,o)|matches!(&o.message,OutputMessage::Cluster(RpcEnvelope{rpc:Rpc::Append{data:Some(data),..},..}) if data.len()==2&&matches!(wire::decode_payload::<Entry>(&data[0].entry),Ok(Entry::Sync{..}))&&matches!(wire::decode_payload::<Entry>(&data[1].entry),Ok(Entry::Cmd{..})))));
    let entries = c.entries("1")?;
    assert_eq!(entries.len(), 6);
    if let Entry::Cmd { match_index, .. } = &entries[2] {
        assert_eq!(match_index[0], 2);
        assert!(match_index[1..].iter().all(|s| *s <= 1));
    } else {
        panic!("second CMD missing")
    }
    let events: Vec<_> = c.nodes["1"]
        .events
        .iter()
        .filter_map(|e| match e {
            Event::Cmd { cmd_seq, .. } => Some(("cmd", *cmd_seq)),
            Event::Sync { sync_seq, .. } => Some(("sync", *sync_seq)),
            _ => None,
        })
        .collect();
    assert_eq!(
        events,
        [
            ("cmd", 1),
            ("sync", 2),
            ("cmd", 3),
            ("sync", 4),
            ("cmd", 5),
            ("sync", 6)
        ]
    );
    Ok(())
}

#[test]
fn forwarded_commands_and_replication_limits_preserve_pair_semantics() -> Result<()> {
    let mut c = Cluster::new(Options {
        rpc_max: 1,
        ..Default::default()
    })?;
    c.start()?;
    c.submit("2", "forwarded", &[b"a"])?;
    c.pump()?;
    assert!(
        matches!(&c.nodes["2"].results[0],ClientReply::Ack{client_id,result:Some(result)} if client_id=="forwarded"&&result.cmd_seq==1)
    );
    c.submit("1", "b", &[b"b"])?;
    c.submit("1", "c", &[b"c"])?;
    c.pump()?;
    assert_eq!(c.nodes["1"].executed, [0, 1, 3, 5]);
    assert!(c.history.iter().all(|(_,o)|!matches!(&o.message,OutputMessage::Cluster(RpcEnvelope{rpc:Rpc::Append{data:Some(data),..},..}) if data.len()>1)));
    Ok(())
}

#[test]
fn drain_rejects_backlog_and_finishes_standalone_sync() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    c.submit("1", "b", &[b"b"])?;
    // Let CMD commit and begin Apply before accepting drain.
    while !c
        .queue
        .iter()
        .any(|(id, o)| id == "1" && matches!(o.message, OutputMessage::Apply(_)))
    {
        let (from, o) = c.queue.pop_front().unwrap();
        match o.message {
            OutputMessage::Cluster(e) => c.input(
                &o.to,
                Task::Receive {
                    from: e.from.clone(),
                    message: wire::decode_payload(&wire::encode_payload(&e)?)?,
                },
            )?,
            OutputMessage::Event(e) => c.nodes.get_mut(&from).unwrap().events.push(e),
            _ => panic!("unexpected output before Apply"),
        }
    }
    c.input(
        "1",
        Task::Drain {
            client_id: "drain".into(),
        },
    )?;
    c.pump()?;
    assert_eq!(c.nodes["1"].executed, [0, 1]);
    assert!(c.nodes["1"].results.iter().any(|r|matches!(r,ClientReply::Err{client_id,error} if client_id=="b"&&error.code==Some(Code::Draining))));
    assert!(
        c.nodes["1"]
            .results
            .iter()
            .any(|r| matches!(r,ClientReply::Ack{client_id,result:None} if client_id=="drain"))
    );
    assert_eq!(c.nodes["1"].engine.applied_seq, 2);
    assert_eq!(c.entries("1")?.len(), 2);
    Ok(())
}

#[test]
fn dissenting_follower_commits_fence_and_stops_before_next_cmd() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.digest_mode = 1;
    c.submit("1", "a", &[b"a"])?;
    c.submit("1", "b", &[b"b"])?;
    c.pump()?;
    assert_eq!(
        c.nodes["3"].engine.fatal_error().unwrap().code,
        Some(Code::RepairOutside)
    );
    assert_eq!(
        c.nodes["3"].store.load()?.repair_state,
        storage::REPAIR_OUTSIDE
    );
    assert_eq!(c.nodes["3"].executed, [0, 1]);
    assert_eq!(c.nodes["1"].executed, [0, 1, 3]);
    Ok(())
}

#[test]
fn no_quorum_is_live_error_while_dissenting_leader_is_fatal() -> Result<()> {
    for mode in [2, 3] {
        let mut c = Cluster::new(Options::default())?;
        c.start()?;
        c.digest_mode = mode;
        c.submit("1", "a", &[b"a"])?;
        c.submit("1", "b", &[b"b"])?;
        c.pump()?;
        if mode == 2 {
            assert!(c.nodes["1"].engine.fatal_error().is_none());
            assert_eq!(
                c.nodes["1"].store.load()?.repair_state,
                storage::REPAIR_NO_QUORUM
            );
            assert!(
                c.nodes["1"].diagnostics.iter().any(
                    |d| matches!(d,Diagnostic::Error(e) if e.code==Some(Code::RepairNoQuorum))
                )
            );
            assert_eq!(c.entries("1")?.len(), 2);
        } else {
            assert_eq!(
                c.nodes["1"].engine.fatal_error().unwrap().code,
                Some(Code::RepairOutside)
            );
            assert_eq!(
                c.nodes["1"].store.load()?.repair_state,
                storage::REPAIR_OUTSIDE
            );
            assert_eq!(c.entries("1")?.len(), 1);
        }
    }
    Ok(())
}

#[test]
fn malformed_votes_appends_and_acks_do_not_advance_terms() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    let term = c.nodes["1"].engine.term;
    let value = Value::Map(vec![
        ("type".into(), "vote".into()),
        ("term".into(), 99.into()),
        ("voteGranted".into(), "wrong".into()),
    ]);
    c.input(
        "1",
        Task::Receive {
            from: "2".into(),
            message: value,
        },
    )?;
    assert_eq!(c.nodes["1"].engine.term, term);
    let value = Value::Map(vec![
        ("type".into(), "append".into()),
        ("term".into(), 99.into()),
        ("cid".into(), "bad".into()),
    ]);
    c.input(
        "1",
        Task::Receive {
            from: "2".into(),
            message: value,
        },
    )?;
    assert_eq!(c.nodes["1"].engine.term, term);
    assert!(c.queue.iter().any(|(_,o)|matches!(&o.message,OutputMessage::Cluster(RpcEnvelope{rpc:Rpc::Err{cid,error,..},..}) if cid=="bad"&&error.code==Some(Code::RpcIllegal))));
    let value = Value::Map(vec![
        ("type".into(), "err".into()),
        ("cid".into(), "unused".into()),
        ("term".into(), 1.into()),
        ("message".into(), "backtrack".into()),
        ("code".into(), Code::ReplBacktrack.number().into()),
        ("sqlCode".into(), Value::Nil),
        ("logSeq".into(), "bad hint".into()),
    ]);
    assert!(matches!(Rpc::parse(value)?, Rpc::Err { log_seq: None, .. }));
    Ok(())
}

#[test]
fn apply_identity_is_bound_to_captured_execution_tick() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    while !c
        .queue
        .iter()
        .any(|(id, o)| id == "1" && matches!(o.message, OutputMessage::Apply(_)))
    {
        let (from, o) = c.queue.pop_front().unwrap();
        match o.message {
            OutputMessage::Cluster(e) => c.input(
                &o.to,
                Task::Receive {
                    from: e.from.clone(),
                    message: wire::decode_payload(&wire::encode_payload(&e)?)?,
                },
            )?,
            OutputMessage::Event(e) => c.nodes.get_mut(&from).unwrap().events.push(e),
            _ => panic!(),
        }
    }
    let execute = c
        .queue
        .iter()
        .find_map(|(id, o)| match &o.message {
            OutputMessage::Apply(ApplyRequest::Execute { execute }) if id == "1" => {
                Some(execute.clone())
            }
            _ => None,
        })
        .unwrap();
    let error = c
        .nodes
        .get_mut("1")
        .unwrap()
        .input(Task::Apply(ApplyCompletion {
            cid: execute.cid,
            tick_count: execute.tick_count + 1,
            receipt: Receipt {
                seq: execute.seq,
                entry_hash: Some(execute.entry_hash),
                local_digest: Some([1; 16]),
            },
            outcomes: vec![Outcome::Fulfilled(Value::Nil)],
        }))
        .unwrap_err();
    assert_eq!(error.code, Some(Code::MonsterIllegal));
    assert_eq!(c.nodes["1"].store.load()?.applied_seq, 0);
    Ok(())
}

#[test]
fn early_outcomes_decide_without_queries_when_leader_apply_finishes() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.submit("1", "slow-leader", &[b"a"])?;
    let mut held = None;
    let mut steps = 0;
    // Followers finish and announce before the leader's own Apply completion.
    while let Some((from, output)) = c.queue.pop_front() {
        steps += 1;
        assert!(steps < 10000);
        if from == "1"
            && matches!(&output.message,
                OutputMessage::Apply(ApplyRequest::Execute { execute }) if execute.seq == 1)
        {
            assert!(held.replace((from, output)).is_none());
        } else {
            c.deliver(from, output)?;
        }
    }
    assert_eq!(c.nodes["1"].executed, [0]);
    assert_eq!(c.nodes["2"].executed, [0, 1]);
    assert_eq!(c.nodes["3"].executed, [0, 1]);
    assert!(c.nodes["1"].results.is_empty());
    assert_eq!(
        c.history
            .iter()
            .filter(|(_, output)| matches!(
                &output.message,
                OutputMessage::Cluster(RpcEnvelope {
                    rpc: Rpc::MonsterOutcome { cmd_seq: 1, .. },
                    ..
                })
            ))
            .count(),
        2
    );

    let (from, output) = held.expect("leader Apply request was held");
    c.deliver(from, output)?;
    assert_eq!(
        c.queue
            .iter()
            .filter(|(from, output)| from == "1"
                && matches!(
                    &output.message,
                    OutputMessage::Cluster(RpcEnvelope {
                        rpc: Rpc::MonsterOutcomeRequest { cmd_seq: 1 },
                        ..
                    })
                ))
            .count(),
        0
    );
    c.pump()?;
    assert_eq!(c.nodes["1"].results.len(), 1);
    assert!(matches!(&c.nodes["1"].results[0], ClientReply::Ack {
        result: Some(result), .. } if result.cmd_seq == 1));
    for node in c.nodes.values() {
        assert_eq!(node.engine.applied_seq, 2);
        assert_eq!(node.executed, [0, 1]);
        assert_eq!(node.clock, 0, "agreement required no retry timer");
    }
    Ok(())
}

#[test]
fn local_and_forwarded_commands_need_no_queries_when_announcements_arrive() -> Result<()> {
    for caller in ["1", "2"] {
        let mut c = Cluster::new(Options::default())?;
        c.start()?;
        for client in ["first", "second"] {
            c.submit(caller, client, &[b"a"])?;
            c.pump()?;
        }
        assert_eq!(c.nodes[caller].results.len(), 2);
        assert!(c.history.iter().all(|(_, output)| !matches!(
            &output.message,
            OutputMessage::Cluster(RpcEnvelope {
                rpc: Rpc::MonsterOutcomeRequest { .. },
                ..
            })
        )));
        for node in c.nodes.values() {
            assert_eq!(node.engine.applied_seq, 4);
            assert_eq!(node.executed, [0, 1, 3]);
            assert_eq!(node.clock, 0);
        }
    }
    Ok(())
}

#[test]
fn lost_announcements_query_only_missing_reports_on_the_existing_retry() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.digest_mode = 1; // Node 3 disagrees, so node 2's missing report is needed.
    c.submit("1", "retry", &[b"a"])?;
    assert_eq!(c.pump_without_outcomes_from(&["2"])?, 1);
    assert!(c.nodes["1"].results.is_empty());
    assert_eq!(c.nodes["1"].store.log.seq, 1);
    c.nodes.get_mut("1").unwrap().clock = 149;
    c.input("1", Task::Tick)?;
    c.pump()?;
    assert!(c.nodes["1"].results.is_empty());
    c.nodes.get_mut("1").unwrap().clock = 150;
    c.input("1", Task::Tick)?;
    let requested: Vec<_> = c
        .queue
        .iter()
        .filter_map(|(_, output)| match &output.message {
            OutputMessage::Cluster(RpcEnvelope {
                rpc: Rpc::MonsterOutcomeRequest { cmd_seq: 1 },
                ..
            }) => Some(output.to.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(requested, ["2"]);
    c.pump()?;
    assert_eq!(c.nodes["1"].results.len(), 1);
    assert_eq!(c.nodes["1"].engine.applied_seq, 2);
    c.nodes.get_mut("1").unwrap().clock = 300;
    c.input("1", Task::Tick)?;
    c.pump()?;
    assert_eq!(
        c.history
            .iter()
            .filter(|(_, output)| matches!(
                &output.message,
                OutputMessage::Cluster(RpcEnvelope {
                    rpc: Rpc::MonsterOutcomeRequest { .. },
                    ..
                })
            ))
            .count(),
        1,
        "agreement cancels the retry"
    );
    Ok(())
}

#[test]
fn inherited_commands_query_immediately_after_restart_or_leadership_change() -> Result<()> {
    for restart in [false, true] {
        let mut c = Cluster::new(Options::default())?;
        c.start()?;
        c.submit("1", "inherited", &[b"a"])?;
        assert_eq!(c.pump_without_outcomes_from(&["2", "3"])?, 2);
        assert!(c.nodes["1"].results.is_empty());
        let leader = if restart {
            let node = c.nodes.get_mut("1").unwrap();
            node.engine = Engine::new(
                "1".into(),
                vec!["1".into(), "2".into(), "3".into()],
                [1; 16],
                Options::default(),
                &node.store,
            )?;
            node.executor = Executor::new();
            c.input("1", Task::Start)?;
            "1"
        } else {
            "2"
        };
        c.input(leader, Task::Election)?;
        c.pump()?;
        assert_eq!(c.nodes[leader].engine.state, State::Leader);
        assert_eq!(
            c.history
                .iter()
                .filter(|(from, output)| from == leader
                    && matches!(
                        &output.message,
                        OutputMessage::Cluster(RpcEnvelope {
                            rpc: Rpc::MonsterOutcomeRequest { cmd_seq: 1 },
                            ..
                        })
                    ))
                .count(),
            2
        );
        for node in c.nodes.values() {
            assert_eq!(node.engine.applied_seq, 3);
            assert_eq!(
                node.executed,
                [0, 1],
                "committed Apply work is not repeated"
            );
            assert_eq!(node.clock, 0, "recovery requires no retry tick");
        }
    }
    Ok(())
}

#[test]
fn retention_does_not_prune_between_pending_cmd_and_sync() -> Result<()> {
    let mut c = Cluster::new(Options {
        keep: Some(Retention {
            target: 2,
            trigger: 5,
        }),
        ..Default::default()
    })?;
    c.start()?;
    for round in 0..5 {
        c.submit("1", &format!("{round}"), &[b"a"])?;
        c.pump()?;
    }
    for id in ["1", "2", "3"] {
        assert_eq!(c.nodes[id].engine.applied_seq, 10);
        assert!(c.nodes[id].store.log.begin >= 7);
        assert_eq!(c.nodes[id].store.load()?.pending_cmd_seq, None);
    }
    Ok(())
}

#[test]
fn failed_pair_insertion_rolls_back_both_entries_and_caller_output() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.nodes["1"].store.db()?.execute_batch("CREATE TRIGGER fail_pair BEFORE INSERT ON raft_log WHEN NEW.seq=3 BEGIN SELECT RAISE(ABORT,'injected pair failure'); END;")?;
    c.submit("1", "a", &[b"a"])?;
    c.submit("1", "b", &[b"b"])?;
    let error = c.pump().unwrap_err();
    assert_eq!(error.code, Some(Code::SqliteError));
    let seq: i64 =
        c.nodes["1"]
            .store
            .db()?
            .query_row("SELECT max(seq) FROM raft_log", [], |row| row.get(0))?;
    assert_eq!(seq, 1);
    assert_eq!(c.nodes["1"].store.log.seq, 1);
    assert!(
        c.nodes["1"].results.is_empty(),
        "the preceding result buffered before insertion must not escape the failed input"
    );
    Ok(())
}

#[test]
fn leadership_loss_settles_old_command_once_and_stale_replies_do_not_revive_it() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    c.submit("1", "b", &[b"b"])?;
    let value = wire::decode_payload(&wire::encode_payload(&RpcEnvelope {
        from: "2".into(),
        rpc: Rpc::Vote {
            term: 99,
            vote_granted: false,
        },
    })?)?;
    c.input(
        "1",
        Task::Receive {
            from: "2".into(),
            message: value,
        },
    )?;
    c.pump()?;
    assert_eq!(c.nodes["1"].engine.state, State::Follower);
    assert_eq!(c.nodes["1"].engine.term, 99);
    assert_eq!(c.nodes["1"].results.len(), 2);
    assert!(
        c.nodes["1"]
            .results
            .iter()
            .all(|r| matches!(r,ClientReply::Err{error,..} if error.code==Some(Code::NotLeader)))
    );
    assert_eq!(c.nodes["1"].executed, [0]);
    Ok(())
}

#[test]
fn malformed_correlated_ack_does_not_settle_request_or_advance_term() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    let cid = c
        .queue
        .iter()
        .find_map(|(from, o)| match &o.message {
            OutputMessage::Cluster(RpcEnvelope {
                rpc: Rpc::Append {
                    cid, data: Some(_), ..
                },
                ..
            }) if from == "1" && o.to == "2" => Some(cid.clone()),
            _ => None,
        })
        .unwrap();
    let value = wire::decode_payload(&wire::encode_payload(&RpcEnvelope {
        from: "2".into(),
        rpc: Rpc::Ack {
            cid,
            term: 100,
            seq: None,
            cmd_seq: Some(7),
            results: Some(vec![]),
        },
    })?)?;
    c.input(
        "1",
        Task::Receive {
            from: "2".into(),
            message: value,
        },
    )?;
    assert_eq!(c.nodes["1"].engine.term, 1);
    c.pump()?;
    assert!(
        matches!(&c.nodes["1"].results[0],ClientReply::Ack{result:Some(result),..} if result.cmd_seq==1)
    );
    Ok(())
}

#[test]
fn follower_cannot_replace_a_committed_entry() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    c.pump()?;
    let before = c.nodes["2"].store.log.range(1, 1, 1)?;
    let rpc = RpcEnvelope {
        from: "1".into(),
        rpc: Rpc::Append {
            cid: "bad-replacement".into(),
            term: 1,
            term_p: 1,
            seq_p: 0,
            commit_seq: 2,
            data: Some(vec![monsterft::log::LogEntry {
                term: 1,
                entry: vec![99],
            }]),
        },
    };
    let message = wire::decode_payload(&wire::encode_payload(&rpc)?)?;
    let error = c
        .nodes
        .get_mut("2")
        .unwrap()
        .input(Task::Receive {
            from: "1".into(),
            message,
        })
        .unwrap_err();
    assert_eq!(error.code, Some(Code::RaftIllegal));
    assert_eq!(c.nodes["2"].store.log.range(1, 1, 1)?, before);
    Ok(())
}

#[test]
fn repeated_final_status_does_not_emit_change_and_candidates_have_no_followers() -> Result<()> {
    let mut c = Cluster::new(Options::default())?;
    c.start()?;
    assert!(
        c.nodes["1"]
            .events
            .iter()
            .filter_map(|e| match e {
                Event::Change { status } if status.state == State::Candidate => Some(status),
                _ => None,
            })
            .all(|s| s.followers.is_empty())
    );
    c.nodes.get_mut("1").unwrap().events.clear();
    c.input("1", Task::Tick)?;
    c.pump()?;
    assert!(c.nodes["1"].events.is_empty());
    Ok(())
}

#[test]
fn fractional_derived_deadlines_round_up_instead_of_firing_early() -> Result<()> {
    let mut c = Cluster::new(Options {
        election_timeout: 11,
        ..Default::default()
    })?;
    c.start()?;
    c.history.clear();
    c.nodes.get_mut("1").unwrap().clock = 3;
    c.input("1", Task::Tick)?;
    c.pump()?;
    assert!(!c.history.iter().any(|(from, o)| from == "1"
        && matches!(
            &o.message,
            OutputMessage::Cluster(RpcEnvelope {
                rpc: Rpc::Append { .. },
                ..
            })
        )));
    c.nodes.get_mut("1").unwrap().clock = 4;
    c.input("1", Task::Tick)?;
    c.pump()?;
    assert!(c.history.iter().any(|(from, o)| from == "1"
        && matches!(
            &o.message,
            OutputMessage::Cluster(RpcEnvelope {
                rpc: Rpc::Append { data: None, .. },
                ..
            })
        )));
    let mut c = Cluster::new(Options {
        election_timeout: 101,
        ..Default::default()
    })?;
    c.start()?;
    c.submit("1", "a", &[b"a"])?;
    let cid = c
        .queue
        .iter()
        .find_map(|(from, o)| match &o.message {
            OutputMessage::Cluster(RpcEnvelope {
                rpc: Rpc::Append {
                    cid, data: Some(_), ..
                },
                ..
            }) if from == "1" && o.to == "2" => Some(cid.clone()),
            _ => None,
        })
        .unwrap();
    c.queue.clear();
    let error = RpcEnvelope {
        from: "2".into(),
        rpc: Rpc::Err {
            cid,
            term: 1,
            error: monsterft::error::Error::new(Code::AppendTimeout, "retry"),
            log_seq: None,
        },
    };
    c.input(
        "1",
        Task::Receive {
            from: "2".into(),
            message: wire::decode_payload(&wire::encode_payload(&error)?)?,
        },
    )?;
    c.queue.clear();
    c.nodes.get_mut("1").unwrap().clock = 10;
    c.input("1", Task::Tick)?;
    assert!(c.queue.is_empty());
    c.nodes.get_mut("1").unwrap().clock = 11;
    c.input("1", Task::Tick)?;
    assert!(c.queue.iter().any(|(_, o)| matches!(
        &o.message,
        OutputMessage::Cluster(RpcEnvelope {
            rpc: Rpc::Append { data: Some(_), .. },
            ..
        })
    )));
    Ok(())
}

// Only remote messages consume simulated time. SQLite and local Apply still use
// the ordinary fixture, so the measurement excludes filesystem/CPU scheduling.
struct DelayedNetwork {
    cluster: Cluster,
    pending: BTreeMap<(u64, u64), (String, Output)>,
    now: u64,
    order: u64,
    delay: u64,
    link_delays: BTreeMap<(String, String), u64>,
    result_times: Vec<u64>,
}
impl DelayedNetwork {
    fn new(delay: u64) -> Result<Self> {
        let mut cluster = Cluster::new(Options {
            election_timeout: 10_000,
            append_timeout: 10_000,
            ..Default::default()
        })?;
        cluster.start()?;
        cluster.history.clear();
        Ok(Self {
            cluster,
            pending: BTreeMap::new(),
            now: 0,
            order: 0,
            delay,
            link_delays: BTreeMap::new(),
            result_times: Vec::new(),
        })
    }
    fn submit(&mut self, index: usize) -> Result<()> {
        self.cluster.nodes.get_mut("1").unwrap().clock = self.now;
        self.cluster
            .submit("1", &format!("latency-{index}"), &[b"value"])
    }
    fn outputs(&mut self) -> Result<()> {
        while let Some((from, output)) = self.cluster.queue.pop_front() {
            if matches!(output.message, OutputMessage::Cluster(_)) {
                self.order += 1;
                let delay = self
                    .link_delays
                    .get(&(from.clone(), output.to.clone()))
                    .copied()
                    .unwrap_or(self.delay);
                self.pending
                    .insert((self.now + delay, self.order), (from, output));
            } else {
                self.cluster.nodes.get_mut(&from).unwrap().clock = self.now;
                if matches!(output.message, OutputMessage::Client(_)) {
                    self.result_times.push(self.now);
                }
                self.cluster.deliver(from, output)?;
            }
        }
        Ok(())
    }
    fn next_message(&mut self) -> Result<bool> {
        let Some(((at, _), (from, output))) = self.pending.pop_first() else {
            return Ok(false);
        };
        self.now = at;
        self.cluster.nodes.get_mut(&output.to).unwrap().clock = self.now;
        self.cluster.deliver(from, output)?;
        Ok(true)
    }
    fn until_results(&mut self, count: usize) -> Result<()> {
        for _ in 0..10_000 {
            self.outputs()?;
            if self.result_times.len() >= count {
                return Ok(());
            }
            assert!(
                self.next_message()?,
                "request has no remaining network progress"
            );
        }
        panic!("delayed protocol must complete");
    }
    fn settle(&mut self) -> Result<()> {
        for _ in 0..10_000 {
            self.outputs()?;
            if !self.next_message()? {
                return Ok(());
            }
        }
        panic!("delayed protocol must quiesce");
    }
    fn replication_requests(&self) -> usize {
        self.cluster.history.iter().filter(|(from, output)| from == "1" && matches!(&output.message, OutputMessage::Cluster(RpcEnvelope { rpc:Rpc::Append { data:Some(data),.. }, .. }) if !data.is_empty())).count()
    }
    fn paired_requests(&self) -> usize {
        self.cluster.history.iter().filter(|(from, output)| from == "1" && matches!(&output.message, OutputMessage::Cluster(RpcEnvelope { rpc:Rpc::Append { data:Some(data),.. }, .. }) if data.len()==2 && matches!(wire::decode_payload::<Entry>(&data[0].entry),Ok(Entry::Sync{..})) && matches!(wire::decode_payload::<Entry>(&data[1].entry),Ok(Entry::Cmd{..})))).count()
    }
}

#[test]
fn fixed_network_latency_measures_the_replication_round_saved_by_pairing() -> Result<()> {
    const COMMANDS: usize = 12;
    const DELAY: u64 = 25;
    let mut serial = DelayedNetwork::new(DELAY)?;
    for index in 0..COMMANDS {
        serial.submit(index)?;
        serial.until_results(index + 1)?;
    }
    serial.settle()?;
    let mut backlog = DelayedNetwork::new(DELAY)?;
    for index in 0..COMMANDS {
        backlog.submit(index)?;
    }
    backlog.until_results(COMMANDS)?;
    backlog.settle()?;

    for network in [&serial, &backlog] {
        for node in network.cluster.nodes.values() {
            assert_eq!(node.engine.applied_seq, (COMMANDS * 2) as i64);
            assert_eq!(
                node.executed,
                (0..=COMMANDS)
                    .map(|i| if i == 0 { 0 } else { (i * 2 - 1) as i64 })
                    .collect::<Vec<_>>()
            );
            assert!(node.diagnostics.is_empty());
        }
        assert_eq!(network.cluster.nodes["1"].results.len(), COMMANDS);
        for (index, reply) in network.cluster.nodes["1"].results.iter().enumerate() {
            assert!(
                matches!(reply,ClientReply::Ack{client_id,result:Some(result)} if client_id==&format!("latency-{index}") && result.cmd_seq==(index*2+1) as i64 && result.results==vec![Outcome::Fulfilled(Value::Binary(b"value".to_vec()))])
            );
        }
    }
    eprintln!(
        "25ms one-way network; {COMMANDS} commands: serial results={:?}, settled={}ms, data APPENDs={}; backlog results={:?}, settled={}ms, data APPENDs={}, paired APPENDs={}",
        serial.result_times,
        serial.now,
        serial.replication_requests(),
        backlog.result_times,
        backlog.now,
        backlog.replication_requests(),
        backlog.paired_requests()
    );
    assert_eq!(serial.result_times[0], 4 * DELAY);
    assert_eq!(backlog.result_times[0], 4 * DELAY);
    assert!(
        serial
            .result_times
            .windows(2)
            .all(|times| times[1] - times[0] == 6 * DELAY)
    );
    assert!(
        backlog
            .result_times
            .windows(2)
            .all(|times| times[1] - times[0] == 4 * DELAY)
    );
    assert_eq!(serial.replication_requests(), COMMANDS * 4);
    assert_eq!(backlog.replication_requests(), (COMMANDS + 1) * 2);
    assert_eq!(serial.paired_requests(), 0);
    assert_eq!(backlog.paired_requests(), (COMMANDS - 1) * 2);
    assert_eq!(
        serial.replication_requests() - backlog.replication_requests(),
        (COMMANDS - 1) * 2
    );
    // Exactly one network round trip per eligible pair; this assertion is about
    // the virtual message schedule, never elapsed wall-clock time.
    assert_eq!(
        serial.result_times[COMMANDS - 1] - backlog.result_times[COMMANDS - 1],
        (COMMANDS as u64 - 1) * DELAY * 2
    );
    Ok(())
}

// The 2-of-3 quorum can use the nearer follower. Parallel RPCs to the farther
// follower do not add their RTT to the network-only command critical path.
#[test]
fn network_floor_uses_fast_quorum_path_and_excludes_final_sync_from_first_reply() -> Result<()> {
    const COMMANDS: usize = 8;
    const FAST_ONE_WAY: u64 = 25;
    for backlog in [false, true] {
        let mut network = DelayedNetwork::new(FAST_ONE_WAY)?;
        network.link_delays.insert(("1".into(), "3".into()), 100);
        network.link_delays.insert(("3".into(), "1".into()), 100);
        for index in 0..COMMANDS {
            network.submit(index)?;
            if !backlog {
                network.until_results(index + 1)?;
            }
        }
        network.until_results(COMMANDS)?;
        assert_eq!(network.result_times[0], 4 * FAST_ONE_WAY);
        let interval = if backlog { 4 } else { 6 } * FAST_ONE_WAY;
        assert!(
            network
                .result_times
                .windows(2)
                .all(|times| times[1] - times[0] == interval)
        );
        let last_reply = *network.result_times.last().unwrap();
        network.settle()?;
        assert!(
            network.now > last_reply,
            "final SYNC still needs replication after caller completion"
        );
        for node in network.cluster.nodes.values() {
            assert_eq!(node.engine.applied_seq, (COMMANDS * 2) as i64);
        }
    }
    Ok(())
}
