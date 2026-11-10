//! Deterministic counterpart of JS test/fixtures/protocol.js. Remote peers and
//! Apply are controlled by the test; all three real Procs own real SQLite files.
use bytes::Bytes;
use monsterft::{
    Value, engine,
    local::{Input, LocalReply, OrderedInput, StartupRequest},
    log::{LogEntry, LogRow},
    proc::{Options, Proc},
    protocol::{ApplyCompletion, Entry, Envelope, Event, Execute, Outcome, Status},
    rpc::{ApplyRequest, Rpc, RpcEnvelope},
    storage::{self, ProcRole, Receipt},
    wire,
};
use rusqlite::{Connection, OpenFlags};
use std::{cell::RefCell, path::PathBuf, rc::Rc};

pub type Outputs = Vec<Envelope<Value>>;
type Replies = Rc<RefCell<Vec<(ProcRole, LocalReply)>>>;

pub struct Protocol {
    _temp: tempfile::TempDir,
    pub base: PathBuf,
    pub id: String,
    pub nodes: Vec<String>,
    pub procs: Vec<Proc>,
    pub clock: u64,
    pub status: Option<Status>,
    pub vote_publications: Rc<RefCell<usize>>,
    options: engine::Options,
    replies: Replies,
    apply: Option<Connection>,
    tick: i64,
    generation: u8,
}

impl Protocol {
    pub fn new(options: engine::Options) -> Self {
        Self::with_members(options, "a", &["a", "b", "c"])
    }

    pub fn with_members(options: engine::Options, id: &str, nodes: &[&str]) -> Self {
        let temp = tempfile::tempdir().unwrap();
        let mut f = Self {
            base: temp.path().join("db"),
            _temp: temp,
            id: id.into(),
            nodes: nodes.iter().map(|s| (*s).into()).collect(),
            procs: vec![],
            clock: 0,
            status: None,
            vote_publications: Rc::new(RefCell::new(0)),
            options,
            replies: Rc::new(RefCell::new(vec![])),
            apply: None,
            tick: 0,
            generation: 0,
        };
        f.open();
        f
    }

    fn open(&mut self) {
        self.tick = 0;
        self.clock = 0;
        self.status = None;
        self.generation += 1;
        let seed = [self.generation; 16];
        let paths = storage::database_paths(&self.base).unwrap();
        for role in ProcRole::ALL {
            let sent = self.replies.clone();
            let path = paths.proc(role).to_owned();
            let id = self.id.clone();
            let votes = self.vote_publications.clone();
            let mut proc = Proc::new(
                role,
                &self.id,
                self.nodes.clone(),
                Box::new(move |to, frame| {
                    assert_eq!(to, "gate");
                    let reply: LocalReply = wire::decode_frame(&frame)?;
                    // Observe durability at publication, using another connection
                    // that cannot see the Proc's uncommitted transaction.
                    if role == ProcRole::A
                        && let LocalReply::Ack {
                            outputs: Some(outputs),
                            ..
                        } = &reply
                    {
                        for payload in outputs {
                            let out: Envelope<Value> = wire::decode_payload(payload)?;
                            if let Ok(envelope) = rmpv::ext::from_value::<RpcEnvelope>(out.message)
                            {
                                let expected = match envelope.rpc {
                                    Rpc::Vote {
                                        term,
                                        vote_granted: true,
                                    } => Some((term, out.to)),
                                    Rpc::VoteRequest { term, .. } => Some((term, id.clone())),
                                    _ => None,
                                };
                                if let Some((term, candidate)) = expected {
                                    let db = Connection::open_with_flags(
                                        &path,
                                        OpenFlags::SQLITE_OPEN_READ_ONLY,
                                    )?;
                                    let state = storage::read_state(&db)?;
                                    assert_eq!(state.election.term, term);
                                    assert_eq!(
                                        state.election.voted_for.as_deref(),
                                        Some(candidate.as_str())
                                    );
                                    *votes.borrow_mut() += 1;
                                }
                            }
                        }
                    }
                    sent.borrow_mut().push((role, reply));
                    Ok(())
                }),
                &self.base,
                Options {
                    protocol: self.options.clone(),
                    ..Default::default()
                },
            )
            .unwrap();
            for request in [
                StartupRequest::Hello {
                    cid: "hello".into(),
                    seed,
                },
                StartupRequest::Open { cid: "open".into() },
            ] {
                proc.on_receive("gate", wire::encode_frame(&request).unwrap());
                assert!(!proc.is_stopped(), "{:?}", proc.fatal_error());
            }
            self.procs.push(proc);
        }
        self.apply = Some(storage::open_apply_database(&self.base, &seed).unwrap());
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
            for proc in &mut self.procs {
                proc.on_receive("gate", wire::encode_frame(&request).unwrap());
                assert!(!proc.is_stopped(), "{:?}", proc.fatal_error());
            }
        }
        let out = self.input(Input::Start);
        assert!(
            executes(&out).is_empty(),
            "restart must import receipts without executing Apply"
        );
    }

    pub fn stop(&mut self) {
        for proc in &mut self.procs {
            proc.close().unwrap();
        }
        self.procs.clear();
        if let Some(db) = self.apply.take() {
            db.close().unwrap();
        }
    }

    pub fn reopen(&mut self) {
        self.stop();
        self.open();
    }

    pub fn try_input(&mut self, input: Input) -> Option<Outputs> {
        self.tick += 1;
        self.replies.borrow_mut().clear();
        let frame = wire::encode_frame(&OrderedInput {
            cid: format!("input-{}", self.tick),
            tick_count: self.tick,
            timems: self.clock,
            input,
        })
        .unwrap();
        for proc in &mut self.procs {
            proc.on_receive("gate", frame.clone());
        }
        let replies = self.replies.borrow();
        if replies.is_empty() {
            assert!(self.procs.iter().all(Proc::is_stopped));
            return None;
        }
        assert_eq!(replies.len(), 3, "each Proc must complete exactly once");
        let payloads: Vec<_> = replies
            .iter()
            .map(|(role, reply)| {
                let LocalReply::Ack {
                    tick_count: Some(tick),
                    outputs: Some(outputs),
                    ..
                } = reply
                else {
                    panic!("{role:?}: {reply:?}")
                };
                assert_eq!(*tick, self.tick);
                outputs
            })
            .collect();
        assert_eq!(payloads[0].len(), payloads[1].len());
        assert_eq!(payloads[0].len(), payloads[2].len());
        for (i, payload) in payloads[0].iter().enumerate() {
            let hash = wire::checksum(payload);
            assert_eq!(payloads[1][i].as_ref(), &hash[..8]);
            assert_eq!(payloads[2][i].as_ref(), &hash[8..]);
        }
        let outputs: Outputs = payloads[0]
            .iter()
            .map(|p| wire::decode_payload(p).unwrap())
            .collect();
        for out in &outputs {
            if out.to == "gate"
                && let Ok(Event::Change { status }) = rmpv::ext::from_value(out.message.clone())
            {
                self.status = Some(status);
            }
        }
        Some(outputs)
    }

    pub fn input(&mut self, input: Input) -> Outputs {
        self.try_input(input).unwrap_or_else(|| {
            panic!(
                "unexpected fatal: {:?}",
                self.procs.iter().map(Proc::fatal_error).collect::<Vec<_>>()
            )
        })
    }
    pub fn rpc_input(&self, from: &str, rpc: Rpc) -> Input {
        Input::ClusterMessage {
            from: from.into(),
            frame: wire::encode_frame(&Envelope {
                to: self.id.clone(),
                message: RpcEnvelope {
                    from: from.into(),
                    rpc,
                },
            })
            .unwrap(),
        }
    }
    pub fn rpc(&mut self, from: &str, rpc: Rpc) -> Outputs {
        self.input(self.rpc_input(from, rpc))
    }
    pub fn tick(&mut self, now: u64) -> Outputs {
        self.clock = now;
        self.input(Input::Tick)
    }
    pub fn elect(&mut self) -> Outputs {
        self.tick(self.clock + 10_000);
        let term = self.status.as_ref().unwrap().term;
        let peers: Vec<_> = self
            .nodes
            .iter()
            .filter(|id| **id != self.id)
            .take(self.options.quorum.unwrap_or(self.nodes.len() / 2 + 1) - 1)
            .cloned()
            .collect();
        let mut out = vec![];
        for peer in peers {
            out = self.rpc(
                &peer,
                Rpc::Vote {
                    term,
                    vote_granted: true,
                },
            );
        }
        out
    }
    pub fn complete(&mut self, execute: Execute) -> Outputs {
        let receipt = Receipt {
            seq: execute.seq,
            entry_hash: Some(execute.entry_hash),
            local_digest: Some(if execute.seq == 0 { [0; 16] } else { [7; 16] }),
        };
        let db = self.apply.as_ref().unwrap();
        db.execute_batch("BEGIN IMMEDIATE").unwrap();
        storage::write_receipt(db, &receipt).unwrap();
        db.execute_batch("COMMIT").unwrap();
        self.input(Input::ApplyResult {
            reply: ApplyCompletion {
                cid: execute.cid,
                tick_count: execute.tick_count,
                receipt,
                outcomes: execute
                    .items
                    .unwrap_or_default()
                    .iter()
                    .map(|_| Outcome::Fulfilled("ok".into()))
                    .collect(),
            },
        })
    }
    pub fn setup(&mut self) {
        let out = self.rpc("b", append(1, -1, -1, 0, Some(vec![noop(1)])));
        self.complete(executes(&out).remove(0));
    }
    pub fn pending(&mut self) {
        let out = self.rpc("b", append(1, 0, 1, 1, Some(vec![command()])));
        self.complete(executes(&out).remove(0));
    }
    pub fn db(&self, role: ProcRole) -> Connection {
        Connection::open(storage::database_paths(&self.base).unwrap().proc(role)).unwrap()
    }
    pub fn snapshot(&self) -> Vec<(storage::State, Vec<LogRow>)> {
        ProcRole::ALL
            .iter()
            .map(|role| {
                let db = self.db(*role);
                let mut statement = db
                    .prepare("SELECT seq,term,entry FROM raft_log ORDER BY seq")
                    .unwrap();
                let rows = statement
                    .query_map([], |r| {
                        Ok(LogRow {
                            seq: r.get(0)?,
                            term: r.get(1)?,
                            entry: r.get(2)?,
                        })
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .unwrap();
                (storage::read_state(&db).unwrap(), rows)
            })
            .collect()
    }
    pub fn sql(&self, sql: &str) {
        for role in ProcRole::ALL {
            self.db(role).execute_batch(sql).unwrap();
        }
    }
    pub fn receipt(&self) -> Receipt {
        storage::read_apply(self.apply.as_ref().unwrap()).unwrap()
    }
}

impl Drop for Protocol {
    fn drop(&mut self) {
        self.stop();
    }
}

pub fn noop(term: i64) -> LogEntry {
    LogEntry {
        term,
        entry: vec![],
    }
}
pub fn command() -> LogEntry {
    LogEntry {
        term: 1,
        entry: wire::encode_payload(&Entry::Cmd {
            match_index: vec![0; 3],
            items: vec![Bytes::from_static(b"next")],
        })
        .unwrap(),
    }
}
pub fn sync(seq: i64) -> LogEntry {
    LogEntry {
        term: 1,
        entry: wire::encode_payload(&Entry::Sync {
            cmd_seq: seq,
            quorum: true,
            agree: vec!["a".into(), "b".into(), "c".into()],
            disagree: vec![],
            digest: Some([7; 16]),
        })
        .unwrap(),
    }
}
pub fn append(
    term: i64,
    seq_p: i64,
    term_p: i64,
    commit_seq: i64,
    data: Option<Vec<LogEntry>>,
) -> Rpc {
    Rpc::Append {
        cid: "page".into(),
        term,
        seq_p,
        term_p,
        commit_seq,
        data,
    }
}
pub fn rpcs(out: &Outputs, to: &str) -> Vec<Rpc> {
    out.iter()
        .filter(|o| o.to == to)
        .map(|o| {
            rmpv::ext::from_value::<RpcEnvelope>(o.message.clone())
                .unwrap()
                .rpc
        })
        .collect()
}
pub fn page(out: &Outputs, to: &str, data: bool) -> Rpc {
    rpcs(out, to)
        .into_iter()
        .find(|r| matches!(r, Rpc::Append { data: entries, .. } if entries.is_some() == data))
        .unwrap_or_else(|| panic!("missing append for {to}: {out:?}"))
}
pub fn ack(request: &Rpc) -> Rpc {
    let Rpc::Append {
        cid, term, seq_p, ..
    } = request
    else {
        panic!("not append")
    };
    Rpc::Ack {
        cid: cid.clone(),
        term: *term,
        seq: Some(seq_p + 1),
        cmd_seq: None,
        results: None,
    }
}
pub fn executes(out: &Outputs) -> Vec<Execute> {
    out.iter()
        .filter(|o| o.to == "apply")
        .map(|o| {
            let ApplyRequest::Execute { execute } =
                rmpv::ext::from_value(o.message.clone()).unwrap();
            execute
        })
        .collect()
}
