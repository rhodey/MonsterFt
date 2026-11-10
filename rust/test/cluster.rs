use bytes::Bytes;
use monsterft::{
    Value,
    apply::{Apply, ApplyContext, apply_async},
    error::{Code, Error, Result},
    gate::{EventKind, Gate, GateEvent, GateOptions},
    local::{Received, SendFrame},
    proc::{Options, Proc},
    rusqlite::Connection,
    storage::ProcRole,
};
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    path::Path,
    rc::Rc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    task::{JoinHandle, LocalSet},
    time::timeout,
};

// These tests exercise real SQLite owners together, not election timing. Allow
// slow CI storage and scheduling without turning them into failover tests.
const LOCAL_TIMEOUT: Duration = Duration::from_secs(10);
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

type Routes = Rc<RefCell<BTreeMap<String, mpsc::Sender<Received>>>>;
struct Cluster {
    gates: Vec<Gate>,
    owners: Vec<JoinHandle<Result<()>>>,
    drivers: Vec<JoinHandle<()>>,
    errors: Rc<RefCell<Vec<Error>>>,
    synced: Vec<watch::Receiver<i64>>,
}
fn enqueue(tx: &mpsc::Sender<Received>, from: &str, frame: Bytes) -> Result<()> {
    tx.try_send(Received {
        from: from.into(),
        frame,
    })
    .map_err(|e| Error::new(Code::SendError, e.to_string()))
}
impl Cluster {
    fn new(path: &Path) -> Self {
        Self::with_fault(path, Rc::new(Cell::new(0)))
    }
    fn with_fault(path: &Path, fault: Rc<Cell<u8>>) -> Self {
        let ids = vec!["1".to_owned(), "2".to_owned(), "3".to_owned()];
        let routes: Routes = Rc::new(RefCell::new(BTreeMap::new()));
        let errors = Rc::new(RefCell::new(Vec::new()));
        let mut gates = Vec::new();
        let mut owners = Vec::new();
        let mut drivers = Vec::new();
        let mut synced = Vec::new();
        for id in &ids {
            let base = path.join(id);
            let mut locals = BTreeMap::new();
            let mut receivers = BTreeMap::new();
            for role in ["procA", "procB", "procC", "apply"] {
                let (tx, rx) = mpsc::channel(64);
                locals.insert(role.to_owned(), tx);
                receivers.insert(role.to_owned(), rx);
            }
            let network = routes.clone();
            let from = id.clone();
            let remote: SendFrame = Box::new(move |to, frame| {
                if let Some(tx) = network.borrow().get(to) {
                    enqueue(tx, &from, frame)?;
                }
                Ok(())
            });
            let inject = fault.clone();
            let local: SendFrame = Box::new(move |to, frame| {
                if inject.get() == 2
                    && to != "procA"
                    && let Ok(monsterft::local::OrderedInput {
                        input: monsterft::local::Input::ApplyResult { reply },
                        ..
                    }) = monsterft::wire::decode_frame(&frame)
                    && reply.receipt.seq > 0
                {
                    return Ok(());
                }
                enqueue(
                    locals
                        .get(to)
                        .ok_or_else(|| Error::new(Code::SendError, "unknown local"))?,
                    "gate",
                    frame,
                )
            });
            let (gate, mut driver) = Gate::new(
                id,
                ids.clone(),
                remote,
                local,
                GateOptions {
                    local_timeout: LOCAL_TIMEOUT,
                    apply_timeout: LOCAL_TIMEOUT,
                    ..Default::default()
                },
            )
            .unwrap();
            let (sync_tx, sync_rx) = watch::channel(-1);
            gate.on(EventKind::Sync, move |event| {
                if let GateEvent::Sync { cmd_seq, .. } = event {
                    sync_tx.send_replace(*cmd_seq);
                }
            })
            .unwrap();
            synced.push(sync_rx);
            let capture = errors.clone();
            driver
                .diagnostics()
                .on_error(move |e| capture.borrow_mut().push(e.clone()));
            routes.borrow_mut().insert(id.clone(), gate.receiver());
            for role in ProcRole::ALL {
                let tx = gate.receiver();
                let from = role.as_str();
                let send: SendFrame = Box::new(move |_, frame| enqueue(&tx, from, frame));
                let mut opts = Options {
                    local_timeout: LOCAL_TIMEOUT,
                    ..Default::default()
                };
                opts.protocol.append_timeout = 10_000;
                let mut proc = Proc::new(role, id, ids.clone(), send, &base, opts).unwrap();
                let capture = errors.clone();
                proc.diagnostics
                    .on_error(move |e| capture.borrow_mut().push(e.clone()));
                owners.push(tokio::task::spawn_local(
                    proc.run(receivers.remove(role.as_str()).unwrap()),
                ));
            }
            let tx = gate.receiver();
            let inject = fault.clone();
            let send: SendFrame = Box::new(move |_, frame| {
                if inject.get() == 1
                    && let Ok(monsterft::local::LocalReply::Ack {
                        receipt: Some(receipt),
                        ..
                    }) = monsterft::wire::decode_frame(&frame)
                    && receipt.seq > 0
                {
                    return Ok(());
                }
                enqueue(&tx, "apply", frame)
            });
            let callback = apply_async(
                async |db: &Connection, command: Option<&[u8]>, _: &ApplyContext| {
                    if let Some(command) = command {
                        db.execute("UPDATE counter SET n=n+1", [])?;
                        tokio::task::yield_now().await;
                        if command == b"reject" {
                            return Err(Error::application("command rejected"));
                        }
                        Ok(db
                            .query_row("SELECT n FROM counter", [], |r| r.get::<_, i64>(0))?
                            .into())
                    } else {
                        db.execute_batch("CREATE TABLE counter(id INTEGER PRIMARY KEY,n INTEGER NOT NULL);INSERT INTO counter VALUES(1,0)")?;
                        Ok(Value::Nil)
                    }
                },
            );
            let mut apply = Apply::new(id, &base, callback).unwrap();
            let capture = errors.clone();
            apply
                .diagnostics()
                .on_error(move |e| capture.borrow_mut().push(e.clone()));
            owners.push(tokio::task::spawn_local(
                apply.run(receivers.remove("apply").unwrap(), send),
            ));
            drivers.push(tokio::task::spawn_local(driver.run()));
            gates.push(gate);
        }
        Self {
            gates,
            owners,
            drivers,
            errors,
            synced,
        }
    }
    async fn open(&self) {
        let result = timeout(Duration::from_secs(15), async {
            let (a, b, c) = tokio::join!(
                self.gates[0].open(),
                self.gates[1].open(),
                self.gates[2].open()
            );
            a?;
            b?;
            c?;
            for gate in &self.gates {
                gate.await_leader(true).await?;
            }
            Ok::<_, Error>(())
        })
        .await;
        assert!(
            matches!(result, Ok(Ok(()))),
            "open {result:?}; errors {:?}",
            self.errors.borrow()
        );
    }
    async fn wait_synced(&self, cmd_seq: i64) {
        // Remember SYNCs even if they arrived before the caller's result.
        timeout(WAIT_TIMEOUT, async {
            for mut synced in self.synced.clone() {
                synced.wait_for(|seq| *seq >= cmd_seq).await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "waiting for SYNC of CMD {cmd_seq}; observed {:?}; errors {:?}",
                self.synced.iter().map(|s| *s.borrow()).collect::<Vec<_>>(),
                self.errors.borrow()
            )
        });
    }
    async fn close(self) {
        let (a, b, c) = tokio::join!(
            self.gates[0].close(),
            self.gates[1].close(),
            self.gates[2].close()
        );
        a.unwrap();
        b.unwrap();
        c.unwrap();
        for owner in self.owners {
            owner.await.unwrap().unwrap();
        }
        assert!(
            self.errors.borrow().is_empty(),
            "{:?}",
            self.errors.borrow()
        );
        drop(self.gates);
        for driver in self.drivers {
            driver.abort();
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cluster_fifo_pairing_rejections_and_retained_restart() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let cluster = Cluster::new(dir.path());
            cluster.open().await;
            let events = Rc::new(RefCell::new(Vec::new()));
            let capture = events.clone();
            cluster.gates[0]
                .on(EventKind::Cmd, move |e| {
                    capture.borrow_mut().push(e.clone())
                })
                .unwrap();
            let mut pending = Vec::new();
            for i in 0..12 {
                pending.push(cluster.gates[0].append(Bytes::from(i.to_string())));
            }
            let results = timeout(Duration::from_secs(12), async {
                let mut values = Vec::new();
                for request in pending {
                    values.push(request.await?);
                }
                Ok::<_, Error>(values)
            })
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                results
                    .iter()
                    .map(|(_, v)| v.as_i64().unwrap())
                    .collect::<Vec<_>>(),
                (1..=12).collect::<Vec<_>>()
            );
            assert!(results.windows(2).all(|w| w[1].0 == w[0].0 + 2));
            let rejection = cluster.gates[1]
                .append(Bytes::from_static(b"reject"))
                .await
                .unwrap_err();
            assert_eq!(rejection.message, "command rejected");
            let (seq, value) = cluster.gates[2]
                .append(Bytes::from_static(b"last"))
                .await
                .unwrap();
            assert_eq!(value, 13.into());
            assert!(
                events
                    .borrow()
                    .iter()
                    .all(|event| matches!(event, GateEvent::Cmd { .. }))
            );
            cluster.wait_synced(seq).await;
            cluster.close().await;
            let reopened = Cluster::new(dir.path());
            reopened.open().await;
            let (seq, value) = reopened.gates[0]
                .append(Bytes::from_static(b"after restart"))
                .await
                .unwrap();
            assert_eq!(value, 14.into());
            reopened.wait_synced(seq).await;
            reopened.close().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_admitted_result_does_not_cancel_command() {
    LocalSet::new()
        .run_until(async {
            let dir = tempfile::tempdir().unwrap();
            let cluster = Cluster::new(dir.path());
            cluster.open().await;
            drop(cluster.gates[0].append(Bytes::from_static(b"accepted")));
            let (seq, value) = cluster.gates[0]
                .append(Bytes::from_static(b"following"))
                .await
                .unwrap();
            assert_eq!(value, 2.into());
            cluster.wait_synced(seq).await;
            cluster.close().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn committed_receipt_recovers_after_ack_loss_and_partial_proc_import() {
    LocalSet::new()
        .run_until(async {
            for mode in [1, 2] {
                let dir = tempfile::tempdir().unwrap();
                let fault = Rc::new(Cell::new(0));
                let cluster = Cluster::with_fault(dir.path(), fault.clone());
                cluster.open().await;
                fault.set(mode);
                let abandoned =
                    cluster.gates[0].append(Bytes::from_static(b"committed-before-interruption"));
                // Stop at the durable interruption point, before local waits expire.
                timeout(WAIT_TIMEOUT, async {
                    loop {
                        let done = ["1", "2", "3"].iter().all(|id| {
                            let path = dir.path().join(format!("{id}2"));
                            let db = Connection::open(path).unwrap();
                            let seq = db
                                .query_row("SELECT seq FROM monsterft_meta_apply", [], |r| {
                                    r.get::<_, i64>(0)
                                })
                                .unwrap();
                            if seq <= 0 {
                                return false;
                            }
                            ["a", "b", "c"].iter().all(|role| {
                                let db = Connection::open(dir.path().join(format!("{id}1{role}")))
                                    .unwrap();
                                let applied: i64 = db
                                    .query_row("SELECT applied_seq FROM monsterft_meta", [], |r| {
                                        r.get(0)
                                    })
                                    .unwrap();
                                if mode == 2 && *role == "a" {
                                    applied == seq
                                } else {
                                    applied < seq
                                }
                            })
                        });
                        if done {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .unwrap();
                // No acknowledged public result exists; all DB2 receipts are durable.
                cluster.close().await;
                assert!(abandoned.await.is_err());
                let recovered = Cluster::new(dir.path());
                recovered.open().await;
                let (seq, value) = recovered.gates[0]
                    .append(Bytes::from_static(b"new-command"))
                    .await
                    .unwrap();
                assert_eq!(
                    value,
                    2.into(),
                    "committed callback must not repeat, interruption {mode}"
                );
                recovered.wait_synced(seq).await;
                for id in ["1", "2", "3"] {
                    let db = Connection::open(dir.path().join(format!("{id}2"))).unwrap();
                    let count: i64 = db
                        .query_row("SELECT n FROM counter", [], |r| r.get(0))
                        .unwrap();
                    assert_eq!(count, 2, "node {id}, interruption {mode}");
                }
                recovered.close().await;
            }
        })
        .await;
}
