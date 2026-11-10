use bytes::Bytes;
use monsterft::{
    Value,
    error::{Code, Error, Result},
    gate::{Gate, GateOptions},
    local::{LocalReply, Received},
    protocol::Envelope,
    rpc::ClientReply,
    storage::{Capture, ProcRole},
    supervisor::{ChildRole, ChildSpec, Children, Setup, Supervisor, SupervisorOptions},
    wire,
};
use std::{
    cell::{Cell, RefCell},
    future::Future,
    rc::Rc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::LocalSet,
};

#[test]
#[ignore]
fn supervised_idle_child() {
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}
fn child_spec() -> ChildSpec {
    ChildSpec::new(std::env::current_exe().unwrap()).args([
        "--exact",
        "supervised_idle_child",
        "--ignored",
        "--nocapture",
    ])
}
fn field<'a>(value: &'a Value, name: &str) -> Option<&'a Value> {
    value
        .as_map()?
        .iter()
        .find(|(key, _)| key.as_str() == Some(name))
        .map(|(_, value)| value)
}
fn run(future: impl Future<Output = Result<()>>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(LocalSet::new().run_until(async {
            tokio::time::timeout(Duration::from_secs(15), future)
                .await
                .expect("supervision test deadline")
                .unwrap()
        }));
}
async fn until(mut predicate: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    panic!("condition timed out")
}
#[derive(Default)]
struct Run {
    requests: RefCell<Vec<String>>,
    close_delay: Cell<Duration>,
    drain_error: RefCell<Option<Error>>,
    children: RefCell<Children>,
    stops: Cell<usize>,
    cleanups: Cell<usize>,
    gate: RefCell<Option<Gate>>,
    failed: RefCell<Option<oneshot::Sender<Result<()>>>>,
}

fn setup(
    record: Rc<Run>,
    cleanup_fails: bool,
    stop_fails: bool,
    close_fails: bool,
) -> Result<Setup> {
    let receiver: Rc<RefCell<Option<mpsc::Sender<Received>>>> = Rc::new(RefCell::new(None));
    let send_to = receiver.clone();
    let logs = record.clone();
    let local_send = Box::new(move |to: &str, frame: Bytes| -> Result<()> {
        let value: Value = wire::decode_frame(&frame)?;
        let kind = field(&value, "type").and_then(Value::as_str).unwrap();
        let cid = field(&value, "cid").and_then(Value::as_str).unwrap();
        logs.requests.borrow_mut().push(format!("{to}/{kind}"));
        let reply = if kind == "close" && close_fails {
            LocalReply::Err {
                cid: cid.into(),
                error: Error::new(Code::SqliteError, "injected close failure"),
            }
        } else if kind == "capture" {
            LocalReply::Ack {
                cid: cid.into(),
                tick_count: None,
                outputs: None,
                selection: Some(Capture {
                    state: [0; 16],
                    receipt: [0; 16],
                    donor: ProcRole::A,
                }),
                receipt: None,
                outcomes: None,
            }
        } else if let Some(tick) = field(&value, "tickCount").and_then(Value::as_i64) {
            let mut outputs = Vec::new();
            if kind == "drain" {
                let client_id = field(&value, "clientId")
                    .and_then(Value::as_str)
                    .unwrap()
                    .to_owned();
                let payload = wire::encode_payload(&Envelope {
                    to: "gate".into(),
                    message: if let Some(error) = logs.drain_error.borrow().clone() {
                        ClientReply::Err { client_id, error }
                    } else {
                        ClientReply::Ack {
                            client_id,
                            result: None,
                        }
                    },
                })?;
                outputs.push(if to == "procA" {
                    Bytes::from(payload)
                } else {
                    let hash = wire::checksum(&payload);
                    Bytes::copy_from_slice(if to == "procB" {
                        &hash[..8]
                    } else {
                        &hash[8..]
                    })
                });
            }
            LocalReply::Ack {
                cid: cid.into(),
                tick_count: Some(tick),
                outputs: Some(outputs),
                selection: None,
                receipt: None,
                outcomes: None,
            }
        } else {
            LocalReply::ack(cid)
        };
        let receiver = send_to.borrow().as_ref().unwrap().clone();
        let message = Received {
            from: to.into(),
            frame: wire::encode_frame(&reply)?,
        };
        let delay = if kind == "close" {
            logs.close_delay.get()
        } else {
            Duration::ZERO
        };
        if delay.is_zero() {
            receiver
                .try_send(message)
                .map_err(|_| Error::new(Code::SendError, "mock local queue full"))?;
        } else {
            tokio::task::spawn_local(async move {
                tokio::time::sleep(delay).await;
                receiver.send(message).await.unwrap();
            });
        }
        Ok(())
    });
    let (gate, runner) = Gate::new(
        "1",
        vec!["1".into(), "2".into(), "3".into()],
        Box::new(|_, _| Ok(())),
        local_send,
        GateOptions {
            tick_interval: Duration::from_secs(60),
            local_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )?;
    *receiver.borrow_mut() = Some(gate.receiver());
    *record.gate.borrow_mut() = Some(gate.clone());
    let gate_task = tokio::task::spawn_local(runner.run());
    let (failed_tx, failed_rx) = oneshot::channel();
    *record.failed.borrow_mut() = Some(failed_tx);
    let ready = record.clone();
    let stop = record.clone();
    let cleanup = record.clone();
    Ok(Setup::new(gate, std::array::from_fn(|_| child_spec()))
        .ready(move |children| async move {
            *ready.children.borrow_mut() = children;
            Ok(())
        })
        .failed(async move { failed_rx.await.unwrap_or(Ok(())) })
        .stop(move || {
            stop.stops.set(stop.stops.get() + 1);
            assert!(stop.gate.borrow().as_ref().unwrap().is_stopped());
            if stop_fails {
                Err(Error::new(Code::SupervisorError, "injected stop failure"))
            } else {
                Ok(())
            }
        })
        .cleanup(move || async move {
            cleanup.cleanups.set(cleanup.cleanups.get() + 1);
            assert!(
                cleanup
                    .children
                    .borrow()
                    .values()
                    .all(|child| child.has_exited()),
                "cleanup runs only after actual owner exits"
            );
            gate_task.abort();
            if cleanup_fails {
                Err(Error::new(Code::FsError, "injected cleanup failure"))
            } else {
                Ok(())
            }
        }))
}
fn options() -> SupervisorOptions {
    SupervisorOptions {
        restart_delay: Duration::from_millis(5),
        startup_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

#[test]
fn forced_and_graceful_close_reap_four_compiled_children_and_retain_first_result() {
    run(async {
        for graceful in [false, true] {
            let record = Rc::new(Run::default());
            let copy = record.clone();
            let (supervisor, runner) = Supervisor::new(
                move || {
                    let copy = copy.clone();
                    async move { setup(copy, false, false, false) }
                },
                options(),
            )?;
            let closes = Rc::new(Cell::new(0));
            let count = closes.clone();
            supervisor.on_close(move || count.set(count.get() + 1))?;
            let task = tokio::task::spawn_local(runner.run());
            supervisor.open().await?;
            assert!(supervisor.gate().unwrap().is_open());
            assert_eq!(record.children.borrow().len(), 4);
            supervisor.close(graceful).await?;
            supervisor.close(!graceful).await?;
            assert!(supervisor.is_stopped());
            assert!(supervisor.gate().is_none());
            assert_eq!(record.cleanups.get(), 1);
            assert_eq!(record.stops.get(), 1);
            assert_eq!(closes.get(), 1);
            let closes = record
                .requests
                .borrow()
                .iter()
                .filter(|r| r.ends_with("/close"))
                .count();
            assert_eq!(closes, if graceful { 4 } else { 0 });
            assert!(supervisor.open().await.is_err());
            task.await.unwrap();
        }
        Ok(())
    })
}

#[test]
fn child_exit_restarts_only_after_old_owners_exit_and_cleanup() {
    run(async {
        let records = Rc::new(RefCell::new(Vec::<Rc<Run>>::new()));
        let copy = records.clone();
        let (supervisor, runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                async move {
                    for old in copy.borrow().iter() {
                        assert_eq!(old.cleanups.get(), 1);
                        assert!(
                            old.children
                                .borrow()
                                .values()
                                .all(|child| child.has_exited())
                        );
                    }
                    let record = Rc::new(Run::default());
                    copy.borrow_mut().push(record.clone());
                    setup(record, false, false, false)
                }
            },
            options(),
        )?;
        let opens = Rc::new(Cell::new(0));
        let count = opens.clone();
        supervisor.on_open(move |_| count.set(count.get() + 1))?;
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        let child = records.borrow()[0].children.borrow()[&ChildRole::Apply].clone();
        child.kill().await?;
        child.wait().await?;
        until(|| opens.get() >= 2).await;
        assert_eq!(records.borrow().len(), 2);
        supervisor.close(false).await?;
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn failed_transport_restarts_without_forwarding_its_diagnostic() {
    run(async {
        let records = Rc::new(RefCell::new(Vec::<Rc<Run>>::new()));
        let copy = records.clone();
        let (supervisor, mut runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                async move {
                    let record = Rc::new(Run::default());
                    copy.borrow_mut().push(record.clone());
                    setup(record, false, false, false)
                }
            },
            options(),
        )?;
        let errors = Rc::new(RefCell::new(Vec::new()));
        let copy = errors.clone();
        runner
            .diagnostics()
            .on_error(move |error| copy.borrow_mut().push(error.clone()));
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        records.borrow()[0]
            .failed
            .borrow_mut()
            .take()
            .unwrap()
            .send(Err(Error::new(
                Code::SendError,
                "transport already reported",
            )))
            .unwrap();
        until(|| records.borrow().len() == 2 && supervisor.gate().is_some_and(|g| g.is_open()))
            .await;
        assert!(errors.borrow().is_empty());
        supervisor.close(false).await?;
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn stop_and_cleanup_failures_are_terminal_but_do_not_skip_owners() {
    run(async {
        for (cleanup_fails, stop_fails) in [(true, false), (false, true)] {
            let record = Rc::new(Run::default());
            let copy = record.clone();
            let (supervisor, runner) = Supervisor::new(
                move || {
                    let copy = copy.clone();
                    async move { setup(copy, cleanup_fails, stop_fails, false) }
                },
                options(),
            )?;
            let task = tokio::task::spawn_local(runner.run());
            supervisor.open().await?;
            let error = supervisor.close(false).await.unwrap_err();
            assert_eq!(error.code, Some(Code::SupervisorError));
            assert_eq!(supervisor.close(true).await.unwrap_err(), error);
            assert_eq!(record.cleanups.get(), 1);
            assert!(
                record
                    .children
                    .borrow()
                    .values()
                    .all(|child| child.has_exited())
            );
            task.await.unwrap();
        }
        Ok(())
    })
}

#[test]
fn drain_succeeds_and_failed_graceful_close_still_cleans_up() {
    run(async {
        for close_fails in [false, true] {
            let record = Rc::new(Run::default());
            let copy = record.clone();
            let (supervisor, runner) = Supervisor::new(
                move || {
                    let copy = copy.clone();
                    async move { setup(copy, false, false, close_fails) }
                },
                options(),
            )?;
            let task = tokio::task::spawn_local(runner.run());
            supervisor.open().await?;
            if close_fails {
                let error = supervisor.close(true).await.unwrap_err();
                assert_eq!(error.code, Some(Code::SqliteError));
                assert_eq!(error.message, "injected close failure");
            } else {
                supervisor.drain_cmd().await?;
                supervisor.drain_cmd().await?;
            }
            assert_eq!(record.cleanups.get(), 1);
            assert!(
                record
                    .children
                    .borrow()
                    .values()
                    .all(|child| child.has_exited())
            );
            task.await.unwrap();
        }
        Ok(())
    })
}

#[test]
fn close_during_setup_waits_for_returned_resource_cleanup_without_spawning() {
    run(async {
        let record = Rc::new(Run::default());
        let copy = record.clone();
        let (release_tx, release_rx) = oneshot::channel();
        let mut release = Some(release_rx);
        let entered = Rc::new(Cell::new(false));
        let copy_entered = entered.clone();
        let (supervisor, runner) = Supervisor::new(
            move || {
                let release = release.take().unwrap();
                let copy = copy.clone();
                copy_entered.set(true);
                async move {
                    let _ = release.await;
                    setup(copy, false, false, false)
                }
            },
            options(),
        )?;
        let task = tokio::task::spawn_local(runner.run());
        let opening = supervisor.open();
        until(|| entered.get()).await;
        let closing = supervisor.close(false);
        release_tx.send(()).unwrap();
        closing.await?;
        assert!(opening.await.is_err());
        assert!(record.children.borrow().is_empty());
        assert_eq!(record.cleanups.get(), 1);
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn startup_timeout_cancels_readiness_and_retries() {
    run(async {
        let records = Rc::new(RefCell::new(Vec::<Rc<Run>>::new()));
        let copy = records.clone();
        let attempts = Rc::new(Cell::new(0));
        let count = attempts.clone();
        let (supervisor, runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                let first = count.get() == 0;
                count.set(count.get() + 1);
                async move {
                    let record = Rc::new(Run::default());
                    copy.borrow_mut().push(record.clone());
                    let setup = setup(record.clone(), false, false, false)?;
                    if first {
                        Ok(setup.ready(move |children| async move {
                            *record.children.borrow_mut() = children;
                            std::future::pending().await
                        }))
                    } else {
                        Ok(setup)
                    }
                }
            },
            SupervisorOptions {
                startup_timeout: Duration::from_millis(80),
                ..options()
            },
        )?;
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        assert!(records.borrow().len() >= 2);
        assert_eq!(records.borrow()[0].cleanups.get(), 1);
        supervisor.close(false).await?;
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn spawn_failure_retries_with_fresh_setup() {
    run(async {
        let records = Rc::new(RefCell::new(Vec::<Rc<Run>>::new()));
        let copy = records.clone();
        let (supervisor, mut runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                async move {
                    let first = copy.borrow().is_empty();
                    let record = Rc::new(Run::default());
                    copy.borrow_mut().push(record.clone());
                    let mut setup = setup(record, false, false, false)?;
                    if first {
                        setup.children[2] = ChildSpec::new("/nonexistent/monsterft-test-program");
                    }
                    Ok(setup)
                }
            },
            options(),
        )?;
        let errors = Rc::new(RefCell::new(Vec::new()));
        let copy = errors.clone();
        runner
            .diagnostics()
            .on_error(move |e| copy.borrow_mut().push(e.clone()));
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        assert_eq!(records.borrow().len(), 2);
        assert_eq!(records.borrow()[0].cleanups.get(), 1);
        assert!(errors.borrow().iter().any(|e|e.code==Some(Code::SupervisorError)&&e.message.contains("procC spawn")));
        supervisor.close(false).await?;
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn close_cancels_retry_and_dropped_waiter_does_not_cancel_cleanup() {
    run(async {
        let attempts = Rc::new(Cell::new(0));
        let copy = attempts.clone();
        let (supervisor, runner) = Supervisor::new(
            move || {
                copy.set(copy.get() + 1);
                async { Err(Error::new(Code::FsError, "setup unavailable")) }
            },
            SupervisorOptions {
                restart_delay: Duration::from_secs(60),
                ..options()
            },
        )?;
        let task = tokio::task::spawn_local(runner.run());
        let opening = supervisor.open();
        until(|| attempts.get() == 1).await;
        drop(supervisor.close(false));
        assert!(opening.await.is_err());
        supervisor.close(true).await?;
        assert_eq!(attempts.get(), 1);
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn automatic_cleanup_failure_prevents_replacement() {
    run(async {
        let record = Rc::new(Run::default());
        let copy = record.clone();
        let attempts = Rc::new(Cell::new(0));
        let count = attempts.clone();
        let (supervisor, runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                count.set(count.get() + 1);
                async move { setup(copy, true, false, false) }
            },
            options(),
        )?;
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        let child = record.children.borrow()[&ChildRole::ProcA].clone();
        child.kill().await?;
        until(|| supervisor.is_stopped()).await;
        assert_eq!(attempts.get(), 1);
        assert!(
            supervisor
                .close(false)
                .await
                .unwrap_err()
                .message
                .contains("cleanup failure")
        );
        assert!(
            record
                .children
                .borrow()
                .values()
                .all(|child| child.has_exited())
        );
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn drain_waits_for_delayed_coordinated_close_after_gate_stops() {
    run(async {
        let record = Rc::new(Run::default());
        record.close_delay.set(Duration::from_millis(20));
        let copy = record.clone();
        let (supervisor, runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                async move { setup(copy, false, false, false) }
            },
            options(),
        )?;
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        let gate = supervisor.gate().unwrap();
        let drain = supervisor.drain_cmd();
        until(|| gate.is_stopped()).await;
        assert_eq!(record.cleanups.get(), 0);
        assert!(
            record
                .children
                .borrow()
                .values()
                .all(|child| !child.has_exited())
        );
        drain.await?;
        assert_eq!(record.cleanups.get(), 1);
        assert_eq!(
            record
                .requests
                .borrow()
                .iter()
                .filter(|r| r.ends_with("/close"))
                .count(),
            4
        );
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn listener_panics_preserve_later_diagnostics_and_reentrant_close() {
    run(async {
        let record = Rc::new(Run::default());
        let copy = record.clone();
        let (supervisor, mut runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                async move { setup(copy, false, false, false) }
            },
            options(),
        )?;
        let diagnostics = Rc::new(RefCell::new(Vec::new()));
        runner.diagnostics().on_error(|_| panic!("error listener"));
        let copy = diagnostics.clone();
        runner
            .diagnostics()
            .on_error(move |e| copy.borrow_mut().push(e.clone()));
        supervisor.on_open(|_| panic!("open listener"))?;
        let copy = supervisor.clone();
        supervisor.on_open(move |_| {
            drop(copy.close(false));
        })?;
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        supervisor.close(false).await?;
        task.await.unwrap();
        assert_eq!(record.cleanups.get(), 1);
        assert_eq!(diagnostics.borrow().len(), 1);
        assert_eq!(diagnostics.borrow()[0].code, Some(Code::SupervisorError));
        Ok(())
    })
}

#[test]
fn failed_drain_preserves_original_error_after_cleanup_failure() {
    run(async {
        let record = Rc::new(Run::default());
        *record.drain_error.borrow_mut() =
            Some(Error::new(Code::RepairOutside, "repair blocked drain"));
        let copy = record.clone();
        let (supervisor, mut runner) = Supervisor::new(
            move || {
                let copy = copy.clone();
                async move { setup(copy, true, false, false) }
            },
            options(),
        )?;
        let diagnostics = Rc::new(RefCell::new(Vec::new()));
        let copy = diagnostics.clone();
        runner
            .diagnostics()
            .on_error(move |e| copy.borrow_mut().push(e.clone()));
        let task = tokio::task::spawn_local(runner.run());
        supervisor.open().await?;
        let error = supervisor.drain_cmd().await.unwrap_err();
        assert_eq!(error.code, Some(Code::RepairOutside));
        assert_eq!(error.message, "repair blocked drain");
        assert_eq!(record.cleanups.get(), 1);
        assert!(record.children.borrow().values().all(|c| c.has_exited()));
        assert_eq!(diagnostics.borrow().len(), 1);
        assert_eq!(diagnostics.borrow()[0].message, "injected cleanup failure");
        task.await.unwrap();
        Ok(())
    })
}

#[test]
fn terminal_cleanup_errors_observe_stopped_and_reentrant_close_keeps_failure() {
    run(async {
        for stop_fails in [false, true] {
            let record = Rc::new(Run::default());
            let copy = record.clone();
            let (supervisor, mut runner) = Supervisor::new(
                move || {
                    let copy = copy.clone();
                    async move { setup(copy, !stop_fails, stop_fails, false) }
                },
                options(),
            )?;
            let observations = Rc::new(RefCell::new(Vec::new()));
            let copy = observations.clone();
            let reentrant = Rc::new(RefCell::new(None));
            let closing = reentrant.clone();
            let owner = supervisor.clone();
            runner.diagnostics().on_error(move |error| {
                copy.borrow_mut()
                    .push((error.message.clone(), owner.is_stopped()));
                if error.message.starts_with("injected") {
                    *closing.borrow_mut() = Some(owner.close(false));
                }
            });
            let closes = Rc::new(Cell::new(0));
            let count = closes.clone();
            supervisor.on_close(move || count.set(count.get() + 1))?;
            let task = tokio::task::spawn_local(runner.run());
            supervisor.open().await?;
            let child = record.children.borrow()[&ChildRole::Apply].clone();
            child.kill().await?;
            task.await.unwrap();
            let failure = {
                let errors = observations.borrow();
                assert_eq!(errors.len(), 2);
                assert!(!errors[0].1, "ordinary retry diagnostics stay nonterminal");
                assert!(errors[1].1, "cleanup diagnostic must observe stopped state");
                errors[1].0.clone()
            };
            let close = reentrant
                .borrow_mut()
                .take()
                .expect("listener requested close");
            assert_eq!(close.await.unwrap_err().message, failure);
            assert_eq!(supervisor.close(true).await.unwrap_err().message, failure);
            assert!(supervisor.open().await.is_err());
            assert_eq!(record.cleanups.get(), 1);
            assert_eq!(closes.get(), 0);
        }
        Ok(())
    })
}
