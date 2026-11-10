use bytes::Bytes;
use futures_util::FutureExt;
use monsterft::{
    Value,
    error::{Code, Error},
    gate::{EventKind, Gate, GateEvent, GateOptions, GateRunner},
    local::{LocalReply, OrderedInput, PROCS},
    storage::{Capture, ProcRole},
    wire,
};
use std::{cell::RefCell, rc::Rc, time::Duration};
use tokio::{sync::mpsc, task::LocalSet};

type Frame = (String, Bytes);
struct Harness {
    gate: Gate,
    runner: Option<GateRunner>,
    local: mpsc::UnboundedReceiver<Frame>,
    remote: mpsc::UnboundedReceiver<Frame>,
    errors: mpsc::UnboundedReceiver<Error>,
}
impl Harness {
    fn new() -> Self {
        Self::with_options(GateOptions {
            tick_interval: Duration::from_secs(3600),
            ..Default::default()
        })
    }
    fn with_options(options: GateOptions) -> Self {
        let (local_tx, local) = mpsc::unbounded_channel();
        let (remote_tx, remote) = mpsc::unbounded_channel();
        let (error_tx, errors) = mpsc::unbounded_channel();
        let (gate, mut runner) = Gate::new(
            "a",
            vec!["a".into(), "b".into(), "c".into()],
            Box::new(move |to, frame| {
                remote_tx.send((to.into(), frame)).unwrap();
                Ok(())
            }),
            Box::new(move |to, frame| {
                local_tx.send((to.into(), frame)).unwrap();
                Ok(())
            }),
            options,
        )
        .unwrap();
        runner.diagnostics().on_error(move |error| {
            error_tx.send(error.clone()).unwrap();
        });
        Self {
            gate,
            runner: Some(runner),
            local,
            remote,
            errors,
        }
    }
    fn start(&mut self) -> tokio::task::JoinHandle<()> {
        tokio::task::spawn_local(self.runner.take().unwrap().run())
    }
    async fn open(&mut self) {
        let opening = self.gate.open();
        let mut starts = Vec::new();
        while starts.len() < 3 {
            let (to, frame) = self.local.recv().await.unwrap();
            let value: Value = wire::decode_frame(&frame).unwrap();
            let fields = value.as_map().unwrap();
            let field = |name: &str| {
                fields
                    .iter()
                    .find(|(key, _)| key.as_str() == Some(name))
                    .map(|(_, value)| value)
                    .unwrap()
            };
            let kind = field("type").as_str().unwrap();
            let cid = field("cid").as_str().unwrap();
            if kind == "start" {
                starts.push((to, wire::decode_frame::<OrderedInput>(&frame).unwrap()));
                continue;
            }
            let mut reply = LocalReply::ack(cid);
            if kind == "capture"
                && let LocalReply::Ack { selection, .. } = &mut reply
            {
                *selection = Some(Capture {
                    state: [1; 16],
                    receipt: [2; 16],
                    donor: ProcRole::A,
                });
            }
            self.gate
                .on_receive(to, wire::encode_frame(&reply).unwrap())
                .unwrap();
        }
        for (to, input) in starts {
            self.complete(&to, &input, vec![]);
        }
        opening.await.unwrap();
    }
    fn complete(&self, to: &str, input: &OrderedInput, outputs: Vec<Bytes>) {
        let reply = LocalReply::Ack {
            cid: input.cid.clone(),
            tick_count: Some(input.tick_count),
            outputs: Some(outputs),
            selection: None,
            receipt: None,
            outcomes: None,
        };
        self.gate
            .on_receive(to, wire::encode_frame(&reply).unwrap())
            .unwrap();
    }
    async fn next_input(&mut self) -> Vec<(String, OrderedInput)> {
        let mut result = Vec::new();
        for _ in 0..3 {
            let (to, frame) = self.local.recv().await.unwrap();
            result.push((to, wire::decode_frame(&frame).unwrap()));
        }
        result
    }
    fn trigger(&self) {
        self.gate
            .on_receive("b", wire::encode_frame(&remote_value()).unwrap())
            .unwrap();
    }
    async fn close(&mut self) {
        let closing = self.gate.close();
        for _ in 0..4 {
            let (to, frame) = self.local.recv().await.unwrap();
            let request: monsterft::local::StartupRequest = wire::decode_frame(&frame).unwrap();
            assert!(matches!(
                request,
                monsterft::local::StartupRequest::Close { .. }
            ));
            self.gate
                .on_receive(
                    to,
                    wire::encode_frame(&LocalReply::ack(request.cid())).unwrap(),
                )
                .unwrap();
        }
        closing.await.unwrap();
    }
}
fn remote_value() -> Value {
    Value::Map(vec![
        ("to".into(), "b".into()),
        (
            "message".into(),
            Value::Map(vec![
                ("type".into(), "vote".into()),
                ("from".into(), "a".into()),
                ("term".into(), 1.into()),
                ("voteGranted".into(), true.into()),
            ]),
        ),
    ])
}
fn outputs(payloads: &[Bytes], role: &str) -> Vec<Bytes> {
    payloads
        .iter()
        .map(|payload| match role {
            "procA" => payload.clone(),
            "procB" => Bytes::copy_from_slice(&wire::checksum(payload)[..8]),
            "procC" => Bytes::copy_from_slice(&wire::checksum(payload)[8..]),
            _ => panic!("unknown role"),
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn full_output_list_agreement_precedes_any_release() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            h.trigger();
            let requests = h.next_input().await;
            let payload = Bytes::from(wire::encode_payload(&remote_value()).unwrap());
            let payloads = vec![payload.clone(), payload];
            for (to, input) in requests {
                let mut reported = outputs(&payloads, &to);
                if to == "procC" {
                    let mut corrupted = reported[1].to_vec();
                    corrupted[0] ^= 1;
                    reported[1] = corrupted.into();
                }
                h.complete(&to, &input, reported);
            }
            assert_eq!(
                h.errors.recv().await.unwrap().code,
                Some(Code::LocalDivergence)
            );
            assert!(h.remote.try_recv().is_err());
            assert!(h.local.try_recv().is_err());
            assert!(h.gate.is_stopped());
            // Fatal handling does not coordinate shutdown; explicit cleanup still does.
            h.close().await;
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn third_completion_gates_outputs_and_the_next_input() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            h.trigger();
            let requests = h.next_input().await;
            let payload = Bytes::from(wire::encode_payload(&remote_value()).unwrap());
            for (to, input) in &requests[..2] {
                h.complete(to, input, outputs(std::slice::from_ref(&payload), to));
            }
            h.trigger();
            tokio::task::yield_now().await;
            assert!(h.remote.try_recv().is_err());
            assert!(h.local.try_recv().is_err());
            let (to, input) = &requests[2];
            h.complete(to, input, outputs(std::slice::from_ref(&payload), to));
            let (to, frame) = h.remote.recv().await.unwrap();
            assert_eq!(to, "b");
            assert_eq!(&frame[16..], payload.as_ref());
            assert_eq!(&frame[..16], wire::checksum(&payload));
            let next = h.next_input().await;
            assert!(
                next.iter()
                    .all(|(_, next)| next.tick_count == input.tick_count + 1)
            );
            for (to, input) in next {
                h.complete(&to, &input, vec![]);
            }
            h.close().await;
            assert!(h.errors.try_recv().is_err());
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn stopped_gate_keeps_close_wait_until_explicit_successful_cleanup() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            let wait = h.gate.await_event(EventKind::Close, |_| Ok(true));
            h.gate
                .on_receive(PROCS[0], Bytes::from_static(b"bad local frame"))
                .unwrap();
            assert_eq!(
                h.errors.recv().await.unwrap().code,
                Some(Code::FrameCorrupt)
            );
            assert!(h.local.try_recv().is_err());
            h.close().await;
            assert_eq!(wait.await.unwrap(), GateEvent::Close);
            assert_eq!(
                h.gate.open().await.unwrap_err().code,
                Some(Code::FrameCorrupt)
            );
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn late_close_wait_does_not_reinvoke_earlier_false_conditions() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            let calls = Rc::new(RefCell::new(0));
            let counter = calls.clone();
            let older = h.gate.await_event(EventKind::Close, move |_| {
                *counter.borrow_mut() += 1;
                Ok(false)
            });
            h.gate.close().await.unwrap();
            assert_eq!(*calls.borrow(), 1);
            assert_eq!(
                h.gate
                    .await_event(EventKind::Close, |_| Ok(true))
                    .await
                    .unwrap(),
                GateEvent::Close
            );
            assert_eq!(
                *calls.borrow(),
                1,
                "registration is not another close event"
            );
            drop(older);
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn panicking_local_transport_stops_gate_and_reports_local_failure() {
    LocalSet::new()
        .run_until(async {
            let (gate, mut runner) = Gate::new(
                "a",
                vec!["a".into(), "b".into(), "c".into()],
                Box::new(|_, _| Ok(())),
                Box::new(|_, _| panic!("local transport panicked")),
                GateOptions::default(),
            )
            .unwrap();
            let errors = Rc::new(RefCell::new(Vec::new()));
            let capture = errors.clone();
            runner
                .diagnostics()
                .on_error(move |error| capture.borrow_mut().push(error.clone()));
            let driver = tokio::task::spawn_local(runner.run());
            assert!(gate.open().await.is_err());
            assert!(
                gate.is_stopped(),
                "transport panic must not leave an apparently live owner"
            );
            assert_eq!(errors.borrow().len(), 1);
            assert!(
                errors.borrow()[0]
                    .message
                    .contains("local transport panicked")
            );
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn input_timeout_stops_gate_without_automatic_close() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::with_options(GateOptions {
                local_timeout: Duration::from_millis(30),
                tick_interval: Duration::from_secs(3600),
                ..Default::default()
            });
            let driver = h.start();
            h.open().await;
            h.trigger();
            let requests = h.next_input().await;
            for (to, input) in &requests[..2] {
                h.complete(to, input, vec![]);
            }
            assert_eq!(
                h.errors.recv().await.unwrap().code,
                Some(Code::LocalTimeout)
            );
            assert!(h.local.try_recv().is_err());
            assert!(h.gate.is_stopped());
            h.close().await;
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_close_failure_preserves_details_and_is_not_retried() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            let wait = h.gate.await_event(EventKind::Close, |_| Ok(true));
            let closing = h.gate.close();
            let expected = Error::new(Code::FsError, "database owner close failed");
            for index in 0..4 {
                let (to, frame) = h.local.recv().await.unwrap();
                let request: monsterft::local::StartupRequest = wire::decode_frame(&frame).unwrap();
                let reply = if index == 0 {
                    LocalReply::Err {
                        cid: request.cid().into(),
                        error: expected.clone(),
                    }
                } else {
                    LocalReply::ack(request.cid())
                };
                h.gate
                    .on_receive(to, wire::encode_frame(&reply).unwrap())
                    .unwrap();
            }
            assert_eq!(closing.await.unwrap_err(), expected);
            assert_eq!(wait.await.unwrap_err(), expected);
            assert_eq!(h.gate.close().await.unwrap_err(), expected);
            assert!(h.local.try_recv().is_err());
            assert!(
                h.errors.try_recv().is_err(),
                "owner diagnostic is not mirrored by Gate"
            );
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn dropped_drain_waiter_does_not_cancel_shutdown_or_skip_final_outputs() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            let closed = h.gate.await_event(EventKind::Close, |_| Ok(true));
            drop(h.gate.drain_cmd());
            let requests = h.next_input().await;
            let monsterft::local::Input::Drain { client_id } = &requests[0].1.input else {
                panic!("expected drain")
            };
            let ack = Value::Map(vec![
                ("to".into(), "gate".into()),
                (
                    "message".into(),
                    Value::Map(vec![
                        ("type".into(), "ack".into()),
                        ("clientId".into(), client_id.as_str().into()),
                    ]),
                ),
            ]);
            let payloads = vec![
                Bytes::from(wire::encode_payload(&ack).unwrap()),
                Bytes::from(wire::encode_payload(&remote_value()).unwrap()),
            ];
            for (to, input) in requests {
                h.complete(&to, &input, outputs(&payloads, &to));
            }
            assert_eq!(h.remote.recv().await.unwrap().0, "b");
            for _ in 0..4 {
                let (to, frame) = h.local.recv().await.unwrap();
                let request: monsterft::local::StartupRequest = wire::decode_frame(&frame).unwrap();
                assert!(matches!(
                    request,
                    monsterft::local::StartupRequest::Close { .. }
                ));
                h.gate
                    .on_receive(
                        to,
                        wire::encode_frame(&LocalReply::ack(request.cid())).unwrap(),
                    )
                    .unwrap();
            }
            assert_eq!(closed.await.unwrap(), GateEvent::Close);
            h.gate.drain_cmd().await.unwrap();
            assert!(h.errors.try_recv().is_err());
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn leader_waits_observe_final_change_not_intermediate_commit_status() {
    use monsterft::protocol::{Envelope, Event, State, Status};
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            let mut plain = h.gate.await_leader(false);
            let mut committed = h.gate.await_leader(true);
            let intermediate = Status {
                state: State::Leader,
                leader: Some("a".into()),
                term: 2,
                seq: 1,
                commit_seq: 1,
                commit_term: 2,
                followers: vec!["b".into()],
            };
            let final_state = Status {
                state: State::Follower,
                leader: None,
                term: 3,
                seq: 1,
                commit_seq: 1,
                commit_term: 2,
                followers: vec![],
            };
            let observed = Rc::new(RefCell::new(Vec::new()));
            let captured = observed.clone();
            let gate = h.gate.clone();
            h.gate
                .on(EventKind::Commit, move |_| {
                    captured.borrow_mut().push(gate.status())
                })
                .unwrap();
            let (notify, mut changes) = mpsc::unbounded_channel();
            h.gate
                .on(EventKind::Change, move |_| {
                    notify.send(()).unwrap();
                })
                .unwrap();
            let payloads = vec![
                Bytes::from(
                    wire::encode_payload(&Envelope {
                        to: "gate".into(),
                        message: Event::Commit {
                            status: intermediate.clone(),
                            seq: 1,
                        },
                    })
                    .unwrap(),
                ),
                Bytes::from(
                    wire::encode_payload(&Envelope {
                        to: "gate".into(),
                        message: Event::Change {
                            status: final_state.clone(),
                        },
                    })
                    .unwrap(),
                ),
            ];
            h.trigger();
            for (to, input) in h.next_input().await {
                h.complete(&to, &input, outputs(&payloads, &to));
            }
            changes.recv().await.unwrap();
            assert!((&mut plain).now_or_never().is_none());
            assert!((&mut committed).now_or_never().is_none());
            assert_eq!(h.gate.status(), final_state);
            assert_eq!(*observed.borrow(), vec![intermediate]);
            let final_state = Status {
                leader: Some("b".into()),
                commit_term: 3,
                ..final_state
            };
            let payloads = vec![Bytes::from(
                wire::encode_payload(&Envelope {
                    to: "gate".into(),
                    message: Event::Change {
                        status: final_state,
                    },
                })
                .unwrap(),
            )];
            h.trigger();
            for (to, input) in h.next_input().await {
                h.complete(&to, &input, outputs(&payloads, &to));
            }
            plain.await.unwrap();
            committed.await.unwrap();
            h.close().await;
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_owner_does_not_leave_a_live_public_handle() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            let stopped = h.gate.wait_stopped();
            driver.abort();
            let _ = driver.await;
            assert_eq!(stopped.await.code, Some(Code::NodeNotOpen));
            assert!(h.gate.is_stopped());
            assert!(!h.gate.is_open());
            assert!(
                h.gate
                    .append(Bytes::from_static(b"not admitted"))
                    .await
                    .is_err()
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn warning_listener_failure_stops_gate_before_later_listeners() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let observed = Rc::new(RefCell::new(false));
            h.runner
                .as_mut()
                .unwrap()
                .diagnostics()
                .on_warn(|_| panic!("warning listener failed"));
            let gate = h.gate.clone();
            let later = observed.clone();
            h.runner.as_mut().unwrap().diagnostics().on_warn(move |_| {
                *later.borrow_mut() = gate.is_stopped();
            });
            h.runner
                .as_mut()
                .unwrap()
                .diagnostics()
                .on_error(|_| panic!("error listener failed"));
            let (notice, mut received) = mpsc::unbounded_channel();
            h.runner.as_mut().unwrap().diagnostics().on_error(move |_| {
                notice.send(()).unwrap();
            });
            let driver = h.start();
            h.open().await;
            h.gate
                .on_receive("b", Bytes::from_static(b"bad remote checksum"))
                .unwrap();
            received.recv().await.unwrap();
            assert!(h.gate.is_stopped());
            assert!(*observed.borrow());
            assert!(
                h.errors
                    .recv()
                    .await
                    .unwrap()
                    .message
                    .contains("warning listener failed")
            );
            assert!(h.local.try_recv().is_err());
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn event_listener_failure_stops_gate_before_later_listeners_or_outputs() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            h.gate
                .on(EventKind::Commit, |_| panic!("commit listener failed"))
                .unwrap();
            let gate = h.gate.clone();
            let (notice, mut received) = mpsc::unbounded_channel();
            h.gate
                .on(EventKind::Commit, move |_| {
                    notice.send(gate.is_stopped()).unwrap();
                })
                .unwrap();
            let status = monsterft::protocol::Status {
                state: monsterft::protocol::State::Follower,
                leader: Some("b".into()),
                term: 1,
                seq: 0,
                commit_seq: 0,
                commit_term: 1,
                followers: vec![],
            };
            let payloads = vec![
                Bytes::from(
                    wire::encode_payload(&monsterft::protocol::Envelope {
                        to: "gate".into(),
                        message: monsterft::protocol::Event::Commit { status, seq: 0 },
                    })
                    .unwrap(),
                ),
                Bytes::from(wire::encode_payload(&remote_value()).unwrap()),
            ];
            h.trigger();
            for (to, input) in h.next_input().await {
                h.complete(&to, &input, outputs(&payloads, &to));
            }
            assert!(received.recv().await.unwrap());
            assert!(h.gate.is_stopped());
            assert!(
                h.errors
                    .recv()
                    .await
                    .unwrap()
                    .message
                    .contains("commit listener failed")
            );
            assert!(h.remote.try_recv().is_err());
            assert!(h.local.try_recv().is_err());
            driver.abort();
        })
        .await;
}

fn shutdown_event_payload(kind: EventKind) -> Bytes {
    use monsterft::protocol::{Envelope, Event, State, Status};
    let status = Status {
        state: State::Follower,
        leader: Some("b".into()),
        term: 1,
        seq: 1,
        commit_seq: 1,
        commit_term: 1,
        followers: vec![],
    };
    let message = match kind {
        EventKind::Commit => Event::Commit { status, seq: 1 },
        EventKind::Cmd => Event::Cmd {
            status,
            cmd_seq: 1,
            cmd_count: 1,
        },
        EventKind::Change => Event::Change { status },
        _ => unreachable!(),
    };
    wire::encode_payload(&Envelope {
        to: "gate".into(),
        message,
    })
    .unwrap()
    .into()
}

#[tokio::test(flavor = "current_thread")]
async fn listener_shutdown_is_immediate_and_skips_later_outputs_and_inputs() {
    LocalSet::new()
        .run_until(async {
            for kind in [EventKind::Change, EventKind::Commit, EventKind::Cmd] {
                for close in [false, true] {
                    let mut h = Harness::new();
                    let driver = h.start();
                    h.open().await;
                    let gate = h.gate.clone();
                    h.gate
                        .on(kind, move |_| {
                            if close {
                                drop(gate.close());
                            } else {
                                gate.stop().unwrap();
                            }
                            assert!(gate.is_stopped());
                            assert!(!gate.is_open());
                        })
                        .unwrap();
                    let gate = h.gate.clone();
                    let (notice, mut received) = mpsc::unbounded_channel();
                    h.gate
                        .on(kind, move |_| {
                            notice.send(gate.is_stopped()).unwrap();
                        })
                        .unwrap();
                    let pending = h.gate.append_batch(vec![Bytes::from_static(b"command")]);
                    let inputs = h.next_input().await;
                    // Already queued work must not start after this input's listener stops Gate.
                    h.trigger();
                    let payloads = vec![
                        shutdown_event_payload(kind),
                        wire::encode_payload(&remote_value()).unwrap().into(),
                    ];
                    for (to, input) in inputs {
                        h.complete(&to, &input, outputs(&payloads, &to));
                    }
                    assert!(received.recv().await.unwrap());
                    assert_eq!(pending.await.unwrap_err().code, Some(Code::NodeNotOpen));
                    assert!(h.remote.try_recv().is_err());
                    assert!(h.errors.try_recv().is_err());
                    if !close {
                        assert!(h.local.try_recv().is_err());
                    }
                    // Accepted close survives dropping its result; explicit close after stop still works.
                    h.close().await;
                    assert!(h.local.try_recv().is_err());
                    driver.abort();
                }
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn condition_shutdown_rejects_itself_and_all_other_ordinary_waits() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            let gate = h.gate.clone();
            let waiting = h.gate.await_event(EventKind::Cmd, move |_| {
                drop(gate.close());
                Ok(true)
            });
            let other = h.gate.await_event(EventKind::Change, |_| {
                panic!("cancelled predicate must not run")
            });
            let pending = h.gate.append_batch(vec![Bytes::from_static(b"command")]);
            let inputs = h.next_input().await;
            let payloads = vec![
                shutdown_event_payload(EventKind::Cmd),
                shutdown_event_payload(EventKind::Change),
            ];
            for (to, input) in inputs {
                h.complete(&to, &input, outputs(&payloads, &to));
            }
            assert_eq!(waiting.await.unwrap_err().code, Some(Code::NodeNotOpen));
            assert_eq!(other.await.unwrap_err().code, Some(Code::NodeNotOpen));
            assert_eq!(pending.await.unwrap_err().code, Some(Code::NodeNotOpen));
            assert!(h.errors.try_recv().is_err());
            h.close().await;
            driver.abort();
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn rejected_shutdown_admission_does_not_mark_gate_stopped() {
    let h = Harness::with_options(GateOptions {
        mailbox_capacity: 1,
        ..Default::default()
    });
    h.gate.on(EventKind::Change, |_| {}).unwrap();
    assert_eq!(
        h.gate.close().await.unwrap_err().code,
        Some(Code::SendError)
    );
    assert!(!h.gate.is_stopped());
    assert!(h.gate.stop().is_err());
    assert!(!h.gate.is_stopped());
}

#[tokio::test(flavor = "current_thread")]
async fn accepted_stop_is_immediate_and_preserves_existing_fatal_error() {
    LocalSet::new()
        .run_until(async {
            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            h.gate.stop().unwrap();
            assert!(h.gate.is_stopped());
            assert!(!h.gate.is_open());
            let error = h.gate.wait_stopped().await;
            assert_eq!(error.code, Some(Code::NodeNotOpen));
            h.gate.stop().unwrap();
            assert_eq!(h.gate.wait_stopped().await, error);
            h.close().await;
            driver.abort();

            let mut h = Harness::new();
            let driver = h.start();
            h.open().await;
            h.gate
                .on_receive("procA", Bytes::from_static(b"bad local frame"))
                .unwrap();
            let error = h.errors.recv().await.unwrap();
            h.gate.stop().unwrap();
            assert_eq!(h.gate.wait_stopped().await, error);
            h.close().await;
            assert_eq!(h.gate.wait_stopped().await, error);
            driver.abort();
        })
        .await;
}
