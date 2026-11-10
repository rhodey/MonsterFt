use bytes::Bytes;
use monsterft::{
    Value,
    apply::{Apply, ApplyContext, apply_async, apply_sync},
    error::{Code, Error},
    protocol::{Execute, Outcome},
    rusqlite::Connection,
};
use std::{cell::RefCell, rc::Rc};

fn request(seq: i64, items: &[&[u8]]) -> Execute {
    Execute {
        cid: format!("execute-{seq}"),
        tick_count: seq + 1,
        term: 1,
        seq,
        entry_hash: [seq as u8 + 1; 16],
        items: if seq == 0 {
            None
        } else {
            Some(items.iter().map(|x| Bytes::copy_from_slice(x)).collect())
        },
        match_index: if seq == 0 { None } else { Some(vec![-1; 3]) },
    }
}
fn initialize_locals(path: &std::path::Path) {
    use monsterft::storage::{ProcRole, ProtocolStore};
    for role in ProcRole::ALL {
        let mut store = ProtocolStore::new(role, path, 1024).unwrap();
        store.open(&[1; 16]).unwrap();
        store.close().unwrap();
    }
}
fn setup(db: &Connection) -> monsterft::error::Result<Value> {
    db.execute_batch("CREATE TABLE counter(id INTEGER PRIMARY KEY, value INTEGER NOT NULL); INSERT INTO counter VALUES(1,0)")?;
    Ok(Value::Nil)
}

#[tokio::test(flavor = "current_thread")]
async fn atomic_receipt_rejections_and_no_reexecution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let warnings = Rc::new(RefCell::new(Vec::new()));
    let warnings2 = warnings.clone();
    let calls = Rc::new(RefCell::new(Vec::new()));
    let calls2 = calls.clone();
    let owner_thread = std::thread::current().id();
    let callback = apply_sync(
        move |db: &Connection, command: Option<&[u8]>, ctx: &ApplyContext| {
            assert_eq!(std::thread::current().id(), owner_thread);
            calls2.borrow_mut().push((ctx.seq, ctx.index));
            let Some(command) = command else {
                return setup(db);
            };
            db.execute("UPDATE counter SET value=value+1", [])?;
            if command == b"reject" {
                return Err(Error {
                    message: "no thanks".into(),
                    code: Some(Code::Other(71)),
                    sql_code: None,
                    stack: Some("shared application stack".into()),
                });
            }
            Ok(db
                .query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?
                .into())
        },
    );
    let mut apply = Apply::new("one", &path, callback).unwrap();
    apply
        .diagnostics()
        .on_warn(move |e| warnings2.borrow_mut().push(e.clone()));
    apply.hello([1; 16]).unwrap();
    apply.open().unwrap();
    apply.ready().unwrap();
    apply.execute(request(0, &[])).await.unwrap();
    let result = apply
        .execute(request(1, &[b"ok", b"reject", b"ok"]))
        .await
        .unwrap();
    assert_eq!(result.outcomes[0], Outcome::Fulfilled(1.into()));
    assert_eq!(result.outcomes[2], Outcome::Fulfilled(2.into()));
    let Outcome::Rejected(error) = &result.outcomes[1] else {
        panic!("expected rejection");
    };
    assert_eq!(error.code, Some(Code::Other(71)));
    assert_eq!(error.stack.as_deref(), Some("shared application stack"));
    assert_eq!(warnings.borrow().len(), 1);
    assert!(apply.is_open());
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        db.query_row("SELECT seq FROM monsterft_meta_apply", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        1
    );
    apply.close().unwrap();
    let mut reopened = Apply::new(
        "one",
        &path,
        apply_sync(|_, _, _| panic!("must not reexecute committed receipt")),
    )
    .unwrap();
    reopened.hello([2; 16]).unwrap();
    reopened.open().unwrap();
    reopened.ready().unwrap();
    assert_eq!(
        reopened
            .execute(request(1, &[b"ok"]))
            .await
            .unwrap_err()
            .code,
        Some(Code::MonsterIllegal)
    );
    assert!(reopened.is_stopped());
}

#[tokio::test(flavor = "current_thread")]
async fn async_callback_is_local_and_stop_prevents_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let local = Rc::new(RefCell::new(0));
    let capture = local.clone();
    let thread = std::thread::current().id();
    let entered = Rc::new(tokio::sync::Notify::new());
    let released = Rc::new(tokio::sync::Notify::new());
    let callback_entered = entered.clone();
    let callback_released = released.clone();
    let callback = apply_async(
        async move |db: &Connection, command: Option<&[u8]>, _: &ApplyContext| {
            if command.is_none() {
                return setup(db);
            }
            db.execute("UPDATE counter SET value=99", [])?;
            callback_entered.notify_one();
            callback_released.notified().await;
            *capture.borrow_mut() += 1;
            assert_eq!(std::thread::current().id(), thread);
            Ok(Value::Nil)
        },
    );
    let mut apply = Apply::new("one", &path, callback).unwrap();
    apply.hello([1; 16]).unwrap();
    apply.open().unwrap();
    apply.ready().unwrap();
    apply.execute(request(0, &[])).await.unwrap();
    let stop = apply.stop_handle();
    let order = Rc::new(RefCell::new(Vec::new()));
    let errors = order.clone();
    let closes = order.clone();
    apply.diagnostics().on_error(move |_| {
        assert!(stop.is_stopped());
        errors.borrow_mut().push("error");
    });
    apply
        .diagnostics()
        .on_close(move || closes.borrow_mut().push("close"));
    let stop = apply.stop_handle();
    let execute = apply.execute(request(1, &[b"ok"]));
    let cancel = async {
        entered.notified().await;
        stop.stop();
        released.notify_one();
    };
    let (result, ()) = tokio::join!(execute, cancel);
    assert!(result.is_err());
    assert_eq!(*local.borrow(), 1);
    assert_eq!(*order.borrow(), vec!["close"]);
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row("SELECT seq FROM monsterft_meta_apply", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn callback_panic_is_terminal_and_error_precedes_close() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let mut apply = Apply::new(
        "one",
        &path,
        apply_async(
            async |db: &Connection, command: Option<&[u8]>, _: &ApplyContext| {
                if command.is_none() {
                    return setup(db);
                }
                db.execute("UPDATE counter SET value=99", [])?;
                tokio::task::yield_now().await;
                panic!("application panic");
            },
        ),
    )
    .unwrap();
    apply.hello([1; 16]).unwrap();
    apply.open().unwrap();
    apply.ready().unwrap();
    apply.execute(request(0, &[])).await.unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let errors = events.clone();
    let closes = events.clone();
    let stop = apply.stop_handle();
    apply
        .diagnostics()
        .on_error(|_| panic!("first diagnostic listener fails"));
    apply.diagnostics().on_error(move |error| {
        assert!(stop.is_stopped());
        assert!(error.message.contains("application panic"));
        errors.borrow_mut().push("error");
    });
    apply
        .diagnostics()
        .on_close(move || closes.borrow_mut().push("close"));
    assert_eq!(
        apply.execute(request(1, &[b"ok"])).await.unwrap_err().code,
        Some(Code::MonsterIllegal)
    );
    assert_eq!(*events.borrow(), vec!["error", "close"]);
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rollback_failure_keeps_original_warning_and_reports_terminal_sqlite_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let mut apply = Apply::new(
        "one",
        &path,
        apply_sync(|db, command, _| {
            if command.is_none() {
                return setup(db);
            }
            db.execute("UPDATE counter SET value=99", [])?;
            db.execute_batch("RELEASE monsterft_item")?;
            Err(Error::new(Code::Other(71), "original callback rejection"))
        }),
    )
    .unwrap();
    apply.hello([1; 16]).unwrap();
    apply.open().unwrap();
    apply.ready().unwrap();
    apply.execute(request(0, &[])).await.unwrap();
    let warnings = Rc::new(RefCell::new(Vec::new()));
    let captured = warnings.clone();
    apply
        .diagnostics()
        .on_warn(move |error| captured.borrow_mut().push(error.clone()));
    let error = apply.execute(request(1, &[b"ok"])).await.unwrap_err();
    assert_eq!(error.code, Some(Code::SqliteError));
    assert!(error.message.contains("DB2 item rollback"));
    assert_eq!(warnings.borrow()[0].message, "original callback rejection");
    assert!(apply.is_stopped());
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn framed_owner_accepts_close_while_async_callback_waits() {
    use monsterft::{
        local::{LocalReply, Received, StartupRequest},
        rpc::ApplyRequest,
        wire,
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let entered = Rc::new(tokio::sync::Notify::new());
    let released = Rc::new(tokio::sync::Notify::new());
    let callback_entered = entered.clone();
    let callback_released = released.clone();
    let mut apply = Apply::new(
        "one",
        &path,
        apply_async(
            async move |db: &Connection, command: Option<&[u8]>, _: &ApplyContext| {
                if command.is_none() {
                    return setup(db);
                }
                db.execute("UPDATE counter SET value=99", [])?;
                callback_entered.notify_one();
                callback_released.notified().await;
                Ok(Value::Nil)
            },
        ),
    )
    .unwrap();
    let stop = apply.stop_handle();
    let events = Rc::new(RefCell::new(Vec::new()));
    let errors = events.clone();
    let closes = events.clone();
    apply.diagnostics().on_error(move |error| {
        errors
            .borrow_mut()
            .push(format!("error: {}", error.message))
    });
    apply
        .diagnostics()
        .on_close(move || closes.borrow_mut().push("close".into()));
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let (sent, mut replies) = tokio::sync::mpsc::unbounded_channel::<LocalReply>();
    let owner = apply.run(
        rx,
        Box::new(move |to, frame| {
            assert_eq!(to, "gate");
            sent.send(wire::decode_frame(&frame)?).unwrap();
            Ok(())
        }),
    );
    let controller = async {
        for request in [
            StartupRequest::Hello {
                cid: "hello".into(),
                seed: [1; 16],
            },
            StartupRequest::Open { cid: "open".into() },
            StartupRequest::Ready {
                cid: "ready".into(),
            },
        ] {
            tx.send(Received {
                from: "gate".into(),
                frame: wire::encode_frame(&request).unwrap(),
            })
            .await
            .unwrap();
            assert_eq!(replies.recv().await.unwrap().cid(), request.cid());
        }
        tx.send(Received {
            from: "gate".into(),
            frame: wire::encode_frame(&ApplyRequest::Execute {
                execute: request(0, &[]),
            })
            .unwrap(),
        })
        .await
        .unwrap();
        assert_eq!(replies.recv().await.unwrap().cid(), "execute-0");
        tx.send(Received {
            from: "gate".into(),
            frame: wire::encode_frame(&ApplyRequest::Execute {
                execute: request(1, &[b"ok"]),
            })
            .unwrap(),
        })
        .await
        .unwrap();
        entered.notified().await;
        tx.send(Received {
            from: "gate".into(),
            frame: wire::encode_frame(&StartupRequest::Close {
                cid: "close".into(),
            })
            .unwrap(),
        })
        .await
        .unwrap();
        while !stop.is_stopped() {
            tokio::task::yield_now().await;
        }
        assert!(replies.try_recv().is_err());
        released.notify_one();
        assert_eq!(replies.recv().await.unwrap().cid(), "close");
        assert!(replies.recv().await.is_none());
    };
    let (result, ()) = tokio::join!(owner, controller);
    result.unwrap();
    assert_eq!(*events.borrow(), vec!["close"]);
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(
        db.query_row("SELECT seq FROM monsterft_meta_apply", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

#[tokio::test(flavor = "current_thread")]
async fn generation_and_tick_fences_precede_application_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let mut old = Apply::new(
        "one",
        &path,
        apply_sync(|db, cmd, _| {
            if cmd.is_none() {
                setup(db)
            } else {
                panic!("superseded callback must not execute")
            }
        }),
    )
    .unwrap();
    old.hello([1; 16]).unwrap();
    old.open().unwrap();
    old.ready().unwrap();
    old.execute(request(0, &[])).await.unwrap();
    let mut new = Apply::new("one", &path, apply_sync(|_, _, _| Ok(Value::Nil))).unwrap();
    new.hello([2; 16]).unwrap();
    new.open().unwrap();
    new.ready().unwrap();
    assert_eq!(
        old.execute(request(1, &[b"ok"])).await.unwrap_err().code,
        Some(Code::GenerationDiff)
    );
    new.close().unwrap();
}

#[test]
fn outcome_encoding_and_digest_exclude_only_shared_stack() {
    use monsterft::{protocol::digest_cmd, wire};
    let err = Error {
        message: "failure".into(),
        code: Some(Code::Other(99)),
        sql_code: Some(3),
        stack: Some("first".into()),
    };
    let a = vec![Outcome::Rejected(err.clone())];
    let mut b = a.clone();
    if let Outcome::Rejected(e) = &mut b[0] {
        e.stack = Some("second".into());
    }
    assert_eq!(
        digest_cmd(1, 3, b"patch", &a).unwrap(),
        digest_cmd(1, 3, b"patch", &b).unwrap()
    );
    assert_ne!(
        wire::encode_payload(&a).unwrap(),
        wire::encode_payload(&b).unwrap()
    );
    assert_eq!(
        wire::decode_payload::<Vec<Outcome>>(&wire::encode_payload(&a).unwrap()).unwrap(),
        a
    );
}

#[tokio::test(flavor = "current_thread")]
async fn excessively_nested_result_is_an_item_rejection_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("node");
    initialize_locals(&path);
    let callback = apply_sync(|db, command, _| {
        if command.is_none() {
            return setup(db);
        }
        db.execute("UPDATE counter SET value=123", [])?;
        let mut value = Value::Nil;
        for _ in 0..140 {
            value = Value::Array(vec![value]);
        }
        Ok(value)
    });
    let mut apply = Apply::new("one", &path, callback).unwrap();
    apply.hello([1; 16]).unwrap();
    apply.open().unwrap();
    apply.ready().unwrap();
    apply.execute(request(0, &[])).await.unwrap();
    let result = apply.execute(request(1, &[b"deep"])).await.unwrap();
    assert!(matches!(&result.outcomes[0],Outcome::Rejected(e) if e.code==Some(Code::ApplyError)));
    assert!(apply.is_open());
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    apply.close().unwrap();
}

#[test]
fn local_binary_fields_do_not_accept_integer_arrays() {
    use monsterft::{local::StartupRequest, wire};
    let value = Value::Map(vec![
        ("type".into(), "hello".into()),
        ("cid".into(), "one".into()),
        ("seed".into(), Value::Array(vec![1.into(); 16])),
    ]);
    assert!(
        wire::decode_payload::<StartupRequest>(&wire::encode_payload(&value).unwrap()).is_err()
    );
    let value = Value::Map(vec![
        ("seq".into(), 1.into()),
        ("entry_hash".into(), Value::Array(vec![1.into(); 16])),
        ("local_digest".into(), Value::Binary(vec![2; 16])),
    ]);
    assert!(
        wire::decode_payload::<monsterft::storage::Receipt>(&wire::encode_payload(&value).unwrap())
            .is_err()
    );
}

#[tokio::test(flavor = "current_thread")]
async fn warning_listener_failure_stops_before_later_listeners_and_rolls_back() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("node");
    initialize_locals(&path);
    let calls = Rc::new(RefCell::new(0));
    let called = calls.clone();
    let mut apply = Apply::new(
        "a",
        &path,
        apply_sync(move |db, command, _| {
            if command.is_none() {
                return setup(db);
            }
            *called.borrow_mut() += 1;
            db.execute("UPDATE counter SET value=value+1", [])?;
            Err(Error::new(Code::ApplyError, "rejected item"))
        }),
    )
    .unwrap();
    apply.hello([1; 16]).unwrap();
    apply.open().unwrap();
    apply.ready().unwrap();
    apply.execute(request(0, &[])).await.unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    apply
        .diagnostics()
        .on_warn(|_| panic!("warning listener failed"));
    let stop = apply.stop_handle();
    let observed = events.clone();
    apply.diagnostics().on_warn(move |_| {
        assert!(stop.is_stopped());
        observed.borrow_mut().push("later warning");
    });
    apply
        .diagnostics()
        .on_error(|_| panic!("error listener failed"));
    let observed = events.clone();
    apply.diagnostics().on_error(move |error| {
        assert!(error.message.contains("warning listener failed"));
        observed.borrow_mut().push("error");
    });
    let observed = events.clone();
    apply
        .diagnostics()
        .on_close(move || observed.borrow_mut().push("close"));
    let error = apply
        .execute(request(1, &[b"first", b"must not run"]))
        .await
        .unwrap_err();
    assert!(error.message.contains("warning listener failed"));
    assert!(apply.is_stopped());
    assert_eq!(*calls.borrow(), 1);
    assert_eq!(*events.borrow(), vec!["error", "later warning", "close"]);
    assert_eq!(apply.fatal_error().unwrap(), &error);
    let db = Connection::open(format!("{}2", path.display())).unwrap();
    assert_eq!(
        db.query_row("SELECT value FROM counter", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        0
    );
    assert_eq!(monsterft::storage::read_apply(&db).unwrap().seq, 0);
}
