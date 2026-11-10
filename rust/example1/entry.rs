use bytes::Bytes;
use monsterft::{
    Value,
    apply::{Apply, apply_sync},
    error::{Code, Error, Result},
    gate::{Gate, GateOptions},
    local::{Received, SendFrame},
    proc::{Options, Proc},
    storage::ProcRole,
};
use std::{cell::RefCell, collections::BTreeMap, path::PathBuf, rc::Rc, time::Duration};
use tokio::{sync::mpsc, task::LocalSet};

fn deliver(receiver: &mpsc::Sender<Received>, from: &str, frame: Bytes) -> Result<()> {
    // Delivery queues bytes; the receiving owner processes them later.
    receiver
        .try_send(Received {
            from: from.into(),
            frame,
        })
        .map_err(|error| Error::new(Code::SendError, error.to_string()))
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let ids = vec!["1".to_string(), "2".to_string(), "3".to_string()];

    // All Gates share this in-process routing table on the main thread.
    let network = Rc::new(RefCell::new(
        BTreeMap::<String, mpsc::Sender<Received>>::new(),
    ));

    let mut gates = Vec::new();
    let mut threads = Vec::new();
    let mut drivers = Vec::new();

    for id in &ids {
        let database_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("example1/data")
            .join(id);

        // Each node has four local inboxes: Proc A/B/C and Apply.
        let mut local = BTreeMap::new();
        let mut receivers = BTreeMap::new();

        for role in ["procA", "procB", "procC", "apply"] {
            let (tx, rx) = mpsc::channel(64);
            local.insert(role.to_string(), tx);
            receivers.insert(role.to_string(), rx);
        }

        // Remote sends go to another Gate; local sends go to this node's owners.
        let routes = network.clone();
        let from = id.clone();

        let send: SendFrame = Box::new(move |to, frame| {
            if let Some(receiver) = routes.borrow().get(to) {
                deliver(receiver, &from, frame)?;
            }

            Ok(())
        });

        let local_send: SendFrame = Box::new(move |to, frame| {
            let receiver = local
                .get(to)
                .ok_or_else(|| Error::new(Code::SendError, "unknown local destination"))?;

            deliver(receiver, "gate", frame)
        });

        let (gate, mut driver) =
            Gate::new(id, ids.clone(), send, local_send, GateOptions::default())?;

        let name = id.clone();
        driver.diagnostics().on_error(move |error| {
            eprintln!("{name}/gate: {error}");
        });

        network.borrow_mut().insert(id.clone(), gate.receiver());

        // Each DB1 owner has its own thread and SQLite connection.
        for role in ProcRole::ALL {
            let node = id.clone();
            let nodes = ids.clone();
            let path = database_path.clone();
            let to_gate = gate.receiver();
            let incoming = receivers.remove(role.as_str()).unwrap();

            threads.push(std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;

                runtime.block_on(async move {
                    let local_send: SendFrame =
                        Box::new(move |_, frame| deliver(&to_gate, role.as_str(), frame));

                    let mut options = Options::default();
                    options.protocol.election_timeout = 100;

                    let mut proc = Proc::new(role, &node, nodes, local_send, path, options)?;
                    proc.diagnostics.on_error(move |error| {
                        eprintln!("{node}/{}: {error}", role.as_str());
                    });

                    proc.run(incoming).await
                })
            }));
        }

        // Apply owns DB2 on a separate thread, so callbacks cannot stall Gate.
        let node = id.clone();
        let to_gate = gate.receiver();
        let incoming = receivers.remove("apply").unwrap();

        threads.push(std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;

            runtime.block_on(async move {
                let local_send: SendFrame =
                    Box::new(move |_, frame| deliver(&to_gate, "apply", frame));

                let callback = apply_sync(|db, command, _| {
                    // Initialize the application table during database setup.
                    if command.is_none() {
                        db.execute_batch(
                            "CREATE TABLE counter(id INTEGER PRIMARY KEY, n INTEGER NOT NULL);
                             INSERT INTO counter VALUES(1, 0)",
                        )?;

                        return Ok(Value::Nil);
                    }

                    db.execute("UPDATE counter SET n=n+1", [])?;
                    let count =
                        db.query_row("SELECT n FROM counter", [], |row| row.get::<_, i64>(0))?;

                    Ok(count.into())
                });

                let mut apply = Apply::new(&node, database_path, callback)?;
                apply.diagnostics().on_error(move |error| {
                    eprintln!("{node}/apply: {error}");
                });

                apply.run(incoming, local_send).await
            })
        }));

        drivers.push(tokio::task::spawn_local(driver.run()));
        gates.push(gate);
    }

    // Open together: the nodes need one another to elect a leader.
    let work = async {
        let (a, b, c) = tokio::join!(gates[0].open(), gates[1].open(), gates[2].open());

        a?;
        b?;
        c?;

        gates[0].await_leader(true).await?;

        let requests: Vec<_> = (0..10)
            .map(|_| gates[0].append(Bytes::from_static(b"increment")))
            .collect();

        for request in requests {
            let (seq, value) = request.await?;
            println!("CMD {seq}: {value}");
        }

        // Allow the final SYNC to reach the other nodes before closing the demo.
        tokio::time::sleep(Duration::from_millis(150)).await;

        Ok::<_, Error>(())
    }
    .await;

    // Stop the owners and join their threads before discarding the Gates.
    for gate in &gates {
        if let Err(error) = gate.close().await {
            eprintln!("{}/close: {error}", gate.id());
        }
    }

    for thread in threads {
        thread
            .join()
            .map_err(|_| Error::new(Code::MonsterIllegal, "component thread panicked"))??;
    }

    network.borrow_mut().clear();
    drop(gates);

    for driver in drivers {
        driver.abort();
    }

    work
}
