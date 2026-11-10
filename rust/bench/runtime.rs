use crate::{
    children,
    config::{Config, NODES, Shared, error, io_error},
    delay::DelaySender,
};

use futures_util::FutureExt;
use monsterft::{
    error::{Code, Error, Result},
    gate::{EventKind, Gate, GateEvent, GateOptions},
    local::{COMPONENTS, SendFrame},
    net::{
        ipc::{IpcEndpoint, IpcNamespace, IpcOptions, cleanup_namespace},
        lifeline::LifetimePipe,
        tcpcluster::{TcpCluster, TcpOptions, TcpPeer},
    },
    supervisor::{ChildSpec, Setup},
};
use std::{
    cell::Cell, collections::BTreeMap, ffi::OsString, panic::AssertUnwindSafe, path::PathBuf,
    rc::Rc,
};
use tokio::sync::{mpsc, oneshot};

fn join_error(error: tokio::task::JoinError) -> Error {
    Error::new(Code::SupervisorError, error.to_string())
}

fn peers(config: &Config) -> Result<BTreeMap<String, TcpPeer>> {
    NODES
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let base_port = config
                .base_port
                .checked_add(index as u16 * 10)
                .ok_or_else(|| error("TCP port overflow"))?;
            Ok((
                (*id).to_owned(),
                TcpPeer {
                    host: config
                        .hosts
                        .as_ref()
                        .map_or_else(|| "127.0.0.1".into(), |hosts| hosts[index].clone()),
                    base_port,
                },
            ))
        })
        .collect()
}

pub async fn setup(
    id: String,
    database_path: PathBuf,
    config: Config,
    shared: Rc<Shared>,
) -> Result<Setup> {
    shared.attempts.set(shared.attempts.get() + 1);
    if shared.attempts.get() != 1 {
        return Err(error("benchmark owner group restarted"));
    }
    // A new namespace and new endpoints belong exclusively to this attempt.
    let directory = tempfile::Builder::new().prefix("mft-").tempdir_in("/tmp")?;
    let mut entropy = [0u8; 8];
    getrandom::fill(&mut entropy).map_err(|e| error(e.to_string()))?;
    let namespace = IpcNamespace {
        directory: directory.path().to_owned(),
        prefix: format!(
            "mft{}_",
            entropy
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        ),
    };
    let mut tcp = TcpCluster::open(&id, peers(&config)?, TcpOptions::default()).await?;
    let tcp_sender = tcp.sender();
    let diagnostic_id = id.clone();
    let tcp_diagnostics = shared.clone();
    tcp.on_error(move |error| {
        eprintln!("{diagnostic_id}/tcp: {error}");
        tcp_diagnostics.fail(format!("TCP: {error}"));
    });

    // No tasks or children are launched until all fallible construction succeeds.
    let construction = (|| -> Result<_> {
        let mut endpoints = Vec::new();
        let mut senders = BTreeMap::new();
        let mut child_descriptors = BTreeMap::new();
        for role in COMPONENTS {
            let (outgoing, incoming) = children::endpoints(role);
            let (endpoint, descriptors) = IpcEndpoint::parent(
                &outgoing,
                &incoming,
                IpcOptions {
                    namespace: Some(namespace.clone()),
                    ..IpcOptions::default()
                },
            )?;
            child_descriptors.insert(role, descriptors);
            senders.insert(role.to_owned(), endpoint.sender());
            endpoints.push((role, endpoint));
        }
        let lifetime = LifetimePipe::new()?;
        let executable = std::env::current_exe()?;

        let mut children = Vec::new();
        for role in COMPONENTS {
            let args = vec![
                OsString::from(role),
                OsString::from(&id),
                database_path.as_os_str().to_owned(),
                namespace.directory.as_os_str().to_owned(),
                OsString::from(&namespace.prefix),
                OsString::from("3"),
            ];
            let mut child = ChildSpec::new(&executable)
                .args(args)
                .fd_mapping(lifetime.child_reader()?, 3);
            // Linux inherits ring and notification descriptors; macOS uses names.
            for (index, fd) in child_descriptors
                .remove(role)
                .unwrap()
                .into_iter()
                .enumerate()
            {
                child = child.fd_mapping(fd, 4 + index as i32);
            }
            children.push(child);
        }
        let children: [ChildSpec; 4] = children
            .try_into()
            .map_err(|_| error("expected four child roles"))?;
        let remote = tcp_sender.clone();
        let transport_stats = shared.clone();
        let delay_us = config
            .one_way_delay_us
            .unwrap_or(config.rtt_us as f64 / 2.0);
        let (delayed, delay_pump) = DelaySender::new(
            std::time::Duration::from_secs_f64(delay_us / 1_000_000.0),
            move |to, frame| {
                let delivered = remote.send(to, frame)?;
                if !delivered {
                    transport_stats
                        .dropped
                        .set(transport_stats.dropped.get() + 1);
                    if transport_stats.active.get() {
                        return Err(std::io::Error::other("TCP dropped a benchmark frame"));
                    }
                }
                // The initial TCP connection attempts are intentionally asynchronous.
                Ok(true)
            },
        );
        let outbound = delayed.clone();
        let statistics = shared.clone();
        let send: SendFrame = Box::new(move |to, frame| {
            if statistics.audit.get() {
                let audit = crate::audit::inspect(&frame)?;
                if audit.has_data {
                    statistics
                        .data_appends
                        .set(statistics.data_appends.get() + 1);
                }
                if audit.uncommitted_pair {
                    statistics
                        .paired_appends
                        .set(statistics.paired_appends.get() + 1);
                }
            }
            if statistics.measuring.get() {
                statistics.sent_frames.set(statistics.sent_frames.get() + 1);
                statistics
                    .sent_bytes
                    .set(statistics.sent_bytes.get() + frame.len() as u64);
            }
            outbound.send(to, frame).map_err(io_error)
        });
        let local = senders.clone();
        let local_send: SendFrame = Box::new(move |to, frame| {
            local
                .get(to)
                .ok_or_else(|| Error::new(Code::SendError, "unknown local destination"))?
                .send(frame)
                .map_err(io_error)
        });
        let (gate, mut driver) = Gate::new(
            &id,
            NODES.iter().map(|id| (*id).to_owned()).collect(),
            send,
            local_send,
            GateOptions {
                tick_interval: std::time::Duration::from_millis(config.tick_ms),
                ..Default::default()
            },
        )?;
        let diagnostic_id = id.clone();
        let gate_diagnostics = shared.clone();
        driver.diagnostics().on_error(move |error| {
            eprintln!("{diagnostic_id}/gate: {error}");
            gate_diagnostics.fail(format!("Gate: {error}"));
        });
        let changes = shared.clone();
        gate.on(EventKind::Change, move |event| {
            if let GateEvent::Change(status) = event
                && let Some((leader, term)) = changes.expected_leader.borrow().as_ref()
                && (status.leader.as_ref() != Some(leader) || status.term != *term)
            {
                changes.fail("leadership changed during benchmark");
            }
        })?;
        let sync = shared.clone();
        gate.on(EventKind::Sync, move |event| {
            if let GateEvent::Sync {
                cmd_seq, quorum, ..
            } = event
            {
                if !quorum {
                    sync.fail("benchmark command did not reach outcome agreement");
                }
                sync.last_sync_cmd.set(*cmd_seq);
            }
        })?;
        Ok((
            endpoints, senders, lifetime, children, gate, driver, delayed, delay_pump,
        ))
    })();
    let (endpoints, senders, lifetime, children, gate, driver, delayed, delay_pump) =
        match construction {
            Ok(resources) => resources,
            Err(failure) => {
                // A failed factory cleans its own partial resources. Failed cleanup
                // prevents safe replacement, so this example exits in that case.
                let cleaned = tcp
                    .close()
                    .await
                    .map_err(Error::from)
                    .and_then(|()| cleanup_namespace(&namespace).map_err(Error::from))
                    .and_then(|()| directory.close().map_err(Error::from));
                if let Err(cleanup) = cleaned {
                    eprintln!("{id}/cleanup: {cleanup}");
                    std::process::exit(1);
                }
                return Err(failure);
            }
        };

    *shared.delay.borrow_mut() = Some(delayed.clone());
    let stopping = Rc::new(Cell::new(false));
    let (failed, mut failure) = mpsc::unbounded_channel::<Error>();
    let delay_failed = failed.clone();
    let delay_stopping = stopping.clone();
    let delay_statistics = shared.clone();
    let delay_task = tokio::task::spawn_local(async move {
        if let Err(error) = delay_pump.run().await
            && !delay_stopping.get()
        {
            delay_statistics.fail(format!("network delay: {error}"));
            let _ = delay_failed.send(io_error(error));
        }
    });
    let mut readiness = Vec::new();
    let mut pumps = Vec::new();
    for (role, mut endpoint) in endpoints {
        let (ready, wait) = oneshot::channel();
        readiness.push(wait);
        let gate = gate.clone();
        let id = id.clone();
        let failed = failed.clone();
        let stopping = stopping.clone();
        pumps.push(tokio::task::spawn_local(async move {
            if let Err(error) = endpoint.ready().await {
                let error = io_error(error);
                let _ = ready.send(Err(error.clone()));
                if !stopping.get() {
                    eprintln!("{id}/{role}/ipc: {error}");
                    let _ = failed.send(error);
                }
                return;
            }
            let _ = ready.send(Ok(()));
            let result = async {
                loop {
                    gate.on_receive(role, endpoint.recv().await.map_err(io_error)?)?;
                }
                #[allow(unreachable_code)]
                Ok::<(), Error>(())
            }
            .await;
            if let Err(error) = result
                && !stopping.get()
            {
                eprintln!("{id}/{role}/ipc: {error}");
                let _ = failed.send(error);
            }
        }));
    }
    let network_gate = gate.clone();
    let network_failed = failed.clone();
    let network_stopping = stopping.clone();
    let network_id = id.clone();
    let incoming_statistics = shared.clone();
    let network = tokio::task::spawn_local(async move {
        loop {
            match tcp.recv().await {
                Ok(Some((from, frame))) => {
                    if incoming_statistics.measuring.get() {
                        incoming_statistics
                            .received_frames
                            .set(incoming_statistics.received_frames.get() + 1);
                        incoming_statistics
                            .received_bytes
                            .set(incoming_statistics.received_bytes.get() + frame.len() as u64);
                    }
                    if let Err(error) = network_gate.on_receive(from, frame) {
                        if !network_stopping.get() {
                            eprintln!("{network_id}/tcp: {error}");
                            let _ = network_failed.send(error);
                        }
                        break;
                    }
                }
                Ok(None) => break,
                Err(error) => {
                    if !network_stopping.get() {
                        let _ = network_failed.send(io_error(error));
                    }
                    break;
                }
            }
        }
        tcp
    });
    let driver_failed = failed.clone();
    let driver_id = id.clone();
    let driver = tokio::task::spawn_local(async move {
        if let Err(panic) = AssertUnwindSafe(driver.run()).catch_unwind().await {
            let message = if let Some(message) = panic.downcast_ref::<&str>() {
                (*message).to_owned()
            } else if let Some(message) = panic.downcast_ref::<String>() {
                message.clone()
            } else {
                "non-string panic".into()
            };
            let error = error(format!("Gate owner panicked: {message}"));
            eprintln!("{driver_id}/gate: {error}");
            let _ = driver_failed.send(error);
        }
    });
    drop(failed);
    let connected = tcp_sender.clone();
    let node_id = id.clone();
    Ok(Setup::new(gate, children)
        .ready(move |_| async move {
            for ready in readiness {
                ready
                    .await
                    .map_err(|_| error("IPC readiness task ended"))??;
            }
            while NODES
                .iter()
                .any(|peer| *peer != node_id && !connected.connected(peer))
            {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Ok(())
        })
        .failed(async move {
            Err(failure
                .recv()
                .await
                .unwrap_or_else(|| error("transport tasks ended")))
        })
        .stop(move || {
            stopping.set(true);
            delayed.stop();
            tcp_sender.stop();
            for sender in senders.values() {
                sender.stop();
            }
            Ok(())
        })
        .cleanup(move || async move {
            // Supervisor has already waited for all four child processes to exit.
            let mut failure = None;
            if let Err(error) = delay_task.await {
                failure.get_or_insert_with(|| join_error(error));
            }
            for pump in pumps {
                if let Err(error) = pump.await {
                    failure.get_or_insert_with(|| join_error(error));
                }
            }
            match network.await {
                Ok(mut tcp) => {
                    if let Err(error) = tcp.close().await {
                        failure.get_or_insert_with(|| Error::from(error));
                    }
                }
                Err(error) => {
                    failure.get_or_insert_with(|| join_error(error));
                }
            }
            driver.abort();
            if let Err(error) = driver.await
                && !error.is_cancelled()
            {
                failure.get_or_insert_with(|| join_error(error));
            }
            drop(lifetime);
            if let Err(error) = cleanup_namespace(&namespace) {
                failure.get_or_insert_with(|| Error::from(error));
                // Preserve the namespace if owner/resource removal could not be proved.
                let _ = directory.keep();
            } else if let Err(error) = directory.close() {
                failure.get_or_insert_with(|| Error::from(error));
            }
            failure.map_or(Ok(()), Err)
        }))
}
