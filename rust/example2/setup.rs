use super::common;

use futures_util::FutureExt;
use monsterft::{
    error::{Code, Error, Result},
    gate::{Gate, GateOptions},
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

pub(super) fn error(message: impl Into<String>) -> Error {
    Error::new(Code::SupervisorError, message)
}

pub(super) fn join_error(error: tokio::task::JoinError) -> Error {
    Error::new(Code::SupervisorError, error.to_string())
}

fn peers(default_port: u16) -> Result<BTreeMap<String, TcpPeer>> {
    let base: u16 = std::env::var("MONSTERFT_BASE_PORT")
        .unwrap_or_else(|_| default_port.to_string())
        .parse()
        .map_err(|_| error("MONSTERFT_BASE_PORT must be a TCP port"))?;

    // Reserve a separate port range for each local cluster node.
    common::NODES
        .iter()
        .enumerate()
        .map(|(index, id)| {
            let base_port = base
                .checked_add(index as u16 * 10)
                .ok_or_else(|| error("TCP port overflow"))?;

            Ok((
                (*id).to_owned(),
                TcpPeer {
                    host: "127.0.0.1".into(),
                    base_port,
                },
            ))
        })
        .collect()
}

// Build one supervision attempt. A restart gets fresh transports and a fresh Gate.
pub async fn setup(
    id: String,
    database_path: PathBuf,
    binary_prefix: &str,
    default_port: u16,
) -> Result<Setup> {
    // A new namespace and new endpoints belong exclusively to this attempt.
    let directory = tempfile::Builder::new().prefix("mft-").tempdir_in("/tmp")?;

    let mut entropy = [0u8; 8];
    getrandom::fill(&mut entropy).map_err(|e| error(e.to_string()))?;
    let suffix: String = entropy.iter().map(|byte| format!("{byte:02x}")).collect();

    let namespace = IpcNamespace {
        directory: directory.path().to_owned(),
        prefix: format!("mft{suffix}_"),
    };

    let mut tcp = TcpCluster::open(&id, peers(default_port)?, TcpOptions::default()).await?;
    let tcp_sender = tcp.sender();

    let diagnostic_id = id.clone();
    tcp.on_error(move |error| {
        eprintln!("{diagnostic_id}/tcp: {error}");
    });

    // No tasks or children are launched until all fallible construction succeeds.
    let construction = (|| -> Result<_> {
        let mut endpoints = Vec::new();
        let mut senders = BTreeMap::new();
        let mut child_descriptors = BTreeMap::new();

        for role in COMPONENTS {
            let (outgoing, incoming) = common::endpoints(role);
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

        // Each child watches the lifetime pipe and has its own pair of IPC channels.
        let lifetime = LifetimePipe::new()?;
        let executable = std::env::current_exe()?;
        let binaries = executable
            .parent()
            .ok_or_else(|| error("example binary directory is missing"))?;

        let mut children = Vec::new();

        for role in COMPONENTS {
            let args = vec![
                OsString::from(&id),
                database_path.as_os_str().to_owned(),
                namespace.directory.as_os_str().to_owned(),
                OsString::from(&namespace.prefix),
                OsString::from("3"),
            ];

            let mut child = ChildSpec::new(binaries.join(format!("{binary_prefix}-{role}")))
                .args(args)
                .fd_mapping(lifetime.child_reader()?, 3);

            // Linux inherits ring and notification descriptors; macOS uses names.
            let descriptors = child_descriptors.remove(role).unwrap();

            for (index, fd) in descriptors.into_iter().enumerate() {
                child = child.fd_mapping(fd, 4 + index as i32);
            }

            children.push(child);
        }

        let children: [ChildSpec; 4] = children
            .try_into()
            .map_err(|_| error("expected four child roles"))?;

        // Gate routes remote frames over TCP and local frames to one child.
        let remote = tcp_sender.clone();
        let local = senders.clone();

        let send: SendFrame = Box::new(move |to, frame| {
            remote
                .send(to, frame)
                .map(|_| ())
                .map_err(common::send_error)
        });

        let local_send: SendFrame = Box::new(move |to, frame| {
            let sender = local
                .get(to)
                .ok_or_else(|| Error::new(Code::SendError, "unknown local destination"))?;

            sender.send(frame).map_err(common::send_error)
        });

        let nodes = common::NODES.iter().map(|id| (*id).to_owned()).collect();

        let (gate, mut driver) = Gate::new(&id, nodes, send, local_send, GateOptions::default())?;

        let diagnostic_id = id.clone();
        driver.diagnostics().on_error(move |error| {
            eprintln!("{diagnostic_id}/gate: {error}");
        });

        Ok((endpoints, senders, lifetime, children, gate, driver))
    })();

    let (endpoints, senders, lifetime, children, gate, driver) = match construction {
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

    // Receive pumps run alongside Gate on the parent's event-loop thread.
    let stopping = Rc::new(Cell::new(false));
    let (failed, mut failure) = mpsc::unbounded_channel::<Error>();
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
                let error = common::send_error(error);
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
                    let frame = endpoint.recv().await.map_err(common::send_error)?;
                    gate.on_receive(role, frame)?;
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

    let network = tokio::task::spawn_local(async move {
        loop {
            match tcp.recv().await {
                Ok(Some((from, frame))) => {
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
                        let _ = network_failed.send(common::send_error(error));
                    }

                    break;
                }
            }
        }

        tcp
    });

    // A panicked Gate task must also fail the supervision attempt.
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

    // Supervisor owns the startup/shutdown order; these hooks own the resources.
    let setup = Setup::new(gate, children)
        .ready(move |_| async move {
            // All four child endpoints must be ready before Gate opens.
            for ready in readiness {
                ready
                    .await
                    .map_err(|_| error("IPC readiness task ended"))??;
            }

            Ok(())
        })
        .failed(async move {
            let error = failure
                .recv()
                .await
                .unwrap_or_else(|| error("transport tasks ended"));

            Err(error)
        })
        .stop(move || {
            // Cancel delivery before Supervisor terminates the children.
            stopping.set(true);
            tcp_sender.stop();

            for sender in senders.values() {
                sender.stop();
            }

            Ok(())
        })
        .cleanup(move || async move {
            // Supervisor has already waited for all four child processes to exit.
            let mut failure = None;

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
        });

    Ok(setup)
}
