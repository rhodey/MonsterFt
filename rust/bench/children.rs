use crate::config::{NODES, error, io_error};
use monsterft::{
    Value,
    apply::{Apply, apply_sync},
    error::{Code, Result},
    local::{Received, SendFrame},
    net::{
        ipc::{IpcEndpoint, IpcNamespace, IpcOptions},
        lifeline::ParentLifetime,
    },
    proc::{Options, Proc},
    storage::ProcRole,
};
use std::{os::fd::RawFd, path::PathBuf};
use tokio::sync::mpsc;

pub fn endpoints(role: &str) -> (String, String) {
    (format!("gate-{role}"), format!("{role}-gate"))
}

pub async fn run(role: &str, args: &[String]) -> Result<()> {
    if args.len() != 5 || !NODES.contains(&args[0].as_str()) {
        return Err(error("invalid child arguments"));
    }
    let id = args[0].clone();
    let database_path = PathBuf::from(&args[1]);
    let namespace = IpcNamespace {
        directory: args[2].clone().into(),
        prefix: args[3].clone(),
    };
    let parent_fd: RawFd = args[4]
        .parse()
        .map_err(|_| error("invalid parent descriptor"))?;
    // This explicit inherited descriptor is adopted before opening transport resources.
    let parent = unsafe { ParentLifetime::from_inherited(parent_fd) }?;
    let (to_child, to_gate) = endpoints(role);
    // SAFETY: Setup maps this child's IPC descriptors consecutively from 4.
    let mut ipc = unsafe {
        IpcEndpoint::from_inherited(
            &to_child,
            &to_gate,
            IpcOptions {
                namespace: Some(namespace),
                ..Default::default()
            },
            4,
        )
    }?;
    let sender = ipc.sender();
    let local_send: SendFrame = Box::new(move |_, frame| sender.send(frame).map_err(io_error));
    let (tx, rx) = mpsc::channel(64);
    ipc.ready().await?;
    let network = async {
        loop {
            tx.try_send(Received {
                from: "gate".into(),
                frame: ipc.recv().await?,
            })
            .map_err(|e| error(e.to_string()))?;
        }
        #[allow(unreachable_code)]
        Ok::<(), monsterft::error::Error>(())
    };
    let owner: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>>>> = if role == "apply"
    {
        let callback = apply_sync(|db, command, _| {
            if command.is_none() {
                db.execute_batch("CREATE TABLE counter(id INTEGER PRIMARY KEY,n INTEGER NOT NULL); INSERT INTO counter VALUES(1,0)")?;
                return Ok(Value::Nil);
            }
            Ok(db
                .prepare_cached("UPDATE counter SET n=n+1 WHERE id=1 RETURNING n")?
                .query_row([], |row| row.get::<_, i64>(0))?
                .into())
        });
        let mut apply = Apply::new(&id, database_path, callback)?;
        apply.diagnostics().on_error(move |e| {
            eprintln!("{id}/apply: {e}");
            std::process::exit(1);
        });
        Box::pin(async move {
            apply.run(rx, local_send).await?;
            std::future::pending().await
        })
    } else {
        let proc_role = match role {
            "procA" => ProcRole::A,
            "procB" => ProcRole::B,
            "procC" => ProcRole::C,
            _ => return Err(error("invalid owner role")),
        };
        let mut proc = Proc::new(
            proc_role,
            &id,
            NODES.iter().map(|n| (*n).into()).collect(),
            local_send,
            database_path,
            Options::default(),
        )?;
        let stopped = proc.stop_handle();
        proc.diagnostics.on_error(move |e| {
            eprintln!("{id}/{}: {e}", proc_role.as_str());
            if e.code != Some(Code::RepairNoQuorum) || stopped.is_stopped() {
                std::process::exit(1);
            }
        });
        Box::pin(async move {
            proc.run(rx).await?;
            std::future::pending().await
        })
    };
    tokio::select! {
        result = owner => result,
        result = network => result,
        result = parent.closed() => { result?; Err(error("supervisor parent exited")) },
    }
}
