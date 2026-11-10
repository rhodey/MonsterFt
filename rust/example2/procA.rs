mod common;

use monsterft::{
    error::{Code, Error, Result},
    local::{Received, SendFrame},
    proc::Proc,
    storage::ProcRole,
};
use tokio::{sync::mpsc, task::LocalSet};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let args = common::ChildArgs::parse()?;

    // Adopt the inherited parent pipe before opening other child resources.
    let parent = common::parent_lifetime(&args)?;

    let mut ipc = common::child_endpoint(&args, "procA")?;
    let sender = ipc.sender();

    // Replies are queued for Gate; the IPC adapter sends them on this event loop.
    let local_send: SendFrame =
        Box::new(move |_, frame| sender.send(frame).map_err(common::send_error));

    let nodes = common::NODES.iter().map(|id| (*id).into()).collect();

    let mut proc = Proc::new(
        ProcRole::A,
        &args.id,
        nodes,
        local_send,
        &args.database_path,
        common::timeouts::proc_options(),
    )?;

    let stopped = proc.stop_handle();
    let id = args.id.clone();

    proc.diagnostics.on_error(move |error| {
        eprintln!("{id}/procA: {error}");

        // A live no-quorum repair state is allowed to keep replicating.
        if error.code != Some(Code::RepairNoQuorum) || stopped.is_stopped() {
            std::process::exit(1);
        }
    });

    // Wait for Gate's endpoint before starting this component.
    ipc.ready().await?;

    let (incoming, receiver) = mpsc::channel(64);

    let owner = async {
        proc.run(receiver).await?;

        // After a graceful close, Supervisor still owns this child's lifetime.
        std::future::pending::<Result<()>>().await
    };

    // Forward frames to the owner queue, without reentering the component.
    let network = async {
        loop {
            let frame = ipc.recv().await?;
            let message = Received {
                from: "gate".into(),
                frame,
            };

            incoming
                .try_send(message)
                .map_err(|error| Error::new(Code::SendError, error.to_string()))?;
        }

        #[allow(unreachable_code)]
        Ok::<(), Error>(())
    };

    // Exit if the owner, transport, or supervising parent fails.
    tokio::select! {
        result = owner => result,
        result = network => result,

        result = parent.closed() => {
            result?;

            Err(Error::new(Code::NodeNotOpen, "supervisor parent exited"))
        },
    }
}
