mod common;
mod http;

use monsterft::{
    apply::{Apply, apply_async},
    error::{Code, Error, Result},
    local::{Received, SendFrame},
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

    let mut ipc = common::child_endpoint(&args, "apply")?;
    let sender = ipc.sender();

    // Replies are queued for Gate; the IPC adapter sends them on this event loop.
    let local_send: SendFrame =
        Box::new(move |_, frame| sender.send(frame).map_err(common::send_error));

    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(4))
        .build()
        .map_err(|e| Error::application(e.to_string()))?;

    let url = common::fact_url()?;
    let node = args.id.clone();

    // All members execute the same HTTP command. The service makes its reply stable.
    let callback =
        apply_async(async move |_, command, _| http::execute(&client, &url, &node, command).await);

    let mut apply = Apply::new(&args.id, &args.database_path, callback)?;
    let id = args.id.clone();

    apply.diagnostics().on_error(move |error| {
        eprintln!("{id}/apply: {error}");
        std::process::exit(1);
    });

    // Wait for Gate's endpoint before starting this component.
    ipc.ready().await?;

    let (incoming, receiver) = mpsc::channel(64);

    let owner = async {
        apply.run(receiver, local_send).await?;

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
