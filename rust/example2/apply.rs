mod common;

use monsterft::{
    Value,
    apply::{Apply, apply_sync},
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
        let count = db.query_row("SELECT n FROM counter", [], |row| row.get::<_, i64>(0))?;

        Ok(count.into())
    });

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
