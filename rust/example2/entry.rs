mod common;
mod setup;

use monsterft::{
    error::{Error, Result},
    supervisor::{Supervisor, SupervisorOptions},
};
use setup::{error, join_error};
use std::path::PathBuf;
use tokio::{
    signal::unix::{SignalKind, signal},
    task::LocalSet,
};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    LocalSet::new().run_until(run()).await
}

async fn run() -> Result<()> {
    let id = std::env::args()
        .nth(1)
        .filter(|id| common::NODES.contains(&id.as_str()))
        .ok_or_else(|| error("expected nodeId 1, 2, or 3"))?;

    let database_path = std::env::var_os("MONSTERFT_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("example2/data"))
        .join(&id);

    // Supervisor calls this factory again for every replacement attempt.
    let factory_id = id.clone();
    let (supervisor, mut driver) = Supervisor::new(
        move || setup::setup(factory_id.clone(), database_path.clone(), "example2", 30400),
        SupervisorOptions::default(),
    )?;

    let diagnostic_id = id.clone();
    driver.diagnostics().on_error(move |error| {
        eprintln!("{diagnostic_id}/supervisor: {error}");
    });

    let driver = tokio::task::spawn_local(driver.run());
    let mut terminate = signal(SignalKind::terminate())?;

    let work = async {
        supervisor.open().await?;

        // The launcher stops nodes 2 and 3 after node 1 finishes.
        if id != "1" {
            return std::future::pending::<Result<()>>().await;
        }

        let gate = supervisor
            .gate()
            .ok_or_else(|| error("supervisor has no Gate"))?;

        gate.await_leader(true).await?;

        // Queue the commands first so the leader can pair SYNC with the next CMD.
        let commands = (0..10)
            .map(|_| gate.append(common::command()))
            .collect::<Vec<_>>();

        for command in commands {
            let (seq, value) = command.await?;
            println!("CMD {seq}: {value}");
        }

        Ok(())
    };

    // Run until node 1 finishes its demonstration or a shutdown signal arrives.
    let result = tokio::select! {
        result = work => result,
        result = tokio::signal::ctrl_c() => result.map_err(Error::from),
        _ = terminate.recv() => Ok(()),
    };

    // Graceful close lets the database owners close before their children exit.
    let closed = supervisor.close(true).await;
    driver.await.map_err(join_error)?;

    result.and(closed)
}
