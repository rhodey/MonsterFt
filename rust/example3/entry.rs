mod common;

// Reuse the IPC and TCP plumbing from the supervised counter example.
#[path = "../example2/setup.rs"]
mod setup;

use monsterft::{
    error::{Error, Result},
    supervisor::{Supervisor, SupervisorOptions},
};
use serde_json::json;
use setup::{error, join_error};
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

    common::fact_url()?;
    let database_path = common::data_directory().join(&id);

    // Supervisor calls this factory again for every replacement attempt.
    let factory_id = id.clone();
    let (supervisor, mut driver) = Supervisor::new(
        move || setup::setup(factory_id.clone(), database_path.clone(), "example3", 30500),
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

        // A successful POST result means a quorum has reported the same fact.
        let request_id = common::request_id()?;
        let post = json!({
            "method": "POST",
            "requestId": request_id,
            "fact": {
                "account": "alice",
                "balance": 42,
            },
        });
        let post_command = post.to_string().into_bytes().into();
        let (seq, result) = gate.append(post_command).await?;

        println!("POST {seq}: {result}");

        // Every member uses this observation ID and receives the same frozen result.
        let get = json!({
            "method": "GET",
            "requestId": request_id,
            "observationId": common::request_id()?,
        });
        let get_command = get.to_string().into_bytes().into();
        let (seq, result) = gate.append(get_command).await?;

        println!("GET {seq}: {result}");

        let result: serde_json::Value =
            rmpv::ext::from_value(result).map_err(|e| error(e.to_string()))?;

        if result["status"] != "confirmed" {
            return Err(error("the remote service did not confirm the fact"));
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
