mod audit;
mod children;
mod config;
mod delay;
mod runtime;
mod workload;

use config::{Config, NODES, Shared, error, read_json, write_json};
use monsterft::{
    error::Result,
    supervisor::{Supervisor, SupervisorOptions},
};
use serde::{Deserialize, Serialize};
use std::{path::Path, rc::Rc, time::Duration};
use tokio::{
    signal::unix::{SignalKind, signal},
    task::LocalSet,
};

#[derive(Clone, Deserialize, Serialize, PartialEq)]
struct Start {
    leader: String,
    term: i64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    LocalSet::new().run_until(run()).await
}
async fn run() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("node") if args.len() == 3 && NODES.contains(&args[1].as_str()) => {
            let config = Config::read(Path::new(&args[2]))?;
            node(args[1].clone(), config).await
        }
        Some(role @ ("procA" | "procB" | "procC" | "apply")) => {
            children::run(role, &args[1..]).await
        }
        _ => Err(error(
            "usage: monsterft-bench node <id> <config.json>, or an owner role with inherited arguments",
        )),
    }
}

async fn node(id: String, config: Config) -> Result<()> {
    let shared = Shared::new();
    let factory_id = id.clone();
    let factory_config = config.clone();
    let factory_shared = shared.clone();
    let (supervisor, mut driver) = Supervisor::new(
        move || {
            runtime::setup(
                factory_id.clone(),
                factory_config.run_dir.join("data").join(&factory_id),
                factory_config.clone(),
                factory_shared.clone(),
            )
        },
        SupervisorOptions::default(),
    )?;
    let diagnostic_id = id.clone();
    let diagnostics = shared.clone();
    driver.diagnostics().on_error(move |e| {
        eprintln!("{diagnostic_id}/supervisor: {e}");
        diagnostics.fail(format!("Supervisor: {e}"));
    });
    let driver = tokio::task::spawn_local(driver.run());
    let mut terminate = signal(SignalKind::terminate())?;
    let result = tokio::select! {
        result = serve(&id, &config, &supervisor, &shared) => result,
        result = monitor(&shared) => result,
        _ = terminate.recv() => Err(error("benchmark interrupted")),
        result = tokio::signal::ctrl_c() => { result?; Err(error("benchmark interrupted")) },
    };
    if let Err(error) = &result {
        let _ = write_json(
            &config.run_dir.join(format!("failed-{id}.json")),
            &serde_json::json!({"id": id, "error": error.to_string()}),
        );
    }
    let close = supervisor.close(result.is_ok()).await;
    driver.await.map_err(|e| error(e.to_string()))?;
    if let Err(error) = &close {
        let _ = write_json(
            &config.run_dir.join(format!("failed-{id}.json")),
            &serde_json::json!({"id": id, "error": error.to_string()}),
        );
    }
    result.and(close)
}

async fn monitor(shared: &Shared) -> Result<()> {
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        shared.check()?;
    }
}

async fn serve(
    id: &str,
    config: &Config,
    supervisor: &Supervisor,
    shared: &Rc<Shared>,
) -> Result<()> {
    supervisor.open().await?;
    let gate = supervisor
        .gate()
        .ok_or_else(|| error("supervisor has no Gate"))?;
    gate.await_leader(true).await?;
    let mut ready = None;
    let start = loop {
        shared.check()?;
        let status = gate.status();
        if let Some(leader) = status.leader
            && status.commit_term == status.term
        {
            let current = Start {
                leader,
                term: status.term,
            };
            if ready.as_ref() != Some(&current) {
                write_json(
                    &config.run_dir.join(format!("ready-{id}.json")),
                    &serde_json::json!({"id": id, "leader": current.leader, "term": current.term, "pid": std::process::id()}),
                )?;
                ready = Some(current);
            }
        }
        if let Some(start) = read_json::<Start>(&config.run_dir.join("go.json"))? {
            break start;
        }
        if config.run_dir.join("stop").exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let status = gate.status();
    if status.leader.as_ref() != Some(&start.leader) || status.term != start.term {
        return Err(error("election changed before benchmark start"));
    }
    *shared.expected_leader.borrow_mut() = Some((start.leader.clone(), start.term));
    shared.active.set(true);
    shared.dropped.set(0);
    if start.leader == id {
        let done = workload::run(&gate, config, shared, &start.leader, start.term).await?;
        write_json(&config.run_dir.join("done.json"), &done)?;
    }
    let done: serde_json::Value = loop {
        shared.check()?;
        if let Some(done) = read_json(&config.run_dir.join("done.json"))? {
            break done;
        }
        if config.run_dir.join("stop").exists() {
            return Err(error("benchmark stopped before completion"));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let last = done["last_cmd_seq"]
        .as_i64()
        .ok_or_else(|| error("missing final command sequence"))?;
    workload::wait_sync(shared, last).await?;
    shared.check()?;
    write_json(
        &config.run_dir.join(format!("settled-{id}.json")),
        &serde_json::json!({
            "id": id, "last_sync_cmd_seq": shared.last_sync_cmd.get(), "term": gate.status().term,
            "errors": shared.errors.borrow().len(), "restarts": shared.attempts.get()-1,
            "dropped_frames": shared.dropped.get(),
        }),
    )?;
    // Measurement and final agreement are complete; peer teardown is now normal.
    shared.active.set(false);
    *shared.expected_leader.borrow_mut() = None;
    while !config.run_dir.join("stop").exists() {
        shared.check()?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    shared.active.set(false);
    Ok(())
}
