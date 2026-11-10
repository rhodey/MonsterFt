use crate::config::{Config, Shared, error};
use bytes::Bytes;
use futures_util::{StreamExt, stream::FuturesUnordered};
use monsterft::{
    error::Result,
    gate::Gate,
    protocol::{CommandResult, Outcome},
};
use serde::Serialize;
use std::{
    future::Future,
    rc::Rc,
    time::{Duration, Instant},
};

struct Completed {
    ordinal: usize,
    result: CommandResult,
    latency_us: f64,
    elapsed_secs: f64,
}
fn submit(
    gate: &Gate,
    ordinal: usize,
    batch_size: usize,
    started: Instant,
) -> impl Future<Output = Result<Completed>> + use<> {
    let admitted = Instant::now();
    let operation = gate.append_batch(vec![Bytes::from_static(b"increment"); batch_size]);
    async move {
        let result = operation.await?;
        let completed = Instant::now();
        Ok(Completed {
            ordinal,
            result,
            latency_us: completed.duration_since(admitted).as_secs_f64() * 1_000_000.0,
            elapsed_secs: completed.duration_since(started).as_secs_f64(),
        })
    }
}

#[derive(Serialize)]
pub struct Latency {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
}
#[derive(Serialize)]
pub struct Phase {
    /// All admitted batches, including their bounded drain after the deadline.
    pub submitted_batches: usize,
    pub submitted_commands: usize,
    pub completed_batches_in_window: usize,
    pub completed_in_window: usize,
    pub elapsed_secs: f64,
    pub admission_window_secs: f64,
    pub drain_secs: f64,
    pub commands_per_sec: f64,
    pub latency_us: Latency,
    pub last_cmd_seq: i64,
}

#[derive(Clone, Copy)]
enum Limit {
    Count(usize),
    Time(Duration),
}
async fn phase(gate: &Gate, config: &Config, ordinal_base: usize, limit: Limit) -> Result<Phase> {
    let started = Instant::now();
    let deadline = match limit {
        Limit::Time(duration) => Some(started + duration),
        _ => None,
    };
    let limit_count = match limit {
        Limit::Count(count) => count,
        _ => usize::MAX,
    };
    let mut requests = FuturesUnordered::new();
    let mut submitted = 0;
    for _ in 0..config.window.min(limit_count) {
        requests.push(submit(
            gate,
            ordinal_base + submitted + 1,
            config.batch_size,
            started,
        ));
        submitted += 1;
    }
    let mut completed = Vec::with_capacity(16_384);
    while let Some(result) = requests.next().await {
        // Keep results opaque during the measured loop. Validate and sort below.
        completed.push(result?);
        if submitted < limit_count && deadline.is_none_or(|due| Instant::now() < due) {
            requests.push(submit(
                gate,
                ordinal_base + submitted + 1,
                config.batch_size,
                started,
            ));
            submitted += 1;
        }
    }
    let finished = Instant::now();
    let admission_window = match limit {
        Limit::Time(duration) => duration.as_secs_f64(),
        Limit::Count(_) => finished.duration_since(started).as_secs_f64(),
    };
    let elapsed = finished.duration_since(started).as_secs_f64();
    // Everything below is outside the timed workload, including correctness checks.
    if completed.len() != submitted {
        return Err(error("benchmark lost a command result"));
    }
    completed.sort_unstable_by_key(|item| item.ordinal);
    let mut last_seq = -1;
    for (index, item) in completed.iter().enumerate() {
        if item.ordinal != ordinal_base + index + 1 || item.result.cmd_seq <= last_seq {
            return Err(error(
                "benchmark results are not ordered exactly-once counter updates",
            ));
        }
        validate_batch(&item.result, item.ordinal, config.batch_size)?;
        last_seq = item.result.cmd_seq;
    }
    let in_window = completed
        .iter()
        .filter(|item| item.elapsed_secs <= admission_window)
        .count();
    let mut latencies: Vec<_> = completed.iter().map(|item| item.latency_us).collect();
    latencies.sort_unstable_by(f64::total_cmp);
    let percentile = |percent: usize| {
        latencies
            .get((latencies.len() * percent).div_ceil(100).saturating_sub(1))
            .copied()
            .unwrap_or(0.0)
    };
    Ok(Phase {
        submitted_batches: submitted,
        submitted_commands: submitted * config.batch_size,
        completed_batches_in_window: in_window,
        completed_in_window: in_window * config.batch_size,
        elapsed_secs: elapsed,
        admission_window_secs: admission_window,
        drain_secs: (elapsed - admission_window).max(0.0),
        commands_per_sec: (submitted * config.batch_size) as f64 / elapsed,
        latency_us: Latency {
            p50: percentile(50),
            p95: percentile(95),
            p99: percentile(99),
            max: latencies.last().copied().unwrap_or(0.0),
        },
        last_cmd_seq: last_seq,
    })
}

/// Verify every returned counter value after timing, including within each batch.
pub fn validate_batch(result: &CommandResult, ordinal: usize, batch_size: usize) -> Result<()> {
    if result.results.len() != batch_size {
        return Err(error("benchmark batch has the wrong result count"));
    }
    let first = (ordinal - 1) * batch_size + 1;
    for (index, outcome) in result.results.iter().enumerate() {
        if !matches!(outcome, Outcome::Fulfilled(value) if value.as_u64() == Some((first + index) as u64))
        {
            return Err(error(
                "benchmark batch results are not exactly-once counter updates",
            ));
        }
    }
    Ok(())
}

pub async fn run(
    gate: &Gate,
    config: &Config,
    shared: &Rc<Shared>,
    leader: &str,
    term: i64,
) -> Result<serde_json::Value> {
    shared.audit.set(true);
    let audit = phase(gate, config, 0, Limit::Count(config.audit_batches)).await?;
    wait_sync(shared, audit.last_cmd_seq).await?;
    shared.audit.set(false);
    let paired = shared.paired_appends.get();
    if (config.window == 1 && paired != 0) || (config.window > 1 && paired == 0) {
        return Err(error(
            "benchmark did not exercise its expected SYNC/CMD pairing path",
        ));
    }
    let warmup = if config.warmup_ms > 0 {
        phase(
            gate,
            config,
            config.audit_batches,
            Limit::Time(Duration::from_millis(config.warmup_ms)),
        )
        .await?
    } else {
        Phase {
            submitted_batches: 0,
            submitted_commands: 0,
            completed_batches_in_window: 0,
            completed_in_window: 0,
            elapsed_secs: 0.0,
            admission_window_secs: 0.0,
            drain_secs: 0.0,
            commands_per_sec: 0.0,
            latency_us: Latency {
                p50: 0.0,
                p95: 0.0,
                p99: 0.0,
                max: 0.0,
            },
            last_cmd_seq: audit.last_cmd_seq,
        }
    };
    wait_sync(shared, warmup.last_cmd_seq).await?;
    shared.check()?;
    shared.sent_frames.set(0);
    shared.sent_bytes.set(0);
    shared.received_frames.set(0);
    shared.received_bytes.set(0);
    shared
        .delay
        .borrow()
        .as_ref()
        .expect("transport is initialized")
        .reset_stats();
    shared.measuring.set(true);
    let measured = phase(
        gate,
        config,
        config.audit_batches + warmup.submitted_batches,
        Limit::Time(Duration::from_millis(config.measure_ms)),
    )
    .await;
    shared.measuring.set(false);
    let measured = measured?;
    shared.check()?;
    let delay = shared
        .delay
        .borrow()
        .as_ref()
        .expect("transport is initialized")
        .stats();
    Ok(serde_json::json!({
        "leader": leader, "term": term, "window": config.window, "batch_size": config.batch_size,
        "warmup_commands": warmup.submitted_commands,
        "warmup_batches": warmup.submitted_batches,
        "audit_commands": config.audit_batches * config.batch_size,
        "audit_batches": config.audit_batches,
        "measured_commands": measured.submitted_commands,
        "measured_batches": measured.submitted_batches,
        "completed_batches_in_admission_window": measured.completed_batches_in_window,
        "completed_in_admission_window": measured.completed_in_window,
        "measured_submitted_commands": measured.submitted_commands,
        "total_commands": config.audit_batches * config.batch_size + warmup.submitted_commands + measured.submitted_commands,
        "total_batches": config.audit_batches + warmup.submitted_batches + measured.submitted_batches,
        "last_cmd_seq": measured.last_cmd_seq,
        "elapsed_secs": measured.elapsed_secs, "admission_window_secs": measured.admission_window_secs, "drain_secs": measured.drain_secs,
        "commands_per_sec": measured.commands_per_sec, "latency_us": measured.latency_us,
        "latency_samples": measured.submitted_batches,
        "errors": 0, "restarts": 0,
        "audit": {"paired_appends": paired, "data_appends": shared.data_appends.get()},
        "traffic": {"sent_frames": shared.sent_frames.get(), "sent_bytes": shared.sent_bytes.get(),
            "received_frames": shared.received_frames.get(), "received_bytes": shared.received_bytes.get()},
        "delay": delay,
        "warmup": warmup,
    }))
}

pub async fn wait_sync(shared: &Shared, command: i64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), async {
        while shared.last_sync_cmd.get() < command {
            shared.check()?;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<(), monsterft::error::Error>(())
    })
    .await
    .map_err(|_| error("final SYNC did not reach the node"))??;
    Ok(())
}
