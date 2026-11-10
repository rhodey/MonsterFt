//! Calibrate actual framed TCP round trips through the benchmark delay adapter.
mod delay;
use bytes::Bytes;
use delay::{DelaySender, DelayStats};
use monsterft::net::tcpcluster::{TcpCluster, TcpOptions, TcpPeer};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    io,
    time::{Duration, Instant},
};
use tokio::{sync::oneshot, task::LocalSet};

fn peers(base: u16) -> BTreeMap<String, TcpPeer> {
    [
        (
            "a".into(),
            TcpPeer {
                host: "127.0.0.1".into(),
                base_port: base,
            },
        ),
        (
            "b".into(),
            TcpPeer {
                host: "127.0.0.1".into(),
                base_port: base + 10,
            },
        ),
    ]
    .into_iter()
    .collect()
}
async fn connected(tcp: &TcpCluster, peer: &str) -> io::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !tcp.sender().connected(peer) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .map_err(io::Error::other)
}
async fn echo(
    base: u16,
    delay: Duration,
    mut stop: oneshot::Receiver<()>,
) -> io::Result<DelayStats> {
    let mut tcp = TcpCluster::open("b", peers(base), TcpOptions::default()).await?;
    let raw = tcp.sender();
    let (sender, pump) = DelaySender::new(delay, move |to, frame| raw.send(to, frame));
    let mut task = tokio::task::spawn_local(pump.run());
    let result = async {
        loop {
            tokio::select! {
                result = &mut task => return Err(io::Error::other(format!("echo delay stopped: {result:?}"))),
                message = tcp.recv() => {
                    let (from, frame) = message?.ok_or_else(||io::Error::other("echo TCP stopped"))?;
                    sender.send(&from, frame)?;
                },
                _ = &mut stop => return Ok(()),
            }
        }
    }.await;
    let stats = sender.stats();
    sender.stop();
    task.abort();
    let _ = task.await;
    tcp.close().await?;
    result?;
    Ok(stats)
}
#[derive(Serialize)]
struct Percentiles {
    min: f64,
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
}
#[derive(Serialize)]
struct Report {
    samples: usize,
    payload_bytes: usize,
    added_one_way_us: f64,
    rtt_us: Percentiles,
    client_delay: DelayStats,
    echo_delay: DelayStats,
}
async fn run(base: u16, delay: Duration, samples: usize) -> io::Result<Report> {
    let (stop, stopped) = oneshot::channel();
    let other = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        LocalSet::new().block_on(&runtime, echo(base, delay, stopped))
    });
    let mut tcp = TcpCluster::open("a", peers(base), TcpOptions::default()).await?;
    connected(&tcp, "b").await?;
    // Both directions must be connected before starting the echo workload.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let raw = tcp.sender();
    let (sender, pump) = DelaySender::new(delay, move |to, frame| raw.send(to, frame));
    let mut task = tokio::task::spawn_local(pump.run());
    sender.reset_stats();
    let mut times = Vec::with_capacity(samples);
    let result = async {
        let warm_until = Instant::now() + Duration::from_secs(1);
        let mut sequence = 0_u64;
        while times.len() < samples {
            let mut payload = [0u8; 256];
            payload[..8].copy_from_slice(&sequence.to_be_bytes());
            let frame = Bytes::copy_from_slice(&payload);
            let began = Instant::now();
            sender.send("b", frame.clone())?;
            let returned = tokio::select! {
                result = &mut task => return Err(io::Error::other(format!("client delay stopped: {result:?}"))),
                message = tcp.recv() => message?.ok_or_else(||io::Error::other("probe TCP stopped"))?,
            };
            let elapsed = began.elapsed().as_secs_f64() * 1e6;
            if returned.0 != "b" || returned.1 != frame { return Err(io::Error::other("probe returned different bytes")); }
            if Instant::now() >= warm_until { times.push(elapsed); }
            sequence += 1;
        }
        Ok(())
    }.await;
    let client_delay = sender.stats();
    sender.stop();
    task.abort();
    let _ = task.await;
    let _ = stop.send(());
    tcp.close().await?;
    let echo_delay = other
        .join()
        .map_err(|_| io::Error::other("probe echo panicked"))??;
    result?;
    times.sort_by(f64::total_cmp);
    let percentile =
        |fraction: f64| times[((times.len() as f64 * fraction).ceil() as usize).saturating_sub(1)];
    Ok(Report {
        samples,
        payload_bytes: 256,
        added_one_way_us: delay.as_secs_f64() * 1e6,
        rtt_us: Percentiles {
            min: times[0],
            p50: percentile(0.5),
            p95: percentile(0.95),
            p99: percentile(0.99),
            max: *times.last().unwrap(),
        },
        client_delay,
        echo_delay,
    })
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 3 {
        return Err(io::Error::other("expected basePort addedOneWayUs samples"));
    }
    let base: u16 = args[0].parse().map_err(io::Error::other)?;
    let micros: f64 = args[1].parse().map_err(io::Error::other)?;
    let samples: usize = args[2].parse().map_err(io::Error::other)?;
    if base > 65_523
        || !micros.is_finite()
        || !(0.0..=1e6).contains(&micros)
        || samples == 0
        || samples > 1_000_000
    {
        return Err(io::Error::other("invalid probe configuration"));
    }
    let report = LocalSet::new()
        .run_until(tokio::time::timeout(
            Duration::from_secs(40),
            run(base, Duration::from_secs_f64(micros / 1e6), samples),
        ))
        .await
        .map_err(io::Error::other)??;
    println!(
        "{}",
        serde_json::to_string(&report).map_err(io::Error::other)?
    );
    Ok(())
}
