#[path = "../bench/delay.rs"]
mod delay;
use bytes::Bytes;
use delay::{DelaySender, Timer};
use std::{
    cell::RefCell,
    io,
    rc::Rc,
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::LocalSet};

#[tokio::test(flavor = "current_thread")]
async fn zero_delay_preserves_exact_bytes_without_a_running_pump() -> io::Result<()> {
    let frames = Rc::new(RefCell::new(Vec::new()));
    let saved = frames.clone();
    let (sender, pump) = DelaySender::new(Duration::ZERO, move |to, frame| {
        saved.borrow_mut().push((to.to_owned(), frame));
        Ok(true)
    });
    let payload = Bytes::from_static(b"exact\0bytes");
    sender.send("2", payload.clone())?;
    assert_eq!(frames.borrow().as_slice(), &[("2".into(), payload)]);
    assert_eq!(sender.stats().messages, 1);
    assert_eq!(sender.stats().p99_residence_us, 0.0);
    sender.reset_stats();
    assert_eq!(sender.stats().messages, 0);
    sender.stop();
    pump.run().await?;
    assert_eq!(
        sender.send("2", Bytes::new()).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn deadlines_are_per_frame_fifo_not_serial_sleeps() -> io::Result<()> {
    LocalSet::new()
        .run_until(async {
            let (tx, mut rx) = mpsc::unbounded_channel();
            let (sender, pump) = DelaySender::new(Duration::from_millis(20), move |to, frame| {
                tx.send((to.to_owned(), frame, Instant::now())).unwrap();
                Ok(true)
            });
            let started = Instant::now();
            for i in 0..20 {
                sender.send(if i % 2 == 0 { "2" } else { "3" }, Bytes::from(vec![i]))?;
            }
            let task = tokio::task::spawn_local(pump.run());
            let mut times = Vec::new();
            for i in 0..20 {
                let (to, bytes, at) = rx.recv().await.unwrap();
                assert_eq!(to, if i % 2 == 0 { "2" } else { "3" });
                assert_eq!(bytes.as_ref(), &[i]);
                assert!(at - started >= Duration::from_millis(20));
                times.push(at);
            }
            // Broad separation: accidentally sleeping 20ms independently for each
            // item would add ~380ms. No microsecond scheduling tolerance is asserted.
            assert!(times[19] - times[0] < Duration::from_millis(200));
            assert_eq!(sender.stats().messages, 20);
            sender.stop();
            task.await.unwrap()?;
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn stop_cancels_long_delay_and_transport_drop_is_an_error() -> io::Result<()> {
    LocalSet::new()
        .run_until(async {
            let (sender, pump) = DelaySender::new(Duration::from_secs(60), |_, _| {
                panic!("stopped frame must not publish")
            });
            sender.send("2", Bytes::from_static(b"pending"))?;
            let task = tokio::task::spawn_local(pump.run());
            tokio::task::yield_now().await;
            sender.stop();
            task.await.unwrap()?;
            let (sender, pump) = DelaySender::new(Duration::from_micros(50), |_, _| Ok(false));
            sender.send("2", Bytes::new())?;
            assert_eq!(
                pump.run().await.unwrap_err().kind(),
                io::ErrorKind::BrokenPipe
            );
            Ok(())
        })
        .await
}

#[tokio::test(flavor = "current_thread")]
async fn timer_can_be_rearmed_after_completion_or_cancellation() -> io::Result<()> {
    let timer = Timer::new()?;
    for us in [50, 220, 50] {
        let deadline = Instant::now() + Duration::from_micros(us);
        timer.wait_until(deadline).await?;
        assert!(Instant::now() >= deadline);
    }
    {
        let waiting = timer.wait_until(Instant::now() + Duration::from_secs(60));
        tokio::pin!(waiting);
        tokio::select! {_=&mut waiting=>panic!("long timer fired early"),_=tokio::task::yield_now()=>{}}
    }
    let deadline = Instant::now() + Duration::from_micros(50);
    timer.wait_until(deadline).await?;
    assert!(Instant::now() >= deadline);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn submillisecond_timer_reports_its_observed_accuracy() -> io::Result<()> {
    let timer = Timer::new()?;
    for us in [50, 220] {
        let mut samples = Vec::new();
        for _ in 0..200 {
            let start = Instant::now();
            timer.wait_until(start + Duration::from_micros(us)).await?;
            samples.push(start.elapsed().as_nanos() as u64);
        }
        samples.sort_unstable();
        eprintln!(
            "native timer requested={us}us median={}us p95={}us p99={}us",
            samples[100] as f64 / 1000.0,
            samples[190] as f64 / 1000.0,
            samples[198] as f64 / 1000.0
        );
        assert!(samples[0] >= us * 1000);
    }
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn delay_queue_capacity_fails_instead_of_dropping_frames() -> io::Result<()> {
    let (sender, pump) = DelaySender::new(Duration::from_secs(60), |_, _| {
        panic!("overflowed queue must never publish")
    });
    for _ in 0..4096 {
        sender.send("2", Bytes::new())?;
    }
    assert_eq!(
        sender.send("2", Bytes::new()).unwrap_err().kind(),
        io::ErrorKind::OutOfMemory
    );
    assert_eq!(
        sender.send("2", Bytes::new()).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    pump.run().await?;
    Ok(())
}
