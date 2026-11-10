//! Benchmark-only propagation delay. It does not change MonsterFt's timers.
//!
//! Each frame gets its own enqueue-time deadline. One OS timer wakes the owner
//! for the earliest deadline; concurrently queued frames do not sleep serially.
use bytes::Bytes;
use serde::Serialize;
use std::{
    cell::{Cell, RefCell},
    collections::VecDeque,
    io,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    rc::Rc,
    time::{Duration, Instant},
};
use tokio::{io::unix::AsyncFd, sync::Notify};

const QUEUED_BYTES: usize = 256 * 1024 * 1024;
const QUEUED_FRAMES: usize = 4096;
const HISTOGRAM_US: usize = 16_384;
type Send = Box<dyn FnMut(&str, Bytes) -> io::Result<bool>>;

#[derive(Clone, Debug, Default, Serialize)]
pub struct DelayStats {
    pub requested_delay_us: f64,
    pub messages: u64,
    pub bytes: u64,
    pub mean_residence_us: f64,
    pub p50_residence_us: f64,
    pub p95_residence_us: f64,
    pub p99_residence_us: f64,
    pub max_residence_us: f64,
    pub mean_overshoot_us: f64,
    pub max_overshoot_us: f64,
    pub histogram_overflow: u64,
}
struct Counters {
    messages: u64,
    bytes: u64,
    residence_ns: u128,
    max_residence_ns: u128,
    overshoot_ns: u128,
    max_overshoot_ns: u128,
    histogram: Vec<u64>,
}
impl Default for Counters {
    fn default() -> Self {
        Self {
            messages: 0,
            bytes: 0,
            residence_ns: 0,
            max_residence_ns: 0,
            overshoot_ns: 0,
            max_overshoot_ns: 0,
            histogram: vec![0; HISTOGRAM_US + 1],
        }
    }
}
impl Counters {
    fn record(&mut self, bytes: usize, residence: Duration, requested: Duration) {
        let ns = residence.as_nanos();
        let overshoot = residence.saturating_sub(requested).as_nanos();
        self.messages += 1;
        self.bytes += bytes as u64;
        self.residence_ns += ns;
        self.max_residence_ns = self.max_residence_ns.max(ns);
        self.overshoot_ns += overshoot;
        self.max_overshoot_ns = self.max_overshoot_ns.max(overshoot);
        self.histogram[ns.div_ceil(1000).min(HISTOGRAM_US as u128) as usize] += 1;
    }
    fn percentile(&self, percent: u64) -> f64 {
        let target = (self.messages * percent).div_ceil(100);
        if target == 0 {
            return 0.0;
        }
        let mut total = 0;
        for (us, &count) in self.histogram.iter().enumerate() {
            total += count;
            if total >= target {
                return if us == HISTOGRAM_US {
                    self.max_residence_ns as f64 / 1000.0
                } else {
                    us as f64
                };
            }
        }
        0.0
    }
    fn stats(&self, delay: Duration) -> DelayStats {
        let denominator = self.messages.max(1) as f64 * 1000.0;
        DelayStats {
            requested_delay_us: delay.as_secs_f64() * 1e6,
            messages: self.messages,
            bytes: self.bytes,
            mean_residence_us: self.residence_ns as f64 / denominator,
            p50_residence_us: self.percentile(50),
            p95_residence_us: self.percentile(95),
            p99_residence_us: self.percentile(99),
            max_residence_us: self.max_residence_ns as f64 / 1000.0,
            mean_overshoot_us: self.overshoot_ns as f64 / denominator,
            max_overshoot_us: self.max_overshoot_ns as f64 / 1000.0,
            histogram_overflow: self.histogram[HISTOGRAM_US],
        }
    }
}
struct Pending {
    to: String,
    frame: Bytes,
    enqueued: Instant,
    deadline: Instant,
}
struct Shared {
    delay: Duration,
    send: RefCell<Send>,
    pending: RefCell<VecDeque<Pending>>,
    queued_bytes: Cell<usize>,
    stopped: Cell<bool>,
    wake: Notify,
    counters: RefCell<Counters>,
}
#[derive(Clone)]
pub struct DelaySender(Rc<Shared>);
pub struct DelayPump(Rc<Shared>);
impl DelaySender {
    pub fn new(
        delay: Duration,
        send: impl FnMut(&str, Bytes) -> io::Result<bool> + 'static,
    ) -> (Self, DelayPump) {
        let shared = Rc::new(Shared {
            delay,
            send: RefCell::new(Box::new(send)),
            pending: RefCell::new(VecDeque::new()),
            queued_bytes: Cell::new(0),
            stopped: Cell::new(false),
            wake: Notify::new(),
            counters: RefCell::new(Counters::default()),
        });
        (Self(shared.clone()), DelayPump(shared))
    }
    pub fn send(&self, to: &str, frame: Bytes) -> io::Result<()> {
        if self.0.stopped.get() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "delay transport stopped",
            ));
        }
        if self.0.delay.is_zero() {
            // The no-delay baseline bypasses the timer and staging queue entirely.
            let size = frame.len();
            self.0.transmit(to, frame)?;
            self.0
                .counters
                .borrow_mut()
                .record(size, Duration::ZERO, Duration::ZERO);
            return Ok(());
        }
        let mut pending = self.0.pending.borrow_mut();
        if pending.len() >= QUEUED_FRAMES
            || frame.len() > QUEUED_BYTES.saturating_sub(self.0.queued_bytes.get())
        {
            drop(pending);
            self.stop();
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "delay queue is full",
            ));
        }
        let enqueued = Instant::now();
        let Some(deadline) = enqueued.checked_add(self.0.delay) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "delay deadline overflow",
            ));
        };
        let empty = pending.is_empty();
        self.0
            .queued_bytes
            .set(self.0.queued_bytes.get() + frame.len());
        pending.push_back(Pending {
            to: to.into(),
            frame,
            enqueued,
            deadline,
        });
        drop(pending);
        if empty {
            self.0.wake.notify_one();
        }
        Ok(())
    }
    pub fn stop(&self) {
        self.0.stopped.set(true);
        self.0.pending.borrow_mut().clear();
        self.0.queued_bytes.set(0);
        self.0.wake.notify_one();
    }
    pub fn reset_stats(&self) {
        *self.0.counters.borrow_mut() = Counters::default();
    }
    pub fn stats(&self) -> DelayStats {
        self.0.counters.borrow().stats(self.0.delay)
    }
}
impl Shared {
    fn transmit(&self, to: &str, frame: Bytes) -> io::Result<()> {
        match (self.send.borrow_mut())(to, frame) {
            Ok(true) => Ok(()),
            Ok(false) => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "benchmark TCP frame was dropped",
            )),
            Err(error) => Err(error),
        }
    }
}
impl DelayPump {
    pub async fn run(self) -> io::Result<()> {
        let timer = if self.0.delay.is_zero() {
            None
        } else {
            Some(Timer::new()?)
        };
        loop {
            if self.0.stopped.get() {
                return Ok(());
            }
            let deadline = self.0.pending.borrow().front().map(|p| p.deadline);
            let Some(deadline) = deadline else {
                self.0.wake.notified().await;
                continue;
            };
            if deadline > Instant::now() {
                tokio::select! {
                    result = timer.as_ref().unwrap().wait_until(deadline) => result?,
                    _ = self.0.wake.notified() => continue,
                }
                continue;
            }
            let pending = self.0.pending.borrow_mut().pop_front().unwrap();
            self.0
                .queued_bytes
                .set(self.0.queued_bytes.get() - pending.frame.len());
            let residence = pending.enqueued.elapsed();
            let size = pending.frame.len();
            self.0.transmit(&pending.to, pending.frame)?;
            self.0
                .counters
                .borrow_mut()
                .record(size, residence, self.0.delay);
        }
    }
}

/// One nonblocking timer descriptor integrated into the current Tokio reactor.
/// Relative OS deadlines avoid relying on Rust Instant's private representation.
pub struct Timer(AsyncFd<OwnedFd>);
impl Timer {
    pub fn new() -> io::Result<Self> {
        let descriptor = native_create()?;
        // SAFETY: AsyncFd owns the descriptor, which is never replaced or closed early.
        let fd =
            unsafe { AsyncFd::register_with_interest(descriptor, tokio::io::Interest::READABLE) }
                .map_err(io::Error::other)?;
        Ok(Self(fd))
    }
    pub async fn wait_until(&self, deadline: Instant) -> io::Result<()> {
        loop {
            let now = Instant::now();
            if now >= deadline {
                return Ok(());
            }
            native_arm(self.0.get_ref().as_raw_fd(), deadline - now)?;
            loop {
                let mut ready = self.0.readable().await?;
                let received = ready.try_io(|fd| native_take(fd.get_ref().as_raw_fd()));
                match received {
                    Ok(Ok(())) => break,
                    Ok(Err(error)) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Ok(Err(error)) => return Err(error),
                    Err(_) => continue,
                }
            }
            // Recheck the exact deadline even if the OS reports an early or stale event.
        }
    }
}
#[cfg(target_os = "linux")]
fn native_create() -> io::Result<OwnedFd> {
    // SAFETY: the successful call returns a new descriptor, immediately owned here.
    let fd = unsafe {
        libc::timerfd_create(
            libc::CLOCK_MONOTONIC,
            libc::TFD_CLOEXEC | libc::TFD_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}
#[cfg(target_os = "macos")]
fn native_create() -> io::Result<OwnedFd> {
    // SAFETY: the successful call returns a new descriptor, immediately owned here.
    let fd = unsafe { libc::kqueue() };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(fd) };
    if unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}
#[cfg(target_os = "linux")]
fn native_arm(fd: RawFd, remaining: Duration) -> io::Result<()> {
    let value = libc::itimerspec {
        it_interval: libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
        it_value: libc::timespec {
            tv_sec: remaining.as_secs().try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "timer duration overflow")
            })?,
            tv_nsec: remaining.subsec_nanos() as _,
        },
    };
    // SAFETY: fd is a live timerfd and value is a fully initialized timer specification.
    if unsafe { libc::timerfd_settime(fd, 0, &value, std::ptr::null_mut()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(target_os = "macos")]
fn native_arm(fd: RawFd, remaining: Duration) -> io::Result<()> {
    let event = libc::kevent {
        ident: 1,
        filter: libc::EVFILT_TIMER,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
        data: remaining
            .as_nanos()
            .try_into()
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "timer duration overflow"))?,
        udata: std::ptr::null_mut(),
    };
    // SAFETY: fd is a live kqueue; registration reads one initialized event and never waits.
    if unsafe { libc::kevent(fd, &event, 1, std::ptr::null_mut(), 0, std::ptr::null()) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
#[cfg(target_os = "linux")]
fn native_take(fd: RawFd) -> io::Result<()> {
    let mut expirations = 0_u64;
    // SAFETY: the destination is writable for eight bytes and fd remains live/nonblocking.
    let count = unsafe { libc::read(fd, (&mut expirations as *mut u64).cast(), 8) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    if count != 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short timerfd read",
        ));
    }
    Ok(())
}
#[cfg(target_os = "macos")]
fn native_take(fd: RawFd) -> io::Result<()> {
    let mut event = std::mem::MaybeUninit::<libc::kevent>::uninit();
    let immediate = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: kevent writes at most one event; the zero timeout makes this nonblocking.
    let count = unsafe { libc::kevent(fd, std::ptr::null(), 0, event.as_mut_ptr(), 1, &immediate) };
    if count < 0 {
        return Err(io::Error::last_os_error());
    }
    if count == 0 {
        return Err(io::ErrorKind::WouldBlock.into());
    }
    let event = unsafe { event.assume_init() };
    if event.flags & libc::EV_ERROR != 0 {
        return Err(io::Error::from_raw_os_error(event.data as i32));
    }
    Ok(())
}
