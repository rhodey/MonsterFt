//! Linux SPSC byte rings. Only readiness/lifetime bytes use a socket;
//! complete MonsterFt frames travel through the rings, without re-encoding.
use bytes::Bytes;
use shmem_ipc::sharedring;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fs::File,
    io::{self, Read, Write},
    os::{
        fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
        unix::net::UnixStream,
    },
    rc::Rc,
};
use tokio::{io::unix::AsyncFd, sync::Notify};

#[derive(Clone, Debug)]
pub struct ShmemOptions {
    pub ring_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_queued_bytes: usize,
    pub max_queued_messages: usize,
}
impl Default for ShmemOptions {
    fn default() -> Self {
        Self {
            ring_bytes: 64 * 1024,
            max_frame_bytes: 128 * 1024 * 1024,
            max_queued_bytes: 256 * 1024 * 1024,
            max_queued_messages: 64,
        }
    }
}
impl ShmemOptions {
    fn validate(&self) -> io::Result<()> {
        if !(4096..=16 * 1024 * 1024).contains(&self.ring_bytes)
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > u32::MAX as usize
            || self.max_queued_bytes < self.max_frame_bytes
            || self.max_queued_messages == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shmem options are illegal",
            ));
        }
        Ok(())
    }
}
fn other(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}
fn stopped() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "shmem is stopped")
}
fn flags(fd: RawFd) -> io::Result<()> {
    // SAFETY: All callers retain an owner for this descriptor. Preserve other flags.
    for (get, set, flag) in [
        (libc::F_GETFL, libc::F_SETFL, libc::O_NONBLOCK),
        (libc::F_GETFD, libc::F_SETFD, libc::FD_CLOEXEC),
    ] {
        let old = unsafe { libc::fcntl(fd, get) };
        if old < 0 || unsafe { libc::fcntl(fd, set, old | flag) } < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}
fn register<T: AsRawFd>(fd: T) -> io::Result<AsyncFd<T>> {
    flags(fd.as_raw_fd())?;
    // SAFETY: AsyncFd owns the stable descriptor until deregistration.
    unsafe { AsyncFd::register_with_interest(fd, tokio::io::Interest::READABLE) }.map_err(other)
}
async fn signal(fd: &AsyncFd<File>) -> io::Result<()> {
    loop {
        let mut ready = fd.readable().await?;
        let result = ready.try_io(|fd| {
            let mut bytes = [0; 8];
            let count = fd.get_ref().read(&mut bytes)?;
            if count != 8 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "short eventfd read",
                ));
            }
            Ok(())
        });
        match result {
            Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {}
            Ok(r) => return r,
            Err(_) => {}
        }
    }
}
async fn control_byte(fd: &AsyncFd<UnixStream>) -> io::Result<u8> {
    loop {
        let mut ready = fd.readable().await?;
        match ready.try_io(|fd| {
            let mut byte = [0];
            if fd.get_ref().read(&mut byte)? == 0 {
                return Err(stopped());
            }
            Ok(byte[0])
        }) {
            Ok(Err(e)) if e.kind() == io::ErrorKind::Interrupted => {}
            Ok(r) => return r,
            Err(_) => {}
        }
    }
}

/// Transfer exactly once to the other process. No named files or stale namespace.
/// Layout: outgoing memfd/empty/full, incoming memfd/empty/full, lifetime socket.
pub struct ShmemHandles(pub [OwnedFd; 7]);
impl ShmemHandles {
    /// Claim descriptors mapped by the parent before opening any other resources.
    ///
    /// # Safety
    /// All seven descriptors must be distinct, live, otherwise unowned descriptors
    /// supplied by `ShmemEndpoint::pair`, with directions and ring capacity unchanged.
    /// There must be exactly one sender and receiver per ring, without reattachment.
    pub unsafe fn from_inherited(fds: [RawFd; 7]) -> io::Result<Self> {
        if fds.iter().any(|fd| *fd < 3)
            || fds.iter().enumerate().any(|(i, fd)| fds[..i].contains(fd))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "illegal shmem descriptors",
            ));
        }
        // SAFETY: The caller transfers unique ownership under the contract above.
        let owned = fds.map(|fd| unsafe { OwnedFd::from_raw_fd(fd) });
        for fd in &owned {
            flags(fd.as_raw_fd())?;
        }
        Ok(Self(owned))
    }
    pub fn open(self, options: ShmemOptions) -> io::Result<ShmemEndpoint> {
        options.validate()?;
        let [sm, se, sf, rm, re, rf, control] = self.0;
        let sm = File::from(sm);
        let rm = File::from(rm);
        // The crate maps the requested size. Reject a capacity/configuration mistake
        // before mapping past a memfd's EOF, which would otherwise cause SIGBUS.
        // SAFETY: sysconf has no pointer arguments and cannot modify our memory.
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(io::Error::other("OS page size is unavailable"));
        }
        let bytes = shmem_ipc::ringbuf::channel_bufsize::<u8>(options.ring_bytes)
            .div_ceil(page_size as usize)
            * page_size as usize;
        if sm.metadata()?.len() != bytes as u64 || rm.metadata()?.len() != bytes as u64 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shmem ring capacity differs",
            ));
        }
        let tx = sharedring::Sender::open(options.ring_bytes, sm, File::from(se), File::from(sf))
            .map_err(other)?;
        let rx = sharedring::Receiver::open(options.ring_bytes, rm, File::from(re), File::from(rf))
            .map_err(other)?;
        ShmemEndpoint::new(tx, rx, UnixStream::from(control), options)
    }
}
struct Queue {
    frames: VecDeque<Bytes>,
    bytes: usize,
    stopped: bool,
}
struct Outbound {
    queue: RefCell<Queue>,
    options: ShmemOptions,
    wake: Notify,
}
#[derive(Clone)]
pub struct ShmemSender(Rc<Outbound>);
impl ShmemSender {
    pub fn send(&self, frame: Bytes) -> io::Result<()> {
        let mut q = self.0.queue.borrow_mut();
        if q.stopped {
            return Err(stopped());
        }
        let error = if frame.len() > self.0.options.max_frame_bytes {
            Some(io::Error::new(
                io::ErrorKind::InvalidInput,
                "shmem frame is too large",
            ))
        } else if q.frames.len() >= self.0.options.max_queued_messages
            || frame.len() > self.0.options.max_queued_bytes.saturating_sub(q.bytes)
        {
            Some(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "shmem outbound queue is full",
            ))
        } else {
            None
        };
        if let Some(error) = error {
            q.stopped = true;
            q.frames.clear();
            q.bytes = 0;
            drop(q);
            self.0.wake.notify_one();
            return Err(error);
        }
        q.bytes += frame.len();
        q.frames.push_back(frame);
        drop(q);
        self.0.wake.notify_one();
        Ok(())
    }
    pub fn stop(&self) {
        let mut q = self.0.queue.borrow_mut();
        q.stopped = true;
        q.frames.clear();
        q.bytes = 0;
        drop(q);
        self.0.wake.notify_one();
    }
    pub fn is_stopped(&self) -> bool {
        self.0.queue.borrow().stopped
    }
    pub fn queued_bytes(&self) -> usize {
        self.0.queue.borrow().bytes
    }
}

/// Drive recv continuously on the owner thread. Partial ring reads/writes survive
/// future cancellation. A large frame can span many wraps of the bounded ring.
pub struct ShmemEndpoint {
    sender: ShmemSender,
    tx: sharedring::Sender<u8>,
    rx: sharedring::Receiver<u8>,
    writable: AsyncFd<File>,
    readable: AsyncFd<File>,
    control: AsyncFd<UnixStream>,
    ready: bool,
    sent: usize,
    header: [u8; 4],
    header_len: usize,
    payload: Vec<u8>,
}
impl ShmemEndpoint {
    pub fn pair(options: ShmemOptions) -> io::Result<(Self, ShmemHandles)> {
        options.validate()?;
        let tx = sharedring::Sender::<u8>::new(options.ring_bytes).map_err(other)?;
        let rx = sharedring::Receiver::<u8>::new(options.ring_bytes).map_err(other)?;
        let (a, b) = UnixStream::pair()?;
        let handles = ShmemHandles([
            rx.memfd().as_file().try_clone()?.into(),
            rx.empty_signal().try_clone()?.into(),
            rx.full_signal().try_clone()?.into(),
            tx.memfd().as_file().try_clone()?.into(),
            tx.empty_signal().try_clone()?.into(),
            tx.full_signal().try_clone()?.into(),
            b.into(),
        ]);
        Ok((Self::new(tx, rx, a, options)?, handles))
    }
    fn new(
        tx: sharedring::Sender<u8>,
        rx: sharedring::Receiver<u8>,
        control: UnixStream,
        options: ShmemOptions,
    ) -> io::Result<Self> {
        for fd in [
            tx.empty_signal(),
            tx.full_signal(),
            rx.empty_signal(),
            rx.full_signal(),
        ] {
            flags(fd.as_raw_fd())?;
        }
        let writable = register(tx.full_signal().try_clone()?)?;
        let readable = register(rx.empty_signal().try_clone()?)?;
        let control = register(control)?;
        // Fresh socket: one byte cannot fill it. This confirms successful attachment.
        control.get_ref().write_all(&[1])?;
        Ok(Self {
            tx,
            rx,
            writable,
            readable,
            control,
            ready: false,
            sent: 0,
            header: [0; 4],
            header_len: 0,
            payload: Vec::new(),
            sender: ShmemSender(Rc::new(Outbound {
                options,
                queue: RefCell::new(Queue {
                    frames: VecDeque::new(),
                    bytes: 0,
                    stopped: false,
                }),
                wake: Notify::new(),
            })),
        })
    }
    pub fn sender(&self) -> ShmemSender {
        self.sender.clone()
    }
    pub async fn ready(&mut self) -> io::Result<()> {
        while !self.ready {
            if self.sender.is_stopped() {
                return Err(stopped());
            }
            tokio::select! {
                byte = control_byte(&self.control) => {
                    match byte {
                        Ok(1) => self.ready = true,
                        Ok(_) => {
                            self.sender.stop();
                            return Err(io::Error::new(io::ErrorKind::InvalidData, "illegal shmem handshake"));
                        }
                        Err(error) => { self.sender.stop(); return Err(error); }
                    }
                },
                _ = self.sender.0.wake.notified() => {},
            }
        }
        Ok(())
    }
    fn publish(&mut self) -> io::Result<()> {
        // Bound each turn even if the consumer continuously frees ring space.
        let mut budget = self.sender.0.options.ring_bytes;
        while budget > 0 {
            let frame = {
                let q = self.sender.0.queue.borrow();
                if q.stopped {
                    return Err(stopped());
                }
                q.frames.front().cloned()
            };
            let Some(frame) = frame else {
                return Ok(());
            };
            let header = (frame.len() as u32).to_be_bytes();
            let mut written = 0;
            let offset = self.sent;
            self.tx
                .send_raw(|ptr, available| {
                    let mut count = available.min(budget).min(4 + frame.len() - offset);
                    let first = if offset < 4 {
                        (4 - offset).min(count)
                    } else {
                        0
                    };
                    // SAFETY: The ring grants this contiguous writable region. Our source
                    // slices are owned, disjoint, and copies are bounded by that region.
                    unsafe {
                        if first > 0 {
                            std::ptr::copy_nonoverlapping(header.as_ptr().add(offset), ptr, first);
                        }
                        count -= first;
                        if count > 0 {
                            std::ptr::copy_nonoverlapping(
                                frame.as_ptr().add(offset + first - 4),
                                ptr.add(first),
                                count,
                            );
                        }
                    }
                    written = first + count;
                    written
                })
                .map_err(other)?;
            if written == 0 {
                break;
            }
            budget -= written;
            self.sent += written;
            if self.sent == 4 + frame.len() {
                let mut q = self.sender.0.queue.borrow_mut();
                q.frames.pop_front();
                q.bytes -= frame.len();
                self.sent = 0;
            }
        }
        Ok(())
    }
    fn read_frame(&mut self) -> io::Result<Option<Bytes>> {
        let mut budget = self.sender.0.options.ring_bytes;
        while budget > 0 {
            let mut consumed = 0;
            let header_needed = self.header_len < 4;
            let limit = if header_needed {
                4 - self.header_len
            } else {
                u32::from_be_bytes(self.header) as usize - self.payload.len()
            };
            if limit == 0 {
                return Ok(Some(self.take_frame()));
            }
            self.rx
                .receive_raw(|ptr, available| {
                    let count = available.min(limit).min(budget);
                    // SAFETY: The ring supplies readable bytes until this callback returns.
                    // Copy into owned storage before consuming; never retain shared references.
                    unsafe {
                        if header_needed {
                            std::ptr::copy_nonoverlapping(
                                ptr,
                                self.header.as_mut_ptr().add(self.header_len),
                                count,
                            );
                        } else {
                            let len = self.payload.len();
                            self.payload.reserve(count);
                            std::ptr::copy_nonoverlapping(
                                ptr,
                                self.payload.as_mut_ptr().add(len),
                                count,
                            );
                            self.payload.set_len(len + count);
                        }
                    }
                    consumed = count;
                    count
                })
                .map_err(other)?;
            if consumed == 0 {
                break;
            }
            budget -= consumed;
            if header_needed {
                self.header_len += consumed;
                if self.header_len == 4
                    && u32::from_be_bytes(self.header) as usize
                        > self.sender.0.options.max_frame_bytes
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "shmem received oversized frame",
                    ));
                }
            }
            if self.header_len == 4
                && self.payload.len() == u32::from_be_bytes(self.header) as usize
            {
                return Ok(Some(self.take_frame()));
            }
        }
        Ok(None)
    }
    fn take_frame(&mut self) -> Bytes {
        self.header_len = 0;
        std::mem::take(&mut self.payload).into()
    }
    async fn receive(&mut self) -> io::Result<Bytes> {
        if !self.ready {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "shmem is not ready",
            ));
        }
        loop {
            self.publish()?;
            if let Some(frame) = self.read_frame()? {
                return Ok(frame);
            }
            let pending = !self.sender.0.queue.borrow().frames.is_empty();
            // A budget, not readiness, may have ended the turn. Recheck the rings
            // before sleeping; counters and eventfds cover concurrent transitions.
            if self.rx.receiver_mut().read_count().map_err(other)? > 0
                || (pending && self.tx.sender_mut().write_count().map_err(other)? > 0)
            {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                _=self.sender.0.wake.notified()=>{},
                result=signal(&self.readable)=>result?,
                result=signal(&self.writable),if pending=>result?,
                result = control_byte(&self.control) => {
                    result?;
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected shmem control byte"));
                },
            }
        }
    }
    pub async fn recv(&mut self) -> io::Result<Bytes> {
        let result = self.receive().await;
        if result.is_err() {
            self.sender.stop();
        }
        result
    }
}
impl Drop for ShmemEndpoint {
    fn drop(&mut self) {
        self.sender.stop();
    }
}
