//! Fixed-membership TCP routing: one incoming and outgoing connection per peer.
use super::tcp::{self, FrameOptions, FrameReader};
use bytes::Bytes;
use futures_util::FutureExt;
use std::{
    cell::{Cell, RefCell},
    collections::BTreeMap,
    future::Future,
    io,
    net::IpAddr,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, mpsc, watch},
    task::{AbortHandle, JoinHandle},
};

#[derive(Clone, Debug)]
pub struct TcpPeer {
    pub host: String,
    pub base_port: u16,
}
#[derive(Clone, Debug)]
pub struct TcpOptions {
    pub bind_host: IpAddr,
    pub reconnect_delay: Duration,
    pub connect_timeout: Duration,
    pub frames: FrameOptions,
    pub inbound_bytes: usize,
    pub inbound_messages: usize,
}
impl Default for TcpOptions {
    fn default() -> Self {
        Self {
            bind_host: IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
            reconnect_delay: Duration::from_millis(250),
            connect_timeout: Duration::from_secs(5),
            frames: FrameOptions::default(),
            inbound_bytes: tcp::MAX_QUEUED_BYTES,
            inbound_messages: 64,
        }
    }
}

#[derive(Clone)]
struct Failure {
    kind: io::ErrorKind,
    message: String,
}
impl Failure {
    fn from(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            message: error.to_string(),
        }
    }
    fn error(&self) -> io::Error {
        io::Error::new(self.kind, self.message.clone())
    }
}
type Listener = Rc<RefCell<Box<dyn FnMut(&io::Error)>>>;

struct Budget {
    bytes: Cell<usize>,
    limit: usize,
    available: Notify,
}
impl Budget {
    fn reserve(self: &Rc<Self>, bytes: usize) -> Option<Charge> {
        if bytes > self.limit.saturating_sub(self.bytes.get()) {
            return None;
        }
        self.bytes.set(self.bytes.get() + bytes);
        Some(Charge {
            budget: self.clone(),
            bytes,
        })
    }
}
struct Charge {
    budget: Rc<Budget>,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.bytes.set(self.budget.bytes.get() - self.bytes);
        self.budget.available.notify_one();
    }
}
struct QueuedFrame {
    frame: Bytes,
    _charge: Charge,
}
struct Outgoing {
    epoch: u64,
    sender: mpsc::Sender<QueuedFrame>,
    budget: Rc<Budget>,
}
struct Incoming {
    epoch: u64,
    abort: Option<AbortHandle>,
}
struct Received {
    peer: String,
    epoch: u64,
    frame: QueuedFrame,
}

struct Shared {
    id: String,
    peers: BTreeMap<String, TcpPeer>,
    options: TcpOptions,
    open: Cell<bool>,
    stopped: Cell<bool>,
    epoch: Cell<u64>,
    failure: watch::Sender<Option<Failure>>,
    stopped_signal: watch::Sender<bool>,
    outgoing: RefCell<BTreeMap<String, Outgoing>>,
    incoming: RefCell<BTreeMap<String, Incoming>>,
    tasks: RefCell<Vec<JoinHandle<()>>>,
    warnings: RefCell<Vec<Listener>>,
    errors: RefCell<Vec<Listener>>,
    mailbox: mpsc::Sender<Received>,
    inbound: Rc<Budget>,
}
impl Shared {
    fn stopped_error(&self) -> io::Error {
        self.failure
            .borrow()
            .as_ref()
            .map(Failure::error)
            .unwrap_or_else(|| {
                io::Error::new(io::ErrorKind::NotConnected, "TCP transport is stopped")
            })
    }
    fn stop(&self) {
        if self.stopped.replace(true) {
            return;
        }
        self.open.set(false);
        self.stopped_signal.send_replace(true);
        self.outgoing.borrow_mut().clear();
        self.incoming.borrow_mut().clear();
        for task in self.tasks.borrow().iter() {
            task.abort();
        }
        self.inbound.available.notify_waiters();
    }
    fn fatal(&self, error: io::Error) {
        if self.stopped.get() {
            return;
        }
        self.failure.send_replace(Some(Failure::from(&error)));
        self.stop();
        let listeners = self.errors.borrow().clone();
        for listener in listeners {
            let _ = catch_unwind(AssertUnwindSafe(|| {
                if let Ok(mut listener) = listener.try_borrow_mut() {
                    listener(&error);
                }
            }));
        }
    }
    fn warn(&self, error: io::Error) {
        if self.stopped.get() {
            return;
        }
        let listeners = self.warnings.borrow().clone();
        for listener in listeners {
            let result = catch_unwind(AssertUnwindSafe(|| {
                listener.borrow_mut()(&error);
            }));
            if result.is_err() {
                self.fatal(io::Error::other("TCP warning listener panicked"));
                return;
            }
            if self.stopped.get() {
                return;
            }
        }
    }
    fn next_epoch(&self) -> io::Result<u64> {
        let next = self
            .epoch
            .get()
            .checked_add(1)
            .ok_or_else(|| io::Error::other("TCP connection identity exhausted"))?;
        self.epoch.set(next);
        Ok(next)
    }
    fn spawn(self: &Rc<Self>, future: impl Future<Output = ()> + 'static) -> AbortHandle {
        let state = self.clone();
        let task = tokio::task::spawn_local(async move {
            if AssertUnwindSafe(future).catch_unwind().await.is_err() {
                state.fatal(io::Error::other("TCP transport task panicked"));
            }
        });
        let abort = task.abort_handle();
        let mut tasks = self.tasks.borrow_mut();
        tasks.retain(|task| !task.is_finished());
        tasks.push(task);
        abort
    }
}

/// Cloneable, owner-thread synchronous transport handle.
#[derive(Clone)]
pub struct TcpSender(Rc<Shared>);
impl TcpSender {
    /// `false` means a lossy remote drop, never an RPC acknowledgment.
    pub fn send(&self, to: &str, frame: Bytes) -> io::Result<bool> {
        let state = &self.0;
        if state.stopped.get() || !state.open.get() {
            return Err(state.stopped_error());
        }
        if to == state.id || !state.peers.contains_key(to) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP destination is illegal",
            ));
        }
        if frame.len() > state.options.frames.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TCP frame is too large",
            ));
        }
        let outgoing = state.outgoing.borrow();
        let Some(connection) = outgoing.get(to) else {
            return Ok(false);
        };
        if connection.sender.is_closed() {
            return Ok(false);
        }
        let Some(charge) = connection.budget.reserve(frame.len() + 4) else {
            drop(outgoing);
            state.warn(io::Error::other(format!("TCP peer {to} queue is full")));
            return Ok(false);
        };
        let result = connection.sender.try_send(QueuedFrame {
            frame,
            _charge: charge,
        });
        drop(outgoing);
        match result {
            Ok(()) => Ok(true),
            Err(mpsc::error::TrySendError::Closed(_)) => Ok(false),
            Err(mpsc::error::TrySendError::Full(_)) => {
                state.warn(io::Error::other(format!("TCP peer {to} queue is full")));
                Ok(false)
            }
        }
    }
    pub fn is_open(&self) -> bool {
        self.0.open.get()
    }
    pub fn is_stopped(&self) -> bool {
        self.0.stopped.get()
    }
    pub fn stop(&self) {
        self.0.stop();
    }
    pub fn on_warn(&self, listener: impl FnMut(&io::Error) + 'static) {
        self.0
            .warnings
            .borrow_mut()
            .push(Rc::new(RefCell::new(Box::new(listener))));
    }
    pub fn on_error(&self, listener: impl FnMut(&io::Error) + 'static) {
        self.0
            .errors
            .borrow_mut()
            .push(Rc::new(RefCell::new(Box::new(listener))));
    }
    pub fn connected(&self, peer: &str) -> bool {
        self.0
            .outgoing
            .borrow()
            .get(peer)
            .is_some_and(|value| !value.sender.is_closed())
    }
    pub fn queued_bytes(&self, peer: &str) -> usize {
        self.0
            .outgoing
            .borrow()
            .get(peer)
            .map_or(0, |value| value.budget.bytes.get())
    }
    /// Normal shutdown leaves this future pending. Terminal helper failures resolve it.
    pub fn failure(&self) -> impl Future<Output = io::Error> + 'static {
        let state = self.0.clone();
        let mut receiver = state.failure.subscribe();
        async move {
            loop {
                if let Some(error) = receiver.borrow().as_ref() {
                    return error.error();
                }
                if receiver.changed().await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

pub struct TcpCluster {
    sender: TcpSender,
    receiver: mpsc::Receiver<Received>,
    close_result: Option<Option<Failure>>,
    close_task: Option<JoinHandle<Option<Failure>>>,
}
impl TcpCluster {
    /// Requires a current-thread runtime with LocalSet. Only local listeners block open.
    pub async fn open(
        id: impl Into<String>,
        peers: BTreeMap<String, TcpPeer>,
        options: TcpOptions,
    ) -> io::Result<Self> {
        let id = id.into();
        validate(&id, &peers, &options)?;
        let ids: Vec<_> = peers.keys().cloned().collect();
        let own_index = ids.iter().position(|peer| peer == &id).unwrap();
        let mut listeners = Vec::new();
        for (index, peer) in ids.iter().enumerate() {
            if peer != &id {
                let port = port(peers[&id].base_port, index)?;
                let listener = TcpListener::bind((options.bind_host, port)).await?;
                listeners.push((peer.clone(), listener));
            }
        }
        let (mailbox, receiver) = mpsc::channel(options.inbound_messages);
        let (failure, _) = watch::channel(None);
        let (stopped_signal, _) = watch::channel(false);
        let state = Rc::new(Shared {
            id,
            peers,
            inbound: Rc::new(Budget {
                bytes: Cell::new(0),
                limit: options.inbound_bytes,
                available: Notify::new(),
            }),
            options,
            open: Cell::new(true),
            stopped: Cell::new(false),
            epoch: Cell::new(0),
            failure,
            stopped_signal,
            outgoing: RefCell::new(BTreeMap::new()),
            incoming: RefCell::new(BTreeMap::new()),
            tasks: RefCell::new(Vec::new()),
            warnings: RefCell::new(Vec::new()),
            errors: RefCell::new(Vec::new()),
            mailbox,
        });
        for (peer, listener) in listeners {
            let own = state.clone();
            state.spawn(async move {
                accept_loop(own, peer, listener).await;
            });
        }
        for peer in ids {
            if peer != state.id {
                let own = state.clone();
                state.spawn(async move {
                    connect_loop(own, peer, own_index).await;
                });
            }
        }
        Ok(Self {
            sender: TcpSender(state),
            receiver,
            close_result: None,
            close_task: None,
        })
    }
    pub fn sender(&self) -> TcpSender {
        self.sender.clone()
    }
    pub fn send(&self, to: &str, frame: Bytes) -> io::Result<bool> {
        self.sender.send(to, frame)
    }
    pub fn on_warn(&self, listener: impl FnMut(&io::Error) + 'static) {
        self.sender.on_warn(listener);
    }
    pub fn on_error(&self, listener: impl FnMut(&io::Error) + 'static) {
        self.sender.on_error(listener);
    }
    pub fn is_open(&self) -> bool {
        self.sender.is_open()
    }
    pub fn is_stopped(&self) -> bool {
        self.sender.is_stopped()
    }
    pub fn stop(&self) {
        self.sender.stop();
    }
    pub fn failure(&self) -> impl Future<Output = io::Error> + 'static {
        self.sender.failure()
    }
    pub async fn recv(&mut self) -> io::Result<Option<(String, Bytes)>> {
        let state = &self.sender.0;
        let mut stopped = state.stopped_signal.subscribe();
        loop {
            if state.stopped.get() {
                return match state.failure.borrow().as_ref() {
                    Some(error) => Err(error.error()),
                    None => Ok(None),
                };
            }
            let message = tokio::select! {message=self.receiver.recv()=>message,_=stopped.changed()=>continue};
            let Some(message) = message else {
                return Ok(None);
            };
            if state
                .incoming
                .borrow()
                .get(&message.peer)
                .is_some_and(|current| current.epoch == message.epoch)
            {
                return Ok(Some((message.peer, message.frame.frame)));
            }
        }
    }
    /// Optional callback driver. A listener panic terminates the transport.
    pub async fn run(&mut self, mut listener: impl FnMut(String, Bytes)) -> io::Result<()> {
        while let Some((peer, frame)) = self.recv().await? {
            if catch_unwind(AssertUnwindSafe(|| listener(peer, frame))).is_err() {
                self.sender
                    .0
                    .fatal(io::Error::other("TCP message listener panicked"));
                return Err(self.sender.0.stopped_error());
            }
        }
        Ok(())
    }
    /// Cleanup starts immediately; dropping the waiter does not cancel it.
    pub fn close(&mut self) -> impl Future<Output = io::Result<()>> + '_ {
        self.stop();
        if self.close_result.is_none() && self.close_task.is_none() {
            let tasks = std::mem::take(&mut *self.sender.0.tasks.borrow_mut());
            self.close_task = Some(tokio::task::spawn_local(async move {
                let mut error = None;
                for task in tasks {
                    if let Err(join) = task.await
                        && !join.is_cancelled()
                    {
                        error.get_or_insert_with(|| {
                            Failure::from(&io::Error::other(join.to_string()))
                        });
                    }
                }
                error
            }));
        }
        async move {
            if let Some(result) = &self.close_result {
                return match result {
                    Some(error) => Err(error.error()),
                    None => Ok(()),
                };
            }
            // Await by reference so cancellation leaves the owned cleanup task available.
            let error = match self.close_task.as_mut().unwrap().await {
                Ok(error) => error,
                Err(error) => Some(Failure::from(&io::Error::other(error.to_string()))),
            };
            self.close_task = None;
            self.receiver.close();
            while self.receiver.try_recv().is_ok() {}
            self.sender.0.warnings.borrow_mut().clear();
            self.sender.0.errors.borrow_mut().clear();
            self.close_result = Some(error.clone());
            match error {
                Some(error) => Err(error.error()),
                None => Ok(()),
            }
        }
    }
}
impl Drop for TcpCluster {
    fn drop(&mut self) {
        self.stop();
        self.sender.0.warnings.borrow_mut().clear();
        self.sender.0.errors.borrow_mut().clear();
    }
}

/// The sender's position in sorted membership determines the destination port.
pub fn port(base_port: u16, sender_index: usize) -> io::Result<u16> {
    u16::try_from(sender_index)
        .ok()
        .and_then(|index| base_port.checked_add(index))
        .filter(|port| *port != 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "TCP derived port is illegal"))
}
fn validate(id: &str, peers: &BTreeMap<String, TcpPeer>, options: &TcpOptions) -> io::Result<()> {
    options.frames.validate()?;
    if id.is_empty() || !peers.contains_key(id) || peers.keys().any(String::is_empty) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TCP node id is illegal",
        ));
    }
    if options.connect_timeout.is_zero()
        || options.inbound_messages == 0
        || options.inbound_bytes < options.frames.max_frame_bytes.saturating_add(4)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "TCP options are illegal",
        ));
    }
    for (node, peer) in peers {
        if peer.host.is_empty() || peer.base_port == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("TCP peer {node} endpoint is illegal"),
            ));
        }
        for (index, sender) in peers.keys().enumerate() {
            if sender != node {
                port(peer.base_port, index)?;
            }
        }
    }
    Ok(())
}

async fn connect_loop(state: Rc<Shared>, peer: String, index: usize) {
    let endpoint = &state.peers[&peer];
    let address = (
        endpoint.host.clone(),
        port(endpoint.base_port, index).unwrap(),
    );
    loop {
        if state.stopped.get() {
            return;
        }
        let result = tcp::connect(
            (address.0.as_str(), address.1),
            state.options.connect_timeout,
        )
        .await;
        match result {
            Ok(stream) => {
                let epoch = match state.next_epoch() {
                    Ok(value) => value,
                    Err(error) => {
                        state.fatal(error);
                        return;
                    }
                };
                let (tx, mut rx) =
                    mpsc::channel::<QueuedFrame>(state.options.frames.max_queued_messages);
                let budget = Rc::new(Budget {
                    bytes: Cell::new(0),
                    limit: state.options.frames.max_queued_bytes,
                    available: Notify::new(),
                });
                state.outgoing.borrow_mut().insert(
                    peer.clone(),
                    Outgoing {
                        epoch,
                        sender: tx,
                        budget,
                    },
                );
                let (reader, mut writer) = stream.into_split();
                let mut reader =
                    FrameReader::new(reader, state.options.frames.max_frame_bytes).unwrap();
                let writing = async {
                    while let Some(frame) = rx.recv().await {
                        tcp::write_frame(
                            &mut writer,
                            &frame.frame,
                            state.options.frames.max_frame_bytes,
                        )
                        .await?;
                    }
                    Ok::<(), io::Error>(())
                };
                let unexpected = async {
                    match reader.recv().await? {
                        Some(_) => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "peer sent on an outgoing connection",
                        )),
                        None => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "disconnected")),
                    }
                };
                let result = tokio::select! {result=writing=>result,result=unexpected=>result};
                let current = state
                    .outgoing
                    .borrow()
                    .get(&peer)
                    .is_some_and(|current| current.epoch == epoch);
                if current {
                    state.outgoing.borrow_mut().remove(&peer);
                    if let Err(error) = result {
                        state.warn(io::Error::new(
                            error.kind(),
                            format!("TCP peer {peer} {error}"),
                        ));
                    }
                }
            }
            Err(error) => state.warn(io::Error::new(
                error.kind(),
                format!("TCP peer {peer} {error}"),
            )),
        }
        if state.stopped.get() {
            return;
        }
        tokio::time::sleep(state.options.reconnect_delay).await;
    }
}

async fn accept_loop(state: Rc<Shared>, peer: String, listener: TcpListener) {
    loop {
        match listener.accept().await {
            Err(error) => {
                state.fatal(error);
                return;
            }
            Ok((stream, _)) => {
                if state.stopped.get() {
                    return;
                }
                if let Err(error) = stream.set_nodelay(true) {
                    state.warn(error);
                    continue;
                }
                let epoch = match state.next_epoch() {
                    Ok(value) => value,
                    Err(error) => {
                        state.fatal(error);
                        return;
                    }
                };
                if let Some(old) = state.incoming.borrow_mut().remove(&peer)
                    && let Some(abort) = old.abort
                {
                    abort.abort();
                }
                let own = state.clone();
                let name = peer.clone();
                let abort = state.spawn(async move {
                    receive_loop(own, name, epoch, stream).await;
                });
                state.incoming.borrow_mut().insert(
                    peer.clone(),
                    Incoming {
                        epoch,
                        abort: Some(abort),
                    },
                );
            }
        }
    }
}

async fn receive_loop(state: Rc<Shared>, peer: String, epoch: u64, stream: TcpStream) {
    let mut reader = FrameReader::new(stream, state.options.frames.max_frame_bytes).unwrap();
    let result: io::Result<()> = async {
        loop {
            let Some(frame) = reader.recv().await? else {
                return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "disconnected"));
            };
            let size = frame.len() + 4;
            let charge = loop {
                let available = state.inbound.available.notified();
                if let Some(charge) = state.inbound.reserve(size) {
                    break charge;
                }
                available.await;
            };
            state
                .mailbox
                .send(Received {
                    peer: peer.clone(),
                    epoch,
                    frame: QueuedFrame {
                        frame,
                        _charge: charge,
                    },
                })
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "TCP receiver is closed"))?;
        }
    }
    .await;
    let current = state
        .incoming
        .borrow()
        .get(&peer)
        .is_some_and(|current| current.epoch == epoch);
    if current {
        if let Some(current) = state.incoming.borrow_mut().get_mut(&peer) {
            current.abort = None;
        }
        if let Err(error) = result {
            state.warn(io::Error::new(
                error.kind(),
                format!("TCP peer {peer} {error}"),
            ));
        }
    }
}
