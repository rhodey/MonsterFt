//! Gate orders inputs and releases only outputs committed by all three Procs.
use crate::{
    Value,
    diagnostics::{Diagnostics, panic_error},
    error::{Code, Error, Result},
    local::{
        self, COMPONENTS, Input, LocalReply, OrderedInput, PROCS, Received, SendFrame,
        StartupRequest,
    },
    protocol::{
        self, ApplyCompletion, CommandResult, Envelope, Event, Execute, Outcome, State, Status,
    },
    storage::Capture,
    wire,
};
use bytes::Bytes;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

#[derive(Clone, Debug)]
pub struct GateOptions {
    pub tick_interval: Duration,
    pub local_timeout: Duration,
    pub apply_timeout: Duration,
    pub mailbox_capacity: usize,
}
impl Default for GateOptions {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_millis(10),
            local_timeout: Duration::from_millis(1000),
            apply_timeout: Duration::from_millis(5000),
            mailbox_capacity: 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum GateEvent {
    Change(Status),
    Commit(i64),
    Cmd {
        cmd_seq: i64,
        cmd_count: usize,
    },
    Sync {
        cmd_seq: i64,
        sync_seq: i64,
        quorum: bool,
        agree: Vec<String>,
        disagree: Vec<String>,
    },
    Close,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventKind {
    Change,
    Commit,
    Cmd,
    Sync,
    Close,
}
impl GateEvent {
    pub fn kind(&self) -> EventKind {
        match self {
            Self::Change(_) => EventKind::Change,
            Self::Commit(_) => EventKind::Commit,
            Self::Cmd { .. } => EventKind::Cmd,
            Self::Sync { .. } => EventKind::Sync,
            Self::Close => EventKind::Close,
        }
    }
}

/// Admission happens when the operation is created. Dropping it only stops waiting.
pub struct Operation<T>(oneshot::Receiver<Result<T>>);
impl<T> Future for Operation<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|_| Err(closed())))
    }
}
impl<T> Operation<T> {
    fn failed(error: Error) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(Err(error));
        Self(rx)
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;
type Listener = Box<dyn FnMut(&GateEvent)>;
type Condition = Box<dyn FnMut(&GateEvent) -> Result<bool>>;
struct View {
    status: Status,
    open: bool,
    stopped: bool,
    closed: bool,
    error: Option<Error>,
    notify: Rc<tokio::sync::Notify>,
}
enum Command {
    Open(Reply<()>),
    Close(Reply<()>),
    Stop,
    Drain(Reply<()>),
    Append(Vec<Bytes>, Reply<CommandResult>),
    Listen(EventKind, Listener),
    Wait(EventKind, Condition, Reply<GateEvent>),
    Leader(bool, Reply<()>),
}
/// Local handle; event listeners and status access remain on its owner thread.
#[derive(Clone)]
pub struct Gate {
    id: String,
    nodes: Vec<String>,
    commands: mpsc::Sender<Command>,
    incoming: mpsc::Sender<Received>,
    view: Rc<RefCell<View>>,
}
impl Gate {
    pub fn new(
        id: impl Into<String>,
        nodes: Vec<String>,
        send: SendFrame,
        local_send: SendFrame,
        options: GateOptions,
    ) -> Result<(Self, GateRunner)> {
        let id = id.into();
        let (nodes, _) = protocol::membership(&id, nodes, None)?;
        if options.tick_interval.is_zero()
            || options.local_timeout.is_zero()
            || options.apply_timeout.is_zero()
            || options.mailbox_capacity == 0
        {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "Gate timeouts and mailbox capacity must be positive",
            ));
        }
        let (commands, command_rx) = mpsc::channel(options.mailbox_capacity);
        let (incoming, incoming_rx) = mpsc::channel(options.mailbox_capacity);
        let view = Rc::new(RefCell::new(View {
            status: Status {
                state: State::Follower,
                leader: None,
                term: 0,
                seq: -1,
                commit_seq: -1,
                commit_term: -1,
                followers: Vec::new(),
            },
            open: false,
            stopped: false,
            closed: false,
            error: None,
            notify: Rc::new(tokio::sync::Notify::new()),
        }));
        let gate = Self {
            id: id.clone(),
            nodes: nodes.clone(),
            commands,
            incoming,
            view: view.clone(),
        };
        let runner = GateRunner {
            id,
            nodes,
            send,
            local_send,
            options,
            view,
            commands: command_rx,
            incoming: incoming_rx,
            diagnostics: Diagnostics::default(),
            seed: [0; 16],
            stage: Stage::Idle,
            hello_done: BTreeSet::new(),
            phase_wait: BTreeSet::new(),
            captures: Vec::new(),
            requests: BTreeMap::new(),
            queue: VecDeque::new(),
            current: None,
            clients: BTreeMap::new(),
            tick_count: 0,
            timems: 0,
            tick_queued: false,
            next_tick: None,
            open_waiters: Vec::new(),
            close_waiters: Vec::new(),
            close_result: None,
            close_error: None,
            closing: false,
            drain_result: None,
            drain_waiters: Vec::new(),
            draining: false,
            drain_ready: false,
            shutdown_error: None,
            listeners: Vec::new(),
            waiters: Vec::new(),
            leader_waiters: Vec::new(),
        };
        Ok((gate, runner))
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn nodes(&self) -> &[String] {
        &self.nodes
    }
    pub fn is_open(&self) -> bool {
        self.view.borrow().open
    }
    pub fn is_stopped(&self) -> bool {
        self.view.borrow().stopped
    }
    pub fn status(&self) -> Status {
        self.view.borrow().status.clone()
    }
    pub fn wait_stopped(&self) -> impl Future<Output = Error> + use<> {
        let view = self.view.clone();
        let notify = view.borrow().notify.clone();
        async move {
            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(error) = view.borrow().error.clone() {
                    return error;
                }
                notified.await;
            }
        }
    }
    pub fn receiver(&self) -> mpsc::Sender<Received> {
        self.incoming.clone()
    }
    pub fn on_receive(&self, from: impl Into<String>, frame: Bytes) -> Result<()> {
        self.incoming
            .try_send(Received {
                from: from.into(),
                frame,
            })
            .map_err(|_| Error::new(Code::SendError, "Gate input queue is full or stopped"))
    }
    fn operation<T>(&self, make: impl FnOnce(Reply<T>) -> Command) -> Operation<T> {
        let (tx, rx) = oneshot::channel();
        if self.commands.try_send(make(tx)).is_err() {
            return Operation::failed(Error::new(
                Code::SendError,
                "Gate API queue is full or stopped",
            ));
        }
        Operation(rx)
    }
    pub fn open(&self) -> Operation<()> {
        self.operation(Command::Open)
    }
    pub fn close(&self) -> Operation<()> {
        let (tx, rx) = oneshot::channel();
        if self.commands.try_send(Command::Close(tx)).is_err() {
            return Operation::failed(Error::new(
                Code::SendError,
                "Gate API queue is full or stopped",
            ));
        }
        self.request_stop();
        Operation(rx)
    }
    pub fn drain_cmd(&self) -> Operation<()> {
        self.operation(Command::Drain)
    }
    pub fn stop(&self) -> Result<()> {
        if self.is_stopped() {
            return Ok(());
        }
        self.commands
            .try_send(Command::Stop)
            .map_err(|_| closed())?;
        self.request_stop();
        Ok(())
    }
    fn request_stop(&self) {
        // Admission disables work immediately; owner cleanup remains queued.
        let mut view = self.view.borrow_mut();
        view.open = false;
        view.stopped = true;
        view.error.get_or_insert_with(closed);
        view.notify.notify_waiters();
    }
    pub fn append_batch(&self, items: Vec<Bytes>) -> Operation<CommandResult> {
        if items.is_empty() {
            return Operation::failed(Error::new(
                Code::ArgumentIllegal,
                "data must be non-empty array of buffers",
            ));
        }
        self.operation(|reply| Command::Append(items, reply))
    }
    pub fn append(&self, item: Bytes) -> impl Future<Output = Result<(i64, Value)>> + use<> {
        let operation = self.append_batch(vec![item]);
        async move {
            let result = operation.await?;
            match result.results.into_iter().next() {
                Some(Outcome::Fulfilled(value)) => Ok((result.cmd_seq, value)),
                Some(Outcome::Rejected(error)) => Err(error),
                None => Err(Error::new(
                    Code::MonsterIllegal,
                    "command result is missing",
                )),
            }
        }
    }
    pub fn await_leader(&self, commit: bool) -> Operation<()> {
        self.operation(|reply| Command::Leader(commit, reply))
    }
    pub fn await_event(
        &self,
        kind: EventKind,
        matches: impl FnMut(&GateEvent) -> Result<bool> + 'static,
    ) -> Operation<GateEvent> {
        self.operation(|reply| Command::Wait(kind, Box::new(matches), reply))
    }
    pub fn on(&self, kind: EventKind, listener: impl FnMut(&GateEvent) + 'static) -> Result<()> {
        self.commands
            .try_send(Command::Listen(kind, Box::new(listener)))
            .map_err(|_| closed())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    Idle,
    Hello,
    Open(usize),
    Capture,
    Adopt,
    Reconcile,
    Ready,
    Starting,
    Running,
    Stopped,
}
#[derive(Clone)]
enum Purpose {
    Hello,
    Startup,
    Input(i64),
    Apply(Execute),
    Close,
}
struct Pending {
    purpose: Purpose,
    deadline: Instant,
}
struct Flight {
    input: OrderedInput,
    outputs: [Option<Vec<Bytes>>; 3],
}
struct Client {
    items: usize,
    reply: Option<Reply<CommandResult>>,
    drain: bool,
}
struct EventWait {
    kind: EventKind,
    condition: Condition,
    reply: Reply<GateEvent>,
}

pub struct GateRunner {
    id: String,
    nodes: Vec<String>,
    send: SendFrame,
    local_send: SendFrame,
    options: GateOptions,
    view: Rc<RefCell<View>>,
    commands: mpsc::Receiver<Command>,
    incoming: mpsc::Receiver<Received>,
    diagnostics: Diagnostics,
    seed: [u8; 16],
    stage: Stage,
    hello_done: BTreeSet<String>,
    phase_wait: BTreeSet<String>,
    captures: Vec<Capture>,
    requests: BTreeMap<(String, String), Pending>,
    queue: VecDeque<Input>,
    current: Option<Flight>,
    clients: BTreeMap<String, Client>,
    tick_count: i64,
    timems: u64,
    tick_queued: bool,
    next_tick: Option<Instant>,
    open_waiters: Vec<Reply<()>>,
    close_waiters: Vec<Reply<()>>,
    close_result: Option<Result<()>>,
    close_error: Option<Error>,
    closing: bool,
    drain_result: Option<Result<()>>,
    drain_waiters: Vec<Reply<()>>,
    draining: bool,
    drain_ready: bool,
    shutdown_error: Option<Error>,
    listeners: Vec<(EventKind, Listener)>,
    waiters: Vec<EventWait>,
    leader_waiters: Vec<(bool, Reply<()>)>,
}
impl GateRunner {
    pub fn diagnostics(&mut self) -> &mut Diagnostics {
        &mut self.diagnostics
    }
    pub async fn run(mut self) {
        loop {
            self.observe_stop();
            let deadline = self
                .requests
                .values()
                .map(|p| p.deadline)
                .chain(self.next_tick)
                .min()
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86400));
            let result = tokio::select! {
                message=self.incoming.recv(), if !self.incoming.is_closed()=>match message {Some(m)=>guarded(||self.receive(m)),None=>Ok(())},
                command=self.commands.recv()=>match command {Some(c)=>guarded(||self.command(c)),None=>{self.stop_with(closed());break;}},
                _=tokio::time::sleep_until(deadline)=>guarded(||self.on_time()),
            };
            if let Err(error) = result {
                self.fatal(error);
            }
            self.observe_stop();
            if let Err(error) = guarded(|| self.pump()) {
                self.fatal(error);
            }
        }
    }
    fn error(&self) -> Error {
        self.shutdown_error.clone().unwrap_or_else(closed)
    }
    fn stopped(&self) -> bool {
        self.view.borrow().stopped
    }
    fn observe_stop(&mut self) {
        if self.stopped() && self.stage != Stage::Stopped {
            self.stop_with(closed());
        }
    }
    fn stop_with(&mut self, error: Error) {
        if self.stage == Stage::Stopped {
            return;
        }
        let error = self.view.borrow().error.clone().unwrap_or(error);
        self.shutdown_error = Some(error.clone());
        self.stage = Stage::Stopped;
        {
            let mut v = self.view.borrow_mut();
            v.open = false;
            v.stopped = true;
            v.error = Some(error.clone());
            v.notify.notify_waiters();
        }
        self.next_tick = None;
        self.requests.clear();
        self.queue.clear();
        self.current = None;
        for (_, client) in std::mem::take(&mut self.clients) {
            if let Some(reply) = client.reply {
                let _ = reply.send(Err(error.clone()));
            }
        }
        settle(&mut self.open_waiters, Err(error.clone()));
        if self.draining {
            self.drain_result = Some(Err(error.clone()));
        }
        settle(&mut self.drain_waiters, Err(error.clone()));
        for (_, reply) in self.leader_waiters.drain(..) {
            let _ = reply.send(Err(error.clone()));
        }
        let waits = std::mem::take(&mut self.waiters);
        for wait in waits {
            if wait.kind == EventKind::Close {
                self.waiters.push(wait);
            } else {
                let _ = wait.reply.send(Err(error.clone()));
            }
        }
    }
    fn fatal(&mut self, error: Error) {
        if self.stopped() {
            self.observe_stop();
            return;
        }
        self.stop_with(error.clone());
        self.diagnostics.error(&error);
    }
    fn warning(&mut self, error: Error) -> Result<()> {
        let mut listeners = self.diagnostics.take_warnings();
        let failure = crate::diagnostics::warn(&mut listeners, &error, |failure| {
            self.observe_stop();
            if self.stopped() {
                self.diagnostics.error(&failure);
            } else {
                self.fatal(failure);
            }
        });
        self.diagnostics.restore_warnings(listeners);
        self.observe_stop();
        failure.map_or(Ok(()), Err)
    }
    fn command(&mut self, command: Command) -> Result<()> {
        match command {
            Command::Open(reply) => {
                if self.stopped() {
                    let _ = reply.send(Err(self.error()));
                    return Ok(());
                }
                if self.stage == Stage::Running {
                    let _ = reply.send(Ok(()));
                    return Ok(());
                }
                self.open_waiters.push(reply);
                if self.stage != Stage::Idle {
                    return Ok(());
                }
                loop {
                    getrandom::fill(&mut self.seed)
                        .map_err(|e| Error::new(Code::FsError, e.to_string()))?;
                    if self.seed != [0; 16] {
                        break;
                    }
                }
                self.stage = Stage::Hello;
                for to in COMPONENTS {
                    self.hello(to)?;
                }
            }
            Command::Stop => self.stop_with(closed()),
            Command::Close(reply) => {
                self.close_waiters.push(reply);
                self.begin_close()?;
            }
            Command::Drain(reply) => {
                if self.draining {
                    if let Some(result) = self.drain_result.clone() {
                        let _ = reply.send(result);
                    } else {
                        self.drain_waiters.push(reply);
                    }
                    return Ok(());
                }
                if !self.view.borrow().open {
                    let _ = reply.send(Err(self.error()));
                    return Ok(());
                }
                self.draining = true;
                self.drain_waiters.push(reply);
                let client_id = random_id()?;
                self.clients.insert(
                    client_id.clone(),
                    Client {
                        items: 0,
                        reply: None,
                        drain: true,
                    },
                );
                self.enqueue(Input::Drain { client_id })?;
            }
            Command::Append(items, reply) => {
                if !self.view.borrow().open {
                    let _ = reply.send(Err(self.error()));
                    return Ok(());
                }
                if self.draining {
                    let _ = reply.send(Err(Error::new(Code::Draining, "node draining")));
                    return Ok(());
                }
                if self.queue.len() >= self.options.mailbox_capacity
                    || self.clients.len() >= self.options.mailbox_capacity
                {
                    let _ = reply.send(Err(Error::new(
                        Code::SendError,
                        "Gate command capacity is exhausted",
                    )));
                    return Ok(());
                }
                let client_id = random_id()?;
                if self.clients.contains_key(&client_id) {
                    let _ =
                        reply.send(Err(Error::new(Code::MonsterIllegal, "duplicate client id")));
                    return Ok(());
                }
                self.clients.insert(
                    client_id.clone(),
                    Client {
                        items: items.len(),
                        reply: Some(reply),
                        drain: false,
                    },
                );
                self.enqueue(Input::Append { client_id, items })?;
            }
            Command::Listen(kind, listener) => self.listeners.push((kind, listener)),
            Command::Wait(kind, condition, reply) => {
                if kind != EventKind::Close && !self.view.borrow().open {
                    let _ = reply.send(Err(self.error()));
                    return Ok(());
                }
                self.waiters.retain(|wait| !wait.reply.is_closed());
                let wait = EventWait {
                    kind,
                    condition,
                    reply,
                };
                if kind == EventKind::Close && self.view.borrow().closed {
                    // This registration observes the completed close; it is not
                    // another close event for earlier unsatisfied predicates.
                    if let Some(wait) = self.resolve_waiter(wait, &GateEvent::Close) {
                        self.waiters.push(wait);
                    }
                } else if kind == EventKind::Close && matches!(self.close_result, Some(Err(_))) {
                    let _ = wait.reply.send(
                        self.close_result
                            .clone()
                            .unwrap()
                            .map(|()| GateEvent::Close),
                    );
                } else {
                    self.waiters.push(wait);
                }
            }
            Command::Leader(commit, reply) => {
                if !self.view.borrow().open {
                    let _ = reply.send(Err(self.error()));
                } else if leader_ready(&self.view.borrow().status, commit) {
                    let _ = reply.send(Ok(()));
                } else {
                    self.leader_waiters.push((commit, reply));
                }
            }
        }
        Ok(())
    }
    fn enqueue(&mut self, input: Input) -> Result<()> {
        if self.queue.len() >= self.options.mailbox_capacity {
            return Err(Error::new(
                Code::SendError,
                "Gate ordered input capacity is exhausted",
            ));
        }
        self.queue.push_back(input);
        Ok(())
    }
    fn request(&mut self, to: &str, cid: String, purpose: Purpose, frame: Bytes) -> Result<()> {
        if self.stopped() && !matches!(purpose, Purpose::Close) {
            return Err(self.error());
        }
        let key = (to.to_owned(), cid);
        if self.requests.contains_key(&key) {
            return Err(Error::new(
                Code::MonsterIllegal,
                "duplicate local request cid",
            ));
        }
        let timeout = if matches!(purpose, Purpose::Apply(_)) {
            self.options.apply_timeout
        } else {
            self.options.local_timeout
        };
        self.requests.insert(
            key.clone(),
            Pending {
                purpose,
                deadline: Instant::now() + timeout,
            },
        );
        if let Err(error) = guarded(|| (self.local_send)(to, frame)) {
            self.requests.remove(&key);
            return Err(error.with_code(Code::SendError).context("localSend "));
        }
        Ok(())
    }
    fn hello(&mut self, to: &str) -> Result<()> {
        let cid = random_id()?;
        let frame = wire::encode_frame(&StartupRequest::Hello {
            cid: cid.clone(),
            seed: self.seed,
        })?;
        self.request(to, cid, Purpose::Hello, frame)
    }
    fn startup(&mut self, stage: Stage, targets: &[&str]) -> Result<()> {
        self.stage = stage;
        self.phase_wait = targets.iter().map(|s| (*s).to_owned()).collect();
        for to in targets {
            let cid = random_id()?;
            let message = match stage {
                Stage::Open(_) => StartupRequest::Open { cid: cid.clone() },
                Stage::Capture => StartupRequest::Capture { cid: cid.clone() },
                Stage::Adopt => StartupRequest::Adopt { cid: cid.clone() },
                Stage::Reconcile => StartupRequest::Reconcile { cid: cid.clone() },
                Stage::Ready => StartupRequest::Ready { cid: cid.clone() },
                _ => return Err(Error::new(Code::MonsterIllegal, "startup stage is illegal")),
            };
            self.request(to, cid, Purpose::Startup, wire::encode_frame(&message)?)?;
        }
        Ok(())
    }
    fn startup_reply(&mut self, from: &str, selection: Option<Capture>) -> Result<()> {
        if self.stage == Stage::Capture {
            self.captures
                .push(selection.ok_or_else(|| {
                    Error::new(Code::RpcIllegal, "capture ACK missing selection")
                })?);
        }
        self.phase_wait.remove(from);
        if !self.phase_wait.is_empty() {
            return Ok(());
        }
        match self.stage {
            Stage::Open(index) if index + 1 < COMPONENTS.len() => {
                self.startup(Stage::Open(index + 1), &[COMPONENTS[index + 1]])
            }
            Stage::Open(_) => self.startup(Stage::Capture, &PROCS),
            Stage::Capture => {
                if self.captures.len() != 3 || !self.captures.iter().all(|c| c == &self.captures[0])
                {
                    return Err(Error::new(
                        Code::LocalDivergence,
                        "startup selections disagree",
                    ));
                }
                self.startup(Stage::Adopt, &PROCS)
            }
            Stage::Adopt => self.startup(Stage::Reconcile, &PROCS),
            Stage::Reconcile => self.startup(Stage::Ready, &["apply"]),
            Stage::Ready => {
                self.stage = Stage::Starting;
                self.view.borrow_mut().open = true;
                self.queue.push_front(Input::Start);
                Ok(())
            }
            _ => Err(Error::new(
                Code::MonsterIllegal,
                "unexpected startup completion",
            )),
        }
    }
    fn begin_close(&mut self) -> Result<()> {
        if let Some(result) = self.close_result.clone() {
            settle(&mut self.close_waiters, result);
            return Ok(());
        }
        if self.closing {
            return Ok(());
        }
        self.closing = true;
        // Stop before creating close requests; fatal stop itself never creates them.
        let drain_waiters = std::mem::take(&mut self.drain_waiters);
        self.stop_with(closed());
        self.drain_waiters = drain_waiters;
        if self.drain_ready || !self.drain_waiters.is_empty() {
            self.drain_result = None;
        }
        let owners: Vec<_> = self.hello_done.iter().cloned().collect();
        for to in owners {
            let cid = random_id()?;
            let frame = wire::encode_frame(&StartupRequest::Close { cid: cid.clone() })?;
            if let Err(error) = self.request(&to, cid, Purpose::Close, frame) {
                self.close_error.get_or_insert(error);
            }
        }
        self.finish_close();
        Ok(())
    }
    fn finish_close(&mut self) {
        if self
            .requests
            .values()
            .any(|r| matches!(r.purpose, Purpose::Close))
        {
            return;
        }
        let result = self
            .close_result
            .get_or_insert_with(|| self.close_error.clone().map_or(Ok(()), Err))
            .clone();
        if self.draining {
            self.drain_result = Some(result.clone());
        }
        if result.is_ok() && !self.view.borrow().closed {
            self.view.borrow_mut().closed = true;
            let _ = self.emit(GateEvent::Close);
        }
        if let Err(error) = &result {
            self.fail_close_waits(error.clone());
        }
        settle(&mut self.close_waiters, result.clone());
        settle(&mut self.drain_waiters, result);
    }
    fn fail_close_waits(&mut self, error: Error) {
        let waits = std::mem::take(&mut self.waiters);
        for wait in waits {
            if wait.kind == EventKind::Close {
                let _ = wait.reply.send(Err(error.clone()));
            } else {
                self.waiters.push(wait);
            }
        }
    }
    fn on_time(&mut self) -> Result<()> {
        let now = Instant::now();
        let expired: Vec<_> = self
            .requests
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(k, _)| k.clone())
            .collect();
        for key in expired {
            let Some(request) = self.requests.remove(&key) else {
                continue;
            };
            let error = Error::new(
                if matches!(request.purpose, Purpose::Apply(_)) {
                    Code::ApplyTimeout
                } else {
                    Code::LocalTimeout
                },
                format!("{} request timeout", key.0),
            );
            match request.purpose {
                Purpose::Hello if !self.stopped() => self.hello(&key.0)?,
                Purpose::Close => {
                    self.close_error.get_or_insert(error);
                    self.finish_close();
                }
                _ => return Err(error),
            }
        }
        if self.next_tick.is_some_and(|deadline| deadline <= now) && !self.stopped() {
            if !self.tick_queued && self.queue.len() < self.options.mailbox_capacity {
                self.enqueue(Input::Tick)?;
                self.tick_queued = true;
            }
            self.next_tick = Some(now + self.options.tick_interval);
        }
        Ok(())
    }
    fn pump(&mut self) -> Result<()> {
        if self.stopped() || !self.view.borrow().open || self.current.is_some() {
            return Ok(());
        }
        let Some(input) = self.queue.pop_front() else {
            return Ok(());
        };
        if matches!(input, Input::Tick) {
            self.tick_queued = false;
        }
        self.tick_count = self
            .tick_count
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "tick count exhausted"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let now = u64::try_from(now)
            .ok()
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "clock exhausted"))?;
        self.timems = self.timems.max(now);
        let input = OrderedInput {
            cid: random_id()?,
            tick_count: self.tick_count,
            timems: self.timems,
            input,
        };
        let frame = wire::encode_frame(&input)?;
        self.current = Some(Flight {
            input: input.clone(),
            outputs: [None, None, None],
        });
        for to in PROCS {
            self.request(
                to,
                input.cid.clone(),
                Purpose::Input(input.tick_count),
                frame.clone(),
            )?;
        }
        Ok(())
    }
    fn receive(&mut self, received: Received) -> Result<()> {
        let Received { from, frame } = received;
        if COMPONENTS.contains(&from.as_str()) {
            if self.stopped() && self.requests.is_empty() {
                return Ok(());
            }
            let reply: LocalReply = wire::decode_frame(&frame)?;
            if reply.cid().is_empty() {
                return Err(Error::new(Code::RpcIllegal, "local ack_err cid is illegal"));
            }
            let key = (from.clone(), reply.cid().to_owned());
            let Some(pending) = self.requests.get(&key) else {
                return Ok(());
            };
            let purpose = pending.purpose.clone();
            if let LocalReply::Ack {
                tick_count,
                outputs,
                ..
            } = &reply
            {
                match &purpose {
                    Purpose::Input(expected) => {
                        if *tick_count != Some(*expected)
                            || outputs.is_none()
                            || from != "procA"
                                && outputs.as_ref().unwrap().iter().any(|p| p.len() != 8)
                        {
                            return Err(Error::new(
                                Code::MonsterIllegal,
                                "Proc completion is illegal",
                            ));
                        }
                    }
                    Purpose::Apply(request) => {
                        if *tick_count != Some(request.tick_count) {
                            return Err(Error::new(
                                Code::MonsterIllegal,
                                "Apply completion tickCount is illegal",
                            ));
                        }
                    }
                    _ => {}
                }
            }
            self.requests.remove(&key);
            match reply {
                LocalReply::Err { error, .. } => {
                    if matches!(purpose, Purpose::Close) {
                        self.close_error.get_or_insert(error);
                        self.finish_close();
                        Ok(())
                    } else {
                        Err(error)
                    }
                }
                LocalReply::Ack {
                    cid,
                    tick_count,
                    outputs,
                    selection,
                    receipt,
                    outcomes,
                } => match purpose {
                    Purpose::Hello => {
                        self.hello_done.insert(from);
                        if self.hello_done.len() == 4 {
                            self.startup(Stage::Open(0), &[COMPONENTS[0]])?;
                        }
                        Ok(())
                    }
                    Purpose::Startup => self.startup_reply(&from, selection),
                    Purpose::Input(_) => self.input_reply(&from, outputs.unwrap()),
                    Purpose::Apply(_) => {
                        let reply = ApplyCompletion {
                            cid,
                            tick_count: tick_count.unwrap(),
                            receipt: receipt.ok_or_else(|| {
                                Error::new(Code::RpcIllegal, "Apply receipt missing")
                            })?,
                            outcomes: outcomes.ok_or_else(|| {
                                Error::new(Code::RpcIllegal, "Apply outcomes missing")
                            })?,
                        };
                        self.enqueue(Input::ApplyResult { reply })
                    }
                    Purpose::Close => {
                        self.finish_close();
                        Ok(())
                    }
                },
            }
        } else {
            if self.stopped() || self.stage == Stage::Idle {
                return Ok(());
            }
            if from == self.id || !self.nodes.contains(&from) {
                return self.warning(Error::new(
                    Code::RpcIllegal,
                    "cluster frame has invalid sender",
                ));
            }
            if let Err(error) = wire::verify_frame(&frame) {
                return self.warning(error.context(format!("cluster frame from {from}: ")));
            }
            if self.queue.len() < self.options.mailbox_capacity {
                self.enqueue(Input::ClusterMessage { from, frame })?;
            }
            Ok(())
        }
    }
    fn input_reply(&mut self, from: &str, outputs: Vec<Bytes>) -> Result<()> {
        let Some(flight) = &mut self.current else {
            return Err(Error::new(
                Code::MonsterIllegal,
                "unexpected Proc completion",
            ));
        };
        let index = PROCS.iter().position(|p| *p == from).unwrap();
        flight.outputs[index] = Some(outputs);
        if flight.outputs.iter().any(Option::is_none) {
            return Ok(());
        }
        let flight = self.current.take().unwrap();
        let [a, b, c] = flight.outputs.map(Option::unwrap);
        if a.len() != b.len() || a.len() != c.len() {
            return Err(Error::new(Code::LocalDivergence, "Proc outputs disagree"));
        }
        let mut hashes = Vec::with_capacity(a.len());
        for ((payload, b), c) in a.iter().zip(&b).zip(&c) {
            let mut hash = [0; 16];
            hash[..8].copy_from_slice(b);
            hash[8..].copy_from_slice(c);
            if wire::checksum(payload) != hash {
                return Err(Error::new(Code::LocalDivergence, "Proc outputs disagree"));
            }
            hashes.push(hash);
        }
        for (payload, hash) in a.into_iter().zip(hashes) {
            if self.stopped() {
                break;
            }
            self.release(payload, hash)?;
        }
        if matches!(flight.input.input, Input::Start) && !self.stopped() {
            self.stage = Stage::Running;
            self.next_tick = Some(Instant::now() + self.options.tick_interval);
            settle(&mut self.open_waiters, Ok(()));
        }
        if self.drain_ready {
            self.drain_ready = false;
            self.begin_close()?;
        }
        Ok(())
    }
    fn release(&mut self, payload: Bytes, hash: [u8; 16]) -> Result<()> {
        let envelope: Envelope<Value> = wire::decode_payload(&payload)?;
        let kind = local::kind(&envelope.message)?.to_owned();
        if envelope.to == "apply" {
            if kind != "execute" {
                return Err(Error::new(Code::RpcIllegal, "Apply request is not execute"));
            }
            let request: Execute = local::decode_value(envelope.message)?;
            request.validate()?;
            if request.seq > 0 && request.match_index.as_ref().unwrap().len() != self.nodes.len() {
                return Err(Error::new(
                    Code::RpcIllegal,
                    "Apply matchIndex has illegal length",
                ));
            }
            self.request(
                "apply",
                request.cid.clone(),
                Purpose::Apply(request),
                wire::frame_payload(&payload, &hash)?,
            )
        } else if envelope.to == "gate" {
            if kind == "ack" || kind == "err" {
                self.client_result(envelope.message)
            } else {
                let event: Event = local::decode_value(envelope.message)?;
                event.validate(&self.nodes, &self.id)?;
                self.view.borrow_mut().status = event.status().clone();
                let event = match event {
                    Event::Change { status } => GateEvent::Change(status),
                    Event::Commit { seq, .. } => GateEvent::Commit(seq),
                    Event::Cmd {
                        cmd_seq, cmd_count, ..
                    } => GateEvent::Cmd { cmd_seq, cmd_count },
                    Event::Sync {
                        cmd_seq,
                        sync_seq,
                        quorum,
                        agree,
                        disagree,
                        ..
                    } => GateEvent::Sync {
                        cmd_seq,
                        sync_seq,
                        quorum,
                        agree,
                        disagree,
                    },
                };
                let is_change = matches!(event, GateEvent::Change(_));
                self.emit(event)?;
                if !is_change {
                    return Ok(());
                }
                let status = self.view.borrow().status.clone();
                let waiters = std::mem::take(&mut self.leader_waiters);
                for (commit, reply) in waiters {
                    if leader_ready(&status, commit) {
                        let _ = reply.send(Ok(()));
                    } else {
                        self.leader_waiters.push((commit, reply));
                    }
                }
                Ok(())
            }
        } else if envelope.to != self.id && self.nodes.contains(&envelope.to) {
            let frame = wire::frame_payload(&payload, &hash)?;
            match catch_unwind(AssertUnwindSafe(|| (self.send)(&envelope.to, frame))) {
                Ok(Ok(())) => (),
                Ok(Err(error)) => {
                    self.warning(error.with_code(Code::SendError).context("send "))?
                }
                Err(panic) => return Err(panic_error(panic).with_code(Code::SendError)),
            }
            Ok(())
        } else {
            Err(Error::new(
                Code::RpcIllegal,
                "output has invalid destination",
            ))
        }
    }
    fn client_result(&mut self, value: Value) -> Result<()> {
        let client_id = local::field(&value, "clientId")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::new(Code::RpcIllegal, "client ack_err clientId is illegal"))?
            .to_owned();
        let Some(mut client) = self.clients.remove(&client_id) else {
            return Ok(());
        };
        let result = if local::kind(&value)? == "err" {
            Err(local::decode_value::<Error>(value)?)
        } else if client.drain {
            self.drain_ready = true;
            Ok(None)
        } else {
            local::decode_value::<CommandResult>(value).and_then(|r| {
                if r.cmd_seq < 0 || r.results.len() != client.items {
                    Err(Error::new(
                        Code::MonsterIllegal,
                        "command result is illegal",
                    ))
                } else {
                    Ok(Some(r))
                }
            })
        };
        if client.drain {
            if let Err(error) = result {
                self.drain_result = Some(Err(error.clone()));
                settle(&mut self.drain_waiters, Err(error));
            }
        } else if let Some(reply) = client.reply.take() {
            let _ = reply.send(result.and_then(|r| {
                r.ok_or_else(|| Error::new(Code::MonsterIllegal, "command result missing"))
            }));
        }
        Ok(())
    }
    fn resolve_waiters(&mut self, event: &GateEvent) {
        let waits = std::mem::take(&mut self.waiters);
        for wait in waits {
            if wait.reply.is_closed() {
                continue;
            }
            if wait.kind != EventKind::Close && self.stopped() {
                let _ = wait.reply.send(Err(self.error()));
                continue;
            }
            if wait.kind != event.kind() {
                self.waiters.push(wait);
                continue;
            }
            if let Some(wait) = self.resolve_waiter(wait, event) {
                self.waiters.push(wait);
            }
        }
    }
    fn resolve_waiter(&mut self, mut wait: EventWait, event: &GateEvent) -> Option<EventWait> {
        if wait.reply.is_closed() {
            return None;
        }
        let result = guarded(|| (wait.condition)(event));
        self.observe_stop();
        if wait.kind != EventKind::Close && self.stopped() {
            let _ = wait.reply.send(Err(self.error()));
            return None;
        }
        match result {
            Ok(true) => {
                let _ = wait.reply.send(Ok(event.clone()));
                None
            }
            Err(error) => {
                let _ = wait.reply.send(Err(error));
                None
            }
            Ok(false) => Some(wait),
        }
    }
    fn emit(&mut self, event: GateEvent) -> Result<()> {
        let mut listeners = std::mem::take(&mut self.listeners);
        for (kind, listener) in &mut listeners {
            if *kind != event.kind() {
                continue;
            }
            let result = catch_unwind(AssertUnwindSafe(|| listener(&event)));
            self.observe_stop();
            if let Err(panic) = result {
                let error = panic_error(panic);
                if self.stopped() {
                    self.diagnostics.error(&error);
                } else {
                    self.fatal(error);
                }
            }
        }
        listeners.append(&mut self.listeners);
        self.listeners = listeners;
        self.resolve_waiters(&event);
        Ok(())
    }
}
fn guarded<T>(action: impl FnOnce() -> Result<T>) -> Result<T> {
    catch_unwind(AssertUnwindSafe(action)).unwrap_or_else(|panic| Err(panic_error(panic)))
}
fn closed() -> Error {
    Error::new(Code::NodeNotOpen, "node not open")
}
fn settle<T: Clone>(waiters: &mut Vec<Reply<T>>, result: Result<T>) {
    for waiter in waiters.drain(..) {
        let _ = waiter.send(result.clone());
    }
}
fn leader_ready(status: &Status, commit: bool) -> bool {
    (status.state == State::Leader || (status.state == State::Follower && status.leader.is_some()))
        && (!commit || status.commit_term == status.term)
}
pub(crate) fn random_id() -> Result<String> {
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|e| Error::new(Code::FsError, e.to_string()))?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

impl Drop for GateRunner {
    fn drop(&mut self) {
        // An aborted owner task must not leave its public handle looking alive.
        self.stop_with(closed());
    }
}
