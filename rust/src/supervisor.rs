//! Process supervision is independent of the selected local transport. Generation
//! guards in DB1/DB2 remain the last defense against an owner left behind by a
//! failed supervisor; normal replacement still waits for every old owner to exit.
use crate::{
    diagnostics::{Diagnostics, panic_error},
    error::{Code, Error, Result},
    gate::Gate,
};
use command_fds::CommandFdExt;
pub use command_fds::FdMapping;
use futures_util::{
    FutureExt,
    future::{LocalBoxFuture, join_all},
};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsString,
    future::Future,
    os::fd::OwnedFd,
    panic::{AssertUnwindSafe, catch_unwind},
    path::PathBuf,
    pin::Pin,
    process::{ExitStatus, Stdio},
    rc::Rc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    process::{Child, Command as ProcessCommand},
    sync::{Notify, mpsc, oneshot},
    time::Instant,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ChildRole {
    ProcA,
    ProcB,
    ProcC,
    Apply,
}
impl ChildRole {
    pub const ALL: [Self; 4] = [Self::ProcA, Self::ProcB, Self::ProcC, Self::Apply];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ProcA => "procA",
            Self::ProcB => "procB",
            Self::ProcC => "procC",
            Self::Apply => "apply",
        }
    }
}

#[derive(Debug)]
pub struct ChildSpec {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub fd_mappings: Vec<FdMapping>,
}
impl ChildSpec {
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            args: Vec::new(),
            fd_mappings: Vec::new(),
        }
    }
    pub fn args(mut self, args: impl IntoIterator<Item = impl Into<OsString>>) -> Self {
        self.args.extend(args.into_iter().map(Into::into));
        self
    }
    pub fn fd_mapping(mut self, parent_fd: OwnedFd, child_fd: i32) -> Self {
        self.fd_mappings.push(FdMapping {
            parent_fd,
            child_fd,
        });
        self
    }
}

struct ProcessState {
    exit: RefCell<Option<Result<ExitStatus>>>,
    notify: Notify,
}
#[derive(Clone)]
pub struct ChildProcess {
    pid: u32,
    state: Rc<ProcessState>,
    kill: mpsc::Sender<oneshot::Sender<Result<()>>>,
}
impl ChildProcess {
    pub fn id(&self) -> u32 {
        self.pid
    }
    pub fn has_exited(&self) -> bool {
        self.state.exit.borrow().is_some()
    }
    pub fn kill(&self) -> Operation<()> {
        if self.has_exited() {
            return Operation::ready(Ok(()));
        }
        let (tx, rx) = oneshot::channel();
        if self.kill.try_send(tx).is_err() {
            return Operation::ready(Err(failure("child control queue is unavailable")));
        }
        Operation(rx)
    }
    pub fn wait(&self) -> impl Future<Output = Result<ExitStatus>> + use<> {
        let state = self.state.clone();
        async move {
            loop {
                let notified = state.notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if let Some(result) = state.exit.borrow().clone() {
                    return result;
                }
                notified.await;
            }
        }
    }
}
pub type Children = BTreeMap<ChildRole, ChildProcess>;

type ReadyHook = Box<dyn FnOnce(Children) -> LocalBoxFuture<'static, Result<()>>>;
type CleanupHook = Box<dyn FnOnce() -> LocalBoxFuture<'static, Result<()>>>;

pub struct Setup {
    pub gate: Gate,
    pub children: [ChildSpec; 4],
    ready: Option<ReadyHook>,
    failed: Option<LocalBoxFuture<'static, Result<()>>>,
    stop: Option<Box<dyn FnOnce() -> Result<()>>>,
    cleanup: Option<CleanupHook>,
}
impl Setup {
    pub fn new(gate: Gate, children: [ChildSpec; 4]) -> Self {
        Self {
            gate,
            children,
            ready: None,
            failed: None,
            stop: None,
            cleanup: None,
        }
    }
    pub fn ready<F, Fut>(mut self, ready: F) -> Self
    where
        F: FnOnce(Children) -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        self.ready = Some(Box::new(|children| Box::pin(ready(children))));
        self
    }
    pub fn failed(mut self, failed: impl Future<Output = Result<()>> + 'static) -> Self {
        self.failed = Some(Box::pin(failed));
        self
    }
    pub fn stop(mut self, stop: impl FnOnce() -> Result<()> + 'static) -> Self {
        self.stop = Some(Box::new(stop));
        self
    }
    pub fn cleanup<F, Fut>(mut self, cleanup: F) -> Self
    where
        F: FnOnce() -> Fut + 'static,
        Fut: Future<Output = Result<()>> + 'static,
    {
        self.cleanup = Some(Box::new(|| Box::pin(cleanup())));
        self
    }
}

#[derive(Clone, Debug)]
pub struct SupervisorOptions {
    pub restart_delay: Duration,
    pub startup_timeout: Duration,
    pub mailbox_capacity: usize,
}
impl Default for SupervisorOptions {
    fn default() -> Self {
        Self {
            restart_delay: Duration::from_millis(1000),
            startup_timeout: Duration::from_secs(30),
            mailbox_capacity: 128,
        }
    }
}

pub struct Operation<T>(oneshot::Receiver<Result<T>>);
impl<T> Operation<T> {
    fn ready(result: Result<T>) -> Self {
        let (tx, rx) = oneshot::channel();
        let _ = tx.send(result);
        Self(rx)
    }
}
impl<T> Future for Operation<T> {
    type Output = Result<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|result| result.unwrap_or_else(|_| Err(stopped())))
    }
}

type Reply = oneshot::Sender<Result<()>>;
type OpenListener = Box<dyn FnMut(&Gate)>;
struct View {
    gate: Option<Gate>,
    stopped: bool,
    opened: bool,
    close_result: Option<Result<()>>,
    drain_result: Option<Result<()>>,
}
enum Control {
    Open(Reply),
    Close { graceful: bool, reply: Reply },
    Drain(Reply),
    OnOpen(OpenListener),
    OnClose(Box<dyn FnMut()>),
}
#[derive(Clone)]
pub struct Supervisor {
    commands: mpsc::Sender<Control>,
    view: Rc<RefCell<View>>,
}
impl Supervisor {
    pub fn new<F, Fut>(mut setup: F, options: SupervisorOptions) -> Result<(Self, SupervisorRunner)>
    where
        F: FnMut() -> Fut + 'static,
        Fut: Future<Output = Result<Setup>> + 'static,
    {
        if options.startup_timeout.is_zero() || options.mailbox_capacity == 0 {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "Supervisor startup timeout and mailbox capacity must be positive",
            ));
        }
        let (commands, receiver) = mpsc::channel(options.mailbox_capacity);
        let view = Rc::new(RefCell::new(View {
            gate: None,
            stopped: false,
            opened: false,
            close_result: None,
            drain_result: None,
        }));
        let handle = Self {
            commands,
            view: view.clone(),
        };
        let runner = SupervisorRunner {
            factory: Box::new(move || Box::pin(setup())),
            options,
            commands: receiver,
            view,
            diagnostics: Diagnostics::default(),
            open_waiters: Vec::new(),
            close_waiters: Vec::new(),
            drain_waiters: Vec::new(),
            open_listeners: Vec::new(),
            close_listeners: Vec::new(),
            requested: None,
            started: false,
        };
        Ok((handle, runner))
    }
    pub fn gate(&self) -> Option<Gate> {
        self.view.borrow().gate.clone()
    }
    pub fn is_stopped(&self) -> bool {
        self.view.borrow().stopped
    }
    pub fn open(&self) -> Operation<()> {
        let view = self.view.borrow();
        if view.stopped {
            return Operation::ready(Err(stopped()));
        }
        if view.opened {
            return Operation::ready(Ok(()));
        }
        drop(view);
        self.operation(Control::Open)
    }
    pub fn close(&self, graceful: bool) -> Operation<()> {
        if let Some(result) = self.view.borrow().close_result.clone() {
            return Operation::ready(result);
        }
        self.shutdown_operation(|reply| Control::Close { graceful, reply })
    }
    pub fn drain_cmd(&self) -> Operation<()> {
        if let Some(result) = self.view.borrow().drain_result.clone() {
            return Operation::ready(result);
        }
        if self.view.borrow().close_result.is_some() {
            return Operation::ready(Err(stopped()));
        }
        self.shutdown_operation(Control::Drain)
    }
    fn shutdown_operation(&self, make: impl FnOnce(Reply) -> Control) -> Operation<()> {
        let (tx, rx) = oneshot::channel();
        if self.commands.try_send(make(tx)).is_err() {
            return Operation::ready(Err(failure("Supervisor API queue is full or stopped")));
        }
        self.view.borrow_mut().stopped = true;
        Operation(rx)
    }
    pub fn on_open(&self, listener: impl FnMut(&Gate) + 'static) -> Result<()> {
        self.commands
            .try_send(Control::OnOpen(Box::new(listener)))
            .map_err(|_| stopped())
    }
    pub fn on_close(&self, listener: impl FnMut() + 'static) -> Result<()> {
        self.commands
            .try_send(Control::OnClose(Box::new(listener)))
            .map_err(|_| stopped())
    }
    fn operation(&self, make: impl FnOnce(Reply) -> Control) -> Operation<()> {
        let (tx, rx) = oneshot::channel();
        if self.commands.try_send(make(tx)).is_err() {
            return Operation::ready(Err(failure("Supervisor API queue is full or stopped")));
        }
        Operation(rx)
    }
}

#[derive(Clone, Copy)]
enum Shutdown {
    Close { graceful: bool },
    Drain,
}
struct Attempt {
    gate: Gate,
    children: Children,
    exits: mpsc::UnboundedReceiver<(ChildRole, Result<ExitStatus>)>,
    failed: LocalBoxFuture<'static, (Error, bool)>,
    ready: Option<LocalBoxFuture<'static, Result<()>>>,
    stop: Option<Box<dyn FnOnce() -> Result<()>>>,
    cleanup: Option<CleanupHook>,
    deadline: Instant,
}
struct StopFailure {
    error: Error,
    report: bool,
}
impl From<Error> for StopFailure {
    fn from(error: Error) -> Self {
        Self {
            error,
            report: true,
        }
    }
}

enum AttemptEnd {
    Failed { error: Error, report: bool },
    Shutdown,
}

pub struct SupervisorRunner {
    factory: Box<dyn FnMut() -> LocalBoxFuture<'static, Result<Setup>>>,
    options: SupervisorOptions,
    commands: mpsc::Receiver<Control>,
    view: Rc<RefCell<View>>,
    diagnostics: Diagnostics,
    open_waiters: Vec<Reply>,
    close_waiters: Vec<Reply>,
    drain_waiters: Vec<Reply>,
    open_listeners: Vec<OpenListener>,
    close_listeners: Vec<Box<dyn FnMut()>>,
    requested: Option<Shutdown>,
    started: bool,
}
impl SupervisorRunner {
    pub fn diagnostics(&mut self) -> &mut Diagnostics {
        &mut self.diagnostics
    }
    fn control(&mut self, command: Option<Control>) {
        match command {
            Some(Control::OnOpen(listener)) => self.open_listeners.push(listener),
            Some(Control::OnClose(listener)) => self.close_listeners.push(listener),
            Some(Control::Open(reply)) => {
                if self.view.borrow().stopped {
                    let _ = reply.send(Err(stopped()));
                } else if self.view.borrow().opened {
                    let _ = reply.send(Ok(()));
                } else {
                    self.open_waiters.push(reply);
                    self.started = true;
                }
            }
            Some(Control::Close { graceful, reply }) => {
                self.close_waiters.push(reply);
                if self.requested.is_none() {
                    self.requested = Some(Shutdown::Close { graceful });
                }
                self.view.borrow_mut().stopped = true;
                settle(&mut self.open_waiters, Err(stopped()));
            }
            Some(Control::Drain(reply)) => {
                self.drain_waiters.push(reply);
                if self.requested.is_none() {
                    self.requested = Some(Shutdown::Drain);
                }
                self.view.borrow_mut().stopped = true;
                settle(&mut self.open_waiters, Err(stopped()));
            }
            None => {
                self.requested
                    .get_or_insert(Shutdown::Close { graceful: false });
                self.view.borrow_mut().stopped = true;
                settle(&mut self.open_waiters, Err(stopped()));
            }
        }
    }
    fn report(&mut self, error: Error) {
        self.diagnostics.error(&error);
    }
    fn opened(&mut self, gate: &Gate) {
        self.view.borrow_mut().opened = true;
        settle(&mut self.open_waiters, Ok(()));
        let mut errors = Vec::new();
        for listener in &mut self.open_listeners {
            if let Err(panic) = catch_unwind(AssertUnwindSafe(|| listener(gate))) {
                errors.push(panic_error(panic).with_code(Code::SupervisorError));
            }
        }
        for error in errors {
            self.report(error);
        }
    }
    fn finish(&mut self, result: Result<()>, drain_result: Option<Result<()>>) {
        {
            let mut view = self.view.borrow_mut();
            view.stopped = true;
            view.gate = None;
            view.close_result = Some(result.clone());
            if let Some(result) = &drain_result {
                view.drain_result = Some(result.clone());
            }
        }
        settle(
            &mut self.open_waiters,
            Err(result.clone().err().unwrap_or_else(stopped)),
        );
        settle(&mut self.close_waiters, result.clone());
        settle(
            &mut self.drain_waiters,
            drain_result.unwrap_or_else(|| Err(stopped())),
        );
        if result.is_ok() {
            let mut errors = Vec::new();
            for listener in &mut self.close_listeners {
                if let Err(panic) = catch_unwind(AssertUnwindSafe(listener)) {
                    errors.push(panic_error(panic).with_code(Code::SupervisorError));
                }
            }
            for error in errors {
                self.report(error);
            }
        }
    }
    async fn factory(&mut self) -> Result<Setup> {
        let future = match catch_unwind(AssertUnwindSafe(|| (self.factory)())) {
            Ok(future) => future,
            Err(panic) => return Err(panic_error(panic).with_code(Code::SupervisorError)),
        };
        let future = AssertUnwindSafe(future).catch_unwind();
        tokio::pin!(future);
        loop {
            tokio::select! {biased;command=self.commands.recv(),if !self.commands.is_closed()=>self.control(command),result=&mut future=>return result.unwrap_or_else(|panic|Err(panic_error(panic).with_code(Code::SupervisorError)))}
        }
    }
    fn attempt(&mut self, setup: Setup) -> (Attempt, Option<Error>) {
        let Setup {
            gate,
            children,
            ready,
            failed,
            stop,
            cleanup,
        } = setup;
        let deadline = Instant::now() + self.options.startup_timeout;
        let (exits_tx, exits) = mpsc::unbounded_channel();
        let mut records = Children::new();
        let mut error = if gate.is_open() || gate.is_stopped() {
            Some(failure("setup requires a fresh Gate"))
        } else {
            None
        };
        if self.requested.is_none() && error.is_none() {
            for (role, spec) in ChildRole::ALL.into_iter().zip(children) {
                match spawn_child(role, spec, exits_tx.clone()) {
                    Ok(child) => {
                        records.insert(role, child);
                    }
                    Err(failure) => {
                        error = Some(failure);
                        break;
                    }
                }
            }
        }
        let failed = Box::pin(async move {
            if let Some(failed) = failed {
                match AssertUnwindSafe(failed).catch_unwind().await {
                    Ok(Err(error)) => return (error, false),
                    Err(panic) => {
                        return (panic_error(panic).with_code(Code::SupervisorError), true);
                    }
                    Ok(Ok(())) => {}
                }
            }
            std::future::pending().await
        });
        let ready = if error.is_none() && self.requested.is_none() {
            ready.map(
                |ready| match catch_unwind(AssertUnwindSafe(|| ready(records.clone()))) {
                    Ok(future) => future,
                    Err(panic) => Box::pin(std::future::ready(Err(
                        panic_error(panic).with_code(Code::SupervisorError)
                    ))) as LocalBoxFuture<'static, Result<()>>,
                },
            )
        } else {
            None
        };
        self.view.borrow_mut().gate = Some(gate.clone());
        (
            Attempt {
                gate,
                children: records,
                exits,
                failed,
                ready,
                stop,
                cleanup,
                deadline,
            },
            error,
        )
    }
    async fn run_attempt(&mut self, attempt: &mut Attempt) -> AttemptEnd {
        let ready = attempt.ready.take();
        let gate = attempt.gate.clone();
        let startup = async move {
            if let Some(ready) = ready {
                AssertUnwindSafe(ready)
                    .catch_unwind()
                    .await
                    .map_err(|p| (panic_error(p).with_code(Code::SupervisorError), true))?
                    .map_err(|e| (e, true))?;
            }
            gate.open().await.map_err(|e| (e, false))
        };
        tokio::pin!(startup);
        let stopped = attempt.gate.wait_stopped();
        tokio::pin!(stopped);
        let mut opened = false;
        loop {
            if self.requested.is_some() {
                return AttemptEnd::Shutdown;
            }
            tokio::select! {biased;
                command=self.commands.recv()=>self.control(command),
                (error,report)=&mut attempt.failed=>return AttemptEnd::Failed{error,report},
                exited=attempt.exits.recv()=>{if let Some((role,result))=exited{return AttemptEnd::Failed{error:exit_error(role,result),report:true}}},
                error=&mut stopped=>return AttemptEnd::Failed{error,report:false},
                result=&mut startup,if !opened=>match result{Ok(())=>{if !attempt.gate.is_open(){return AttemptEnd::Failed{error:stopped_error(),report:false}}opened=true;self.opened(&attempt.gate);},Err((error,report))=>return AttemptEnd::Failed{error,report}},
                _=tokio::time::sleep_until(attempt.deadline),if !opened=>return AttemptEnd::Failed{error:failure("supervisor startup timeout"),report:true},
            }
        }
    }
    async fn stop_attempt(
        &mut self,
        attempt: &mut Attempt,
        graceful: bool,
    ) -> std::result::Result<(), StopFailure> {
        self.view.borrow_mut().gate = None;
        let mut gate_error = None;
        if !attempt.gate.is_stopped() {
            match attempt.gate.stop() {
                Ok(()) => {
                    attempt.gate.wait_stopped().await;
                }
                Err(error) => gate_error = Some(error),
            }
        }
        if graceful && let Err(error) = attempt.gate.close().await {
            gate_error = Some(error);
        }
        let stop_error =
            attempt
                .stop
                .take()
                .and_then(|stop| match catch_unwind(AssertUnwindSafe(stop)) {
                    Ok(result) => result.err(),
                    Err(panic) => Some(panic_error(panic)),
                });
        // Request every termination before awaiting any process. A stop-hook
        // failure must never strand the remaining database owners.
        let kills: Vec<_> = attempt.children.values().map(ChildProcess::kill).collect();
        let killed = join_all(kills).await;
        let mut kill_error = None;
        let mut waits = Vec::new();
        for (child, result) in attempt.children.values().zip(killed) {
            match result {
                Ok(()) => waits.push(child.wait()),
                Err(error) => {
                    kill_error.get_or_insert(error);
                }
            }
        }
        for result in join_all(waits).await {
            if let Err(error) = result {
                kill_error.get_or_insert(error);
            }
        }
        if let Some(error) = kill_error {
            return Err(error.with_code(Code::SupervisorError).into());
        }
        if let Some(cleanup) = attempt.cleanup.take() {
            let future = catch_unwind(AssertUnwindSafe(cleanup))
                .map_err(|panic| panic_error(panic).with_code(Code::SupervisorError))?;
            AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .map_err(|panic| panic_error(panic).with_code(Code::SupervisorError))?
                .map_err(|error| error.with_code(Code::SupervisorError))?;
        }
        if let Some(error) = stop_error {
            return Err(error.with_code(Code::SupervisorError).into());
        }
        gate_error.map_or(Ok(()), |error| {
            Err(StopFailure {
                error,
                report: false,
            })
        })
    }
    async fn delay(&mut self) {
        let sleep = tokio::time::sleep(self.options.restart_delay);
        tokio::pin!(sleep);
        loop {
            if self.requested.is_some() {
                return;
            }
            tokio::select! {biased;command=self.commands.recv()=>self.control(command),_=&mut sleep=>return}
        }
    }
    async fn drain_attempt(&mut self, attempt: &mut Attempt) -> Result<()> {
        let draining = attempt.gate.drain_cmd();
        tokio::pin!(draining);
        // A successful drain stops Gate before coordinated owner close completes.
        // Its result, rather than the stopped notification, determines success.
        loop {
            tokio::select! {biased;
                result=&mut draining=>return result,
                (error,report)=&mut attempt.failed=>{if report{self.report(error.clone())}return Err(error)},
                exited=attempt.exits.recv()=>if let Some((role,result))=exited{let error=exit_error(role,result);self.report(error.clone());return Err(error)},
                command=self.commands.recv()=>self.control(command),
            }
        }
    }
    async fn explicit_shutdown(&mut self, mut attempt: Option<Attempt>) {
        let shutdown = self
            .requested
            .unwrap_or(Shutdown::Close { graceful: false });
        let drain = if matches!(shutdown, Shutdown::Drain) {
            Some(
                if let Some(attempt) = attempt.as_mut().filter(|a| a.gate.is_open()) {
                    self.drain_attempt(attempt).await
                } else {
                    Err(stopped())
                },
            )
        } else {
            None
        };
        let result = if let Some(attempt) = attempt.as_mut() {
            self.stop_attempt(
                attempt,
                matches!(shutdown, Shutdown::Close { graceful: true }),
            )
            .await
        } else {
            Ok(())
        };
        if let Err(failure) = &result
            && failure.report
        {
            self.report(failure.error.clone());
        }
        let result = result.map_err(|failure| failure.error);
        let drain_result = drain.map(|drain| drain.and(result.clone()));
        // Calls admitted while shutdown awaited a child or cleanup share its first
        // result; they do not start another cleanup attempt.
        while let Ok(command) = self.commands.try_recv() {
            self.control(Some(command));
        }
        self.finish(result, drain_result);
    }
    pub async fn run(mut self) {
        while !self.started && self.requested.is_none() {
            let command = self.commands.recv().await;
            self.control(command);
        }
        loop {
            if self.requested.is_some() {
                self.explicit_shutdown(None).await;
                return;
            }
            let setup = match self.factory().await {
                Ok(setup) => setup,
                Err(error) => {
                    if self.requested.is_none() {
                        self.report(
                            error
                                .with_code(Code::SupervisorError)
                                .context("Supervisor setup "),
                        );
                        self.delay().await;
                    }
                    continue;
                }
            };
            let (mut attempt, error) = self.attempt(setup);
            if self.requested.is_some() {
                self.explicit_shutdown(Some(attempt)).await;
                return;
            }
            let ended = if let Some(error) = error {
                AttemptEnd::Failed {
                    error,
                    report: true,
                }
            } else {
                self.run_attempt(&mut attempt).await
            };
            match ended {
                AttemptEnd::Shutdown => {
                    self.explicit_shutdown(Some(attempt)).await;
                    return;
                }
                AttemptEnd::Failed { error, report } => {
                    if report {
                        self.report(error.with_code(Code::SupervisorError));
                    }
                }
            }
            if let Err(failure) = self.stop_attempt(&mut attempt, false).await {
                self.view.borrow_mut().stopped = true;
                if failure.report {
                    self.report(failure.error.clone());
                }
                // A diagnostic listener may request close after observing the
                // terminal state. It shares this already-failed cleanup result.
                while let Ok(command) = self.commands.try_recv() {
                    self.control(Some(command));
                }
                self.finish(Err(failure.error), None);
                return;
            }
            self.delay().await;
        }
    }
}

fn spawn_child(
    role: ChildRole,
    spec: ChildSpec,
    exits: mpsc::UnboundedSender<(ChildRole, Result<ExitStatus>)>,
) -> Result<ChildProcess> {
    if spec.program.as_os_str().is_empty() {
        return Err(failure(format!("{} program is empty", role.as_str())));
    }
    let mut command = ProcessCommand::new(spec.program);
    command
        .args(spec.args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    command
        .fd_mappings(spec.fd_mappings)
        .map_err(|error| failure(error.to_string()))?;
    let child = command.spawn().map_err(|error| {
        Error::new(
            Code::SupervisorError,
            format!("{} spawn {error}", role.as_str()),
        )
    })?;
    let pid = child
        .id()
        .ok_or_else(|| failure("spawned process has no pid"))?;
    let state = Rc::new(ProcessState {
        exit: RefCell::new(None),
        notify: Notify::new(),
    });
    let (kill, receiver) = mpsc::channel(8);
    let handle = ChildProcess {
        pid,
        state: state.clone(),
        kill,
    };
    tokio::task::spawn_local(monitor_child(role, child, state, receiver, exits));
    Ok(handle)
}
async fn monitor_child(
    role: ChildRole,
    mut child: Child,
    state: Rc<ProcessState>,
    mut kills: mpsc::Receiver<oneshot::Sender<Result<()>>>,
    exits: mpsc::UnboundedSender<(ChildRole, Result<ExitStatus>)>,
) {
    let mut kills_open = true;
    let result = loop {
        tokio::select! {biased;
            request=kills.recv(),if kills_open=>match request{Some(reply)=>{let result=child.start_kill().map_err(|error|Error::new(Code::SupervisorError,format!("{} kill {error}",role.as_str())));let _=reply.send(result);},None=>{kills_open=false;let _=child.start_kill();}},
            status=child.wait()=>break status.map_err(|error|Error::new(Code::SupervisorError,format!("{} wait {error}",role.as_str()))),
        }
    };
    *state.exit.borrow_mut() = Some(result.clone());
    state.notify.notify_waiters();
    while let Ok(reply) = kills.try_recv() {
        let _ = reply.send(Ok(()));
    }
    let _ = exits.send((role, result));
}
fn settle(waiters: &mut Vec<Reply>, result: Result<()>) {
    for reply in waiters.drain(..) {
        let _ = reply.send(result.clone());
    }
}
fn stopped() -> Error {
    Error::new(Code::NodeNotOpen, "supervisor is stopped")
}
fn stopped_error() -> Error {
    Error::new(Code::NodeNotOpen, "Gate stopped during startup")
}
fn failure(message: impl Into<String>) -> Error {
    Error::new(Code::SupervisorError, message)
}
fn exit_error(role: ChildRole, result: Result<ExitStatus>) -> Error {
    match result {
        Ok(status) => failure(format!("{} exited ({status})", role.as_str())),
        Err(error) => error,
    }
}
