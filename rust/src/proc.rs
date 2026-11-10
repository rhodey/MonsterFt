//! One of the three independent protocol owners. No output escapes an uncommitted input.
use crate::{
    Value,
    diagnostics::{Diagnostics, StopHandle, panic_error},
    engine::{self, Context, Engine, Task},
    error::{Code, Error, Result},
    executor::Executor,
    local::{self, Input, LocalReply, OrderedInput, Received, SendFrame, StartupRequest},
    protocol,
    rpc::{Diagnostic, Output},
    startup::{Startup, StartupAction},
    storage::{ProcRole, ProtocolStore},
    wire,
};
use bytes::Bytes;
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    path::Path,
    time::Duration,
};

#[derive(Clone, Debug)]
pub struct Options {
    pub protocol: engine::Options,
    pub local_timeout: Duration,
    pub iter_step: usize,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            protocol: engine::Options::default(),
            local_timeout: Duration::from_millis(1000),
            iter_step: 1024,
        }
    }
}

pub struct Proc {
    pub role: ProcRole,
    pub id: String,
    pub nodes: Vec<String>,
    pub tick_count: i64,
    pub store: ProtocolStore,
    pub diagnostics: Diagnostics,
    options: Options,
    local_send: SendFrame,
    startup: Option<Startup>,
    engine: Option<Engine>,
    executor: Executor<Task>,
    watchdog: Option<tokio::time::Instant>,
    stop: StopHandle,
    stopped: bool,
    close_error: Option<Error>,
    fatal_error: Option<Error>,
}

struct InputContext<'a> {
    engine: &'a mut Engine,
    store: &'a mut ProtocolStore,
    outputs: &'a mut Vec<Output>,
    diagnostics: &'a mut Vec<Diagnostic>,
    tick_count: i64,
}

impl Proc {
    pub fn new(
        role: ProcRole,
        id: impl Into<String>,
        nodes: Vec<String>,
        local_send: SendFrame,
        base: impl AsRef<Path>,
        options: Options,
    ) -> Result<Self> {
        let id = id.into();
        let (nodes, _) = protocol::membership(&id, nodes, options.protocol.quorum)?;
        options.protocol.validate()?;
        if options.local_timeout.is_zero() || options.local_timeout.as_millis() > i64::MAX as u128 {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "localTimeout must be positive milliseconds",
            ));
        }
        let store = ProtocolStore::new(role, base, options.iter_step)?;
        Ok(Self {
            role,
            id,
            nodes,
            tick_count: 0,
            store,
            diagnostics: Diagnostics::default(),
            options,
            local_send,
            startup: Some(Startup::new()),
            engine: None,
            executor: Executor::new(),
            watchdog: None,
            stop: StopHandle::default(),
            stopped: false,
            close_error: None,
            fatal_error: None,
        })
    }

    pub fn is_open(&self) -> bool {
        self.startup.is_none() && !self.is_stopped()
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped || self.stop.is_stopped()
    }
    pub fn stop_handle(&self) -> StopHandle {
        self.stop.clone()
    }
    pub fn fatal_error(&self) -> Option<&Error> {
        self.fatal_error.as_ref()
    }
    pub fn watchdog_deadline(&self) -> Option<tokio::time::Instant> {
        self.watchdog
    }

    pub fn close(&mut self) -> Result<()> {
        self.close_with(None)
    }

    fn close_with(&mut self, error: Option<Error>) -> Result<()> {
        if self.stopped {
            return self.close_error.clone().map_or(Ok(()), Err);
        }
        self.stopped = true;
        self.stop.stop();
        self.watchdog = None;
        if let Some(engine) = &mut self.engine {
            engine.stop();
        }
        self.executor.clear();
        self.close_error = self.store.close().err();
        if let Some(error) = error {
            self.diagnostics.error(&error);
        }
        if let Some(error) = &self.close_error {
            self.diagnostics.error(error);
            return Err(error.clone());
        }
        self.diagnostics.close();
        Ok(())
    }

    fn fatal(&mut self, error: Error) {
        if self.stopped {
            return;
        }
        self.fatal_error = Some(error.clone());
        let _ = self.close_with(Some(error));
    }

    fn warn(&mut self, error: Error) {
        let mut listeners = self.diagnostics.take_warnings();
        crate::diagnostics::warn(&mut listeners, &error, |failure| {
            if self.stopped {
                self.diagnostics.error(&failure);
            } else {
                self.fatal(failure);
            }
        });
        self.diagnostics.restore_warnings(listeners);
        if self.stop.is_stopped() && !self.stopped {
            let _ = self.close();
        }
    }

    fn send_reply(&mut self, reply: LocalReply) {
        let sent = wire::encode_frame(&reply).and_then(|frame| (self.local_send)("gate", frame));
        if let Err(mut error) = sent {
            error.code = Some(Code::SendError);
            error.message = format!("Proc send {}", error.message);
            self.fatal(error);
        }
    }

    fn startup_request(&mut self, request: StartupRequest) -> Result<()> {
        if request.cid().is_empty() {
            return Err(Error::new(Code::RpcIllegal, "Proc request cid is illegal"));
        }
        if matches!(request, StartupRequest::Close { .. }) {
            let cid = request.cid().to_owned();
            let reply = match self.close() {
                Ok(()) => LocalReply::ack(cid),
                Err(error) => LocalReply::Err { cid, error },
            };
            self.send_reply(reply);
            return Ok(());
        }
        if self.stopped {
            return Ok(());
        }
        if self.stop.is_stopped() {
            self.close()?;
            return Ok(());
        }
        let startup = self
            .startup
            .as_mut()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "Proc startup is complete"))?;
        let action = startup.receive(&request, &mut self.store)?;
        let mut reply = LocalReply::ack(request.cid());
        match action {
            StartupAction::Ack => (),
            StartupAction::Capture(capture) => {
                if let LocalReply::Ack { selection, .. } = &mut reply {
                    *selection = Some(capture);
                }
            }
            StartupAction::ConstructEngine => {
                self.engine = Some(Engine::new(
                    self.id.clone(),
                    self.nodes.clone(),
                    startup.seed()?,
                    self.options.protocol.clone(),
                    &self.store,
                )?);
            }
        }
        self.send_reply(reply);
        Ok(())
    }

    fn prepare_input(&self, input: &OrderedInput) -> Result<(Task, Option<Error>)> {
        input.validate()?;
        let task = match &input.input {
            Input::Start => Task::Start,
            Input::Tick => Task::Tick,
            Input::Append { client_id, items } => Task::Submit {
                client_id: client_id.clone(),
                items: items.clone(),
            },
            Input::Drain { client_id } => Task::Drain {
                client_id: client_id.clone(),
            },
            Input::ApplyResult { reply } => Task::Apply(reply.clone()),
            Input::ClusterMessage { from, frame } => {
                if from == &self.id || !self.nodes.contains(from) {
                    return Err(Error::new(
                        Code::RpcIllegal,
                        "Proc cluster sender is illegal",
                    ));
                }
                // Gate accepted this checksum already. A failure here is local and terminal.
                let payload = wire::verify_frame(frame)?;
                let parsed = (|| -> Result<Value> {
                    let envelope: protocol::Envelope<Value> = wire::decode_payload(payload)?;
                    if envelope.to != self.id
                        || local::field(&envelope.message, "from").and_then(Value::as_str)
                            != Some(from.as_str())
                    {
                        return Err(Error::new(
                            Code::RpcIllegal,
                            "Proc cluster envelope is illegal",
                        ));
                    }
                    local::kind(&envelope.message)?;
                    Ok(envelope.message)
                })();
                match parsed {
                    Ok(message) => Task::Receive {
                        from: from.clone(),
                        message,
                    },
                    Err(mut error) => {
                        error.message = format!("cluster frame from {from}: {}", error.message);
                        return Ok((Task::Tick, Some(error)));
                    }
                }
            }
        };
        Ok((task, None))
    }

    fn input(&mut self, input: OrderedInput) -> Result<()> {
        let (task, warning) = self.prepare_input(&input)?;
        if self
            .startup
            .as_ref()
            .is_some_and(|startup| !startup.is_reconciled())
        {
            return Err(Error::new(
                Code::MonsterIllegal,
                "Proc startup step is illegal",
            ));
        }
        let starting = matches!(input.input, Input::Start);
        if self.startup.is_some() && !starting {
            return Err(Error::new(
                Code::MonsterIllegal,
                "Proc runtime has not started",
            ));
        }
        if self.startup.is_none() && starting {
            return Err(Error::new(Code::MonsterIllegal, "Proc startup is complete"));
        }
        if self.tick_count.checked_add(1) != Some(input.tick_count)
            || input.timems < self.executor.timems()
        {
            return Err(Error::new(
                Code::MonsterIllegal,
                "Proc input order is illegal",
            ));
        }
        self.tick_count = input.tick_count;
        self.store
            .db()?
            .prepare_cached("BEGIN IMMEDIATE")?
            .execute([])?;
        self.store.check_generation()?;
        let mut outputs = Vec::new();
        let mut diagnostics = Vec::new();
        if let Some(warning) = warning {
            diagnostics.push(Diagnostic::Warn(warning));
        }
        let engine = self
            .engine
            .as_mut()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "Proc engine is missing"))?;
        let mut context = InputContext {
            engine,
            store: &mut self.store,
            outputs: &mut outputs,
            diagnostics: &mut diagnostics,
            tick_count: input.tick_count,
        };
        self.executor.run(
            input.timems,
            task,
            &mut context,
            |context, executor, task| {
                context.engine.dispatch(
                    task,
                    &mut Context {
                        store: context.store,
                        executor,
                        outputs: context.outputs,
                        diagnostics: context.diagnostics,
                        tick_count: context.tick_count,
                    },
                )
            },
            |context, executor| {
                context.engine.change(&mut Context {
                    store: context.store,
                    executor,
                    outputs: context.outputs,
                    diagnostics: context.diagnostics,
                    tick_count: context.tick_count,
                })
            },
        )?;
        // Capture and encode while the transaction still belongs to this input.
        // Encoding failure must roll it back, just like a processing failure.
        let fatal_error = self.engine.as_ref().and_then(Engine::fatal_error).cloned();
        let committed_outputs = if fatal_error.is_some() {
            Vec::new()
        } else {
            outputs
                .iter()
                .map(|output| {
                    let payload = wire::encode_payload(output)?;
                    Ok(match self.role {
                        ProcRole::A => payload.into(),
                        ProcRole::B => Bytes::copy_from_slice(&wire::checksum(&payload)[..8]),
                        ProcRole::C => Bytes::copy_from_slice(&wire::checksum(&payload)[8..]),
                    })
                })
                .collect::<Result<Vec<_>>>()?
        };
        self.store.db()?.prepare_cached("COMMIT")?.execute([])?;
        if let Some(error) = fatal_error {
            // Repair fences survive; final completion and protocol outputs do not.
            self.fatal(error);
            return Ok(());
        }
        if starting {
            self.startup = None;
        }
        self.watchdog = Some(tokio::time::Instant::now() + self.options.local_timeout);
        self.send_reply(LocalReply::Ack {
            cid: input.cid,
            tick_count: Some(input.tick_count),
            outputs: Some(committed_outputs),
            selection: None,
            receipt: None,
            outcomes: None,
        });
        // Local diagnostics can call user code only after committing and attempting ACK.
        for diagnostic in diagnostics {
            match diagnostic {
                Diagnostic::Warn(error) => self.warn(error),
                Diagnostic::Error(error) => self.diagnostics.error(&error),
            }
            if self.stop.is_stopped() && !self.stopped {
                let _ = self.close();
            }
        }
        Ok(())
    }

    pub fn on_receive(&mut self, from: &str, frame: Bytes) {
        if from != "gate" {
            if !self.is_stopped() {
                self.warn(Error::new(
                    Code::RpcIllegal,
                    format!("Proc sender {from} is illegal, expected gate"),
                ));
            }
            return;
        }
        let processed = catch_unwind(AssertUnwindSafe(|| -> Result<()> {
            let value: Value = wire::decode_frame(&frame)?;
            let kind = local::kind(&value)?;
            if matches!(
                kind,
                "hello" | "open" | "capture" | "adopt" | "reconcile" | "ready" | "close"
            ) {
                return self.startup_request(local::decode_value(value)?);
            }
            if self.stopped {
                return Ok(());
            }
            if self.stop.is_stopped() {
                self.close()?;
                return Ok(());
            }
            self.input(local::decode_value(value)?)
        }));
        match processed {
            Ok(Ok(())) => (),
            Ok(Err(error)) => self.fatal(error),
            Err(panic) => self.fatal(panic_error(panic)),
        }
    }

    pub fn check_watchdog(&mut self, now: tokio::time::Instant) {
        if self.watchdog.is_some_and(|deadline| deadline <= now) {
            self.fatal(Error::new(Code::LocalTimeout, "Gate communication timeout"));
        }
        if self.stop.is_stopped() && !self.stopped {
            let _ = self.close();
        }
    }

    pub async fn run(mut self, mut received: tokio::sync::mpsc::Receiver<Received>) -> Result<()> {
        let stop = self.stop.clone();
        loop {
            let deadline = self.watchdog;
            tokio::select! {
                _=stop.stopped()=>{self.close()?;break;},
                message=received.recv()=>match message {
                    Some(message)=>self.on_receive(&message.from,message.frame),
                    None=>{self.close()?;break;},
                },
                _=async {match deadline {Some(deadline)=>tokio::time::sleep_until(deadline).await,None=>std::future::pending().await}}=>self.check_watchdog(tokio::time::Instant::now()),
            }
            if self.stopped {
                break;
            }
        }
        self.fatal_error.or(self.close_error).map_or(Ok(()), Err)
    }
}
