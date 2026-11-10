//! The sole DB2 writer. Application changes and their receipt commit together.
use crate::{
    Value,
    diagnostics::{Diagnostics, StopHandle, panic_error},
    error::{Code, Error, Result},
    local::{self, LocalReply, Received, SendFrame, StartupRequest},
    protocol::{ApplyCompletion, Execute, Outcome, Seed, digest_cmd},
    rpc::ApplyRequest,
    storage, wire,
};
use futures_util::FutureExt;
use rusqlite::{Connection, session::Session};
use std::{
    cell::{RefCell, RefMut},
    future::Future,
    ops::AsyncFnMut,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
    rc::Rc,
};

/// Context shared by all items in one CMD. Setup receives sequence zero and no command.
#[derive(Clone, Debug)]
pub struct ApplyContext {
    pub term: i64,
    pub seq: i64,
    pub index: usize,
    pub match_index: Option<Vec<i64>>,
}

pub trait ApplyCallback {
    fn call<'a>(
        &'a mut self,
        db: &'a Connection,
        command: Option<&'a [u8]>,
        context: &'a ApplyContext,
    ) -> impl Future<Output = Result<Value>> + 'a;
}
pub struct SyncApply<F>(F);
pub struct AsyncApply<F>(F);
pub fn apply_sync<F>(callback: F) -> SyncApply<F>
where
    F: FnMut(&Connection, Option<&[u8]>, &ApplyContext) -> Result<Value>,
{
    SyncApply(callback)
}
pub fn apply_async<F>(callback: F) -> AsyncApply<F>
where
    F: for<'a> AsyncFnMut(&'a Connection, Option<&'a [u8]>, &'a ApplyContext) -> Result<Value>,
{
    AsyncApply(callback)
}
impl<F> ApplyCallback for SyncApply<F>
where
    F: FnMut(&Connection, Option<&[u8]>, &ApplyContext) -> Result<Value>,
{
    async fn call<'a>(
        &'a mut self,
        db: &'a Connection,
        command: Option<&'a [u8]>,
        context: &'a ApplyContext,
    ) -> Result<Value> {
        (self.0)(db, command, context)
    }
}
impl<F> ApplyCallback for AsyncApply<F>
where
    F: for<'a> AsyncFnMut(&'a Connection, Option<&'a [u8]>, &'a ApplyContext) -> Result<Value>,
{
    async fn call<'a>(
        &'a mut self,
        db: &'a Connection,
        command: Option<&'a [u8]>,
        context: &'a ApplyContext,
    ) -> Result<Value> {
        (self.0)(db, command, context).await
    }
}

pub struct Apply<C> {
    id: String,
    path: PathBuf,
    callback: C,
    db: Option<Connection>,
    seed: Option<Seed>,
    past_hello: bool,
    ready: bool,
    tick_count: i64,
    stop: StopHandle,
    close_result: Option<Result<()>>,
    fatal_error: Option<Error>,
    diagnostics: Rc<RefCell<Diagnostics>>,
}
impl<C: ApplyCallback> Apply<C> {
    pub fn new(id: impl Into<String>, path: impl AsRef<Path>, callback: C) -> Result<Self> {
        let id = id.into();
        if id.is_empty() {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "id must be non-empty string",
            ));
        }
        let path = path.as_ref().to_owned();
        if path.as_os_str().is_empty() || path == Path::new(":memory:") {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "Apply requires a database file",
            ));
        }
        Ok(Self {
            id,
            path,
            callback,
            db: None,
            seed: None,
            past_hello: false,
            ready: false,
            tick_count: 0,
            stop: StopHandle::default(),
            close_result: None,
            fatal_error: None,
            diagnostics: Rc::new(RefCell::new(Diagnostics::default())),
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn is_open(&self) -> bool {
        self.db.is_some() && !self.stop.is_stopped()
    }
    pub fn is_stopped(&self) -> bool {
        self.stop.is_stopped()
    }
    pub fn stop_handle(&self) -> StopHandle {
        self.stop.clone()
    }
    pub fn diagnostics(&mut self) -> RefMut<'_, Diagnostics> {
        self.diagnostics.borrow_mut()
    }
    pub fn fatal_error(&self) -> Option<&Error> {
        self.fatal_error.as_ref()
    }

    pub fn hello(&mut self, seed: Seed) -> Result<()> {
        self.check_running()?;
        if seed == [0; 16] {
            return self.fail(Error::new(Code::RpcIllegal, "seed must be nonzero"));
        }
        if self.past_hello {
            return self.fail(Error::new(
                Code::MonsterIllegal,
                "Apply received hello after startup",
            ));
        }
        self.seed = Some(seed);
        Ok(())
    }
    pub fn open(&mut self) -> Result<()> {
        let result = (|| {
            self.check_running()?;
            let seed = self
                .seed
                .ok_or_else(|| Error::new(Code::MonsterIllegal, "Apply has not received hello"))?;
            self.past_hello = true;
            if self.db.is_some() {
                return Err(Error::new(Code::MonsterIllegal, "Apply is already open"));
            }
            self.db = Some(storage::open_apply_database(&self.path, &seed)?);
            Ok(())
        })();
        if let Err(e) = result {
            return self.fail(e);
        }
        Ok(())
    }
    pub fn ready(&mut self) -> Result<()> {
        let result = (|| {
            self.check_running()?;
            if self.ready {
                return Err(Error::new(Code::MonsterIllegal, "Apply startup is illegal"));
            }
            let db = self
                .db
                .as_ref()
                .ok_or_else(|| Error::new(Code::MonsterIllegal, "Apply is not open"))?;
            storage::check_generation(
                db,
                "monsterft_meta_apply",
                self.seed.as_ref().ok_or_else(|| {
                    Error::new(Code::MonsterIllegal, "Apply has not received hello")
                })?,
            )?;
            self.ready = true;
            Ok(())
        })();
        if let Err(e) = result {
            return self.fail(e);
        }
        Ok(())
    }
    fn check_running(&self) -> Result<()> {
        if self.stop.is_stopped() {
            Err(self
                .fatal_error
                .clone()
                .unwrap_or_else(|| Error::new(Code::NodeNotOpen, "Apply is stopped")))
        } else {
            Ok(())
        }
    }
    fn fail<T>(&mut self, error: Error) -> Result<T> {
        if !self.stop.is_stopped() {
            self.stop.stop();
            self.ready = false;
            self.fatal_error = Some(error.clone());
            self.diagnostics.borrow_mut().error(&error);
        }
        let _ = self.close();
        Err(error)
    }
    /// The owner runner retains this future until completion, even if its caller stops waiting.
    pub async fn execute(&mut self, request: Execute) -> Result<ApplyCompletion> {
        let result = AssertUnwindSafe(self.execute_inner(&request))
            .catch_unwind()
            .await;
        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => self.fail(error),
            Err(panic) => self.fail(panic_error(panic)),
        }
    }
    async fn execute_inner(&mut self, request: &Execute) -> Result<ApplyCompletion> {
        self.check_running()?;
        request.validate()?;
        if !self.ready {
            return Err(Error::new(Code::MonsterIllegal, "Apply is not ready"));
        }
        if request.tick_count <= self.tick_count {
            return Err(Error::new(
                Code::MonsterIllegal,
                "Apply execution tickCount is not advancing",
            ));
        }
        self.tick_count = request.tick_count;
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "Apply is not open"))?;
        db.prepare_cached("BEGIN IMMEDIATE")?.execute([])?;
        storage::check_generation(db, "monsterft_meta_apply", self.seed.as_ref().unwrap())?;
        let previous = storage::read_apply(db)?;
        if request.seq <= previous.seq || (previous.seq == -1 && request.seq != 0) {
            return Err(Error::new(
                Code::MonsterIllegal,
                "Apply sequence is not advancing",
            ));
        }
        let mut context = ApplyContext {
            term: request.term,
            seq: request.seq,
            index: 0,
            match_index: request.match_index.clone(),
        };
        let mut outcomes = Vec::new();
        let local_digest;
        if request.seq == 0 {
            self.callback.call(db, None, &context).await?;
            check_transaction(db, &self.stop)?;
            local_digest = [0; 16];
        } else {
            let mut session = Session::new(db)?;
            session.attach::<&str>(None)?;
            for (index, command) in request.items.as_ref().unwrap().iter().enumerate() {
                check_transaction(db, &self.stop)?;
                context.index = index;
                db.prepare_cached("SAVEPOINT monsterft_item")?.execute([])?;
                let result = self
                    .callback
                    .call(db, Some(command), &context)
                    .await
                    .and_then(|value| {
                        crate::protocol::validate_result_value(&value)?;
                        crate::wire::encode_payload(&value)
                            .map_err(|e| e.with_code(Code::ApplyError))?;
                        Ok(value)
                    });
                match result {
                    Ok(value) => {
                        check_transaction(db, &self.stop)?;
                        db.prepare_cached("RELEASE monsterft_item")?.execute([])?;
                        outcomes.push(Outcome::Fulfilled(value));
                    }
                    Err(error) => {
                        if self.stop.is_stopped() {
                            return Err(Error::new(Code::NodeNotOpen, "Apply is stopped"));
                        }
                        let mut warning = error.clone();
                        warning.code = Some(Code::ApplyError);
                        if let Some(panic) = emit_warning(
                            &self.stop,
                            &self.diagnostics,
                            &mut self.fatal_error,
                            &warning,
                        ) {
                            return Err(panic);
                        }
                        if self.stop.is_stopped() {
                            return Err(Error::new(Code::NodeNotOpen, "Apply is stopped"));
                        }
                        if db.is_autocommit() {
                            return Err(error);
                        }
                        db.execute_batch("ROLLBACK TO monsterft_item; RELEASE monsterft_item")
                            .map_err(|error| {
                                let mut error = Error::from(error);
                                error.message = format!("DB2 item rollback {}", error.message);
                                error
                            })?;
                        outcomes.push(Outcome::Rejected(error));
                    }
                }
            }
            let mut patchset = Vec::new();
            session.patchset_strm(&mut patchset)?;
            drop(session); // Receipt metadata must not enter the application digest.
            local_digest = digest_cmd(request.term, request.seq, &patchset, &outcomes)?;
        }
        check_transaction(db, &self.stop)?;
        let receipt = storage::Receipt {
            seq: request.seq,
            entry_hash: Some(request.entry_hash),
            local_digest: Some(local_digest),
        };
        storage::write_receipt(db, &receipt)?;
        check_transaction(db, &self.stop)?;
        db.prepare_cached("COMMIT")?.execute([])?;
        Ok(ApplyCompletion {
            cid: request.cid.clone(),
            tick_count: request.tick_count,
            receipt,
            outcomes,
        })
    }
    /// Stops once and retains the first cleanup result. An active runner accepts stop before awaiting work.
    pub fn close(&mut self) -> Result<()> {
        if let Some(result) = &self.close_result {
            return result.clone();
        }
        self.stop.stop();
        self.ready = false;
        let result = match self.db.take() {
            None => Ok(()),
            Some(db) => match db.close() {
                Ok(()) => Ok(()),
                Err((db, error)) => {
                    self.db = Some(db);
                    Err(Error::from(error))
                }
            },
        };
        self.close_result = Some(result.clone());
        match &result {
            Ok(()) => self.diagnostics.borrow_mut().close(),
            Err(error) => self.diagnostics.borrow_mut().error(error),
        }
        result
    }

    /// Run the transport-facing owner. An admitted execution remains owned here
    /// until it settles, independently of the remote caller's result waiter.
    pub async fn run(
        mut self,
        mut received: tokio::sync::mpsc::Receiver<Received>,
        mut send: SendFrame,
    ) -> Result<()> {
        let owner_stop = self.stop.clone();
        loop {
            let message =
                tokio::select! {message=received.recv()=>message,_=owner_stop.stopped()=>None};
            let Some(message) = message else {
                break;
            };
            if message.from != "gate" {
                let warning = Error::new(
                    Code::RpcIllegal,
                    format!("Apply sender {} is illegal, expected gate", message.from),
                );
                let failure = emit_warning(
                    &self.stop,
                    &self.diagnostics,
                    &mut self.fatal_error,
                    &warning,
                );
                if let Some(error) = failure {
                    let _ = self.fail::<()>(error);
                }
                if self.is_stopped() {
                    let _ = self.close();
                    break;
                }
                continue;
            }
            let request = match parse_request(&message.frame) {
                Ok(request) => request,
                Err(error) => {
                    let _ = self.fail::<()>(error);
                    break;
                }
            };
            match request {
                Request::Startup(request) => {
                    let cid = request.cid().to_owned();
                    let close = matches!(request, StartupRequest::Close { .. });
                    let result = match request {
                        StartupRequest::Hello { seed, .. } => self.hello(seed),
                        StartupRequest::Open { .. } => self.open(),
                        StartupRequest::Ready { .. } => self.ready(),
                        StartupRequest::Close { .. } => self.close(),
                        _ => {
                            self.fail(Error::new(Code::RpcIllegal, "Apply request is not execute"))
                        }
                    };
                    match result {
                        Ok(()) => {
                            if let Err(error) = send_reply(&mut send, LocalReply::ack(cid)) {
                                let _ = self.fail::<()>(error);
                            }
                        }
                        Err(error) if close => {
                            let _ = send_reply(&mut send, LocalReply::Err { cid, error });
                        }
                        Err(_) => (),
                    }
                }
                Request::Execute(request) => {
                    let stop = self.stop.clone();
                    let diagnostics = self.diagnostics.clone();
                    let mut close_requests = Vec::new();
                    let mut control_failure = None;
                    let mut channel_closed = false;
                    // Only these owner-local handles remain accessible while the
                    // callback borrows DB2. Control never touches that connection.
                    let result = {
                        let execution = self.execute(request);
                        tokio::pin!(execution);
                        loop {
                            tokio::select! {
                                result=&mut execution=>break result,
                                incoming=received.recv(),if !channel_closed=>{
                                    let Some(incoming)=incoming else {channel_closed=true;stop.stop();continue;};
                                    if incoming.from!="gate" {
                                        if !stop.is_stopped() {
                                            let warning=Error::new(Code::RpcIllegal,format!("Apply sender {} is illegal, expected gate",incoming.from));
                                            emit_warning(&stop,&diagnostics,&mut control_failure,&warning);
                                        }
                                        continue;
                                    }
                                    match parse_request(&incoming.frame) {
                                        Ok(Request::Startup(StartupRequest::Close{cid}))=>{stop.stop();close_requests.push(cid);},
                                        Ok(_) if stop.is_stopped()=>(),
                                        Ok(Request::Startup(StartupRequest::Hello{..}))=>control_fatal(&stop,&diagnostics,&mut control_failure,Error::new(Code::MonsterIllegal,"Apply received hello after startup")),
                                        Ok(Request::Startup(StartupRequest::Open{..}))=>control_fatal(&stop,&diagnostics,&mut control_failure,Error::new(Code::MonsterIllegal,"Apply is already open")),
                                        Ok(Request::Startup(StartupRequest::Ready{..}))=>control_fatal(&stop,&diagnostics,&mut control_failure,Error::new(Code::MonsterIllegal,"Apply startup is illegal")),
                                        Ok(Request::Startup(_))=>control_fatal(&stop,&diagnostics,&mut control_failure,Error::new(Code::RpcIllegal,"Apply request is not execute")),
                                        Ok(Request::Execute(_))=>control_fatal(&stop,&diagnostics,&mut control_failure,Error::new(Code::MonsterIllegal,"Apply is already executing")),
                                        Err(error)=>control_fatal(&stop,&diagnostics,&mut control_failure,error),
                                    }
                                },
                            }
                        }
                    };
                    if let Some(error) = control_failure {
                        self.fatal_error = Some(error);
                    }
                    if self.is_stopped() {
                        let closed = self.close();
                        for cid in close_requests {
                            let reply = match &closed {
                                Ok(()) => LocalReply::ack(cid),
                                Err(error) => LocalReply::Err {
                                    cid,
                                    error: error.clone(),
                                },
                            };
                            let _ = send_reply(&mut send, reply);
                        }
                    } else if let Ok(completion) = result
                        && let Err(error) =
                            send_reply(&mut send, LocalReply::completion(completion))
                    {
                        let _ = self.fail::<()>(error);
                    }
                }
            }
            if self.is_stopped() {
                break;
            }
        }
        let close = self.close();
        self.fatal_error.map_or(close, Err)
    }
}

enum Request {
    Startup(StartupRequest),
    Execute(Execute),
}

fn parse_request(frame: &[u8]) -> Result<Request> {
    let mut value: Value = wire::decode_frame(frame)?;
    if local::field(&value, "to").and_then(Value::as_str) == Some("apply") {
        value = local::field(&value, "message")
            .cloned()
            .ok_or_else(|| Error::new(Code::RpcIllegal, "Apply envelope is illegal"))?;
    }
    if local::kind(&value)? == "execute" {
        let ApplyRequest::Execute { execute } = local::decode_value(value)?;
        execute.validate()?;
        return Ok(Request::Execute(execute));
    }
    let request: StartupRequest = local::decode_value(value)?;
    if request.cid().is_empty() {
        return Err(Error::new(Code::RpcIllegal, "Apply request cid is illegal"));
    }
    Ok(Request::Startup(request))
}

fn send_reply(send: &mut SendFrame, reply: LocalReply) -> Result<()> {
    match catch_unwind(AssertUnwindSafe(|| {
        wire::encode_frame(&reply).and_then(|frame| send("gate", frame))
    })) {
        Ok(Ok(())) => Ok(()),
        Ok(Err(mut error)) => {
            error.code = Some(Code::SendError);
            error.message = format!("Apply send {}", error.message);
            Err(error)
        }
        Err(panic) => Err(panic_error(panic).with_code(Code::SendError)),
    }
}

fn emit_warning(
    stop: &StopHandle,
    diagnostics: &Rc<RefCell<Diagnostics>>,
    failure: &mut Option<Error>,
    warning: &Error,
) -> Option<Error> {
    let mut listeners = diagnostics.borrow_mut().take_warnings();
    let first = crate::diagnostics::warn(&mut listeners, warning, |error| {
        if stop.is_stopped() {
            diagnostics.borrow_mut().error(&error);
        } else {
            control_fatal(stop, diagnostics, failure, error);
        }
    });
    diagnostics.borrow_mut().restore_warnings(listeners);
    first
}

fn control_fatal(
    stop: &StopHandle,
    diagnostics: &Rc<RefCell<Diagnostics>>,
    failure: &mut Option<Error>,
    error: Error,
) {
    if stop.is_stopped() {
        return;
    }
    stop.stop();
    *failure = Some(error.clone());
    diagnostics.borrow_mut().error(&error);
}

fn check_transaction(db: &Connection, stop: &StopHandle) -> Result<()> {
    if stop.is_stopped() {
        return Err(Error::new(Code::NodeNotOpen, "Apply is stopped"));
    }
    if db.is_autocommit() {
        return Err(Error::new(
            Code::SqliteError,
            "DB2 application transaction ended",
        ));
    }
    Ok(())
}

/// Convenience for applications returning a Serde value, preserving map order.
pub fn result_value<T: serde::Serialize>(value: T) -> Result<Value> {
    rmpv::ext::to_value(value).map_err(|error| Error::new(Code::ApplyError, error.to_string()))
}
