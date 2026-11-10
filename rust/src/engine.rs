//! Synchronous protocol state owned by one Proc. All work borrows the Proc's
//! executor, transaction, and output buffers for one ordered input.
use crate::{
    Value,
    error::{Code, Error, Result},
    executor::{Executor, TimerId},
    log::LogRow,
    protocol::{self, ApplyCompletion, CommandResult, Hash, Outcome, State, Status},
    random::Random,
    rpc::{ClientReply, Diagnostic, Output, OutputMessage, Rpc, RpcEnvelope},
    storage::ProtocolStore,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug)]
pub struct Options {
    pub election_timeout: u64,
    pub append_timeout: u64,
    pub rpc_max: usize,
    pub rpc_max_bytes: usize,
    pub quorum: Option<usize>,
    pub keep: Option<Retention>,
}
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    pub target: i64,
    pub trigger: i64,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            election_timeout: 1500,
            append_timeout: 1500,
            rpc_max: 1024,
            rpc_max_bytes: 67_108_864,
            quorum: None,
            keep: None,
        }
    }
}
impl Options {
    pub fn validate(&self) -> Result<()> {
        if self.election_timeout < 10 || self.election_timeout > i64::MAX as u64 {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "electionTimeout is illegal",
            ));
        }
        if self.append_timeout == 0 || self.append_timeout > i64::MAX as u64 {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "appendTimeout is illegal",
            ));
        }
        if self.rpc_max == 0 || self.rpc_max > i64::MAX as usize {
            return Err(Error::new(Code::ArgumentIllegal, "rpcMax is illegal"));
        }
        if self.rpc_max_bytes == 0 {
            return Err(Error::new(Code::ArgumentIllegal, "rpcMaxBytes is illegal"));
        }
        if self
            .keep
            .is_some_and(|k| k.target < 2 || k.target >= k.trigger)
        {
            return Err(Error::new(
                Code::ArgumentIllegal,
                "keepTarget must be >= 2 and < keepTrigger",
            ));
        }
        Ok(())
    }
}

pub struct Context<'a> {
    pub store: &'a mut ProtocolStore,
    pub executor: &'a mut Executor<Task>,
    pub outputs: &'a mut Vec<Output>,
    pub diagnostics: &'a mut Vec<Diagnostic>,
    pub tick_count: i64,
}

#[derive(Clone, Debug)]
pub enum Task {
    Start,
    Tick,
    Receive {
        from: String,
        message: Value,
    },
    Submit {
        client_id: String,
        items: Vec<Bytes>,
    },
    Drain {
        client_id: String,
    },
    Apply(ApplyCompletion),
    Election,
    Heartbeat,
    ApplyReady,
    PumpLeader,
    Decide,
    AskOutcomes,
    RetryNoop {
        term: i64,
    },
    Replicate {
        to: String,
        id: u64,
    },
    RetryReplicate {
        to: String,
        id: u64,
    },
    RequestTimeout {
        cid: String,
    },
    RequestComplete {
        request: Box<PendingRequest>,
        error: Option<Error>,
        reply: Option<Rpc>,
    },
    AppendTimeout {
        id: u64,
    },
    AppendComplete {
        kind: AppendKind,
        error: Option<Error>,
    },
}

#[derive(Clone, Debug)]
pub enum RequestKind {
    Heartbeat {
        to: String,
        id: u64,
        term: i64,
        cid: String,
    },
    Replication {
        to: String,
        id: u64,
        term: i64,
        begin: i64,
        seq_p: i64,
        matched: i64,
    },
    Forward {
        request: CommandRequest,
    },
}
#[derive(Clone, Debug)]
pub struct PendingRequest {
    pub(crate) to: String,
    pub(crate) timer: TimerId,
    pub(crate) kind: RequestKind,
    pub(crate) timeout: Error,
}
#[derive(Clone, Debug)]
pub enum AppendKind {
    Noop { term: i64 },
    Command { id: u64 },
}
#[derive(Clone, Debug)]
pub struct CommandRequest {
    pub(crate) to: String,
    pub(crate) call_id: String,
    pub(crate) items: Vec<Bytes>,
    pub(crate) replied: bool,
}
#[derive(Clone, Debug)]
pub(crate) struct AppendWaiter {
    pub seq: i64,
    pub timer: TimerId,
    pub kind: AppendKind,
}
#[derive(Clone, Debug)]
pub(crate) struct Replication {
    pub id: u64,
    pub term: i64,
    pub next_index: i64,
    pub match_index: i64,
    pub cid: Option<String>,
    pub retry_timer: Option<TimerId>,
    pub cancelled: bool,
    pub heartbeat_cids: BTreeSet<String>,
}
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Phase {
    Cmd,
    Decision,
    Sync,
}
#[derive(Clone, Debug)]
pub(crate) struct LeaderCommand {
    pub id: u64,
    pub term: i64,
    pub request: Option<CommandRequest>,
    pub cmd_seq: i64,
    pub outcomes: Option<Vec<Outcome>>,
    pub phase: Phase,
    pub sync_seq: Option<i64>,
}
#[derive(Clone, Debug)]
pub(crate) struct PendingCommand {
    pub seq: i64,
    pub digest: Hash,
}
#[derive(Clone, Debug)]
pub(crate) struct PendingApply {
    pub cid: String,
    pub seq: i64,
    pub hash: Hash,
    pub tick_count: i64,
    pub count: usize,
}

pub struct Engine {
    pub id: String,
    pub nodes: Vec<String>,
    pub quorum: usize,
    pub state: State,
    pub leader: Option<String>,
    pub term: i64,
    pub commit_seq: i64,
    pub commit_term: i64,
    pub applied_seq: i64,
    pub(crate) opts: Options,
    pub(crate) random: Random,
    pub(crate) voted_for: Option<String>,
    pub(crate) requests: BTreeMap<String, PendingRequest>,
    pub(crate) replication: BTreeMap<String, Replication>,
    pub(crate) append_waiters: BTreeMap<u64, AppendWaiter>,
    pub(crate) pongs: BTreeMap<String, u64>,
    pub(crate) votes: BTreeSet<String>,
    pub(crate) election_timer: Option<TimerId>,
    pub(crate) heartbeat_timer: Option<TimerId>,
    pub(crate) last_change: Option<Status>,
    pub(crate) stopped: bool,
    pub(crate) next_operation: u64,
    pub(crate) repair_state: i64,
    pub(crate) pending_command: Option<PendingCommand>,
    pub(crate) pending_apply: Option<PendingApply>,
    pub(crate) reports: BTreeMap<String, Hash>,
    pub(crate) commands: VecDeque<CommandRequest>,
    pub(crate) leader_command: Option<LeaderCommand>,
    pub(crate) report_timer: Option<TimerId>,
    pub(crate) draining: bool,
    pub(crate) drain_requests: Vec<String>,
    pub(crate) fatal_error: Option<Error>,
}

impl Engine {
    pub fn new(
        id: String,
        nodes: Vec<String>,
        seed: [u8; 16],
        opts: Options,
        store: &ProtocolStore,
    ) -> Result<Self> {
        opts.validate()?;
        let (nodes, quorum) = protocol::membership(&id, nodes, opts.quorum)?;
        let meta = store.load()?;
        let commit_seq = meta.applied_seq;
        let commit_term = if commit_seq < 0 {
            -1
        } else {
            store
                .log
                .range(commit_seq, commit_seq, 1)?
                .first()
                .ok_or_else(|| Error::new(Code::LogCorrupt, "applied entry is missing"))?
                .term
        };
        let pending_command = meta.pending_cmd_seq.map(|seq| PendingCommand {
            seq,
            digest: meta.applied_local_digest.expect("validated metadata"),
        });
        Ok(Self {
            id,
            nodes,
            quorum,
            state: State::Follower,
            leader: None,
            term: store.log.election.term,
            voted_for: store.log.election.voted_for.clone(),
            commit_seq,
            commit_term,
            applied_seq: commit_seq,
            opts,
            random: Random::new(seed)?,
            requests: BTreeMap::new(),
            replication: BTreeMap::new(),
            append_waiters: BTreeMap::new(),
            pongs: BTreeMap::new(),
            votes: BTreeSet::new(),
            election_timer: None,
            heartbeat_timer: None,
            last_change: None,
            stopped: false,
            next_operation: 0,
            repair_state: meta.repair_state,
            pending_command,
            pending_apply: None,
            reports: BTreeMap::new(),
            commands: VecDeque::new(),
            leader_command: None,
            report_timer: None,
            draining: false,
            drain_requests: Vec::new(),
            fatal_error: None,
        })
    }
    pub fn fatal_error(&self) -> Option<&Error> {
        self.fatal_error.as_ref()
    }
    pub fn is_stopped(&self) -> bool {
        self.stopped
    }
    pub fn stop(&mut self) {
        self.stopped = true;
        self.requests.clear();
        self.append_waiters.clear();
        self.replication.clear();
    }
    pub(crate) fn operation(&mut self) -> Result<u64> {
        self.next_operation = self
            .next_operation
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "operation identifier overflow"))?;
        Ok(self.next_operation)
    }
    pub(crate) fn retryms(&self) -> u64 {
        self.opts.election_timeout.div_ceil(10).max(2)
    }
    pub(crate) fn entry(&self, ctx: &Context<'_>, seq: i64) -> Result<Option<LogRow>> {
        if seq < 0 || seq > ctx.store.log.seq || seq < ctx.store.log.begin {
            return Ok(None);
        }
        Ok(ctx.store.log.range(seq, seq, 1)?.into_iter().next())
    }
    pub fn followers(&self, now: u64) -> Vec<String> {
        if self.state != State::Leader {
            return Vec::new();
        }
        self.nodes
            .iter()
            .filter(|id| {
                *id != &self.id
                    && self
                        .pongs
                        .get(*id)
                        .is_some_and(|at| now.saturating_sub(*at) < self.opts.election_timeout)
            })
            .cloned()
            .collect()
    }
    pub fn status(&self, ctx: &Context<'_>) -> Status {
        Status {
            state: self.state.clone(),
            leader: self.leader.clone(),
            term: self.term,
            seq: ctx.store.log.seq,
            commit_seq: self.commit_seq,
            commit_term: self.commit_term,
            followers: self.followers(ctx.executor.timems()),
        }
    }
    pub fn change(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        let status = self.status(ctx);
        if self.last_change.as_ref() != Some(&status) {
            ctx.outputs.push(Output {
                to: "gate".into(),
                message: OutputMessage::Event(protocol::Event::Change {
                    status: status.clone(),
                }),
            });
            self.last_change = Some(status);
        }
        Ok(())
    }
    pub(crate) fn event(&self, ctx: &mut Context<'_>, event: protocol::Event) {
        ctx.outputs.push(Output {
            to: "gate".into(),
            message: OutputMessage::Event(event),
        });
    }
    pub(crate) fn warn(&self, ctx: &mut Context<'_>, error: Error) {
        ctx.diagnostics.push(Diagnostic::Warn(error));
    }
    pub(crate) fn send(&self, ctx: &mut Context<'_>, to: String, rpc: Rpc) {
        ctx.outputs.push(Output {
            to,
            message: OutputMessage::Cluster(RpcEnvelope {
                from: self.id.clone(),
                rpc,
            }),
        });
    }
    pub(crate) fn rpc_error(
        &self,
        ctx: &mut Context<'_>,
        to: String,
        cid: String,
        mut error: Error,
        log_seq: Option<i64>,
    ) {
        error.stack = None;
        self.send(
            ctx,
            to,
            Rpc::Err {
                cid,
                term: self.term,
                error,
                log_seq,
            },
        );
    }
    pub(crate) fn reply(
        &self,
        ctx: &mut Context<'_>,
        request: &mut CommandRequest,
        error: Option<Error>,
        seq: i64,
        outcomes: Option<Vec<Outcome>>,
    ) {
        if request.replied {
            return;
        }
        request.replied = true;
        let message = if request.to == "gate" {
            OutputMessage::Client(match error {
                Some(mut error) => {
                    error.stack = None;
                    ClientReply::Err {
                        client_id: request.call_id.clone(),
                        error,
                    }
                }
                None => ClientReply::Ack {
                    client_id: request.call_id.clone(),
                    result: Some(CommandResult {
                        cmd_seq: seq,
                        results: outcomes.unwrap_or_default(),
                    }),
                },
            })
        } else {
            OutputMessage::Cluster(RpcEnvelope {
                from: self.id.clone(),
                rpc: match error {
                    Some(mut error) => {
                        error.stack = None;
                        Rpc::Err {
                            cid: request.call_id.clone(),
                            term: self.term,
                            error,
                            log_seq: None,
                        }
                    }
                    None => Rpc::Ack {
                        cid: request.call_id.clone(),
                        term: self.term,
                        seq: None,
                        cmd_seq: Some(seq),
                        results: outcomes,
                    },
                },
            })
        };
        ctx.outputs.push(Output {
            to: request.to.clone(),
            message,
        });
    }
    pub fn dispatch(&mut self, task: Task, ctx: &mut Context<'_>) -> Result<()> {
        if self.stopped || self.fatal_error.is_some() {
            return Ok(());
        }
        match task {
            Task::Start => self.start(ctx),
            Task::Tick => Ok(()),
            Task::Receive { from, message } => self.receive(ctx, from, message),
            Task::Submit { client_id, items } => self.submit(ctx, client_id, items),
            Task::Drain { client_id } => self.drain(ctx, client_id),
            Task::Apply(reply) => self.receive_apply(ctx, reply),
            Task::Election => {
                self.vote_self(ctx)?;
                if self.state != State::Leader {
                    self.reset_election(ctx)?;
                }
                Ok(())
            }
            Task::Heartbeat => self.heartbeat(ctx),
            Task::ApplyReady => self.apply_ready(ctx),
            Task::PumpLeader => self.pump_leader(ctx),
            Task::Decide => self.decide(ctx),
            Task::AskOutcomes => {
                self.ask_outcomes(ctx);
                Ok(())
            }
            Task::RetryNoop { term } => {
                if self.state == State::Leader && self.term == term {
                    self.append_noop(ctx)?;
                }
                Ok(())
            }
            Task::Replicate { to, id } => self.replicate(ctx, &to, id),
            Task::RetryReplicate { to, id } => {
                if let Some(state) = self.replication.get_mut(&to).filter(|s| s.id == id) {
                    state.retry_timer = None;
                }
                self.replicate(ctx, &to, id)
            }
            Task::RequestTimeout { cid } => {
                if let Some(request) = self.requests.remove(&cid) {
                    let error = request.timeout.clone();
                    self.request_complete(ctx, request, Some(error), None)?;
                }
                Ok(())
            }
            Task::RequestComplete {
                request,
                error,
                reply,
            } => self.request_complete(ctx, *request, error, reply),
            Task::AppendTimeout { id } => {
                self.finish_append(
                    ctx,
                    id,
                    Some(Error::new(Code::AppendTimeout, "quorum append timeout")),
                );
                Ok(())
            }
            Task::AppendComplete { kind, error } => self.append_complete(ctx, kind, error),
        }
    }
}
