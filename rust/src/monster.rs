//! Command execution and outcome agreement layered on the ordered Raft log.
use crate::{
    engine::*,
    error::{Code, Error, Result},
    protocol::{ApplyCompletion, Entry, Event, Execute, Hash, State},
    rpc::{ApplyRequest, ClientReply, Diagnostic, Output, OutputMessage, Rpc},
    storage::{self, REPAIR_NO_QUORUM, REPAIR_NONE, REPAIR_OUTSIDE},
    wire,
};
use bytes::Bytes;
use rusqlite::{Params, params};
use std::collections::BTreeMap;

const MAX_SQLITE_BYTES: usize = 999_999_000;
fn command_bytes(items: &[Bytes], nodes: usize) -> usize {
    items.iter().fold(
        64usize.saturating_add(9usize.saturating_mul(nodes)),
        |total, item| total.saturating_add(item.len()).saturating_add(5),
    )
}

impl Engine {
    fn repair_error(&self, state: Option<i64>) -> Error {
        Error::new(
            Code::from_number(state.unwrap_or(self.repair_state)),
            "repair required",
        )
    }
    fn available(&self) -> Result<()> {
        if self.repair_state != REPAIR_NONE {
            return Err(self.repair_error(None));
        }
        if self.draining {
            return Err(Error::new(Code::Draining, "commands are draining"));
        }
        Ok(())
    }
    fn fatal(&mut self, ctx: &mut Context<'_>, error: Error) {
        if self.fatal_error.is_none() {
            self.fatal_error = Some(error);
            ctx.executor.clear();
        }
    }
    fn write_one(
        &self,
        ctx: &Context<'_>,
        sql: &str,
        args: impl Params,
        context: &str,
    ) -> Result<()> {
        let count = ctx
            .store
            .db()?
            .prepare_cached(sql)?
            .execute(args)
            .map_err(|e| Error::from(e).context(format!("DB1 {context} ")))?;
        if count != 1 {
            return Err(Error::new(
                Code::SqliteError,
                format!("DB1 {context} update count not one"),
            ));
        }
        Ok(())
    }
    fn advance_applied(&mut self, ctx: &mut Context<'_>, seq: i64, hash: Hash) -> Result<()> {
        self.write_one(
            ctx,
            "UPDATE monsterft_meta SET applied_seq=?, applied_entry_hash=? WHERE id=1",
            params![seq, hash.as_slice()],
            "advance applied",
        )?;
        self.applied_seq = seq;
        Ok(())
    }
    fn prune(&self, ctx: &mut Context<'_>) -> Result<()> {
        let Some(keep) = self.opts.keep else {
            return Ok(());
        };
        if ctx.store.log.begin < 0
            || self.pending_command.is_some()
            || i128::from(self.applied_seq) - i128::from(ctx.store.log.begin) + 1
                < i128::from(keep.trigger)
        {
            return Ok(());
        }
        ctx.store
            .db()?
            .prepare_cached("DELETE FROM raft_log WHERE seq < ?")?
            .execute([self.applied_seq - keep.target + 1])?;
        ctx.store.log.read_head()?;
        Ok(())
    }
    pub(crate) fn start(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.repair_state == REPAIR_OUTSIDE {
            return Err(self.repair_error(None));
        }
        self.prune(ctx)?;
        self.reset_election(ctx)
    }
    pub(crate) fn protocol_failure(
        &mut self,
        ctx: &mut Context<'_>,
        id: u64,
        error: Error,
    ) -> Result<()> {
        if self.leader_command.as_ref().is_none_or(|c| c.id != id) {
            return Ok(());
        }
        let mut command = self.leader_command.take().expect("identity checked");
        if command.request.as_ref().is_none_or(|r| r.replied) {
            self.warn(ctx, error.clone());
        }
        if let Some(request) = &mut command.request {
            self.reply(ctx, request, Some(error), -1, None);
        }
        let term = command.term;
        self.leader_command = Some(command);
        if self.state == State::Leader && self.term == term {
            self.follower(ctx, None, true)?;
        }
        Ok(())
    }
    fn fence_leader(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        self.write_one(
            ctx,
            "UPDATE monsterft_meta SET repair_state=? WHERE id=1",
            [REPAIR_OUTSIDE],
            "write repair",
        )?;
        self.repair_state = REPAIR_OUTSIDE;
        self.follower(ctx, None, true)?;
        self.fatal(ctx, self.repair_error(None));
        Ok(())
    }
    fn finish_drain(&mut self, ctx: &mut Context<'_>) -> bool {
        if !self.draining || self.pending_apply.is_some() || self.pending_command.is_some() {
            return false;
        }
        for client_id in std::mem::take(&mut self.drain_requests) {
            ctx.outputs.push(Output {
                to: "gate".into(),
                message: OutputMessage::Client(ClientReply::Ack {
                    client_id,
                    result: None,
                }),
            });
        }
        true
    }
    pub(crate) fn drain(&mut self, ctx: &mut Context<'_>, client_id: String) -> Result<()> {
        self.draining = true;
        self.drain_requests.push(client_id);
        while let Some(mut request) = self.commands.pop_front() {
            self.reply(
                ctx,
                &mut request,
                Some(Error::new(Code::Draining, "commands are draining")),
                -1,
                None,
            );
        }
        self.finish_drain(ctx);
        Ok(())
    }
    fn execute(
        &mut self,
        ctx: &mut Context<'_>,
        term: i64,
        seq: i64,
        hash: Hash,
        entry: Option<Entry>,
    ) -> Result<()> {
        let cid = self.random.cid();
        let (items, match_index) = match entry {
            Some(Entry::Cmd { items, match_index }) => (Some(items), Some(match_index)),
            None => (None, None),
            _ => return Err(Error::new(Code::MonsterIllegal, "Apply entry is not CMD")),
        };
        // Reports belong to this execution, including any received before Apply finishes.
        self.reports.clear();
        self.pending_apply = Some(PendingApply {
            cid: cid.clone(),
            seq,
            hash,
            tick_count: ctx.tick_count,
            count: items.as_ref().map_or(0, Vec::len),
        });
        ctx.outputs.push(Output {
            to: "apply".into(),
            message: OutputMessage::Apply(ApplyRequest::Execute {
                execute: Execute {
                    cid,
                    tick_count: ctx.tick_count,
                    term,
                    seq,
                    entry_hash: hash,
                    items,
                    match_index,
                },
            }),
        });
        Ok(())
    }
    fn sync_agrees(&self, entry: &Entry, digest: &Hash) -> bool {
        let Entry::Sync {
            quorum,
            agree,
            disagree,
            digest: agreed,
            ..
        } = entry
        else {
            return false;
        };
        *quorum
            && (agree.contains(&self.id)
                || (!disagree.contains(&self.id) && agreed.as_ref() == Some(digest)))
    }
    fn apply_sync(
        &mut self,
        ctx: &mut Context<'_>,
        entry: Entry,
        seq: i64,
        hash: Hash,
    ) -> Result<()> {
        entry.validate(&self.nodes, self.quorum, seq)?;
        let Entry::Sync {
            cmd_seq,
            quorum,
            ref agree,
            ref disagree,
            ..
        } = entry
        else {
            return Err(Error::new(Code::MonsterCorrupt, "decision is not SYNC"));
        };
        let command = self
            .pending_command
            .as_ref()
            .filter(|c| c.seq == cmd_seq)
            .ok_or_else(|| Error::new(Code::MonsterCorrupt, "SYNC entry is missing CMD"))?;
        let repair = if !quorum {
            REPAIR_NO_QUORUM
        } else if self.sync_agrees(&entry, &command.digest) {
            REPAIR_NONE
        } else {
            REPAIR_OUTSIDE
        };
        self.write_one(ctx,"UPDATE monsterft_meta SET pending_cmd_seq=NULL, applied_seq=?, applied_entry_hash=?, repair_state=? WHERE id=1 AND pending_cmd_seq=?",params![seq,hash.as_slice(),repair,cmd_seq],"write CMD decision")?;
        self.pending_command = None;
        self.applied_seq = seq;
        self.repair_state = repair;
        self.reports.clear();
        self.event(
            ctx,
            Event::Sync {
                status: self.status(ctx),
                cmd_seq,
                sync_seq: seq,
                quorum,
                agree: agree.clone(),
                disagree: disagree.clone(),
            },
        );
        if repair == REPAIR_NO_QUORUM {
            ctx.diagnostics
                .push(Diagnostic::Error(self.repair_error(None)));
        }
        if repair == REPAIR_OUTSIDE {
            self.fatal(ctx, self.repair_error(None));
        }
        Ok(())
    }
    fn remember_report(&mut self, ctx: &mut Context<'_>, from: String, digest: Hash) {
        self.reports.entry(from).or_insert(digest);
        if self
            .leader_command
            .as_ref()
            .is_some_and(|c| c.phase == Phase::Decision)
        {
            ctx.executor.enqueue(Task::Decide);
        }
    }
    fn announce_outcome(&mut self, ctx: &mut Context<'_>) {
        let Some(command) = self.pending_command.clone() else {
            return;
        };
        if self.state == State::Leader {
            self.remember_report(ctx, self.id.clone(), command.digest);
        } else if let Some(leader) = &self.leader {
            self.send(
                ctx,
                leader.clone(),
                Rpc::MonsterOutcome {
                    cmd_seq: command.seq,
                    digest: command.digest,
                },
            );
        }
    }
    fn append_command(
        &mut self,
        ctx: &mut Context<'_>,
        request: CommandRequest,
        sync: Option<Entry>,
    ) -> Result<()> {
        let mut entries = Vec::new();
        if let Some(sync) = sync {
            entries.push(wire::encode_payload(&sync)?);
        }
        let id = self.operation()?;
        let previous_seq = ctx
            .store
            .log
            .seq
            .checked_add(entries.len() as i64)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "CMD sequence overflow"))?;
        let match_index = self
            .nodes
            .iter()
            .map(|node| {
                if node == &self.id {
                    previous_seq
                } else {
                    self.replication.get(node).map_or(-1, |s| s.match_index)
                }
            })
            .collect();
        entries.push(wire::encode_payload(&Entry::Cmd {
            items: request.items.clone(),
            match_index,
        })?);
        self.leader_command = Some(LeaderCommand {
            id,
            term: self.term,
            request: Some(request),
            cmd_seq: -1,
            outcomes: None,
            phase: Phase::Cmd,
            sync_seq: None,
        });
        let seq = self.append_entries(ctx, entries, AppendKind::Command { id })?;
        self.leader_command
            .as_mut()
            .expect("command assigned synchronously")
            .cmd_seq = seq;
        Ok(())
    }
    pub(crate) fn decide(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        let Some(command) = self.leader_command.as_ref() else {
            return Ok(());
        };
        if command.phase != Phase::Decision
            || self.state != State::Leader
            || self.term != command.term
        {
            return Ok(());
        }
        let mut groups: BTreeMap<Hash, Vec<String>> = BTreeMap::new();
        for (node, digest) in &self.reports {
            groups.entry(*digest).or_default().push(node.clone());
        }
        let agreement = groups.iter().find(|(_, nodes)| nodes.len() >= self.quorum);
        let largest = groups.values().map(Vec::len).max().unwrap_or(0);
        let impossible =
            agreement.is_none() && largest + self.nodes.len() - self.reports.len() < self.quorum;
        if agreement.is_none() && !impossible {
            return Ok(());
        }
        if let Some(timer) = self.report_timer.take() {
            ctx.executor.cancel(timer);
        }
        let sync = if let Some((digest, nodes)) = agreement {
            Entry::Sync {
                cmd_seq: command.cmd_seq,
                quorum: true,
                digest: Some(*digest),
                agree: nodes.clone(),
                disagree: self
                    .reports
                    .iter()
                    .filter(|(_, d)| *d != digest)
                    .map(|(n, _)| n.clone())
                    .collect(),
            }
        } else {
            Entry::Sync {
                cmd_seq: command.cmd_seq,
                quorum: false,
                digest: None,
                agree: Vec::new(),
                disagree: self.reports.keys().cloned().collect(),
            }
        };
        let local = self
            .pending_command
            .as_ref()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "decision has no pending CMD"))?
            .digest;
        let agrees = self.sync_agrees(&sync, &local);
        let has_quorum = matches!(sync, Entry::Sync { quorum: true, .. });
        let mut command = self.leader_command.take().unwrap();
        if let Some(request) = &mut command.request {
            self.reply(
                ctx,
                request,
                if agrees {
                    None
                } else {
                    Some(self.repair_error(Some(if has_quorum {
                        REPAIR_OUTSIDE
                    } else {
                        REPAIR_NO_QUORUM
                    })))
                },
                command.cmd_seq,
                command.outcomes.clone(),
            );
        }
        self.leader_command = Some(command);
        if has_quorum && !agrees {
            return self.fence_leader(ctx);
        }
        if agrees
            && !self.draining
            && let Some(request) = self.commands.pop_front()
        {
            return self.append_command(ctx, request, Some(sync));
        }
        let command = self.leader_command.as_mut().unwrap();
        command.phase = Phase::Sync;
        command.sync_seq = None;
        let id = command.id;
        let seq = self.append_entries(
            ctx,
            vec![wire::encode_payload(&sync)?],
            AppendKind::Command { id },
        )?;
        self.leader_command.as_mut().unwrap().sync_seq = Some(seq);
        Ok(())
    }
    pub(crate) fn ask_outcomes(&self, ctx: &mut Context<'_>) {
        if self.state != State::Leader {
            return;
        }
        let Some(command) = self
            .leader_command
            .as_ref()
            .filter(|c| c.phase == Phase::Decision)
        else {
            return;
        };
        for to in &self.nodes {
            if to != &self.id && !self.reports.contains_key(to) {
                self.send(
                    ctx,
                    to.clone(),
                    Rpc::MonsterOutcomeRequest {
                        cmd_seq: command.cmd_seq,
                    },
                );
            }
        }
    }
    fn await_decision(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        let digest = self
            .pending_command
            .as_ref()
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "decision has no pending CMD"))?
            .digest;
        self.remember_report(ctx, self.id.clone(), digest);
        self.decide(ctx)?;
        let Some(command) = self
            .leader_command
            .as_ref()
            .filter(|c| c.phase == Phase::Decision)
        else {
            return Ok(());
        };
        // An inherited CMD has no caller; its followers may have already announced.
        if command.request.is_none() {
            self.ask_outcomes(ctx);
        }
        if let Some(timer) = self.report_timer.take() {
            ctx.executor.cancel(timer);
        }
        self.report_timer = Some(ctx.executor.after(
            self.retryms(),
            Task::AskOutcomes,
            self.retryms(),
        )?);
        Ok(())
    }
    pub(crate) fn leader_changed(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.state != State::Leader
            || self
                .leader_command
                .as_ref()
                .is_some_and(|c| c.term != self.term)
        {
            if let Some(timer) = self.report_timer.take() {
                ctx.executor.cancel(timer);
            }
            let error = Error::new(Code::NotLeader, "node not leader");
            if let Some(mut command) = self.leader_command.take()
                && let Some(request) = &mut command.request
            {
                self.reply(ctx, request, Some(error.clone()), -1, None);
            }
            while let Some(mut request) = self.commands.pop_front() {
                self.reply(ctx, &mut request, Some(error.clone()), -1, None);
            }
            return Ok(());
        }
        ctx.executor.enqueue(Task::PumpLeader);
        Ok(())
    }
    pub(crate) fn pump_leader(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.stopped || self.fatal_error.is_some() || self.state != State::Leader {
            return Ok(());
        }
        if let Some(command) = self.leader_command.as_mut() {
            if command.term != self.term {
                return Ok(());
            }
            if command.phase == Phase::Cmd
                && self
                    .pending_command
                    .as_ref()
                    .is_some_and(|p| p.seq == command.cmd_seq)
            {
                command.phase = Phase::Decision;
                self.await_decision(ctx)?;
            } else if command.phase == Phase::Sync
                && command.sync_seq.is_some_and(|seq| self.applied_seq >= seq)
            {
                self.leader_command = None;
                self.pump_leader(ctx)?;
            }
            return Ok(());
        }
        if self.commit_term != self.term
            || self.applied_seq < self.commit_seq
            || self.pending_apply.is_some()
        {
            return Ok(());
        }
        if self.repair_state != REPAIR_NONE {
            while let Some(mut request) = self.commands.pop_front() {
                self.reply(ctx, &mut request, Some(self.repair_error(None)), -1, None);
            }
            return Ok(());
        }
        if let Some(pending) = &self.pending_command {
            let seq = pending.seq;
            let id = self.operation()?;
            self.leader_command = Some(LeaderCommand {
                id,
                term: self.term,
                request: None,
                cmd_seq: seq,
                outcomes: None,
                phase: Phase::Decision,
                sync_seq: None,
            });
            self.await_decision(ctx)?;
            return Ok(());
        }
        if self.finish_drain(ctx) {
            return Ok(());
        }
        if let Some(request) = self.commands.pop_front() {
            self.append_command(ctx, request, None)?;
        }
        Ok(())
    }
    pub(crate) fn apply_ready(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.stopped || self.fatal_error.is_some() || self.pending_apply.is_some() {
            return Ok(());
        }
        while self.applied_seq < self.commit_seq {
            let seq = self
                .applied_seq
                .checked_add(1)
                .ok_or_else(|| Error::new(Code::MonsterIllegal, "applied sequence overflow"))?;
            let record = self.entry(ctx, seq)?.ok_or_else(|| {
                Error::new(Code::MonsterCorrupt, format!("apply entry {seq} not found"))
            })?;
            let hash = storage::digest_entry(record.term, &record.entry);
            if record.entry.is_empty() {
                if seq == 0 {
                    return self.execute(ctx, record.term, seq, hash, None);
                }
                self.advance_applied(ctx, seq, hash)?;
            } else {
                let entry: Entry = wire::decode_payload(&record.entry)
                    .map_err(|e| e.with_code(Code::MonsterCorrupt))?;
                if matches!(entry, Entry::Cmd { .. }) {
                    if self.draining {
                        self.finish_drain(ctx);
                        return Ok(());
                    }
                    if self.repair_state != REPAIR_NONE {
                        self.fatal(ctx, self.repair_error(None));
                        return Ok(());
                    }
                    entry.validate(&self.nodes, self.quorum, seq)?;
                    if self.pending_command.is_some() {
                        return Err(Error::new(
                            Code::MonsterIllegal,
                            "CMD entry while another CMD is pending",
                        ));
                    }
                    return self.execute(ctx, record.term, seq, hash, Some(entry));
                }
                self.apply_sync(ctx, entry, seq, hash)?;
                if self.fatal_error.is_some() {
                    return Ok(());
                }
            }
            self.prune(ctx)?;
            if self.finish_drain(ctx) {
                return Ok(());
            }
        }
        self.pump_leader(ctx)?;
        self.finish_drain(ctx);
        Ok(())
    }
    pub(crate) fn receive_apply(
        &mut self,
        ctx: &mut Context<'_>,
        reply: ApplyCompletion,
    ) -> Result<()> {
        let pending = self
            .pending_apply
            .as_ref()
            .filter(|p| p.cid == reply.cid && p.tick_count == reply.tick_count)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "Apply reply has no matching request"))?
            .clone();
        reply.receipt.validate()?;
        if reply.receipt.seq != pending.seq
            || reply.receipt.entry_hash != Some(pending.hash)
            || reply.outcomes.len() != pending.count
            || (reply.receipt.seq == 0 && reply.receipt.local_digest != Some([0; 16]))
        {
            return Err(Error::new(
                Code::MonsterCorrupt,
                "Apply reply does not match execution",
            ));
        }
        storage::copy_apply_into(ctx.store.db()?, &reply.receipt)?;
        self.applied_seq = reply.receipt.seq;
        self.pending_apply = None;
        if reply.receipt.seq > 0 {
            let digest = reply.receipt.local_digest.expect("receipt validated");
            self.pending_command = Some(PendingCommand {
                seq: reply.receipt.seq,
                digest,
            });
            if let Some(command) = self
                .leader_command
                .as_mut()
                .filter(|c| c.cmd_seq == reply.receipt.seq)
            {
                command.outcomes = Some(reply.outcomes);
            }
            self.event(
                ctx,
                Event::Cmd {
                    status: self.status(ctx),
                    cmd_seq: reply.receipt.seq,
                    cmd_count: pending.count,
                },
            );
            self.announce_outcome(ctx);
        }
        self.prune(ctx)?;
        self.apply_ready(ctx)
    }
    pub(crate) fn submit(
        &mut self,
        ctx: &mut Context<'_>,
        client_id: String,
        items: Vec<Bytes>,
    ) -> Result<()> {
        let mut request = CommandRequest {
            to: "gate".into(),
            call_id: client_id,
            items,
            replied: false,
        };
        let validation = self.available().and_then(|()| {
            if command_bytes(&request.items, self.nodes.len()) > MAX_SQLITE_BYTES {
                Err(Error::new(
                    Code::ArgumentIllegal,
                    "command items are illegal",
                ))
            } else if self.state != State::Leader && self.leader.is_none() {
                Err(Error::new(Code::NoLeader, "forward no leader"))
            } else {
                Ok(())
            }
        });
        if let Err(error) = validation {
            self.reply(ctx, &mut request, Some(error), -1, None);
            return Ok(());
        }
        if self.state == State::Leader {
            self.commands.push_back(request);
            self.pump_leader(ctx)?;
        } else {
            let to = self.leader.clone().expect("leader checked");
            let cid = self.random.cid();
            let message = Rpc::FwdCmd {
                cid,
                term: self.term,
                items: request.items.clone(),
            };
            self.request(
                ctx,
                to,
                message,
                self.opts.append_timeout,
                RequestKind::Forward { request },
                Error::new(Code::AppendTimeout, "fwd_cmd timeout"),
            )?;
        }
        Ok(())
    }
    pub(crate) fn receive_monster(
        &mut self,
        ctx: &mut Context<'_>,
        from: String,
        message: Rpc,
    ) -> Result<()> {
        match message {
            Rpc::FwdCmd { cid, term, items } => {
                let mut request = CommandRequest {
                    to: from,
                    call_id: cid,
                    items,
                    replied: false,
                };
                let validation =
                    if command_bytes(&request.items, self.nodes.len()) > MAX_SQLITE_BYTES {
                        Err(Error::new(Code::RpcIllegal, "forward CMD illegal"))
                    } else if self.state != State::Leader {
                        Err(Error::new(Code::NotLeader, "node not leader"))
                    } else if term != self.term {
                        Err(Error::new(Code::TermDiff, "node term different"))
                    } else {
                        self.available()
                    };
                if let Err(error) = validation {
                    self.reply(ctx, &mut request, Some(error.clone()), -1, None);
                    self.warn(ctx, error);
                    return Ok(());
                }
                self.commands.push_back(request);
                self.pump_leader(ctx)?;
            }
            Rpc::MonsterOutcomeRequest { cmd_seq } => {
                if let Some(command) = self.pending_command.as_ref().filter(|c| c.seq == cmd_seq) {
                    self.send(
                        ctx,
                        from,
                        Rpc::MonsterOutcome {
                            cmd_seq,
                            digest: command.digest,
                        },
                    );
                }
            }
            Rpc::MonsterOutcome { cmd_seq, digest } => {
                if self.state == State::Leader
                    && (self
                        .pending_command
                        .as_ref()
                        .is_some_and(|c| c.seq == cmd_seq)
                        || self
                            .pending_apply
                            .as_ref()
                            .is_some_and(|c| c.seq == cmd_seq))
                {
                    self.remember_report(ctx, from, digest);
                }
            }
            _ => {
                return Err(Error::new(
                    Code::MonsterIllegal,
                    "unhandled Raft message reached Monster",
                ));
            }
        }
        Ok(())
    }
}
