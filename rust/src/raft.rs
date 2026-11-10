//! Raft transitions and request continuations, preserving the JS operation order.
use crate::{
    Value,
    engine::*,
    error::{Code, Error, Result},
    log::LogEntry,
    protocol::{Event, State},
    rpc::{self, Rpc},
};
use std::collections::BTreeSet;

impl Engine {
    pub(crate) fn reset_election(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if let Some(timer) = self.election_timer.take() {
            ctx.executor.cancel(timer);
        }
        if self.state == State::Leader {
            return Ok(());
        }
        let jitter = self
            .random
            .int(0, (self.opts.election_timeout / 5).saturating_sub(1))?;
        let delay = self
            .opts
            .election_timeout
            .checked_add(jitter)
            .ok_or_else(|| Error::new(Code::RaftIllegal, "election deadline overflow"))?;
        self.election_timer = Some(ctx.executor.after(delay, Task::Election, 0)?);
        Ok(())
    }
    fn cancel_leadership(&mut self, ctx: &mut Context<'_>, error: Error) {
        if let Some(timer) = self.heartbeat_timer.take() {
            ctx.executor.cancel(timer);
        }
        for (_, state) in std::mem::take(&mut self.replication) {
            if let Some(timer) = state.retry_timer {
                ctx.executor.cancel(timer);
            }
            for cid in state.cid.into_iter().chain(state.heartbeat_cids) {
                if let Some(waiting) = self.requests.remove(&cid) {
                    ctx.executor.cancel(waiting.timer);
                }
            }
        }
        let ids: Vec<_> = self.append_waiters.keys().copied().collect();
        for id in ids {
            self.finish_append(ctx, id, Some(error.clone()));
        }
    }
    pub(crate) fn follower(
        &mut self,
        ctx: &mut Context<'_>,
        leader: Option<String>,
        reset: bool,
    ) -> Result<()> {
        self.state = State::Follower;
        self.leader = leader;
        self.cancel_leadership(ctx, Error::new(Code::NotLeader, "node not leader"));
        self.pongs.clear();
        self.votes.clear();
        if reset {
            self.reset_election(ctx)?;
        }
        self.leader_changed(ctx)
    }
    fn advance_term(
        &mut self,
        ctx: &mut Context<'_>,
        term: i64,
        leader: Option<String>,
        reset: bool,
    ) -> Result<bool> {
        if term <= self.term {
            return Ok(false);
        }
        self.term = term;
        self.voted_for = None;
        ctx.store.log.election(term, None)?;
        self.follower(ctx, leader, reset)?;
        Ok(true)
    }
    pub(crate) fn vote_self(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        let term = self
            .term
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::RaftIllegal, "election term exhausted"))?;
        self.cancel_leadership(ctx, Error::new(Code::NotLeader, "node not leader"));
        self.term = term;
        self.state = State::Candidate;
        self.leader = None;
        self.voted_for = Some(self.id.clone());
        self.votes = BTreeSet::from([self.id.clone()]);
        self.pongs.clear();
        ctx.store.log.election(term, Some(self.id.clone()))?;
        self.leader_changed(ctx)?;
        for to in &self.nodes {
            if to != &self.id {
                self.send(
                    ctx,
                    to.clone(),
                    Rpc::VoteRequest {
                        term,
                        term_p: ctx.store.log.term,
                        seq_p: ctx.store.log.seq,
                    },
                );
            }
        }
        Ok(())
    }
    fn vote_request(
        &mut self,
        ctx: &mut Context<'_>,
        from: String,
        term: i64,
        term_p: i64,
        seq_p: i64,
    ) -> Result<()> {
        let mut granted = false;
        if term >= self.term {
            let was_leader = self.state == State::Leader;
            self.advance_term(ctx, term, None, was_leader)?;
            granted = (self.voted_for.is_none() || self.voted_for.as_ref() == Some(&from))
                && term >= term_p
                && (term_p > ctx.store.log.term
                    || (term_p == ctx.store.log.term && seq_p >= ctx.store.log.seq));
            if granted {
                if self.state != State::Follower {
                    self.follower(ctx, None, true)?;
                }
                self.voted_for = Some(from.clone());
                ctx.store.log.election(self.term, Some(from.clone()))?;
                self.reset_election(ctx)?;
            }
        }
        self.send(
            ctx,
            from,
            Rpc::Vote {
                term: self.term,
                vote_granted: granted,
            },
        );
        Ok(())
    }
    fn vote_reply(
        &mut self,
        ctx: &mut Context<'_>,
        from: String,
        term: i64,
        granted: bool,
    ) -> Result<()> {
        if self.advance_term(ctx, term, None, true)? {
            return Ok(());
        }
        if self.state != State::Candidate || term != self.term || !granted {
            return Ok(());
        }
        self.votes.insert(from.clone());
        self.pongs.insert(from, ctx.executor.timems());
        if self.votes.len() < self.quorum {
            return Ok(());
        }
        self.state = State::Leader;
        self.leader = Some(self.id.clone());
        if let Some(timer) = self.election_timer.take() {
            ctx.executor.cancel(timer);
        }
        let next_index = ctx
            .store
            .log
            .seq
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::RaftIllegal, "replication sequence exhausted"))?;
        for to in self.nodes.clone() {
            if to != self.id {
                let id = self.operation()?;
                self.replication.insert(
                    to,
                    Replication {
                        id,
                        term: self.term,
                        next_index,
                        match_index: -1,
                        cid: None,
                        retry_timer: None,
                        cancelled: false,
                        heartbeat_cids: BTreeSet::new(),
                    },
                );
            }
        }
        let interval = (self.opts.election_timeout / 10) * 3
            + ((self.opts.election_timeout % 10) * 3).div_ceil(10);
        self.heartbeat_timer = Some(ctx.executor.after(interval, Task::Heartbeat, interval)?);
        self.append_noop(ctx)?;
        self.leader_changed(ctx)
    }
    pub(crate) fn append_entries(
        &mut self,
        ctx: &mut Context<'_>,
        entries: Vec<Vec<u8>>,
        kind: AppendKind,
    ) -> Result<i64> {
        if self.state != State::Leader {
            return Err(Error::new(Code::NotLeader, "node not leader"));
        }
        let entries: Vec<_> = entries
            .into_iter()
            .map(|entry| LogEntry {
                term: self.term,
                entry,
            })
            .collect();
        ctx.store.log.append_batch(&entries, None)?;
        let seq = ctx.store.log.seq;
        let id = self.operation()?;
        let timer = ctx
            .executor
            .after(self.opts.append_timeout, Task::AppendTimeout { id }, 0)?;
        self.append_waiters
            .insert(id, AppendWaiter { seq, timer, kind });
        let states: Vec<_> = self
            .replication
            .iter()
            .map(|(to, s)| (to.clone(), s.id))
            .collect();
        for (to, id) in states {
            self.replicate(ctx, &to, id)?;
        }
        Ok(seq)
    }
    pub(crate) fn finish_append(&mut self, ctx: &mut Context<'_>, id: u64, error: Option<Error>) {
        if let Some(waiter) = self.append_waiters.remove(&id) {
            ctx.executor.cancel(waiter.timer);
            ctx.executor.enqueue(Task::AppendComplete {
                kind: waiter.kind,
                error,
            });
        }
    }
    pub(crate) fn append_noop(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.state == State::Leader && self.commit_term != self.term {
            self.append_entries(ctx, vec![Vec::new()], AppendKind::Noop { term: self.term })?;
        }
        Ok(())
    }
    pub(crate) fn append_complete(
        &mut self,
        ctx: &mut Context<'_>,
        kind: AppendKind,
        error: Option<Error>,
    ) -> Result<()> {
        match kind {
            AppendKind::Noop { term } => {
                if let Some(error) = error
                    && self.state == State::Leader
                    && self.term == term
                    && self.commit_term != term
                {
                    self.warn(ctx, error);
                    ctx.executor
                        .after(self.retryms(), Task::RetryNoop { term }, 0)?;
                }
            }
            AppendKind::Command { id } => {
                if let Some(error) = error {
                    self.protocol_failure(ctx, id, error)?;
                } else {
                    self.pump_leader(ctx)?;
                }
            }
        }
        Ok(())
    }
    fn check_commit(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.state != State::Leader {
            return Ok(());
        }
        let mut matches = vec![ctx.store.log.seq];
        matches.extend(
            self.followers(ctx.executor.timems())
                .iter()
                .map(|id| self.replication.get(id).map_or(-1, |s| s.match_index)),
        );
        matches.sort_unstable_by(|a, b| b.cmp(a));
        let seq = matches.get(self.quorum - 1).copied().unwrap_or(-1);
        if seq <= self.commit_seq {
            return Ok(());
        }
        let record = self.entry(ctx, seq)?.ok_or_else(|| {
            Error::new(Code::RaftIllegal, format!("check commit {seq} not found"))
        })?;
        if record.term != self.term {
            return Ok(());
        }
        self.commit_seq = seq;
        self.commit_term = record.term;
        self.event(
            ctx,
            Event::Commit {
                status: self.status(ctx),
                seq,
            },
        );
        ctx.executor.enqueue(Task::ApplyReady);
        let ids: Vec<_> = self
            .append_waiters
            .iter()
            .filter(|(_, w)| w.seq <= seq)
            .map(|(id, _)| *id)
            .collect();
        for id in ids {
            self.finish_append(ctx, id, None);
        }
        let states: Vec<_> = self
            .replication
            .iter()
            .map(|(to, s)| (to.clone(), s.id))
            .collect();
        for (to, id) in states {
            self.heartbeat_follower(ctx, &to, id)?;
        }
        Ok(())
    }
    pub(crate) fn request(
        &mut self,
        ctx: &mut Context<'_>,
        to: String,
        message: Rpc,
        timeout: u64,
        kind: RequestKind,
        timeout_error: Error,
    ) -> Result<String> {
        let cid = message
            .cid()
            .ok_or_else(|| Error::new(Code::RaftIllegal, "request has no CID"))?
            .to_owned();
        if self.requests.contains_key(&cid) {
            return Err(Error::new(
                Code::RaftIllegal,
                "request CID is already pending",
            ));
        }
        let timer = ctx
            .executor
            .after(timeout, Task::RequestTimeout { cid: cid.clone() }, 0)?;
        self.requests.insert(
            cid.clone(),
            PendingRequest {
                to: to.clone(),
                timer,
                kind,
                timeout: timeout_error,
            },
        );
        self.send(ctx, to, message);
        Ok(cid)
    }
    fn heartbeat_follower(&mut self, ctx: &mut Context<'_>, to: &str, id: u64) -> Result<()> {
        if self.state != State::Leader
            || !self
                .replication
                .get(to)
                .is_some_and(|s| s.id == id && s.term == self.term)
        {
            return Ok(());
        }
        let cid = self.random.cid();
        let message = Rpc::Append {
            cid: cid.clone(),
            term: self.term,
            seq_p: ctx.store.log.seq,
            term_p: ctx.store.log.term,
            commit_seq: self.commit_seq,
            data: None,
        };
        let kind = RequestKind::Heartbeat {
            to: to.into(),
            id,
            term: self.term,
            cid: cid.clone(),
        };
        self.request(
            ctx,
            to.into(),
            message,
            self.opts.election_timeout,
            kind,
            Error::new(Code::HeartbeatTimeout, "heartbeat timeout"),
        )?;
        self.replication
            .get_mut(to)
            .expect("state checked")
            .heartbeat_cids
            .insert(cid);
        Ok(())
    }
    pub(crate) fn heartbeat(&mut self, ctx: &mut Context<'_>) -> Result<()> {
        if self.state != State::Leader {
            return Ok(());
        }
        if self.followers(ctx.executor.timems()).len() + 1 < self.quorum {
            return self.follower(ctx, None, true);
        }
        let states: Vec<_> = self
            .replication
            .iter()
            .map(|(to, s)| (to.clone(), s.id))
            .collect();
        for (to, id) in states {
            self.heartbeat_follower(ctx, &to, id)?;
        }
        Ok(())
    }
    pub(crate) fn replicate(&mut self, ctx: &mut Context<'_>, to: &str, id: u64) -> Result<()> {
        let Some(state) = self.replication.get(to) else {
            return Ok(());
        };
        if self.state != State::Leader
            || state.id != id
            || state.term != self.term
            || state.cancelled
            || state.cid.is_some()
            || state.retry_timer.is_some()
        {
            return Ok(());
        }
        let begin = state.next_index;
        if begin > ctx.store.log.seq {
            return Ok(());
        }
        let seq_p = begin - 1;
        let required = seq_p.max(0);
        if required < ctx.store.log.begin {
            self.replication
                .get_mut(to)
                .expect("state checked")
                .cancelled = true;
            self.warn(
                ctx,
                Error::new(
                    Code::ReplForgot,
                    format!(
                        "replication wants forgotten {required} have {}",
                        ctx.store.log.begin
                    ),
                ),
            );
            return Ok(());
        }
        let previous = self.entry(ctx, seq_p)?;
        if seq_p >= 0 && previous.is_none() {
            return Err(Error::new(
                Code::RaftIllegal,
                format!("replication predecessor {seq_p} not found"),
            ));
        }
        let mut bytes = 0usize;
        let mut rows = 0usize;
        for size in ctx.store.log.sizes(begin, self.opts.rpc_max)? {
            if rows > 0 && bytes.saturating_add(size) > self.opts.rpc_max_bytes {
                break;
            }
            bytes = bytes
                .checked_add(size)
                .ok_or_else(|| Error::new(Code::RaftIllegal, "replication page size overflow"))?;
            rows += 1;
        }
        if rows == 0 {
            return Err(Error::new(Code::RaftIllegal, "replication page is missing"));
        }
        let data: Vec<_> = ctx
            .store
            .log
            .range(begin, ctx.store.log.seq, rows)?
            .into_iter()
            .map(|r| LogEntry {
                term: r.term,
                entry: r.entry,
            })
            .collect();
        if data.is_empty() {
            return Err(Error::new(Code::RaftIllegal, "replication page is missing"));
        }
        let matched = seq_p
            .checked_add(data.len() as i64)
            .ok_or_else(|| Error::new(Code::RaftIllegal, "replication sequence overflow"))?;
        let cid = self.random.cid();
        let message = Rpc::Append {
            cid: cid.clone(),
            term: self.term,
            seq_p,
            term_p: previous.map_or(-1, |r| r.term),
            commit_seq: self.commit_seq,
            data: Some(data),
        };
        let kind = RequestKind::Replication {
            to: to.into(),
            id,
            term: self.term,
            begin,
            seq_p,
            matched,
        };
        self.request(
            ctx,
            to.into(),
            message,
            self.opts.append_timeout,
            kind,
            Error::new(Code::AppendTimeout, "append timeout"),
        )?;
        self.replication.get_mut(to).expect("state checked").cid = Some(cid);
        Ok(())
    }
    pub(crate) fn request_complete(
        &mut self,
        ctx: &mut Context<'_>,
        request: PendingRequest,
        mut error: Option<Error>,
        reply: Option<Rpc>,
    ) -> Result<()> {
        match request.kind {
            RequestKind::Heartbeat { to, id, term, cid } => {
                let Some(state) = self.replication.get_mut(&to).filter(|s| s.id == id) else {
                    return Ok(());
                };
                state.heartbeat_cids.remove(&cid);
                if self.state != State::Leader || self.term != term {
                    return Ok(());
                }
                if reply.as_ref().is_some_and(|r| r.term() != Some(term)) {
                    error = Some(Error::new(Code::TermDiff, "node term different"));
                }
                if let Some(error) = error {
                    self.warn(ctx, error);
                    return Ok(());
                }
                self.pongs.insert(to.clone(), ctx.executor.timems());
                if state.match_index < ctx.store.log.seq {
                    state.next_index = state.next_index.min(ctx.store.log.seq);
                }
                self.check_commit(ctx)?;
                ctx.executor.enqueue(Task::Replicate { to, id });
            }
            RequestKind::Replication {
                to,
                id,
                term,
                begin,
                seq_p,
                matched,
            } => {
                let Some(state) = self.replication.get_mut(&to).filter(|s| s.id == id) else {
                    return Ok(());
                };
                state.cid = None;
                if self.state != State::Leader || self.term != term || state.cancelled {
                    return Ok(());
                }
                if reply.as_ref().is_some_and(|r| r.term() != Some(term)) {
                    error = Some(Error::new(Code::TermDiff, "node term different"));
                }
                if let Some(error) = error {
                    let code = error.code;
                    self.warn(ctx, error);
                    if code == Some(Code::ReplForgot) {
                        self.replication.get_mut(&to).unwrap().cancelled = true;
                        return Ok(());
                    }
                    if code == Some(Code::ReplBacktrack) {
                        if seq_p < 0 {
                            self.replication.get_mut(&to).unwrap().cancelled = true;
                            self.warn(ctx, Error::new(Code::RaftIllegal, "genesis rejected"));
                            return Ok(());
                        }
                        self.pongs.insert(to.clone(), ctx.executor.timems());
                        let mut next = begin - 1;
                        if let Some(Rpc::Err {
                            term: reply_term,
                            log_seq: Some(log_seq),
                            ..
                        }) = reply
                            && reply_term == term
                            && log_seq >= -1
                            && let Some(hint) = log_seq.checked_add(1)
                        {
                            next = next.min(hint);
                        }
                        let state = self.replication.get_mut(&to).unwrap();
                        state.next_index = state.next_index.min(next);
                        ctx.executor.enqueue(Task::Replicate { to, id });
                    } else {
                        let timer = ctx.executor.after(
                            self.retryms(),
                            Task::RetryReplicate { to: to.clone(), id },
                            0,
                        )?;
                        self.replication.get_mut(&to).unwrap().retry_timer = Some(timer);
                    }
                    return Ok(());
                }
                self.pongs.insert(to.clone(), ctx.executor.timems());
                let next = matched.checked_add(1).ok_or_else(|| {
                    Error::new(Code::RaftIllegal, "replication sequence overflow")
                })?;
                let state = self.replication.get_mut(&to).unwrap();
                state.match_index = state.match_index.max(matched);
                state.next_index = state.next_index.max(next);
                self.check_commit(ctx)?;
                ctx.executor.enqueue(Task::Replicate { to, id });
            }
            RequestKind::Forward { mut request } => {
                if let Some(mut error) = error {
                    if reply
                        .as_ref()
                        .and_then(Rpc::error)
                        .and_then(|e| e.stack.as_ref())
                        .is_none()
                    {
                        error.stack = None;
                    }
                    if !request.replied {
                        request.replied = true;
                        ctx.outputs.push(crate::rpc::Output {
                            to: "gate".into(),
                            message: crate::rpc::OutputMessage::Client(
                                crate::rpc::ClientReply::Err {
                                    client_id: request.call_id,
                                    error,
                                },
                            ),
                        });
                    }
                } else if let Some(Rpc::Ack {
                    cmd_seq: Some(seq),
                    results: Some(outcomes),
                    ..
                }) = reply
                {
                    self.reply(ctx, &mut request, None, seq, Some(outcomes));
                } else {
                    return Err(Error::new(
                        Code::MonsterIllegal,
                        "forward completion is not command result",
                    ));
                }
            }
        }
        Ok(())
    }
    fn validate_ack(&self, request: &PendingRequest, message: &Rpc) -> Result<bool> {
        if matches!(message, Rpc::Err { .. }) {
            return Ok(true);
        }
        match (&request.kind, message) {
            (
                RequestKind::Heartbeat { term, .. } | RequestKind::Replication { term, .. },
                Rpc::Ack {
                    term: reply_term,
                    seq,
                    ..
                },
            ) => {
                if reply_term < term {
                    return Ok(false);
                }
                if seq.is_none_or(|seq| seq < 0) {
                    return Err(Error::new(Code::RpcIllegal, "append ACK seq is illegal"));
                }
                Ok(true)
            }
            (
                RequestKind::Forward { request },
                Rpc::Ack {
                    cmd_seq, results, ..
                },
            ) => {
                if cmd_seq.is_none_or(|seq| seq < 0)
                    || results
                        .as_ref()
                        .is_none_or(|r| r.len() != request.items.len())
                {
                    return Err(Error::new(
                        Code::MonsterIllegal,
                        "command result fields are illegal",
                    ));
                }
                Ok(true)
            }
            _ => Err(Error::new(Code::RpcIllegal, "ACK shape is illegal")),
        }
    }
    #[allow(clippy::too_many_arguments)] // The already-validated append fields.
    fn append_request(
        &mut self,
        ctx: &mut Context<'_>,
        from: String,
        cid: String,
        term: i64,
        term_p: i64,
        seq_p: i64,
        commit_seq: i64,
        data: Option<Vec<LogEntry>>,
    ) -> Result<()> {
        if term < self.term {
            self.rpc_error(
                ctx,
                from,
                cid,
                Error::new(Code::TermDiff, "term is lesser"),
                None,
            );
            return Ok(());
        }
        self.advance_term(ctx, term, Some(from.clone()), true)?;
        if self.state != State::Follower || self.leader.as_ref() != Some(&from) {
            self.follower(ctx, Some(from.clone()), true)?;
        }
        self.reset_election(ctx)?;
        let mut matched = ctx.store.log.seq == seq_p && ctx.store.log.term == term_p;
        let count = data.as_ref().map_or(0, Vec::len) as i64;
        if let Some(data) = data {
            if !matched {
                if ctx.store.log.begin > 0 && seq_p.max(0) < ctx.store.log.begin {
                    let error = Error::new(
                        Code::ReplForgot,
                        format!(
                            "rx append wants forgotten {seq_p} have {}",
                            ctx.store.log.begin
                        ),
                    );
                    self.warn(ctx, error.clone());
                    self.rpc_error(ctx, from, cid, error, None);
                    return Ok(());
                }
                let previous = self.entry(ctx, seq_p)?;
                if (seq_p >= 0 && previous.is_none())
                    || previous.as_ref().map_or(-1, |p| p.term) != term_p
                {
                    self.rpc_error(
                        ctx,
                        from,
                        cid,
                        Error::new(
                            Code::ReplBacktrack,
                            format!("predecessor mismatch at {seq_p}"),
                        ),
                        Some(ctx.store.log.seq),
                    );
                    return Ok(());
                }
                let mut same = 0;
                for (index, want) in data.iter().enumerate() {
                    let have = self.entry(ctx, seq_p + 1 + index as i64)?;
                    if !have.is_some_and(|have| have.term == want.term && have.entry == want.entry)
                    {
                        break;
                    }
                    same += 1;
                }
                if same < data.len() {
                    let trim = seq_p + same as i64;
                    if trim < self.commit_seq {
                        return Err(Error::new(
                            Code::RaftIllegal,
                            format!(
                                "rx append cannot trim committed {} to {trim}",
                                self.commit_seq
                            ),
                        ));
                    }
                    ctx.store.log.trim(trim)?;
                    ctx.store.log.append_batch(&data[same..], Some(trim + 1))?;
                }
            } else {
                ctx.store.log.append_batch(&data, Some(seq_p + 1))?;
            }
            matched = true;
        }
        if matched {
            let next = commit_seq.min(seq_p + count);
            if next > self.commit_seq {
                let record = self.entry(ctx, next)?.ok_or_else(|| {
                    Error::new(
                        Code::RaftIllegal,
                        format!("rx append commit {next} not found"),
                    )
                })?;
                self.commit_seq = next;
                self.commit_term = record.term;
                self.event(
                    ctx,
                    Event::Commit {
                        status: self.status(ctx),
                        seq: next,
                    },
                );
                ctx.executor.enqueue(Task::ApplyReady);
            }
        }
        let seq = seq_p
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::RaftIllegal, "append ACK sequence overflow"))?;
        self.send(
            ctx,
            from,
            Rpc::Ack {
                cid,
                term: self.term,
                seq: Some(seq),
                cmd_seq: None,
                results: None,
            },
        );
        Ok(())
    }
    pub(crate) fn receive(
        &mut self,
        ctx: &mut Context<'_>,
        from: String,
        value: Value,
    ) -> Result<()> {
        if self.stopped || from == self.id || !self.nodes.contains(&from) {
            return Ok(());
        }
        let kind = rpc::field(&value, "type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let cid = rpc::field(&value, "cid")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_owned);
        let message = match Rpc::parse(value) {
            Ok(message) => message,
            Err(error) => {
                match kind.as_str() {
                    "vote_request" => {
                        self.warn(ctx, error);
                        self.send(
                            ctx,
                            from,
                            Rpc::Vote {
                                term: self.term,
                                vote_granted: false,
                            },
                        );
                    }
                    "append" | "fwd_cmd" => {
                        if let Some(cid) = cid {
                            self.rpc_error(ctx, from, cid, error.clone(), None);
                            if kind == "fwd_cmd" {
                                self.warn(ctx, error);
                            }
                        } else {
                            self.warn(ctx, error);
                        }
                    }
                    _ => self.warn(ctx, error),
                }
                return Ok(());
            }
        };
        match message {
            message @ (Rpc::Ack { .. } | Rpc::Err { .. }) => {
                let cid = message.cid().unwrap().to_owned();
                let Some(waiting) = self.requests.get(&cid).filter(|w| w.to == from) else {
                    return Ok(());
                };
                match self.validate_ack(waiting, &message) {
                    Ok(true) => {}
                    Ok(false) => return Ok(()),
                    Err(error) => {
                        self.warn(ctx, error);
                        return Ok(());
                    }
                }
                self.advance_term(ctx, message.term().unwrap(), None, true)?;
                if let Some(request) = self.requests.remove(&cid) {
                    ctx.executor.cancel(request.timer);
                    ctx.executor.enqueue(Task::RequestComplete {
                        request: Box::new(request),
                        error: message.error().cloned(),
                        reply: Some(message),
                    });
                }
            }
            Rpc::VoteRequest {
                term,
                term_p,
                seq_p,
            } => self.vote_request(ctx, from, term, term_p, seq_p)?,
            Rpc::Vote { term, vote_granted } => self.vote_reply(ctx, from, term, vote_granted)?,
            Rpc::Append {
                cid,
                term,
                term_p,
                seq_p,
                commit_seq,
                data,
            } => self.append_request(ctx, from, cid, term, term_p, seq_p, commit_seq, data)?,
            message => self.receive_monster(ctx, from, message)?,
        }
        Ok(())
    }
}
