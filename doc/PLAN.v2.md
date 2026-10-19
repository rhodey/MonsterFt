# V2 code review plan

Review the 18 v2 source guides in ten focused passes. Start with the shared
utilities and durable-state rules, then follow them into coordinated execution
and the protocols. Finish with the public API and shutdown paths, where the
components' guarantees come together.

The scopes below assign all 183 documented function sections and the public
exports module. Ranges are inclusive and follow each guide's section order.
Follow dependencies as needed, including helpers omitted from the guides,
while keeping each pass centered on its assigned sections.

## Method for each pass

- Read the assigned summaries, source, and relevant tests. Check behavior
  against [PLAN.md](../PLAN.md) and [DIRECTION.md](../DIRECTION.md). Use the v1
  implementation and review notes to find lost guarantees and related bugs,
  while respecting intentional v2 changes.
- Look for failures reachable through supported calls, actual protocol
  messages, transport timing, callbacks, restarts, or interrupted writes.
  Private-method calls or invented runtime state alone do not establish a bug.
- Respect accepted limitations in [AGENTS.md](../AGENTS.md), [RAFT.md](RAFT.md),
  [LOG.md](LOG.md), and [MONSTER.md](MONSTER.md) where still applicable. The
  agreed v2 design takes precedence where it explicitly changes an old policy.
- For each issue, record the trigger, consequence, source location, and
  supporting evidence under the responsible function's `### findings`.
  Distinguish confirmed bugs from concerns needing evidence. Cross-reference
  related sections instead of duplicating a finding. Leave `no findings` where
  nothing was found; keep function summaries brief.
- Use focused tests or small reproductions when they help establish a finding.
  After each pass, report findings in chat, update its progress checkbox, and
  stop for discussion. Source patches are separate work unless requested.

## 1. Deterministic runtime and shared checks

Cover all sections in [ERROR.v2.md](ERROR.v2.md), [UTIL.v2.md](UTIL.v2.md),
[EXECUTOR.v2.md](EXECUTOR.v2.md), and [RANDOM.v2.md](RANDOM.v2.md).

- Check argument boundaries, error normalization, stack preservation, and
  filesystem error handling.
- Check identical random choices and CIDs, supplied-time ordering, timeout
  cancellation, and repeating timers. Verify runnable continuations finish in
  the current input without waiting for another clock tick.

## 2. Frames and transport

Cover all sections in [WIRE.v2.md](WIRE.v2.md),
[STREAM.v2.md](STREAM.v2.md), and [TCP.v2.md](TCP.v2.md).

- Check exact-byte checksum handling, independent decoding, Buffer transport,
  and preservation of shared error details.
- Trace fragmented and combined frames, backpressure, malformed input, and
  teardown during callbacks. Check that errors remain visible and pending
  operations settle when a connection or stream fails.

## 3. SQLite log operations

Cover all sections in [LOG.v2.md](LOG.v2.md).

- Check stored-value validation, term/payload separation, sequence boundaries,
  cached state, iteration, trimming, and reopening.
- Check standalone and caller-owned transactions, especially trim followed by
  replacement append. Trace write and commit failures to ensure partial work
  cannot be published as success or reused after fatal failure.

## 4. Metadata, reconciliation, and certification

Cover all sections in [UTILS.v2.md](UTILS.v2.md),
[STORAGE.v2.md](STORAGE.v2.md), and [CERTIFY.v2.md](CERTIFY.v2.md).

- Check fresh and interrupted initialization, missing or invalid established
  files, checkpoint hashes, receipt gaps, and retained-history cases.
- Trace capture, combined-majority selection, adoption, and receipt import
  across interruptions. Check that completed Apply work is preserved and that
  certification validates before changing files, honors pending commands, and
  leaves DB2 unchanged.

## 5. Proc execution and Gate coordination

Cover all sections in [PROC.v2.md](PROC.v2.md), plus `constructor` through
`_release` in [GATE.v2.md](GATE.v2.md).

- Check hello retries and seeds, startup phase barriers, request correlation,
  and native watchdogs. Exercise synchronous and delayed local delivery.
- Check atomic DB1 inputs, identical fanout, ordered output agreement, and the
  all-three completion barrier. Trace a delayed third Proc, immediate replies,
  failed commits, and timeouts without allowing another input or uncommitted
  output to escape.

## 6. Raft elections and request handling

Cover `validRecords` through `voteResponse` in [NODE.v2.md](NODE.v2.md).

- Check restored state, membership, live-follower tracking, request validation,
  and late, duplicate, or wrong-sender replies.
- Trace durable term/vote changes, rejected requests, election deadlines, and
  leadership changes. Check that pending work is cancelled consistently and
  externally visible responses agree with durable state.

## 7. Raft replication and commitment

Cover `appendNoop` through `stop` in [NODE.v2.md](NODE.v2.md).

- Check election no-ops, replication pages, predecessor checks, backtracking
  hints, committed-history protection, and retention boundaries.
- Trace ACK, ERR, timeout, and leadership-change paths through commitment and
  append completion. Verify ready next pages and application work continue
  within the enabling input, with timer waits confined to intended delays.

## 8. Apply execution, receipts, and retention

Cover all sections in [APPLY.v2.md](APPLY.v2.md); `commandBytes` through
`receiveApply` in [MONSTER.v2.md](MONSTER.v2.md); and `outcomesWire` through
`digestEntry`, plus `normalizeKeepOptions`, in [UTILM.v2.md](UTILM.v2.md).

- Check setup, single commands, batches, savepoints, rejected or unencodable
  results, warnings, transaction loss, and shutdown during an apply callback.
- Trace DB2 commit through the fixed reply receipt into DB1. Check digest
  stability, no repeated completed work, pending-command protection, and
  pruning only after a valid durable checkpoint.

## 9. Monster agreement and command progress

Cover `syncAgrees` through `finishDrain` in [MONSTER.v2.md](MONSTER.v2.md),
and `validateOutcomeRequestMsg` through `isFwdCmdAck` in
[UTILM.v2.md](UTILM.v2.md).

- Trace local and forwarded commands through reports, agreement, SYNC, and the
  next queued command. Check duplicate or stale reports, malformed decisions,
  batches, and leadership changes during each phase.
- Check dissent and impossible quorum, durable repair fences, caller results,
  warning/error visibility, and draining. Verify one active command progresses
  without losing or responding twice to queued callers.

## 10. Public API, events, and shutdown

Cover `_event` through `close` in [GATE.v2.md](GATE.v2.md) and the exports
module in [INDEX.v2.md](INDEX.v2.md).

- Check public results, event waits, listener exceptions or shutdown, drain,
  partial startup failure, and close while Apply or a Proc is busy. Check
  explicit component wiring and the exported API against the examples.
- Trace representative append, drain, and fatal-close operations across the
  reviewed components. Check that promises settle, owners finish before
  replacement, and no late callback publishes success or changes closed state.
  Record export-only findings under an `Exports` section in INDEX.v2.md if needed.

## Progress

Check a pass after examining every assigned section and recording its findings.
Use this checklist for progress; keep status notes out of the function guides.

- [x] 1. Deterministic runtime and shared checks
- [x] 2. Frames and transport
- [x] 3. SQLite log operations
- [x] 4. Metadata, reconciliation, and certification
- [x] 5. Proc execution and Gate coordination
- [x] 6. Raft elections and request handling
- [x] 7. Raft replication and commitment
- [ ] 8. Apply execution, receipts, and retention
- [ ] 9. Monster agreement and command progress
- [ ] 10. Public API, events, and shutdown
