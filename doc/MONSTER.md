# MonsterNode and MonsterFt

Function notes for [src/monster.js](../src/monster.js) and
[src/monsterft.js](../src/monsterft.js), in source order. The notes cover the
module helpers and every MonsterNode method. Each findings area lists only
unresolved items: open, deferred, skipped by decision, or accepted. "None"
means no unresolved item is recorded. Review rules are in [RAFT.md](RAFT.md).

## Policies and accepted limitations

- **Error codes:** `MONSTER_CORRUPT` covers malformed CMD/SYNC records, a
  SYNC without its pending CMD, unknown entry types, and DB2 metadata
  corruption. SQLiteLog row, sequence, and term errors use `LOG_CORRUPT`.
- **Hashing:** patchsets are hashed as opaque buffers, so every member must
  run the same Node.js version, as AGENTS.md notes. Agreement digests
  exclude error stacks. The default msgpackr `pack` does not use records, so
  equal values encode identically. Results must be deterministic, including
  object key order.
- **Input ownership:** callers must not change submitted Buffers or batch
  arrays until the operation settles.
- **Sparse arrays:** out of scope under AGENTS.md. See `appendBatch`.
- **Exotic thrown values:** see `_monsterApplyCmd` and `wrapError` in
  RAFT.md.
- **Application transactions:** see the open doc gap under
  `_monsterApplyCmd`.
- **Repair:** certification is an offline step. Retention and repair are not
  being redesigned; see `_monsterPrune`.

## `asBuffer`

Converts byte views to Buffers without copying, preserving existing Buffers,
null, and undefined.

### findings

None.

## `digestEntry`

Produces the checkpoint hash identifying an entry's term and payload.

### findings

None.

## `outcomesWire`

Converts command outcomes into tuples for transport or agreement hashing,
optionally including error stacks.

### findings

None.

## `digestCmd`

Produces the agreement digest for a command, its database changes, and its
outcomes. Excludes error stacks from agreement.

### findings

None.

## `assertEncodable`

Rejects results that cannot be encoded as MessagePack, using `APPLY_ERROR`.

### findings

None.

## `constructor`

Validates the application callback, database pair, and retention settings.
Initializes Raft and Monster state, including command processing and leader
recovery.

### findings

- **Skipped by decision: the file-pair check can become stale before
  `open()`.** It runs only in the constructor. If DB1 is deleted after
  construction, `open()` creates a fresh DB1 with term zero and no vote,
  because DB2 still holds a valid initial checkpoint. The node can then grant
  a second vote in a term where it already voted. Decision: no open-time
  check for now.
- **Open, left as is: nodes with different `quorum` settings reject each
  other's SYNC records.** A leader decides using its own `quorum`, but each
  node validates a SYNC's agree list against its local `quorum`. A SYNC from
  a leader with a smaller value is fatal on peers with larger values. In a
  three-node example, two of three nodes closed after the client had
  received success. Raising `quorum` across the cluster would likewise break
  older SYNC records that a replica has not yet applied. Nothing documents
  that `quorum` must match on every node. Decision: leave open.
- **Minor, left as is: the derived DB2 path can be another node's DB1.** For
  example, `node1` + `2` is `node12`. The node that opens second fails closed
  with a misleading `LOG_CORRUPT` or `MONSTER_CORRUPT`. Paths ending in `.db`
  or `.sqlite` avoid it. Decision: leave open.

## `_autoRaft`

Defers Raft startup until MonsterNode has initialized and validated DB2.

### findings

None.

## `_monsterSortNodes`

Removes duplicate node IDs and returns them in sorted order for SYNC records.

### findings

None.

## `_monsterSyncAgrees`

Determines whether the local command outcome agrees with a SYNC decision.

### findings

None.

## `_monsterPrune`

Prunes applied history to the retention target when its trigger is reached.
Preserves unapplied entries and defers pruning while a CMD is unresolved.

### findings

- **Accepted: drain can hide a retention failure.** Pruning runs outside the
  DB queue. If normal drain closure wins the race, the retention error is
  never reported. The DB2 checkpoint is already durable, and startup retries
  any pruning that is still due.
- **Deferred: repairing one replica after retention needs a leadership
  reset.** The surviving leader may already have cancelled replication to a
  replica whose required history was pruned. Copying a donor pair to that
  replica and reopening only the donor and target leaves it stuck at the
  copied checkpoint until leadership changes or the leader restarts.
  REPAIR.md should require that. Decision: deferred pending possible
  bigger changes to retention.

## `_monsterFatalError`

Emits a fatal error if shutdown has not already started and returns the same
error for the caller to throw or propagate.

### findings

None.

## `_monsterRepairError`

Creates the public repair-required error for either supported repair state.
Treats an unexpected runtime repair state as a fatal Monster invariant error.

### findings

None.

## `_monsterAssertAvailable`

Rejects new commands when the node is closed, requires repair, or is draining.

### findings

None.

## `_monsterAssertLeader`

Requires the node to be open and acting as leader. When supplied a term,
also rejects work that belongs to a different leadership term.

### findings

None.

## `_monsterStepDownForTerm`

Steps down only when still leader in the supplied term and not closing.
Returns whether leadership changed.

### findings

None.

## `_monsterWriteTx`

Runs DB2 work atomically and rolls it back on failure. Reports both the
original error and a fatal cleanup error if rollback fails.

### findings

None.

## `_monsterAdvanceApplied`

Persists the applied entry's sequence and hash in DB2.

### findings

None.

## `_monsterInstallPendingCmd`

Records a newly applied CMD as pending agreement, saves its checkpoint, and
returns its local digest.

### findings

None.

## `_monsterWriteCmdDecision`

Records a SYNC decision's checkpoint and repair state and clears the matching
pending CMD.

### findings

None.

## `_monsterWriteRepair`

Persists the repair state while preserving the checkpoint and pending command.

### findings

None.

## `_monsterReleaseDb`

Settles the active DB operation and allows queued work to continue. Retains
the first failure for reporting during drain.

### findings

None.

## `_monsterDrainDb`

Runs queued DB operations one at a time in submission order.

### findings

None.

## `_monsterRunDb`

Queues a DB operation and returns a promise for its result. Rejects new work
once shutdown begins.

### findings

None.

## `_monsterRunDbGetCmd`

Returns the pending command after earlier DB work completes, optionally
requiring a specific sequence. Returns null when no matching command remains.

### findings

None.

## `_monsterInit`

Initializes fresh DB2 metadata or validates and restores its checkpoint,
repair state, and pending command for startup.

### findings

- **Skipped by decision: startup does not cross-check pending metadata with
  the log.** Monster writes the pending fields and the checkpoint together in
  one statement, so they stay consistent on their own. They can disagree only
  if someone hand-edits the DB2 row. For example, the pending sequence could
  point at an entry that is not a CMD, or it could be cleared while a CMD is
  unresolved. Startup would accept such an edit, and later outcome replies or
  startup pruning would act on it. If this is revisited, note that election
  no-ops may follow a pending CMD, and the pending digest cannot be
  recomputed from the log.

## `_monsterVerifyAppliedEntry`

Verifies that DB2's applied checkpoint matches the corresponding DB1 entry.

### findings

- **Skipped by decision: an initial checkpoint is accepted even when
  retention has removed sequence zero.** With `applied_seq = -1` and
  `log.begin > 0`, the node opens, then fails application with
  `LOG_CORRUPT`. Decision: the later error is acceptable.

## `_monsterRememberReport`

Records each node's first reported digest for the pending command.

### findings

None.

## `_monsterDecision`

Determines whether reported command outcomes have reached agreement or made
agreement impossible, and returns the supporting reports.

### findings

None.

## `_monsterRxOutcomeRequest`

Answers valid requests for the local pending command's digest. Warns on
malformed requests.

### findings

None.

## `_monsterRxOutcome`

While leader, records valid peer outcome reports for the pending command.
Warns on malformed reports.

### findings

None.

## `_monsterAnnounceOutcome`

Reports the local command digest to the leader's agreement process.

### findings

None.

## `_monsterApplyNoop`

Applies a no-op and advances the checkpoint. At sequence zero, also runs the
application's schema setup as part of the same transaction.

### findings

None. The application transaction rule under `_monsterApplyCmd` also
applies to this sequence-zero callback.

## `_monsterApplyCmd`

Applies CMD items and collects their outcomes for agreement. Warns locally
with `APPLY_ERROR` for failed items and rolls back their writes. Persists the
pending command and checkpoint, then announces the local outcome digest.

### findings

- **Open: the application contract does not cover SQL after a caught
  automatic rollback.** The callback receives the raw DB2 connection. If it
  catches a native SQLite error that ended the transaction, any further SQL
  autocommits. Monster then fails the CMD and closes, but those writes are
  already durable. On reopening, the replayed command applies them again;
  for example, a counter reached 2 instead of 1. SYNC fences the replica if
  only one is affected. If all are affected, they agree on the wrong state.
  Suggested doc change: after catching a database error, if `db.isTransaction`
  is false, rethrow and issue no further SQL. No change has been made.

## `_monsterApplySync`

Records the agreement decision, clears the pending command, and emits SYNC.
Reports required repair: quorum impossibility blocks new commands, while
being outside agreement closes the node.

### findings

None.

## `_monsterApplyEntry`

Dispatches committed Monster entries for application, respecting drain and
repair state. Rejects unknown entry types as corruption.

### findings

None.

## `_monsterApply`

Applies a batch of committed Monster entries in order and returns their
results. Prunes eligible history as application advances.

### findings

- **Accepted: normal drain may suppress a retention error.** See
  `_monsterPrune`.

## `_monsterAppendEntry`

Adds the current match indexes to CMD records and appends Monster records
through Raft.

### findings

None.

## `_monsterAwaitDecision`

Waits as leader until command outcomes establish agreement or make it
impossible. Returns the resulting SYNC decision and whether the leader agrees.

### findings

None.

## `_monsterAppendSync`

Appends a SYNC while still leader in the expected term and returns its
sequence.

### findings

None.

## `_monsterFenceLeader`

Steps down, marks the leader as requiring repair for being outside agreement,
and closes it through fatal reporting.

### findings

None.

## `_monsterLeaderQueue`

Runs commands one at a time after leader recovery, from CMD submission
through agreement and SYNC or repair. Returns the caller's outcome once
agreement is known and steps down if the protocol fails in the same term.

### findings

None.

## `_monsterOnChange`

Recovers any pending command when leadership begins, completing its SYNC or
requiring repair. Steps down and warns if recovery fails.

### findings

None.

## `_monsterRxFwdCmd`

Validates and handles forwarded command requests as leader. Replies with
agreed outcomes or an error, warning locally when request processing fails.

### findings

None.

## `onReceive`

Routes member messages to the Monster or Raft protocol handlers. Reports
unexpected outcome-handler failures as fatal.

### findings

None.

## `_monsterSendAndAwaitCmdAck`

Sends a forwarded command and waits for a valid response, respecting timeout
and shutdown. Returns the acknowledgement or throws the reported error.

### findings

None.

## `_monsterFwdCmdToLeader`

Forwards command items to the known leader and returns their sequence and
validated outcome tuples.

### findings

None.

## `_monsterAppendOrFwdCmd`

Submits commands to the local or remote leader and returns their sequence
and public settled outcomes.

### findings

None.

## `append`

Submits one command and returns its sequence and agreed value. Rejects with
the command's error when its agreed outcome is failure.

### findings

None.

## `appendBatch`

Submits a batch as one CMD and returns its sequence and settled outcome for
each item.

### findings

- **Accepted: a sparse batch commits a CMD that every replica rejects.** The
  `every` check skips holes, and a hole is encoded as `undefined` in the CMD.
  Every replica fails with `CMD entry items must be buffers` and fails again
  on restart. The same happens when an in-process forward passes the
  caller's array directly; MessagePack forwarding instead gets
  `RPC_ILLEGAL`. Decision: sparse arrays remain out of scope under AGENTS.md.

## `drainCmd`

Stops accepting commands and waits for any pending command's decision before
closing. Reports saved DB failures and returns a promise for drain completion.

### findings

- **Accepted: normal closure may suppress a concurrent retention error.** See
  `_monsterPrune`. The retention error would not happen again or it would be
  thrown again on next open.

## `open`

Opens the database pair, restores Monster state, and starts Raft. Refuses to
open a node marked outside agreement and cleans up if startup fails.

### findings

- **Accepted: an interrupted first creation can leave only DB1.** The
  constructor then rejects the incomplete pair until it is cleaned up. This
  fails closed.

## `_monsterStop`

Rejects active and queued DB work during shutdown.

### findings

None.

## `close`

Stops the node and closes both databases. Reports cleanup failures while
allowing failed closes to be retried.

### findings

- **Accepted policy: only the first close failure is thrown.** If both the
  DB2 and DB1 close fail, the DB1 error is discarded.

## `del`

Deletes both databases, requiring the pair to be closed.

### findings

None.

## `requireNormalFile` (`src/monsterft.js`)

Checks that a certification path identifies a regular file, reporting invalid
paths as `FS_ERROR`.

### findings

None.

## `MonsterFt.certify` (`src/monsterft.js`)

Clears a closed database pair's repair state and returns its applied
sequence. Requires pending commands to be resolved unless explicitly
overridden, and preserves the checkpoint and pending command metadata.

### findings

- **Deferred:** repairing one replica can also need a leadership reset; see
  `_monsterPrune`.
