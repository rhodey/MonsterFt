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

Preserves null, undefined, and existing Buffers. Converts another byte view
to a Buffer sharing its backing memory, offset, and length.

### findings

None.

## `digestEntry`

Hashes the term's eight-byte little-endian encoding followed by the payload
with SHA-256 to produce the DB2 applied checkpoint hash. A no-op hashes only
the term encoding. That encoding exists only for hashing and is not stored
or replicated with entries.

### findings

None.

## `outcomesWire`

Converts command outcomes into compact result tuples for transport and
hashing. Rejected outcomes include the error message and codes, with an
optional stack string; fulfilled outcomes carry their result value.

### findings

None.

## `digestCmd`

Hashes the MessagePack encoding of the term, command sequence, SQLite
patchset, and outcome tuples to produce the local agreement digest.
Excludes error stacks so different traces do not change consensus results.

### findings

None.

## `assertEncodable`

Encodes an application result with MessagePack. If encoding fails, it throws
`APPLY_ERROR: apply result is not MessagePack-encodable`, and the result is
rejected like a thrown application error.

### findings

None.

## `constructor`

Requires an application callback and delegates Raft configuration to the
base constructor. Derives the DB2 path as DB1's path with `2` appended, and
checks that both database files exist or both are absent. Initializes
retention and Monster state, and registers the `change` hook used for
leader recovery.

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

Disables the base class's automatic Raft startup so MonsterNode can initialize
and validate DB2 before starting elections or replication.

### findings

None.

## `_monsterSortNodes`

Removes duplicate node IDs and returns them in sorted order for SYNC records.

### findings

None.

## `_monsterSyncAgrees`

Determines whether the local command belongs to a SYNC's agreed outcome.
Returns false for a quorum-impossible decision, honors explicit membership in the
agree/disagree lists, and otherwise compares the local and agreed digests.

### findings

None.

## `_monsterPrune`

Uses cached log and application boundaries to prune applied history when
retention reaches its trigger, leaving the target history and all unapplied
entries. Skips disabled retention and unresolved CMDs, and refreshes cached
log boundaries after deleting rows.

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
Uses the corresponding public error code for each condition.

### findings

None.

## `_monsterAssertLeader`

Requires the node to be open and acting as leader. When supplied a term,
also rejects work that belongs to a different leadership term.

### findings

None.

## `_monsterStepDownForTerm`

Steps down only if shutdown has not begun and the node is leader in the
supplied term. Returns whether it changed state, protecting newer leadership
from failures belonging to an older operation.

### findings

None.

## `_monsterWriteTx`

Runs DB2 work in a transaction and rolls it back on failure. Neither commits
nor rolls back after shutdown begins, and skips rollback when SQLite has
already ended the transaction. If rollback fails, it emits the original
error, then reports the rollback failure as fatal.

### findings

None.

## `_monsterAdvanceApplied`

Updates the DB2 singleton's applied sequence and entry hash. Requires exactly
one changed row and adds DB2 operation context to SQLite failures.

### findings

None.

## `_monsterInstallPendingCmd`

Computes the local command digest and stores the pending CMD together with
its applied checkpoint in DB2. Requires previously empty pending fields and
exactly one updated row, then returns the digest.

### findings

None.

## `_monsterWriteCmdDecision`

Clears the matching pending CMD while updating the applied checkpoint and
repair state for its SYNC. Requires exactly one matching metadata row and
normalizes failures with operation-specific DB2 context.

### findings

None.

## `_monsterWriteRepair`

Updates the singleton's repair state without advancing its checkpoint or
clearing the pending command. Requires one changed row and normalizes DB2
write failures.

### findings

None.

## `_monsterReleaseDb`

Clears the active DB work slot and settles that operation with its result or
error. Retains the first failure for drain reporting, then starts the next
queued operation unless shutdown has begun.

### findings

None.

## `_monsterDrainDb`

Starts the oldest queued DB operation when none is active, enforcing serial
access to DB2. Routes synchronous throws and asynchronous completion through
the common work-release path.

### findings

None.

## `_monsterRunDb`

Enqueues a DB operation and returns a promise for its result, starting the
queue if it is idle. Rejects new work immediately once shutdown begins.

### findings

None.

## `_monsterRunDbGetCmd`

Reads the cached pending command through the DB queue so the lookup follows
earlier queued mutations. Optionally matches a specific command sequence,
returning null if no matching command remains.

### findings

None.

## `_monsterInit`

Initializes fresh DB2 metadata only when both DB2 and the log are empty.
Otherwise restores the persisted checkpoint, repair state, and pending
command. Checks field shapes and bounds before caching the metadata and
allowing startup retention or Raft activity.

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

Validates the DB2 checkpoint's sequence and hash against the exact DB1 term
and payload using `digestEntry`. For the initial checkpoint, checks that the
hash is null. A missing checkpoint row below the head appears as a DB1
sequence gap, which SQLiteLog iteration rejects as `LOG_CORRUPT`.

### findings

- **Skipped by decision: an initial checkpoint is accepted even when
  retention has removed sequence zero.** With `applied_seq = -1` and
  `log.begin > 0`, the node opens, then fails application with
  `LOG_CORRUPT`. Decision: the later error is acceptable.

## `_monsterRememberReport`

Records the first digest received from a node for the current pending command.
Normalizes the digest to a Buffer and ignores later reports from that node.

### findings

None.

## `_monsterDecision`

Groups reported nodes by command digest and identifies a group that reaches
quorum. Also determines whether agreement has become impossible even if all
unreported nodes join the largest group, returning that status and the reports.

### findings

None.

## `_monsterRxOutcomeRequest`

Validates an outcome request and looks up its pending CMD through the DB queue.
Replies with the local digest when the command still matches and the node is
not closing; malformed requests emit warnings.

### findings

None.

## `_monsterRxOutcome`

Validates an incoming digest report, warning on malformed fields. While
leader, records it only if its command matches the pending CMD, rechecking
leadership after the queued lookup.

### findings

None.

## `_monsterAnnounceOutcome`

Records the local digest directly when leader, or sends it to the known
leader when following. Sends nothing if no leader is known.

### findings

None.

## `_monsterApplyNoop`

Advances the durable and cached applied checkpoint for a no-op. At sequence
zero, also calls the application with null data for schema setup and commits
that initialization together with the checkpoint in one DB2 transaction. It
writes the checkpoint only if that transaction is still active after the
callback.

### findings

None. The application transaction rule under `_monsterApplyCmd` also
applies to this sequence-zero callback.

## `_monsterApplyCmd`

Applies each command under a savepoint and checks that each result can be
MessagePack-encoded, collecting successful results and rejected errors. A
rejected item's writes are rolled back to its savepoint. If SQLite has
already ended the transaction, the original error is rethrown. If savepoint
cleanup fails, the original error is emitted before the cleanup failure
propagates. Persists the pending command digest and applied checkpoint, then
announces the local digest for agreement.

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
- **Accepted: a follower's application error is not reported in detail.**
  Only the leader returns outcomes to the caller. A dissenting follower
  reports only `REPAIR_OUTSIDE_AGREEMENT`, without the original message or
  stack. Decision: keep the current behavior.

## `_monsterApplySync`

Applies the agreement decision, clears the pending command, and persists the
checkpoint and repair state. Emits the SYNC event. Quorum impossibility is
reported as a nonfatal `REPAIR_QUORUM_IMPOSSIBLE` error, while Raft stays
online and new commands are blocked. A node outside the agreed quorum closes
fatally.

### findings

None.

## `_monsterApplyEntry`

Dispatches committed no-op, CMD, and SYNC entries to their application paths.
Defers CMD processing during drain, fatally rejects CMDs while repair is
required, and rejects unknown entry types as Monster corruption.

### findings

None.

## `_monsterApply`

Processes Raft's application batch sequentially, decoding Monster records
and hashing each entry before dispatch. Checks for shutdown and prunes
eligible history after each successful entry, returning one result per entry.

### findings

- **Accepted: normal drain may suppress a retention error.** See
  `_monsterPrune`.

## `_monsterAppendEntry`

Adds the current match indexes to CMD records and appends Monster records
through Raft.

### findings

None.

## `_monsterAwaitDecision`

Registers the leader's own digest and repeatedly requests follower outcomes
until a quorum agrees or agreement becomes impossible, checking leadership
throughout. Builds the corresponding SYNC record and determines whether the
leader's local result belongs to the agreed group.

### findings

None.

## `_monsterAppendSync`

Checks that the caller is still leader in the expected term and appends a
SYNC through the shared Monster append helper. Checks shutdown after
completion and returns the SYNC sequence.

### findings

None.

## `_monsterFenceLeader`

Steps down and persists the outside-agreement repair state through the DB
queue. Reports either a persistence failure or the resulting repair-required
condition as fatal, then throws the error.

### findings

None.

## `_monsterLeaderQueue`

Serializes command protocols after leader recovery. Checks leadership before
capturing the term and waiting for that term's recovery, then requires the
same term afterward. Settles the caller's result when agreement is known.
Before fencing, it waits for that result promise, so a forwarding handler
can reply first. Then completes the SYNC or repair operation. Steps down in
the same term if the CMD/SYNC operation fails, and warns about later
failures while open.

### findings

None.

## `_monsterOnChange`

Starts recovery once per leadership term, waiting for election readiness
and committed application before examining pending command state. Resolves
any pending CMD through SYNC or fences a leader outside agreement. On
failure, it steps down and warns while the node is open.

### findings

None.

## `_monsterRxFwdCmd`

Validates a forwarded command request and checks command availability,
including drain, before submitting it to the leader's protocol queue. Replies
with agreed outcomes or an RPC error. After sending an error reply, it warns
locally while open.

### findings

None.

## `onReceive`

Dispatches Monster messages from members and delegates ordinary Raft
messages to the base class. Invalid outcome fields produce warnings in their
handlers. Unexpected failures in detached outcome handlers enter the fatal
reporting path.

### findings

None.

## `_monsterSendAndAwaitCmdAck`

Sends a forwarded command with an append deadline and response validator.
Returns an ACK or reconstructs the RPC error, racing the operation against
shutdown.

### findings

None.

## `_monsterFwdCmdToLeader`

Requires a known leader and forwards the command items with the current term
and a unique correlation ID. Validates the reply's term and outcome shape,
then returns the command sequence and serialized outcomes.

### findings

None.

## `_monsterAppendOrFwdCmd`

Checks command availability and either queues the items locally as leader
or forwards them to the leader. Converts either path's results into public
settled outcomes, restoring error codes and available stacks.

### findings

None.

## `append`

Validates one command Buffer and submits it through the local-or-forwarded
path with shutdown cancellation. Returns its sequence and value on success,
or throws the command's rejected outcome.

### findings

None.

## `appendBatch`

Validates a nonempty array of command Buffers and submits them as one CMD
through the local-or-forwarded path. Returns the command sequence and settled
outcomes for all items, racing against shutdown.

### findings

- **Accepted: a sparse batch commits a CMD that every replica rejects.** The
  `every` check skips holes, and a hole is encoded as `undefined` in the CMD.
  Every replica fails with `CMD entry items must be buffers` and fails again
  on restart. The same happens when an in-process forward passes the
  caller's array directly; MessagePack forwarding instead gets
  `RPC_ILLEGAL`. Decision: sparse arrays remain out of scope under AGENTS.md.

## `drainCmd`

Stops accepting commands, waits for an existing pending command to receive
its decision, reports any saved DB failure, and closes the node. Returns the
same drain promise on subsequent calls. On a node that is not open, it
rejects without entering the draining state.

### findings

- **Accepted: normal closure may suppress a concurrent retention error.** See
  `_monsterPrune`. The retention error would not happen again or it would be
  thrown again on next open.

## `open`

Loads DB1, initializes or validates DB2, checks repair state, prunes eligible
history, and starts Raft. Repair state 1 opens live; repair state 2 refuses
to open. On startup failure, closes the node's resources and rethrows the
error.

### findings

- **Accepted: an interrupted first creation can leave only DB1.** The
  constructor then rejects the incomplete pair until it is cleaned up. This
  fails closed.

## `_monsterStop`

Removes and rejects queued DB work and rejects any active DB operation using
the shutdown error. This settles callers even if the active operation itself
has not finished.

### findings

None.

## `close`

Stops Raft and rejects DB work, then attempts to close both databases.
Preserves failed handles for another close attempt and throws the first
cleanup failure.

### findings

- **Accepted policy: only the first close failure is thrown.** If both the
  DB2 and DB1 close fail, the DB1 error is discarded.

## `del`

Requires both database handles to be closed before deleting the database
pair. Delegates DB1 deletion to the base class and removes DB2's SQLite files.

### findings

None.

## `requireNormalFile` (`src/monsterft.js`)

Requires each certification path to identify a normal file before opening
DB2. Converts missing, inaccessible, and non-file paths into `FS_ERROR`.

### findings

None.

## `MonsterFt.certify` (`src/monsterft.js`)

Validates the database pair and override flag, then clears DB2's repair
state in an exclusive transaction. It first sets exclusive locking mode, so
it refuses a pair still open by a running node with `SQLITE_BUSY`. A missing
metadata row is `MONSTER_CORRUPT`. By default it requires no unresolved CMD.
It preserves the checkpoint and pending fields and returns the applied
sequence.

### findings

- **Deferred:** repairing one replica can also need a leadership reset; see
  `_monsterPrune`.
