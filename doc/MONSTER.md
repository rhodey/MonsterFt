# MonsterEngine

Function notes for [src/monster.js](../src/monster.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `commandBytes`

Estimates a command record’s encoded size for the SQLite size limit.

### findings

no findings

## `constructor`

Restores Monster protocol state and initializes command, agreement, retention,
and drain tracking.

### findings

no findings

## `available`

Rejects new commands when repair is required or commands are draining.

### findings

no findings

## `start`

Checks the startup repair fence, prunes eligible history, and starts Raft.

### findings

no findings

## `write`

Performs a protocol metadata update that must change exactly one DB1 row.

### findings

no findings

## `advanceApplied`

Advances the durable and runtime protocol checkpoint.

### findings

no findings

## `prune`

Removes eligible applied history when retention reaches its trigger, preserving
history while a command awaits agreement.

### findings

no findings

## `applyReady`

Processes committed protocol entries and requests Apply execution when setup or
a command is ready. Apply progress alone does not emit `change`.

### findings

no findings

## `execute`

Requests setup or command execution from Apply and records the expected reply.

### findings

no findings

## `receiveApply`

Validates an Apply completion against its original execution request, copies its
receipt into DB1, and resumes protocol progress.

### findings

no findings

## `syncAgrees`

Determines whether this node’s command outcome agrees with a SYNC decision.

### findings

no findings

## `applySync`

Records a command’s agreement decision, clears its pending state, and reports
any required repair locally.

### findings

no findings

## `fatal`

Records a fatal protocol outcome and prevents further runnable work.

### findings

no findings

## `fenceLeader`

Records the leader’s repair fence and ends its participation after disagreement.

### findings

no findings

## `respond`

Returns a command result or error to its local or forwarded caller at most once.

### findings

no findings

## `submit`

Checks command size and availability, then queues a parsed command request
locally or forwards it to the leader.

### findings

no findings

## `leaderChanged`

Rejects work invalidated by a leadership change or resumes the leader’s command
processing.

### findings

no findings

## `pumpLeader`

Advances one leader command through execution and agreement before starting the
next queued command.

### findings

no findings

## `protocolFailure`

Reports a failed active protocol append and steps down if its leader term
remains current.

### findings

no findings

## `announceOutcome`

Reports this node’s pending command digest to the leader.

### findings

no findings

## `rememberReport`

Records a node’s first outcome report and schedules agreement evaluation when
needed.

### findings

no findings

## `awaitDecision`

Starts agreement evaluation and requests missing outcome reports while a
decision remains pending.

### findings

no findings

## `decide`

Determines command agreement or quorum impossibility, returns the caller’s
result, and records the decision or fences a dissenting leader.

### findings

no findings

## `receiveCustom`

Handles forwarded commands and command-outcome requests and reports.

### findings

- **Deferred: duplicate forwarded requests execute twice.**
  The `fwd_cmd` branch in `src/monster.js:352` creates a new queued request
  for every delivery, including the same sender and CID. Delivering one genuine
  forwarded frame twice in a three-node cluster made a single public `append()`
  execute at sequences 1 and 3 on every node; the caller received only the first
  result. Duplicate outcome reports are handled separately and do not have this
  problem. V1 has the same behavior. By decision, addressing this is deferred.
  A later change can clarify that cluster transports must deliver each request
  at most once or introduce request deduplication.

## `drain`

Stops admitting commands and requests completion after outstanding protocol work
settles.

### findings

no findings

## `finishDrain`

Completes drain requests once neither Apply execution nor command agreement
remains pending.

### findings

no findings
