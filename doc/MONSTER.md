# MonsterEngine

Function notes for [src/monster.js](../src/monster.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `commandBytes`

Estimates a command record’s encoded size for the SQLite size limit.

### findings

no findings

## `constructor`

Prepares the Monster protocol engine from stored state.

### findings

no findings

## `available`

Rejects new commands when repair is required or commands are draining.

### findings

no findings

## `fatal`

Records a fatal protocol outcome and prevents further runnable work.

### findings

no findings

## `writeOne`

Performs a protocol metadata update that must change exactly one DB1 row.

### findings

no findings

## `advanceApplied`

Advances the durable and runtime protocol checkpoint.

### findings

no findings

## `prune`

Applies configured retention limits to completed log history.

### findings

no findings

## `start`

Starts Monster protocol processing.

### findings

no findings

## `reply`

Delivers a command result or error to its caller at most once.

### findings

no findings

## `protocolFailure`

Handles a failed protocol append for the active leader command.

### findings

no findings

## `fenceLeader`

Stops a dissenting leader's participation and marks it as requiring repair.

### findings

no findings

## `finishDrain`

Completes drain requests when outstanding protocol work has finished.

### findings

no findings

## `drain`

Stops admitting commands and requests completion after outstanding protocol work
settles.

### findings

no findings

## `execute`

Requests setup or command execution from Apply.

### findings

no findings

## `syncAgrees`

Determines whether this node’s command outcome agrees with a SYNC decision.

### findings

no findings

## `applySync`

Applies a command agreement decision, including any required repair status.

### findings

no findings

## `rememberReport`

Accepts a node's first command outcome report.

### findings

no findings

## `announceOutcome`

Reports this node’s pending command digest to the leader.

### findings

no findings

## `decide`

Decides whether command outcomes have quorum agreement.

### findings

no findings

## `awaitDecision`

Waits for the command outcome reports needed to decide agreement.

### findings

no findings

## `leaderChanged`

Updates command processing for a leadership change.

### findings

no findings

## `pumpLeader`

Processes queued leader commands one at a time.

### findings

no findings

## `applyReady`

Processes committed log entries.

### findings

no findings

## `receiveApply`

Accepts an Apply completion and advances the corresponding protocol state.

### findings

no findings

## `submit`

Submits a command for execution by the cluster leader.

### findings

no findings

## `receive`

Handles incoming Raft and Monster protocol messages.

### findings

- **Deferred: duplicate forwarded requests execute twice.**
  The `fwd_cmd` branch creates a new queued request
  for every delivery, including the same sender and CID. Delivering one genuine
  forwarded frame twice in a three-node cluster made a single public `append()`
  execute at sequences 1 and 3 on every node; the caller received only the first
  result. Duplicate outcome reports are handled separately and do not have this
  problem. V1 has the same behavior. By decision, addressing this is deferred.
  A later change can clarify that cluster transports must deliver each request
  at most once or introduce request deduplication.
