# Executor

Function notes for [src/executor.js](../src/executor.js), in source order.
Trivial initialization and queue helpers are omitted.

## `after`

Registers a one-time or repeating task against the supplied protocol clock.
Returns an identifier for cancellation.

### findings

no findings

## `run`

Processes one input and all runnable continuations before due timers.
Enforces ordered protocol time and deterministic timer ordering without
replaying missed intervals. An optional completion callback runs after successful
processing, while the executor is still active.

### findings

no findings

## `drain`

Runs queued tasks in order until no runnable work remains, including tasks
added during execution.

### findings

no findings
