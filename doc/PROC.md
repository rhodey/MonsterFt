# MonsterProc

Function notes for [src/proc.js](../src/proc.js), in source order.
Trivial getters and delegation helpers are omitted, and nested callbacks are
covered with their enclosing function.

## `constructor`

Prepares a Proc instance to execute the cluster protocol.

### findings

no findings

## `_emitSafe`

Delivers Proc events and treats non-error listener failures as fatal.

### findings

no findings

## `_fatal`

Shuts down the Proc and reports an unrecoverable error.

### findings

no findings

## `_send`

Sends a local reply to Gate and treats transport failure as fatal.

### findings

no findings

## `_armWatchdog`

Restarts the watchdog that closes the Proc if Gate communication stops.

### findings

no findings

## `queueLocalEvent`

Queues local diagnostics for delivery after the active input commits.

### findings

no findings

## `output`

Records the active input's outgoing message or verification checksum for Gate.

### findings

no findings

## `_close`

Shuts down the Proc and reports any fatal or cleanup errors.

### findings

no findings

## `_input`

Processes an ordered protocol input and persists the resulting DB1 state.

### findings

no findings

## `onReceive`

Handles Gate's startup, runtime, and shutdown requests for the Proc.

### findings

no findings
