# MonsterProc v2

Function notes for [src/proc.v2.js](../src/proc.v2.js), in source order.
Trivial getters and delegation helpers are omitted, and nested callbacks are
covered with their enclosing function. Findings start empty for future review;
`no findings` does not mean the function has already been reviewed.

## `constructor`

Validates Proc configuration and prepares its protocol store and executor for
startup through Gate.

### findings

no findings

## `_emitSafe`

Delivers events while containing listener failures and reporting them through
error listeners where possible.

### findings

no findings

## `_fatal`

Closes the Proc after an unrecoverable failure and reports its error and fatal
events.

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

## `_output`

Records an output for the active input, retaining message bytes for Proc A and
checksums for Procs B and C.

### findings

no findings

## `_validateInput`

Validates ordered Gate inputs against the Proc's startup phase, clock, and
expected request shapes, independently checking incoming cluster frames.

### findings

no findings

## `_input`

Processes one ordered input and its runnable protocol work atomically in DB1,
then publishes its completion and local diagnostics.

### findings

no findings

## `onReceive`

Handles Gate's startup, runtime, and shutdown requests, enforcing the Proc
lifecycle and reporting failures.

### findings

no findings

## `close`

Stops the engine, executor, and watchdog and closes the Proc's protocol store,
reporting any shutdown failures.

### findings

no findings
