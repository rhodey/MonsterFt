# MonsterProc

Function notes for [src/proc.js](../src/proc.js), in source order.
Trivial getters and delegation helpers are omitted, and nested callbacks are
covered with their enclosing function.

## `constructor`

Validates Proc configuration and prepares its protocol store and executor for
startup through Gate.

### findings

no findings

## `_emitSafe`

Delivers events and stops the Proc when a non-error listener throws. Reports
listener failures without recursively closing an already stopping Proc.

### findings

no findings

## `_fatal`

Stops the Proc after an unrecoverable failure and reports its error before the
close notification.

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

## `_input`

Processes one parsed input and its runnable work atomically in DB1,
checking order, lifecycle, and generation before protocol work. Malformed remote
messages warn locally without interrupting input completion. Fatal repair fences
commit without publishing outputs; quorum-impossible decisions permit replication.

### findings

no findings

## `onReceive`

Parses Gate's startup, runtime, and shutdown requests, enforcing the Proc
lifecycle and reporting failures. Claims the startup generation when opening DB1.

### findings

no findings

## `_close`

Stops the engine, executor, and watchdog and closes the Proc's protocol store,
reporting any fatal error and shutdown failures before a successful close
notification. Cleanup runs once; later close calls preserve its result.

### findings

no findings
