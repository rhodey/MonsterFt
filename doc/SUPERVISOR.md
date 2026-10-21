# Supervisor

Function notes for [src/supervisor.js](../src/supervisor.js), in source order.
Trivial getters and promise helpers are omitted.

## `constructor`

Validates supervision settings and prepares the lifecycle. The host supplies
Gate instances, child entrypoints, and transport readiness and cleanup.
`isStopped` reports permanently disabled retries, including during cleanup;
ordinary startup and restart attempts leave it false.

### findings

no findings

## `_emitSafe`

Delivers lifecycle events and local diagnostics without allowing one listener
failure to prevent later listeners from running.

### findings

no findings

## `open`

Starts supervision and waits for its first successful startup. Repeated calls
share that initial wait; stopped supervisors cannot reopen.

### findings

no findings

## `_validate`

Checks the setup result and normalizes the four child launch descriptions.

### findings

no findings

## `_fail`

Stops Gate after the current attempt's first failure and wakes supervision.
Reports only failures owned by Supervisor, leaving Gate and transport diagnostics
with their owners.

### findings

no findings

## `_spawn`

Launches a child and tracks its errors and actual process exit. Unexpected exits
fail the attempt.

### findings

no findings

## `_start`

Launches all four children, waits for host-supplied readiness, and opens Gate.
Cancelled attempts cannot advance into Gate startup.

### findings

no findings

## `_loop`

Runs successive startup and execution attempts until explicitly stopped.
The optional transport failure promise can end an attempt during startup or
operation. Replacements require successful owner termination and transport cleanup.

### findings

no findings

## `_delay`

Waits between attempts, allowing explicit shutdown to cancel the delay.

### findings

no findings

## `_stopRun`

Stops Gate and calls the optional synchronous transport stop hook before killing
children, then waits for their exits before host cleanup. A failed stop hook still
allows owner termination and cleanup; either hook's failure prevents replacement.

### findings

no findings

## `close`

Permanently disables supervision and waits for owner termination and transport
cleanup. Repeated calls retain the first result, including failure. Its close
notification confirms successful cleanup, not Gate's coordinated-close protocol.

### findings

no findings

## `drainCmd`

Disables restarts and explicitly drains the current Gate before stopping its
children. Cleanup still runs after a drain failure, preserving that failure.

### findings

no findings
