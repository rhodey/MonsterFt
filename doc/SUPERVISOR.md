# Supervisor

Function notes for [src/supervisor.js](../src/supervisor.js), in source order.
Trivial getters and promise helpers are omitted.

## `constructor`

Configures supervision of Gate and its child processes using a host-provided
setup callback.

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

## `_fail`

Marks the current supervision attempt as failed and stops Gate.

### findings

no findings

## `_delay`

Waits between attempts, allowing explicit shutdown to cancel the delay.

### findings

no findings

## `_validate`

Checks the setup result and normalizes the four child launch descriptions.

### findings

no findings

## `_spawn`

Launches and monitors a child process for the current supervision attempt.

### findings

no findings

## `_start`

Starts Gate and its child processes for a supervision attempt.

### findings

no findings

## `_stopRun`

Stops a supervision attempt and waits for child termination and host cleanup.
Optional graceful shutdown gives the database owners a chance to close first;
forced cleanup still runs if that fails.

### findings

no findings

## `close`

Permanently stops supervision and waits for the current attempt to shut down.
`close(true)` requests graceful owner closure first; `close()` remains forceful.
The first call selects the shutdown mode, and failures are retained without retries.

### findings

no findings

## `drainCmd`

Drains commands through the current Gate and shuts down supervision.

### findings

no findings

## `_loop`

Retries failed Gate startup and runtime attempts while supervision remains active.

### findings

no findings
