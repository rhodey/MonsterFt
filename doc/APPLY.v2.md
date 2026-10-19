# MonsterApply v2

Function notes for [src/apply.v2.js](../src/apply.v2.js), in source order.
Trivial getters, guards, and delegation helpers are omitted, and nested
callbacks are covered with their enclosing function.

## `constructor`

Validates Apply configuration and prepares the DB2 owner and user callback for
startup through Gate.

### findings

no findings

## `_emitSafe`

Delivers events while containing listener failures and reporting them as errors
where possible.

### findings

no findings

## `_fatal`

Begins closing Apply after an unrecoverable failure and reports its error and
fatal events.

### findings

no findings

## `_send`

Sends a reply to Gate and treats transport failure as fatal.

### findings

no findings

## `_validateExecute`

Validates the identity and contents of setup or command execution requests.

### findings

no findings

## `onReceive`

Handles Gate's startup, execution, and shutdown requests, permitting only one
execution at a time and returning its receipt, outcomes, and warnings.

### findings

- **Open: a synchronous failure sending ERR can hide the original fatal
  error.** The catch sends its error reply before calling `_fatal(err)`.
  If `localSend` throws, `_send` starts shutdown with `SEND_ERROR`, and the
  subsequent `_fatal` call discards the original error because Apply is already
  closing. A real cluster with a failing setup callback emitted only the
  transport error locally and eventually `APPLY_TIMEOUT` at Gate; the setup
  message and stack reached neither. An asynchronous send rejection preserves
  the original. Establish and report the original fatal error before attempting
  its fallible reply transmission. Source: `src/apply.v2.js`, `onReceive` catch
  and `_send`.

## `_writeReceipt`

Advances DB2's durable apply receipt and rejects a missing or nonadvancing update.

### findings

no findings

## `_execute`

Runs setup or a command batch and commits its apply receipt with the resulting
DB2 changes, preserving individual command failures as outcomes and warnings.

### findings

no findings

## `close`

Stops accepting work, waits for any active execution to settle, and closes DB2.

### findings

no findings
