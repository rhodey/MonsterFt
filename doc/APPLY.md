# MonsterApply

Function notes for [src/apply.js](../src/apply.js), in source order.
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

Begins closing Apply after an unrecoverable failure and reports its error
locally.

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
execution at a time and returning its receipt and outcomes.

### findings

no findings

## `_writeReceipt`

Advances DB2's durable apply receipt and rejects a missing or nonadvancing update.

### findings

no findings

## `_execute`

Runs setup or a command batch and commits its apply receipt with the resulting
DB2 changes, preserving individual command failures as outcomes and local warnings.

### findings

- **Open: the apply contract does not cover SQL after a caught automatic
  rollback.** The callback receives the raw DB2 connection. If it catches a
  SQLite error that ended the transaction, later SQL can autocommit before the
  library detects the lost transaction. Those writes survive failure and can
  be repeated on replay. This applies to setup and command callbacks. The
  README should say that after a caught database error, if `db.isTransaction`
  is false, the callback must rethrow and issue no further SQL. That guidance
  has not yet been added.

## `close`

Stops accepting work, waits for any active execution to settle, and closes DB2.

### findings

no findings
