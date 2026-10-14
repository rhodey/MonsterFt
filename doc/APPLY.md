# MonsterApply

Function notes for [src/apply.js](../src/apply.js), in source order.
Trivial getters, guards, and delegation helpers are omitted, and nested
callbacks are covered with their enclosing function.

## `constructor`

Prepares an Apply instance to run the user callback against DB2.

### findings

no findings

## `_emitSafe`

Delivers Apply events and treats non-error listener failures as fatal.

### findings

no findings

## `close`

Closes Apply and DB2 after active execution settles, retaining the result for
repeated calls.

### findings

no findings

## `_fatal`

Reports an unrecoverable error and shuts down Apply.

### findings

no findings

## `_send`

Sends a reply to Gate and treats transport failure as fatal.

### findings

no findings

## `_writeReceipt`

Advances DB2's durable apply receipt and rejects a missing or nonadvancing update.

### findings

no findings

## `_execute`

Executes setup or a command batch in DB2, returning command outcomes and a
durable apply receipt.

### findings

- **Open: the apply contract does not cover SQL after a caught automatic
  rollback.** The callback receives the raw DB2 connection. If it catches a
  SQLite error that ended the transaction, later SQL can autocommit before the
  library detects the lost transaction. Those writes survive failure and can
  be repeated on replay. This applies to setup and command callbacks. The
  README should say that after a caught database error, if `db.isTransaction`
  is false, the callback must rethrow and issue no further SQL. That guidance
  has not yet been added. Such autocommit writes also bypass the generation
  check protecting the original transaction.

## `onReceive`

Handles Gate's startup, execution, and shutdown requests for Apply.

### findings

no findings
