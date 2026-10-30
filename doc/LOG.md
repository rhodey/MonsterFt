# SQLiteLog

Function notes for [src/log.js](../src/log.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `constructor`

Prepares a file-backed log from the supplied configuration.

### findings

- **Deferred: an explicit `undefined` does not select the default.**
  Passing `iterStepSize: undefined` overrides the default and fails validation
  with `ARGUMENT_ILLEGAL`. Leave this behavior unchanged for now.

## `_assertOpen`

Rejects operations on a closed log.

### findings

no findings

## `_validateSeq`

Validates a sequence or term against the supported bigint range, optionally
allowing the empty-log sentinel.

### findings

no findings

## `_validateEntry`

Validates an append argument's term and Buffer payload.

### findings

no findings

## `_validateElection`

Validates an election term and vote supplied for storage.

### findings

no findings

## `_entryFromRow`

Validates and returns a stored log record for the expected sequence.

### findings

no findings

## `sizes`

Returns payload sizes for a bounded log range.

### findings

no findings

## `readHead`

Restores the cached head, retained boundary, and election state from storage.

### findings

- **Skipped by decision:** the saved election term is not compared with the
  log head's term.
- **Skipped by decision:** a saved vote is not checked against cluster
  membership. Any null or nonempty string vote is accepted.

## `iter`

Returns an iterator over validated records from the requested sequence through
the head captured when iteration was requested.

### findings

- **Accepted: a missing trailing range ends iteration without an error,**
  even before the captured head is reached. Callers requiring a particular
  entry must check that it was returned.

## `election`

Persists the election term and vote.

### findings

no findings

## `trim`

Deletes log entries after the supplied sequence.

### findings

no findings

## `append`

Appends one log record and returns its sequence.

### findings

- **Accepted: the cached head aliases the supplied Buffer.** SQLite stores
  its own copy, but `log.head` retains the supplied payload. Modifying it later
  changes the cache without changing stored rows. Internal callers must leave
  that Buffer unchanged.

## `appendBatch`

Appends a batch in the caller's transaction and returns its first sequence.

### findings

- **Accepted: the cached head aliases the supplied Buffer,** as described
  under `append`.

## `close`

Closes the log database.

### findings

no findings

## `del`

Deletes the database files, requiring the log to be closed.

### findings

no findings

## `open`

Opens the log database, initializing a fresh log when needed.

### findings

- **Accepted helper behavior: an empty schema is initialized as a fresh log.**
  Normal node startup first checks the four local databases and rejects a
  missing schema beside established state; see `checkLocals` in
  [STORAGE.md](STORAGE.md).
