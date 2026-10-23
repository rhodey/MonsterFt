# SQLiteLog

Function notes for [src/log.js](../src/log.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `constructor`

Validates the file-backed log configuration and initializes a closed log
instance.

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

Converts a stored row into a validated log record and checks an expected
sequence when supplied.

### findings

no findings

## `readHead`

Restores the cached head, retained boundary, and election state from storage.

### findings

- **Skipped by decision:** the saved election term is not compared with the
  log head's term.
- **Skipped by decision:** a saved vote is not checked against cluster
  membership. Any null or nonempty string vote is accepted.

## `open`

Opens or initializes the log database and validates its schema and head
metadata.

### findings

- **Accepted helper behavior: an empty schema is initialized as a fresh log.**
  Normal node startup first checks the four-file database family and rejects a
  missing schema beside established state; see `checkFamily` in
  [STORAGE.md](STORAGE.md).

## `close`

Closes the database and clears the cached state after a successful close.

### findings

no findings

## `append`

Appends one validated record immediately after the current head and returns its
sequence.

### findings

- **Accepted: the cached head aliases the supplied Buffer.** SQLite stores
  its own copy, but `log.head` retains the supplied payload. Modifying it later
  changes the cache without changing stored rows. Internal callers must leave
  that Buffer unchanged.

## `appendBatch`

Appends a validated, nonempty batch within the caller’s transaction or a
transaction of its own, returning its first sequence.

### findings

- **Accepted: the cached head aliases the supplied Buffer,** as described
  under `append`.

## `election`

Saves the election term and vote and updates the cached election state.

### findings

no findings

## `trim`

Deletes entries after the supplied sequence and refreshes the cached log
boundaries.

### findings

no findings

## `iter`

Returns an iterator over validated records from the requested sequence through
the head captured when iteration was requested.

### findings

- **Accepted: a missing trailing range ends iteration without an error,**
  even before the captured head is reached. Callers requiring a particular
  entry must check that it was returned.

## `sizes`

Returns payload sizes for a bounded log range.

### findings

no findings

## `del`

Deletes the database files, requiring the log to be closed.

### findings

no findings
