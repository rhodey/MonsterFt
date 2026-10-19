# SQLiteLog

Function notes for [src/log.v2.js](../src/log.v2.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `constructor`

Validates the file-backed log configuration and initializes a closed log
instance.

### findings

no findings

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

Validates a record’s term and Buffer payload, distinguishing invalid arguments
from corrupt stored entries.

### findings

no findings

## `_validateElection`

Validates an election term and vote for storage or restoration.

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

no findings

## `open`

Opens or initializes the log database and validates its schema and head
metadata.

### findings

no findings

## `close`

Closes the database and clears the cached state after a successful close.

### findings

no findings

## `append`

Appends one validated record immediately after the current head and returns its
sequence.

### findings

no findings

## `appendBatch`

Appends a validated, nonempty batch within the caller’s transaction or a
transaction of its own, returning its first sequence.

### findings

no findings

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

no findings

## `sizes`

Returns payload sizes for a bounded log range.

### findings

no findings

## `del`

Deletes the database files, requiring the log to be closed.

### findings

no findings

