# SQLiteLog

Function notes for [src/log.js](../src/log.js), in source order. Trivial getters
and direct delegation helpers are omitted, and nested callbacks are covered
with their enclosing method. Each findings area lists only unresolved items:
open, deferred, skipped by decision, or accepted. "None" means no unresolved
item is recorded. Review rules are in [RAFT.md](RAFT.md).

## `constructor`

Validates the log configuration and initializes a closed log instance.

### findings

None.

## `_assertOpen`

Rejects operations on a closed log with `LOG_NOT_OPEN`.

### findings

None.

## `_validateSeq`

Validates a sequence or term against the supported bigint range, optionally
allowing the empty-log sentinel `-1n`.

### findings

None.

## `_validateEntry`

Validates a `{ term, entry }` record's term and Buffer payload, allowing empty
payloads. Distinguishes invalid arguments from corrupt stored entries.

### findings

None.

## `_validateElection`

Validates an election term and vote for storage or restoration.

### findings

None.

## `_entryFromRow`

Converts a stored row into a validated `{ term, entry }` record, optionally
requiring a specific sequence. Reports invalid stored data as `LOG_CORRUPT`.

### findings

None.

## `readHead`

Refreshes the cached log head, retained boundary, and election state from
storage. Validates the restored metadata, including for an empty log.

### findings

- **Skipped by decision:** the election term is not compared with the log
  head, and a saved vote is not checked against cluster membership. See
  `_startRaft` in RAFT.md.

## `open`

Opens the database, initializing a new log or restoring an existing one.
Validates the schema and head metadata, leaving historical row validation to
iteration. Cleans up and reports errors if opening fails.

### findings

- **Accepted: a fully erased schema looks like a new database.** It is
  initialized as a fresh log, with term zero and no vote.

## `close`

Closes the database and clears the log's cached state. Leaves the log intact
if closing fails.

### findings

None.

## `append`

Appends one validated `{ term, entry }` record immediately after the current
head and returns its sequence.

### findings

- **Accepted: the cached head aliases the caller's Buffer.** SQLite stores
  its own copy, but `log.head` is the caller's payload. Modifying that Buffer
  afterward changes `head` but not `iter()` or a reopened log. Callers must
  not modify Buffers after passing them to the log. RaftNode's local append
  path passes the caller's Buffer without copying, so the same rule applies
  there.

## `appendBatch`

Appends a validated, nonempty batch of records atomically and returns its
first sequence. Attempts recovery before reporting a failed append.

### findings

- **Accepted: the cached head aliases the caller's Buffer,** as in `append`.
- **Accepted in current Raft usage: a failed rollback leaves an active
  transaction.** The recovery path suppresses rollback failures and
  refreshes the cache from rows the connection can see, which may be
  uncommitted. The log stays open, and a later `append()` reports success
  for an entry that other connections cannot see and a reopen discards.
  RaftNode treats any failed batch append as fatal and begins shutdown
  synchronously, so it never uses that connection again. Standalone callers
  must stop using the connection after such a failure.

## `election`

Validates and saves the election term and vote, returning the stored pair.

### findings

None.

## `trim`

Deletes entries after the supplied sequence. The default sequence of `-1n`
removes all log entries.

### findings

None.

## `iter`

Returns an iterator over validated `{ term, entry }` records from the requested
sequence through the head at the time of the call.

### findings

- **Accepted: iteration is not a snapshot.** Pages are fetched lazily and
  buffered between yields. If the log is trimmed and replaced between steps,
  an old prefetched row can be followed by a replacement row. RaftNode
  consumes each iterator synchronously, so this cannot happen there.
- **Accepted: a missing trailing range ends iteration without an error,**
  even before the captured head is reached. `test/04iter.js` tests this
  behavior. RaftNode's apply and replication paths check that they
  received the range they required.

## `sizes`

Returns entry payload sizes for a bounded range, allowing replication pages
to be limited by bytes.

### findings

None.

## `del`

Deletes the database files, requiring the log to be closed.

### findings

None.
