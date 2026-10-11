# SQLiteLog

Function notes for [src/log.js](../src/log.js), in source order. Trivial getters
and direct delegation helpers are omitted, and nested callbacks are covered
with their enclosing method. Each findings area lists only unresolved items:
open, deferred, skipped by decision, or accepted. "None" means no unresolved
item is recorded. Review rules are in [RAFT.md](RAFT.md).

## `constructor`

Validates the database path and iteration batch size, records whether the log
uses an in-memory database, and initializes the database handle and cached log
and election metadata. The database remains closed until `open()` is called.

### findings

None.

## `_assertOpen`

Rejects operations on a closed log with `LOG_NOT_OPEN`.

### findings

None.

## `_validateSeq`

Checks that a sequence or term is a bigint within SQLite's supported
nonnegative signed-integer range. Callers can allow the empty-log sentinel
`-1n` and supply the error code and field name used in failures.

### findings

None.

## `_validateEntry`

Requires a `{ term, entry }` record with a bigint term between zero and
`MAX_SEQ` and a Buffer payload; an empty payload represents a no-op. Invalid
caller-supplied entries produce `ARGUMENT_ILLEGAL`, and invalid stored
entries produce `LOG_CORRUPT`.

### findings

None.

## `_validateElection`

Validates an election term's bigint type and supported bounds, and requires
the vote to be null or a nonempty string. Both writes and restoration use
this helper with the appropriate argument or corruption error code.

### findings

None.

## `_entryFromRow`

Validates a stored row's sequence, optionally checks it against the expected
sequence, and checks its term and payload types. Returns `{ term, entry }`
with a Buffer view of the stored payload, reporting invalid data as
`LOG_CORRUPT`.

### findings

None.

## `readHead`

Reads the newest entry, oldest retained sequence, and persisted election
record into the log's cached fields. Uses empty-log sentinels when there are
no entries. Requires a valid persisted election record even for an empty
log; a missing or invalid record is `LOG_CORRUPT`.

### findings

- **Skipped by decision:** the election term is not compared with the log
  head, and a saved vote is not checked against cluster membership. See
  `_startRaft` in RAFT.md.

## `open`

Creates the database directory when needed and opens SQLite with WAL and
FULL synchronous mode. A database with no schema objects is initialized in
one transaction, creating `raft_log`, `raft_election`, and the initial
`(0, null)` election row. An existing schema must contain both tables and an
election row; nothing is recreated or reconstructed. Then prepares
statements and loads cached metadata before marking the log open. On
failure, it resets the fields, attempts to close the database, and throws a
contextual error. Startup checks the head and cached metadata only. Historical
rows and sequence continuity are validated as they are iterated.

### findings

- **Accepted: a fully erased schema looks like a new database.** It is
  initialized as a fresh log, with term zero and no vote.

## `close`

Closes an open database and clears the prepared statements and cached
metadata. Returns immediately when already closed, and preserves the fields
if closing the database throws.

### findings

None.

## `append`

Validates one `{ term, entry }` record and requires its sequence to
immediately follow the current head. Inserts it, updates the cached head and
retained boundary, and returns the new sequence. An empty payload is bound as
a shared empty Buffer, because `node:sqlite` can bind a zero-byte view as
NULL.

### findings

- **Accepted: the cached head aliases the caller's Buffer.** SQLite stores
  its own copy, but `log.head` is the caller's payload. Modifying that Buffer
  afterward changes `head` but not `iter()` or a reopened log. Callers must
  not modify Buffers after passing them to the log. RaftNode's local append
  path passes the caller's Buffer without copying, so the same rule applies
  there.

## `appendBatch`

Validates a nonempty record array and its sequence range, then inserts the
batch in an immediate SQLite transaction. After commit, updates the cached
head and returns the first sequence. If the transaction fails, it attempts
to roll back and refresh the cached metadata before rethrowing.

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

Validates and persists the current term and voted-for node in the singleton
election record. Updates the corresponding cached fields and returns the
stored pair.

### findings

None.

## `trim`

Deletes entries after the supplied sequence and refreshes the cached log
metadata, doing nothing if the head is already at or before that point. The
default sequence of `-1n` removes all log entries.

### findings

None.

## `iter`

Validates the starting sequence and page size, then returns a generator whose
upper bound is the head sequence at the time of the call. The generator
fetches rows in pages, validates each returned row against the expected
sequence, and yields `{ term, entry }` records with contextual errors on
failure.

### findings

- **Accepted: iteration is not a snapshot.** Pages are fetched lazily and
  buffered between yields. If the log is trimmed and replaced between steps,
  an old prefetched row can be followed by a replacement row. RaftNode
  consumes each iterator synchronously, so this cannot happen there.
- **Accepted: a missing trailing range ends iteration without an error,**
  even before the captured head is reached. `test/04iter.js` tests this
  behavior. RaftNode's application and replication paths check that they
  received the range they required.

## `del`

Requires the log to be closed, then delegates deletion of the SQLite files
to the filesystem helper. Wraps failures with the log operation context.

### findings

None.
