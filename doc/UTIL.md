# General utilities

Function notes for [src/util.js](../src/util.js), in source order.

All hashes use XXH3-128 with seed zero and canonical 16-byte big-endian
output. The seed is fixed across nodes and restarts, independently of Gate's
generation. Hashing is synchronous; byte views are passed to the native addon
as Buffers without copying their contents.

## `isCid`

Checks that a request identifier is a nonempty string.

### findings

no findings

## `isTerm`

Checks that a term is a bigint within the supported nonnegative range.

### findings

no findings

## `isSeq`

Checks that a sequence is a bigint within the supported range, including the
empty-log sentinel.

### findings

no findings

## `isRpc`

Checks the basic shape and type field of an RPC message.

### findings

no findings

## `schemas` / `raftSchemas`

Define shared protocol fields and the accepted shapes of Raft requests and
`ack_err` headers. Additional fields remain allowed and values are never coerced.

### findings

no findings

## `firstIssue` / `validate`

Validate a message and return its original object, or report a concise invalid
field using the caller's error code. Union failures prefer the matching tagged
variant's field error.

### findings

no findings

## `hashParts`

Hashes consecutive byte sequences as one input without concatenating them.

### findings

no findings

## `createSQLiteDirectory`

Ensures the database's parent directory exists, reporting filesystem failures.

### findings

no findings

## `deleteSQLiteFiles`

Removes a database and its SQLite sidecar files, reporting filesystem failures.

### findings

no findings
