# General utilities

Function notes for [src/util.js](../src/util.js), in source order.

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

## `createSQLiteDirectory`

Ensures the database's parent directory exists, reporting filesystem failures.

### findings

no findings

## `deleteSQLiteFiles`

Removes a database and its SQLite sidecar files, reporting filesystem failures.

### findings

no findings
