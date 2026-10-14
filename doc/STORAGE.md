# Protocol storage

Function notes for [src/storage.js](../src/storage.js), in source order.
Trivial helpers and getters are omitted, and nested callbacks are covered with
their enclosing function.

Each metadata table stores Gate's startup seed as its `generation`. All four
owners claim their databases before capture; later guarded transactions reject
a different generation. The generation is excluded from protocol and receipt
digests and survives donor adoption. Databases require the generation column;
there is no migration.

Digests use canonical 16-byte XXH3-128 values. The hash format change requires
fresh databases and matching software on every local component and cluster
member; existing hashes are not converted.

Claims wait on SQLite's existing writer exclusion: an active write transaction
can prevent replacement from opening. Concurrent startups can supersede one
another, so normal supervision still stops and reaps owners before replacement.

Proc owns teardown after store operation failures. Temporary readers close their
own connections without replacing an earlier operation error.

## `databasePaths`

Returns the database paths for the three Procs and Apply.

### findings

no findings

## `fileExists`

Checks whether a database file exists, rejecting paths that are not files.

### findings

no findings

## `receiptDigest`

Produces the digest used to compare captured Apply receipts.

### findings

no findings

## `stateDigest`

Produces the digest used to compare captured Proc states.

### findings

no findings

## `readDatabase`

Runs a reader against a consistent, read-only view of an existing database.

### findings

no findings

## `readState`

Reads and validates a Proc database's election state, protocol metadata, and
log head.

### findings

no findings

## `checkLocals`

Checks whether the local databases can be opened or initialized.

### findings

no findings

## `openApplyDatabase`

Opens or initializes the Apply database for the current startup generation.

### findings

no findings

## `copyDb1Into`

Replaces a Proc's stored protocol state with the selected donor's state.

### findings

no findings

## `ProtocolStore.constructor`

Prepares a Proc's database paths and log.

### findings

no findings

## `ProtocolStore.checkGeneration`

Rejects work when another startup has claimed this Proc's database.

### findings

no findings

## `ProtocolStore.close`

Closes the Proc database.

### findings

no findings

## `ProtocolStore.open`

Opens or initializes the Proc database for the current startup generation.

### findings

no findings

## `ProtocolStore.capture`

Selects and captures a majority protocol state with the Apply receipt.

### findings

no findings

## `ProtocolStore.adopt`

Brings this Proc's stored protocol state into agreement with the captured majority.

### findings

no findings

## `ProtocolStore.reconcile`

Reconciles the adopted protocol state with the captured Apply receipt.

### findings

no findings
