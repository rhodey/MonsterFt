# Protocol storage

Function notes for [src/storage.js](../src/storage.js), in source order.
Trivial helpers and getters are omitted, and nested callbacks are covered with
their enclosing function.

Each metadata table stores Gate's startup seed as its `generation`. All four
owners claim their databases before capture; later guarded transactions reject
a different generation. The generation is excluded from protocol and receipt
digests and survives donor adoption. Databases require the generation column;
there is no migration.

Claims wait on SQLite's existing writer exclusion: an active write transaction
can prevent replacement from opening. Concurrent startups can supersede one
another, so normal supervision still stops and reaps owners before replacement.

Proc owns teardown after store operation failures. Temporary readers close their
own connections without replacing an earlier operation error.

## `databasePaths`

Validates the base database path and returns the paths for the three Proc
databases and the Apply database.

### findings

no findings

## `fileExists`

Checks whether a database file exists, rejecting paths that are not files.

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

## `stateDigest`

Produces the digest used to compare captured Proc states.

### findings

no findings

## `receiptDigest`

Produces the digest used to compare captured Apply receipts.

### findings

no findings

## `copyProtocolState`

Replaces a Proc's stored protocol state with the selected donor's state and
checks that the replacement matches the selected digest.
Preserves the destination's generation.

### findings

no findings

## `checkFamily`

Checks whether the four database files form an established family or an
incomplete fresh family that can safely finish initialization.

### findings

no findings

## `openApplyDatabase`

Opens the Apply database, creating fresh receipt metadata when appropriate and
validating the stored receipt. Claims this startup's generation atomically.

### findings

no findings

## `ProtocolStore.constructor`

Validates the Proc role and prepares its database paths and log instance.

### findings

no findings

## `ProtocolStore.open`

Opens the Proc database and validates its metadata after checking the database
family, claiming this startup's generation.

### findings

no findings

## `ProtocolStore.checkGeneration`

Rejects work when another startup has claimed this Proc's database. Reuses a
prepared statement while reading the stored generation on every check.

### findings

no findings

## `ProtocolStore.capture`

Captures the three Proc states and Apply receipt, selecting a majority state
or rejecting startup when none exists. Requires all four owners to have claimed
the same generation.

### findings

no findings

## `ProtocolStore.adopt`

Brings this Proc's stored state into agreement with the captured majority,
rejecting changes to the captured state or ownership generation.

### findings

no findings

## `ProtocolStore.reconcile`

Reconciles the adopted protocol state with the captured Apply receipt and
returns the resulting metadata, rejecting a superseded owner.

### findings

no findings

## `ProtocolStore.close`

Closes the Proc database and clears its startup reconciliation state.

### findings

no findings
