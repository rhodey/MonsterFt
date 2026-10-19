# Protocol storage v2

Function notes for [src/storage.v2.js](../src/storage.v2.js), in source order.
Trivial helpers and getters are omitted, and nested callbacks are covered with
their enclosing function. Findings start empty for the upcoming review; this
does not indicate that the functions have been reviewed.

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

### findings

no findings

## `checkFamily`

Checks whether the four database files form an established family or an
incomplete fresh family that can safely finish initialization.

### findings

no findings

## `openApplyDatabase`

Opens the Apply database, creating fresh receipt metadata when appropriate and
validating the stored receipt.

### findings

no findings

## `ProtocolStore.constructor`

Validates the Proc role and prepares its database paths and log instance.

### findings

no findings

## `ProtocolStore.open`

Opens the Proc database and validates its metadata after checking the database
family.

### findings

no findings

## `ProtocolStore.capture`

Captures the three Proc states and Apply receipt, selecting a majority state
or rejecting startup when none exists.

### findings

no findings

## `ProtocolStore.adopt`

Brings this Proc's stored state into agreement with the captured majority,
rejecting changes to the captured state.

### findings

no findings

## `ProtocolStore.reconcile`

Reconciles the adopted protocol state with the captured Apply receipt and
returns the resulting metadata.

### findings

no findings

## `ProtocolStore.close`

Closes the Proc database and clears its startup reconciliation state.

### findings

no findings
