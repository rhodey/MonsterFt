# SQL helpers

Function notes for [src/utils.js](../src/utils.js), in source order.
Trivial helpers and direct delegation functions are omitted.

## Metadata and receipt schemas

Define valid stored generations, DB1 metadata, and Apply receipts, including
their initial states.

### findings

no findings

## `validateReceipt`

Validates an Apply receipt's sequence and hashes, including its initial state.

### findings

no findings

## `checkGeneration`

Rejects a corrupt generation or one belonging to a different startup.

### findings

no findings

## `claimGeneration`

Claims database ownership for the supplied startup generation.

### findings

no findings

## `verifyEntry`

Verifies that a stored log entry matches an expected hash.

### findings

no findings

## `createApply`

Initializes DB2's Apply receipt metadata.

### findings

no findings

## `createMeta`

Initializes DB1's protocol metadata.

### findings

no findings

## `readApply`

Reads and validates the singleton Apply receipt in DB2.

### findings

no findings

## `readMeta`

Reads and validates the singleton protocol metadata in DB1.

### findings

no findings

## `copyApplyInto`

Records a completed Apply receipt in DB1's protocol metadata.

### findings

no findings

## `reconcileReceipt`

Reconciles DB1's protocol progress with a completed Apply receipt.

### findings

- **Accepted: reconciliation checks entry identity, not entry type.** A
  retained entry must match the receipt's sequence and hash, but need not
  decode as CMD or sequence-zero setup. Ordinary protocol application still
  validates entry types. A matching receipt alone does not detect a manually
  cleared `pending_cmd_seq`.
- **Skipped by decision:** initial protocol metadata and an initial receipt
  are accepted even when retention has removed sequence zero. Ordinary apply
  later reports the missing entry as `MONSTER_CORRUPT`; that later rejection
  is acceptable.
