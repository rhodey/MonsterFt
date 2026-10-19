# SQL helpers

Function notes for [src/utils.js](../src/utils.js), in source order.
Trivial helpers and direct delegation functions are omitted.

## `readMeta`

Reads and validates the singleton protocol metadata in DB1.

### findings

no findings

## `readApply`

Reads and validates the singleton Apply receipt in DB2.

### findings

no findings

## `validateReceipt`

Validates an Apply receipt's sequence and hashes, including its initial state.

### findings

no findings

## `verifyEntry`

Checks that DB1 contains the referenced entry and that its term and payload
match the supplied hash.

### findings

no findings

## `copyApply`

Advances DB1's protocol metadata to a completed Apply receipt, recording a
pending command when applicable.

### findings

no findings

## `reconcileReceipt`

Checks that DB1's checkpoint and an Apply receipt are consistent, copying an
eligible receipt into DB1 when its checkpoint is behind.

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

## `createMeta`

Creates DB1's protocol metadata table and its initial singleton row.

### findings

no findings

## `createApply`

Creates DB2's Apply receipt table and its initial singleton row.

### findings

no findings
