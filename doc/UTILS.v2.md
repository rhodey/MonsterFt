# SQL helpers v2

Function notes for [src/utils.v2.js](../src/utils.v2.js), in source order.
Trivial helpers and direct delegation functions are omitted. Findings start
empty for the upcoming review; this does not indicate that the functions have
been reviewed.

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

no findings

## `createMeta`

Creates DB1's protocol metadata table and its initial singleton row.

### findings

no findings

## `createApply`

Creates DB2's Apply receipt table and its initial singleton row.

### findings

no findings
