# Error handling

Function notes for [src/error.js](../src/error.js), in source order.

`RPC_ILLEGAL` covers invalid message formats, unsupported types, inconsistent
fields, and fixed routing contracts. `RAFT_ILLEGAL` covers violations of Raft
state or transaction requirements. `MONSTER_ILLEGAL` covers violations of
startup, execution order, pending work, or local coordination state, including
the Proc executor. These categories do not determine warn or fatal handling.

`MONSTER_CORRUPT` covers invalid Monster records and protocol or Apply metadata.
`LOG_CORRUPT` covers invalid log rows, sequences, terms, and election fields.
`GENERATION_DIFF` stops an owner superseded by another startup.

Repair states are `REPAIR_NONE` (0), `REPAIR_NO_QUORUM` (10019), and
`REPAIR_OUTSIDE` (10020). Repair errors use the same codes as their states.

## `ErrorWithCode.constructor`

Creates an error with library and SQLite error codes.

### findings

no findings

## `wrapError`

Normalizes thrown values into coded errors, preserving available stack traces.
Supports message prefixes and explicit code overrides.

### findings

- **Accepted limitation: exotic thrown values can make normalization throw.**
  `String(err)` throws for a null-prototype object, and modifying a frozen
  `ErrorWithCode` throws. Leave these JavaScript-specific cases unchanged.
