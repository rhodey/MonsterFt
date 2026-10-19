# Error handling

Function notes for [src/error.v2.js](../src/error.v2.js), in source order.

`MONSTER_CORRUPT` covers invalid Monster records and protocol or Apply metadata.
`LOG_CORRUPT` covers invalid log rows, sequences, terms, and election fields.

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
