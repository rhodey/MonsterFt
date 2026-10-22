# Error handling

Function notes for [src/error.js](../src/error.js), in source order.

`MONSTER_CORRUPT` covers invalid Monster records and protocol or Apply metadata.
`LOG_CORRUPT` covers invalid log rows, sequences, terms, and election fields.
`GENERATION_DIFF` stops an owner superseded by another startup.

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
