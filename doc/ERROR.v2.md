# Error handling

Function notes for [src/error.v2.js](../src/error.v2.js), in source order.
Findings sections are placeholders for the upcoming review.

## `ErrorWithCode.constructor`

Creates an error with library and SQLite error codes.

### findings

no findings

## `wrapError`

Normalizes thrown values into coded errors, preserving available stack traces.
Supports message prefixes and explicit code overrides.

### findings

no findings
