# Certification

Function notes for [src/certify.js](../src/certify.js), in source order.
Nested callbacks are covered with their enclosing function.

## `requireFile`

Requires a database path to identify an existing normal file.

### findings

no findings

## `clearFence`

Reconciles a Proc's metadata with the Apply receipt and clears its repair
fence, rejecting unresolved commands unless explicitly allowed.

### findings

no findings

## `certify`

Certifies an offline database family by validating the matching copies, adopting
its majority protocol state, reconciling the Apply receipt, and clearing repair
fences. Returns the agreed applied sequence.

### findings

no findings
