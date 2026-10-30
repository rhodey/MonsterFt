# Local Unix socket router

Function notes for [src/net/udxrouter.js](../../src/net/udxrouter.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Configures routing for named local endpoints.

### findings

no findings

## `_cleanup`

Removes the router's temporary socket directory.

### findings

no findings

## `_connectionError`

Reports a local connection failure as terminal.

### findings

no findings

## `send`

Sends a frame to a local endpoint, treating delivery failure as terminal.

### findings

no findings

## `_install`

Registers a local endpoint connection and tracks readiness.

### findings

no findings

## `_start`

Starts listeners for the named local endpoints.

### findings

no findings
