# Local Unix socket router

Function notes for [src/net/udxrouter.js](../src/net/udxrouter.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Validates endpoint names and the optional parent directory, and prepares
connection readiness.

### findings

no findings

## `_start`

Creates a private temporary directory and opens one listener per local endpoint.
The generated socket paths are available through `paths`.

### findings

no findings

## `_install`

Associates a connection with its endpoint and completes readiness when all endpoints connect. Losing an established connection stops the router.

### findings

no findings

## `_connectionError`

Reports a local connection failure as terminal.

### findings

no findings

## `send`

Sends an unchanged Buffer to a local endpoint. Disconnection or queue exhaustion stops the router.

### findings

no findings

## `_cleanup`

Removes the router's private temporary directory after sockets and listeners close. Caller-owned directories remain intact.

### findings

no findings

