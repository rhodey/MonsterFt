# Local Unix socket router

Function notes for [src/net/udsrouter.js](../src/net/udsrouter.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Validates endpoint names and prepares private or explicitly configured socket paths and connection readiness.

### findings

no findings

## `_start`

Opens one listener per local endpoint without replacing existing paths. Default paths live in a private temporary directory.

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

