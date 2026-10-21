# Transport lifecycle

Function notes for [src/net/transport.js](../src/net/transport.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Prepares framing limits, lifecycle state, and the terminal failure promise shared by the optional transport helpers.

### findings

no findings

## `_emitSafe`

Delivers messages and diagnostics. Listener failures stop the transport, while error-listener exceptions cannot hide diagnostics from later listeners.

### findings

no findings

## `_fatal`

Stops the transport and reports its terminal failure locally.

### findings

no findings

## `_validateSend`

Checks that the transport can accept a send request and that its payload fits the configured frame limit.

### findings

no findings

## `_track`

Tracks accepted and connected sockets until their underlying resources have closed.

### findings

no findings

## `_listen`

Opens a framed listener and routes connection errors separately from listener failures.

### findings

no findings

## `_closeServer`

Begins closing a listener and retains its cleanup result.

### findings

no findings

## `open`

Opens the helper once. Startup failure stops its resources; close waits for cleanup.

### findings

no findings

## `stop`

Disables routing and cancels connections, listeners, and pending work without reporting an intentional shutdown as an error.

### findings

no findings

## `close`

Waits for startup cancellation and resource cleanup. Repeated calls retain the first result, including failure.

### findings

no findings

