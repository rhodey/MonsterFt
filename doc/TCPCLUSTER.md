# TCP cluster transport

Function notes for [src/net/tcpcluster.js](../src/net/tcpcluster.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Validates cluster endpoints and transport options. Sorted membership determines each pair's listening port without exchanging node IDs.

### findings

no findings

## `_start`

Opens the local peer-specific listeners and begins connecting to higher-ranked members. Startup does not wait for remote peers.

### findings

no findings

## `_install`

Associates a duplex connection with its configured peer and replaces any previous connection. Stale callbacks cannot affect its replacement.

### findings

no findings

## `_connectionError`

Reports current peer connection errors as local warnings.

### findings

no findings

## `_connect`

Makes one bounded connection attempt to a peer, arranging another attempt after failure.

### findings

no findings

## `_retry`

Schedules reconnection for an outgoing peer while the transport remains active.

### findings

no findings

## `send`

Sends an unchanged Buffer to a peer. Disconnected peers and full queues drop unsent frames for the protocol to retry.

### findings

no findings

## `_cancel`

Cancels dialing and reconnect timers and disables peer routing.

### findings

no findings

