# TCP cluster transport

Function notes for [src/net/tcpcluster.js](../../src/net/tcpcluster.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Validates cluster endpoints and transport options. Sorted membership determines
each sender's listening port at every destination without exchanging node IDs.

### findings

no findings

## `_start`

Opens a listener and a separate outgoing connection for every other member.
Startup does not wait for remote peers.

### findings

no findings

## `_install`

Associates a connection with its peer and message direction, replacing any
previous connection in that direction. Incoming connections deliver messages;
outgoing connections reject unexpected received frames. Stale callbacks cannot
affect the replacement or the opposite direction.

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

Sends an unchanged Buffer through a peer's outgoing connection. Disconnected
peers and full queues drop unsent frames for the protocol to retry.

### findings

no findings

## `_cancel`

Cancels dialing and reconnect timers and disables peer routing.

### findings

no findings

