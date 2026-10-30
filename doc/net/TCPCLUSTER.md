# TCP cluster transport

Function notes for [src/net/tcpcluster.js](../../src/net/tcpcluster.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Configures TCP routing among cluster members.

### findings

no findings

## `_cancel`

Cancels pending connection work and stops peer routing.

### findings

no findings

## `_connectionError`

Reports current peer connection errors as local warnings.

### findings

no findings

## `send`

Sends a frame to a peer or drops it when disconnected or overloaded.

### findings

no findings

## `_retry`

Schedules a peer reconnection attempt.

### findings

no findings

## `_install`

Installs a peer's incoming or outgoing connection.

### findings

no findings

## `_connect`

Attempts to establish an outgoing peer connection.

### findings

no findings

## `_start`

Starts the cluster transport without waiting for remote peers.

### findings

no findings
