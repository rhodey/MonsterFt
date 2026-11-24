# TCP cluster transport

Function notes for [src/net/tcpcluster.js](../../src/net/tcpcluster.js), in source order.
Trivial getters and forwarding methods are omitted.

## `constructor`

Configures a single listening port for each member using `{ host, port }`. Each node dials the other members independently.

### findings

no findings

## `_cancel`

Cancels pending connection work and stops peer routing.

### findings

no findings

## `_connectionError`

Reports framing and socket errors on accepted connections as local warnings.

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

## `_installOutgoing`

Installs the connection for sends to a peer and schedules a new dial when it closes.

### findings

no findings

## `_connect`

Attempts to establish an outgoing peer connection.

### findings

no findings

## `_start`

Binds one listener and starts dialing without waiting for the other nodes. Each accepted stream emits individual Buffer frames. Incoming connections can overlap during reconnect. The cluster protocol reads the sender ID from each frame.

### findings

no findings
