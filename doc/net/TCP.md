# TCP transport

Function notes for [src/net/tcp.js](../../src/net/tcp.js), in source order.
Framing is shared with UDX and documented in [STREAM.md](STREAM.md).

## `tcpClient`

Connects to a TCP listener and returns a framed stream for sending and receiving
Buffers.

### findings

no findings

## `tcpServer`

Starts a TCP listener that delivers Buffer frames with their reply connections.

### findings

no findings
