# TCP transport

Function notes for [src/net/tcp.js](../../src/net/tcp.js), in source order.
Framing is shared with UDX and documented in [STREAM.md](STREAM.md).

## `tcpServer`

Starts a plaintext TCP listener and delivers unchanged Buffer frames with their
reply connections. Supports a bind host and an optional connection callback;
error callbacks identify the connection, or use null for listener errors.

### findings

no findings

## `tcpClient`

Connects to a TCP listener and returns a framed stream for sending and receiving
Buffers. Accepts frame limits, dial cancellation, and a connection timeout.

### findings

no findings
