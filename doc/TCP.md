# TCP transport

Function notes for [src/tcp.js](../src/tcp.js), in source order.
Trivial constructors are omitted; nested callbacks are covered with their
enclosing function.

## `FrameStream._transform`

Validates outgoing Buffer frames for transmission over TCP. Object mode
preserves message boundaries while buffers wait for encryption.

### findings

no findings

## `tcpServer`

Starts an encrypted TCP server that delivers received frames to the supplied
callback. Encrypted-record lengths delimit messages without an outer codec.
Provides a reply stream per connection and reports connection failures.

### findings

no findings

## `tcpClient`

Connects to an encrypted TCP server and returns a stream for sending frames.
Delivers received frames to the supplied callback and reports connection failures.

### findings

no findings
