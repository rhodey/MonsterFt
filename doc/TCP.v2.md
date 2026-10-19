# TCP transport

Function notes for [src/tcp.v2.js](../src/tcp.v2.js), in source order.
Trivial constructors are omitted; nested callbacks are covered with their
enclosing function. Findings sections are placeholders for the upcoming review.

## `FramePackrStream._transform`

Validates and encodes outgoing Buffer frames for transmission over TCP.

### findings

no findings

## `requireFrame`

Rejects received TCP messages that are not Buffer frames.

### findings

no findings

## `tcpServer`

Starts an encrypted TCP server that delivers received frames to the supplied
callback. Provides a reply stream per connection and reports connection failures.

### findings

no findings

## `tcpClient`

Connects to an encrypted TCP server and returns a stream for sending frames.
Delivers received frames to the supplied callback and reports connection failures.

### findings

no findings
