# Unix-domain transport

Function notes for [src/net/udx.js](../../src/net/udx.js), in source order.
Framing is shared with TCP and documented in [STREAM.md](STREAM.md).

## `udxClient`

Connects to a Unix socket and returns a framed duplex connection.

### findings

no findings

## `udxServer`

Listens on a Unix socket and delivers framed duplex connections to the caller.

### findings

no findings
