# Unix-domain transport

Function notes for [src/net/udx.js](../src/net/udx.js), in source order.
Framing is shared with TCP and documented in [STREAM.md](STREAM.md).

## `udxServer`

Listens on a Unix socket and delivers framed duplex connections to the caller.
Reports listening and connection failures without replacing existing socket paths.

### findings

no findings

## `udxClient`

Connects to a Unix socket and returns a framed duplex connection. Accepts frame
limits and cancellation, and rejects unsuccessful connection attempts.

### findings

no findings
