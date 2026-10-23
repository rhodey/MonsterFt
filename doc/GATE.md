# Gate

Function notes for [src/gate.js](../src/gate.js), in source order.
Trivial getters and delegation helpers are omitted, and nested callbacks are
covered with their enclosing function.

## `constructor`

Validates cluster and transport configuration and initializes the node's Gate.

### findings

no findings

## `_emitSafe`

Delivers events and stops Gate when a non-error listener throws. Reports errors
before any resulting successful close notification, without repeating teardown.

### findings

no findings

## `_request`

Sends a local request and waits for its matching reply, transport failure, or
timeout.

### findings

no findings

## `onReceive`

Validates local replies against their requests and verifies cluster frames before
queuing their original bytes. Invalid remote senders and frames warn and are
discarded; local failures remain fatal. The Procs independently decode and
validate cluster envelopes, warning locally on remote failures.

### findings

- **Accepted: malformed replies during explicit cleanup may end in
  `LOCAL_TIMEOUT`.** Gate is already stopped, and a corrupt reply may have no
  usable CID. Leave the close request pending rather than infer its identity.
  If no valid reply arrives, cleanup rejects without emitting `close`.

## `_hello`

Establishes contact with a local component and shares the startup seed used for
database generations and Proc randomness. Retries hello timeouts until the
component responds or Gate closes.

### findings

no findings

## `open`

Coordinates local startup and reconciliation, then starts ordered protocol
processing and clock ticks. All four owners claim their generations before capture.

### findings

no findings

## `_scheduleTick`

Schedules periodic clock input without accumulating multiple queued ticks.

### findings

no findings

## `_schedulePump`

Dispatches queued inputs in order with a shared tick count and timestamp,
waiting for each input to complete before starting the next.

### findings

no findings

## `_runInput`

Collects Proc completions and releases matching outputs once A and another Proc
agree, while requiring all three completions before the input finishes.

### findings

no findings

## `_release`

Validates and delivers agreed outputs to Apply, callers, event listeners, or
remote cluster nodes. Malformed command results stop Gate; valid command
rejections remain nonfatal.

### findings

- **Deferred optimization: routing parses the complete output.** Gate
  decodes all of an agreed output to read its destination and message type,
  even when forwarding its original bytes to a remote node or Apply.
  Separately readable routing metadata could avoid that parse, but requires
  a framing change. Destination and request identity must remain covered by
  output agreement; preserve the current forwarding of exact agreed bytes.

## `_event`

Updates Gate's public node status before delivering each agreed protocol event
to listeners. A `change` payload contains only the role, leader, log and commit
positions, and live followers.

### findings

no findings

## `_command`

Queues a public request and returns a promise for its eventual result.

### findings

no findings

## `append`

Submits one command and returns its sequence and result, or rejects with the
command error.

### findings

- **Accepted input-ownership policy:** callers must leave a submitted Buffer
  unchanged until the operation settles.

## `appendBatch`

Submits a nonempty command batch and returns its sequence and individual item
outcomes.

### findings

- **Accepted input-ownership policy:** callers must leave the submitted array
  and its Buffers unchanged until the operation settles. The queued request
  retains the array, and the reply's result count is checked against the original
  item count.
- **Accepted: sparse arrays are unsupported.** The public `every` check skips
  holes; encoding and Proc validation can subsequently reject the request.
  Leave this JavaScript-specific case unchanged, as noted in
  [AGENTS.md](../AGENTS.md).

## `drainCmd`

Stops accepting new commands and waits for protocol draining to finish before
closing the node.

### findings

no findings

## `awaitEvent`

Waits for a matching event, rejecting if the condition throws. Shutdown rejects
ordinary waits; close waits observe successful cleanup, including success before
registration, or reject if cleanup fails. Waiting never starts cleanup.

### findings

no findings

## `awaitLeader`

Waits until a leader is known, optionally requiring a committed entry from the
current term.

### findings

no findings

## `_stop`

Stops input scheduling and rejects outstanding requests, queued work, and
non-close event waits with the shutdown error. Retains the last protocol status
without emitting `change`.

### findings

no findings

## `_fatal`

Stops Gate's local work and reports an unrecoverable error without requesting
component shutdown.

### findings

no findings

## `_closeComponents`

Requests shutdown of components that completed hello and reports completion
after all close replies succeed. Repeated calls share the same result, including
failure, without retrying cleanup.

### findings

- **Accepted policy: only one close failure is returned to the caller.**
  Gate waits for every close request to settle and rejects with the first
  failure in its request list. Each database owner reports its own close error
  locally, so additional owner diagnostics remain available.

## `stop`

Permanently stops Gate's local work without requesting owner shutdown or
emitting `close`. Repeated calls preserve the original shutdown error.

### findings

no findings

## `close`

Permanently stops Gate and waits for its local components to close, including
after a fatal failure.

### findings

no findings
