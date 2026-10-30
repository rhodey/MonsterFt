# Gate

Function notes for [src/gate.js](../src/gate.js), in source order.
Trivial getters and delegation helpers are omitted, and nested callbacks are
covered with their enclosing function.

## `constructor`

Prepares Gate for the cluster and its local components.

### findings

no findings

## `_emitSafe`

Delivers Gate events and handles listener failures.

### findings

no findings

## `_emitEvent`

Publishes agreed protocol events with the corresponding public node status.

### findings

no findings

## `_clientResult`

Completes a pending public request with an agreed command result or error.

### findings

no findings

## `_clientApiCall`

Submits a public operation and returns a promise for its result.

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

## `awaitEvent`

Waits for an event satisfying the caller's condition.

### findings

no findings

## `awaitLeader`

Waits until a leader is known, optionally requiring a committed entry from the
current term.

### findings

no findings

## `_stop`

Stops Gate's processing and cancels outstanding work.

### findings

no findings

## `stop`

Stops Gate's local work without closing the other components.

### findings

no findings

## `_fatal`

Stops Gate and reports an unrecoverable local error.

### findings

no findings

## `_request`

Sends a local request and waits for its reply or failure.

### findings

no findings

## `_hello`

Establishes contact with a local component for the current startup.

### findings

no findings

## `_closeComponents`

Closes the local components that participated in startup.

### findings

- **Accepted policy: only one close failure is returned to the caller.**
  Gate waits for every close request to settle and rejects with the first
  failure in its request list. Each database owner reports its own close error
  locally, so additional owner diagnostics remain available.

## `close`

Stops Gate and waits for coordinated local component shutdown.

### findings

no findings

## `drainCmd`

Drains outstanding commands and closes the node.

### findings

no findings

## `_scheduleTick`

Schedules periodic protocol clock inputs.

### findings

no findings

## `_release`

Delivers agreed outputs to their local or remote destinations.

### findings

- **Deferred optimization: routing parses the complete output.** Gate
  decodes all of an agreed output to read its destination and message type,
  even when forwarding its original bytes to a remote node or Apply.
  Separately readable routing metadata could avoid that parse, but requires
  a framing change. Destination and request identity must remain covered by
  output agreement; preserve the current forwarding of exact agreed bytes.

## `_runInput`

Coordinates three-Proc execution and output agreement for one input.

### findings

no findings

## `_schedulePump`

Schedules ordered processing of queued inputs.

### findings

no findings

## `onReceive`

Handles incoming local replies and cluster messages.

### findings

- **Accepted: malformed replies during explicit cleanup may end in
  `LOCAL_TIMEOUT`.** Gate is already stopped, and a corrupt reply may have no
  usable CID. Leave the close request pending rather than infer its identity.
  If no valid reply arrives, cleanup rejects without emitting `close`.

## `open`

Coordinates local startup and prepares the node for protocol processing.

### findings

no findings
