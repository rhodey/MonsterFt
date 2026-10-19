# Gate

Function notes for [src/gate.v2.js](../src/gate.v2.js), in source order.
Trivial getters and delegation helpers are omitted, and nested callbacks are
covered with their enclosing function.

## `constructor`

Validates cluster and transport configuration and initializes the node's Gate.

### findings

no findings

## `_emitSafe`

Delivers an event to listeners while containing listener failures and reporting
them as errors where possible.

### findings

no findings

## `_request`

Sends a local request and waits for its matching reply, transport failure, or
timeout.

### findings

no findings

## `onReceive`

Accepts local component replies and cluster messages, resolving local requests
or queuing cluster input.

### findings

no findings

## `_hello`

Establishes contact with a local component, sharing Proc randomness and retrying
hello timeouts until the component responds or Gate closes.

### findings

no findings

## `open`

Coordinates local startup and reconciliation, then starts ordered protocol
processing and clock ticks.

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

Delivers agreed outputs to Apply, callers, event listeners, or remote cluster
nodes.

### findings

no findings

## `_event`

Updates Gate's public node status before delivering each agreed protocol event
to listeners.

### findings

no findings

## `_command`

Queues a public request and returns a promise for its eventual result.

### findings

no findings

## `_outcomes`

Validates command-result messages and converts them into public fulfilled or
rejected outcomes.

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
  retains the array, and the reply's result count is checked against its length.
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

Waits for an event that satisfies the supplied condition, rejecting if the
condition throws or the node shuts down.

### findings

no findings

## `awaitLeader`

Waits until a leader is known, optionally requiring a committed entry from the
current term.

### findings

no findings

## `_stop`

Stops input scheduling and rejects outstanding requests, queued work, and event
waits with the shutdown error.

### findings

no findings

## `_fatal`

Reports an unrecoverable error and stops Gate without requesting component
shutdown.

### findings

no findings

## `_closeComponents`

Requests shutdown of components that completed hello and reports completion
after all close replies succeed. Failed attempts can be retried explicitly.

### findings

- **Accepted policy: only one close failure is returned to the caller.**
  Gate waits for every close request to settle and rejects with the first
  failure in its request list. Each database owner reports its own close error
  locally, so additional owner diagnostics remain available.

## `close`

Stops Gate and waits for its local components to close, including after a
fatal failure.

### findings

no findings
