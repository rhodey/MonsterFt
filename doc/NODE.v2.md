# RaftEngine

Function notes for [src/node.v2.js](../src/node.v2.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `validRecords`

Checks that replication data contains a nonempty array of valid term-and-payload
records.

### findings

no findings

## `constructor`

Validates membership and protocol options, restores the committed position, and
initializes Raft runtime state.

### findings

no findings

## `followers`

Returns the peers whose recent responses qualify them as live followers.

### findings

no findings

## `entry`

Returns a retained log record at the requested sequence, or null when it is
unavailable.

### findings

no findings

## `warn`

Reports a warning locally and prepares its protocol event with the appropriate
stack visibility.

### findings

no findings

## `change`

Publishes the current role, leader, log progress, and live followers.

### findings

no findings

## `request`

Sends an RPC and registers its response validation, completion callback, and
timeout.

### findings

no findings

## `receive`

Validates and routes incoming Raft messages and completes matching outstanding
requests.

### findings

no findings

## `start`

Starts election timing and publishes the initial Raft state.

### findings

no findings

## `resetElection`

Schedules a new election deadline for a node that is not the leader.

### findings

no findings

## `cancelLeadership`

Cancels leader replication work and rejects pending appends.

### findings

no findings

## `follower`

Transitions to follower state and notifies the protocol of the leadership
change.

### findings

no findings

## `advanceTerm`

Persists a newer election term and steps down to follower state.

### findings

no findings

## `elect`

Starts a new election, records the local vote, and requests votes from peers.

### findings

no findings

## `voteRequest`

Evaluates a candidate and records a granted vote before returning the vote
response.

### findings

no findings

## `voteResponse`

Collects election votes and starts leader replication when a quorum elects this
node.

### findings

no findings

## `appendNoop`

Establishes a committed entry in the current leader term, retrying failed
election no-ops while needed.

### findings

no findings

## `heartbeat`

Checks leader liveness and sends periodic replication or heartbeat requests.

### findings

no findings

## `appendEntry`

Appends a leader entry and waits for quorum commitment or failure.

### findings

no findings

## `finishAppend`

Completes a pending append once and schedules its callback.

### findings

no findings

## `replicate`

Replicates a bounded log page or heartbeat to a follower and handles progress,
backtracking, and retries.

### findings

no findings

## `checkCommit`

Advances commitment when a quorum has replicated a current-term entry and
releases the work it enables.

### findings

no findings

## `appendRequest`

Handles replication and heartbeats, reconciling follower history while
protecting committed entries.

### findings

no findings

## `stop`

Stops the engine and discards outstanding protocol work.

### findings

no findings

