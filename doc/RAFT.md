# RaftEngine

Function notes for [src/raft.js](../src/raft.js), in source
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

- **Deferred: an explicit `undefined` does not select some defaults.**
  `electionTimeout`, `appendTimeout`, `rpcMax`, and `rpcMaxBytes` override their
  defaults with `undefined` and then fail validation. Leave this behavior
  unchanged for now.

## `followers`

Returns the leader's live peers, sorted by node ID. Other roles return an empty
array.

### findings

no findings

## `entry`

Returns a retained log record at the requested sequence, or null when it is
unavailable.

### findings

no findings

## `event`

Publishes a protocol event with the role, leader, log and commit positions,
and live followers at the point it occurred.

### findings

no findings

## `warn`

Reports a warning to the owning Proc's local listeners.

### findings

no findings

## `change`

Publishes the final role, leader, log progress, and live followers for a completed
input. The initial snapshot is always published; unchanged snapshots are
suppressed.

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

Starts election timing.

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

Transitions to follower state and updates protocol work for the leadership
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

Checks leader liveness and sends periodic heartbeats.

### findings

no findings

## `heartbeatFollower`

Sends a follower a heartbeat independently of data replication. Uses its reply
to update liveness, check commitment, and resume eligible replication.

### findings

no findings

## `appendEntry`

Appends a leader entry and waits for quorum commitment or failure.

### findings

- **Accepted: an append can wait for its deadline when peers cannot provide
  quorum.** Peer replication failures do not by themselves reject the append
  early; commitment, leadership change, shutdown, or its timeout settles it.

## `finishAppend`

Completes a pending append once and schedules its callback.

### findings

no findings

## `replicate`

Replicates a bounded log page to a follower and handles progress,
backtracking, and retries. Warnings include ordinary backtracking replies.

### findings

- **Deferred: retention can cancel replication to a caught-up follower after
  a lost page ACK.** This behavior is reproduced in both v1 and v2. A missing
  ACK causes a timeout and retry from the same position; only an explicit
  `REPL_BACKTRACK` response moves that position backward. If retention removes
  the predecessor needed to construct the retry, the leader locally raises
  `REPL_FORGOT` and cancels replication to that follower for the current
  leadership, without receiving a rejection from it.

  With `keepTarget: 2` and `keepTrigger: 5`, delayed delivery and a lost ACK
  left all three nodes applied through 4 with identical retained entries 3–4.
  Heartbeats had communicated the commit, but the leader's unconfirmed retry
  still required deleted predecessor 2. Later appends advanced the other two
  nodes while that follower stayed at 4.

  Leave this unchanged for now. Retention settings should leave generous room
  for replication delays and retries, reducing the risk of deleting history
  before confirmation arrives. This is a mitigation, not a guarantee under
  prolonged message loss; revisit the behavior with a broader retention
  redesign.

  A possible smaller fix is to include `matched` in heartbeat ACKs, reporting
  the existing cached head sequence-and-term comparison without another SQL
  read. A matching reply in the current leader term could advance `matchIndex`
  and `nextIndex` using that heartbeat request's captured sequence and resume
  replication cancelled by retention. A nonmatching reply would still confirm
  only liveness. Older replies and retries must not undo the confirmed progress.
  This could address the caught-up follower case, but would not recover missing
  history or locate a shared prefix in divergent logs. This idea is also deferred;
  no implementation change is planned now.

- **Deferred: donor-copy repair can also require a leadership reset.** Once
  retention cancels replication to a target, copying a donor's databases and
  restarting only the donor and target does not reset the surviving leader's
  cancelled state. The target can remain at the copied checkpoint until the
  leader restarts or leadership changes. This requirement is still absent
  from [REPAIR.md](../REPAIR.md); it remains deferred with retention redesign.

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
