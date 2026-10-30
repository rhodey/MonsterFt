# RaftEngine

Function notes for [src/raft.js](../src/raft.js), in source
order. Trivial helpers are omitted, and nested callbacks are covered with their
enclosing function.

## `constructor`

Prepares the Raft engine for a Proc and its cluster.

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

## `warn`

Reports a warning locally through the owning Proc.

### findings

no findings

## `event`

Publishes a protocol event for Gate with a snapshot of node status.

### findings

no findings

## `change`

Publishes a completed input's final node status, suppressing unchanged snapshots.

### findings

no findings

## `finishAppend`

Notifies a pending append's caller of completion or failure.

### findings

no findings

## `request`

Sends an RPC and reports its reply or timeout to the caller.

### findings

no findings

## `stop`

Stops Raft protocol work.

### findings

no findings

## `resetElection`

Schedules a new election deadline for a node that is not the leader.

### findings

no findings

## `start`

Starts election timing.

### findings

no findings

## `cancelLeadership`

Cancels leader replication work and rejects pending appends.

### findings

no findings

## `follower`

Transitions the node to follower state.

### findings

no findings

## `advanceTerm`

Adopts a newer election term as a follower.

### findings

no findings

## `voteSelf`

Starts an election for this node.

### findings

no findings

## `voteRequest`

Responds to a candidate's request for a vote.

### findings

no findings

## `voteReply`

Processes peer votes to determine whether this node has won its election.

### findings

no findings

## `appendEntry`

Appends a leader entry and waits for quorum commitment or failure.

### findings

- **Accepted: an append can wait for its deadline when peers cannot provide
  quorum.** Peer replication failures do not by themselves reject the append
  early; commitment, leadership change, shutdown, or its timeout settles it.

## `appendNoop`

Appends the leader's election no-op.

### findings

no findings

## `checkCommit`

Advances the committed log position when replication permits it.

### findings

no findings

## `heartbeatFollower`

Checks a follower's liveness and communicates the leader's log and commit positions.

### findings

no findings

## `heartbeat`

Checks leader liveness and sends periodic heartbeats.

### findings

no findings

## `replicate`

Replicates log entries to one follower.

### findings

- **Deferred: retention can cancel replication to a caught-up follower after
  a lost page ACK.** This behavior is reproduced in both v1 and v2. A missing
  ACK causes a timeout and retry from the same position; only an explicit
  `REPL_BACKTRACK` reply moves that position backward. If retention removes
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

## `appendRequest`

Processes incoming log replication and heartbeat requests.

### findings

no findings

## `receive`

Handles incoming Raft messages and reports whether their type is recognized.

### findings

no findings
