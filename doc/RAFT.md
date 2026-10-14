# RaftNode

Function notes for [src/node.js](../src/node.js), in source order, followed by
the error, TCP, and stream helpers it depends on. Trivial getters and direct
delegation helpers are omitted, and nested callbacks are covered with their
enclosing method. Each findings area lists only unresolved items: open,
deferred, skipped by decision, or accepted. "None" means no unresolved item
is recorded.

## Review rules

- Report a finding only when a supported trigger reaches it: public
  operations, real election timers, genuine protocol messages, transport
  delivery, or documented callbacks. Read-only instrumentation is useful,
  but tests that call private methods or edit internal state do not show
  that a failure is reachable.
- Keep the accepted and deferred items here, in [AGENTS.md](../AGENTS.md),
  [LOG.md](LOG.md), and [MONSTER.md](MONSTER.md) unless the user
  reopens them.

## Accepted limitations

- **Exotic thrown values:** a frozen `ErrorWithCode` or a null-prototype
  object makes `wrapError` throw. See `_apply` and `wrapError`.
- **Sparse arrays:** out of scope under AGENTS.md. See `_routeApply`.
- **Input ownership:** callers must not change submitted Buffers or batch
  arrays until the operation settles. See `_fwdToLeader` and LOG.md.
- **Timer bounds:** timeout options require positive safe integers, with no
  upper bound. Values beyond Node's timer range, 2^31-1 ms, are accepted, and
  Node then fires those timers after 1 ms with a warning.
- **Clock:** a backwards `Date.now()` jump can disrupt elections, as noted in
  AGENTS.md.
- **Versions:** mixed-version clusters are unsupported.

## `constructor`

Validates node configuration and initializes the log, transport, and Raft
state. Establishes transport-error reporting and fatal shutdown handling.

### findings

- **Deferred: an explicit `undefined` does not select a default.**
  `{ ...defaults, ...opts }` keeps it, so `electionTimeout`, `appendTimeout`,
  `rpcMax`, `rpcMaxBytes`, `applyMax`, and SQLiteLog's `iterStepSize` throw
  `ARGUMENT_ILLEGAL`. `apply` and `quorum` do fall back to their defaults.
  Decision: valid, but not a current focus.

## `_throwIfClosing`

Throws `NODE_NOT_OPEN` once shutdown has begun.

### findings

None.

## `_emitSafe`

Emits an event without letting a listener's exception prevent delivery to
later listeners. Reports listener failures as error events.

### findings

None.

## `_change`

Emits a `change` event describing the node's current state.

### findings

None.

## `_stopTimers`

Clears the election timeout and heartbeat interval.

### findings

None.

## `_isQuorum`

Checks whether the supplied peers and eligible local node form a quorum.
Defaults to the current followers.

### findings

None.

## `_isQuorumErr`

Checks whether the supplied peer failures make quorum impossible.

### findings

None.

## `_persistElection`

Saves the current election term and vote when they have changed. Returns
whether persistence succeeded, reporting failure as fatal.

### findings

None.

## `_replyVote`

Replies to a vote request only after ensuring any granted vote is durable.

### findings

None.

## `_raceShutdown`

Cancels an operation's wait on shutdown while allowing an already-completing
operation to surface its more specific result.

### findings

None.

## `_delay`

Waits for a delay, rejecting if shutdown begins first.

### findings

None.

## `_awaitResponse`

Waits for a valid response from the expected peer to a particular request.
Rejects on cancellation or deadline failure.

### findings

None.

## `_sendAndAwaitResponse`

Sends an RPC and waits for its response within the supplied timeout. Rejects
new requests after shutdown begins.

### findings

None.

## `_sendAndAwaitAck`

Sends ping, replication, or forwarded-append requests and validates their
acknowledgements. Reports RPC errors and updates peer liveness for replies
in the current term.

### findings

None.

## `_subscribeApply`

Creates a waiter for apply results covering an inclusive sequence range.

### findings

None.

## `_routeApply`

Delivers apply results to matching waiters and resolves completed waits.

### findings

- **Accepted: holes in an apply result array are never routed.**
  `forEach` skips holes, and `_apply` checks only the array length. A waiter
  covering a hole never resolves, and no deadline rescues it because
  replication has already succeeded. Results filled only for non-null
  payloads leak one never-settled no-op waiter per leadership. A fully sparse
  `new Array(n)` leaves leader-local appends pending, and forwarded appends
  time out, although the commands were applied. Decision: the apply callback
  must return one result per entry; Arrays with holes remain out of scope.

## `_rejectApplyWaiters`

Rejects every currently registered apply waiter with the supplied
error.

### findings

None.

## `_apply`

Applies committed entries in order through the configured callback. Delivers
results, advances the applied boundary, and emits `apply`. Failures reject
apply waiters and trigger fatal shutdown.

### findings

- **Accepted limitation: exotic thrown values can stall apply.** The
  catch block calls `wrapError` before rejecting waiters or starting fatal
  shutdown. If the apply callback throws a frozen `ErrorWithCode` or a
  null-prototype object, `wrapError` throws and skips both steps.
  `_applyPrev` stays rejected, so all later apply calls are skipped. The
  node stays open with no error event, and the leader's append never
  settles. Decision: leave unchanged as a JavaScript-specific case.

## `_beginLeaderReady`

Creates a readiness waiter for the supplied leadership term.

### findings

None.

## `_cancelLeaderReady`

Rejects and clears the current leadership readiness waiter if one exists.

### findings

None.

## `_replicationActive`

Checks whether peer replication remains valid for the current leadership.

### findings

None.

## `_replicationState`

Returns or creates a peer's replication state for the requested term.

### findings

None.

## `_resolveReplicationWaiters`

Resolves waits for log ranges that a peer has confirmed replicated.

### findings

None.

## `_cancelReplicationState`

Cancels a peer's replication work and rejects its pending waits.

### findings

None.

## `_cancelReplication`

Cancels leadership readiness and all peer replication work with the supplied
error.

### findings

None.

## `_reset`

Resets volatile Raft state and cancels timers and replication work.

### findings

None.

## `_toFollower`

Transitions to follower state under the supplied leader, ending prior
leadership work. Optionally restarts the election timer and announces the
state change.

### findings

None.

## `_advanceTerm`

Adopts a higher term durably and becomes a follower. Returns whether the
transition completed without shutdown.

### findings

None.

## `_pruneFollowers`

Removes inactive followers and reports whether quorum remains. Announces
changes to the surviving follower set while quorum is available.

### findings

None.

## `_startElectionTimer`

Schedules election attempts when leader activity has been absent long enough.

### findings

None.

## `_startPingTimer`

Starts periodic heartbeats and sends an initial round immediately.

### findings

None.

## `_voteForSelf`

Starts an election in a new term, durably votes for itself, and requests votes
from the other members.

### findings

None.

## `_rxVoteRequest`

Evaluates a candidate's vote request against the local election state and log.
Persists any election changes and replies with the decision.

### findings

None.

## `_rxVote`

Processes vote responses, becoming leader on quorum or stepping down for a
higher term. Establishes replication and readiness for new leadership.

### findings

None.

## `_pingFollowers`

Sends heartbeats and initiates follower catch-up. Warns about failed requests
and steps down if active followers no longer provide quorum.

### findings

None.

## `_checkCommit`

Advances the commit boundary when a current-term entry reaches replication
quorum. Resolves leadership readiness, announces commitment, and starts
applying entries.

### findings

None.

## `_replicationPage`

Sends a bounded page of log entries to a follower and updates its replication
progress, skipping missing history using the follower's head hint. Handles
retries, unavailable history, and fatal log failures.

### findings

None.

## `_runReplication`

Replicates entries to a peer until it reaches the target or replication stops.

### findings

None.

## `_startReplication`

Starts or resumes peer replication, keeping at most one worker active for
each peer state.

### findings

None.

## `_catchUpFollower`

Initiates replication to bring a follower up to the leader's current log head.

### findings

None.

## `_appendToFollower`

Replicates a requested log range to one peer and waits for confirmation within
the append timeout.

### findings

None.

## `_appendToFollowers`

Replicates a log range to quorum and verifies its commitment within the
append timeout. Announces new commitment to followers and rejects if the
operation fails or shutdown begins.

### findings

- **Accepted: early quorum failure follows membership, not selected
  followers.** Aggregation rejects early only when the failed peers make
  quorum impossible for the whole cluster. If the followers selected for an
  append are exhausted while other members are merely absent, the request
  waits for the append timeout. Decision: keep this behavior.

## `_appendToSelfAndFollowers`

Appends payloads locally and replicates them to quorum. Returns the starting
sequence and results after apply completes; local storage failures are fatal.

### findings

None.

## `_leaderAppendNoOp`

Appends an election no-op to establish readiness for the current leadership.
Warns and retries while that leadership still needs the entry.

### findings

None.

## `_fwdToLeader`

Forwards a single append or batch to the known leader and returns its sequence
and validated results. Rejects when no leader is known.

### findings

- **Accepted: callers must leave a submitted batch unchanged until it
  settles.** The outgoing message keeps the caller's array, and the result
  count is checked against `data.length` after the reply. The local leader
  path copies the batch into its own records immediately. Clearing or
  reusing the array while a forwarded append is pending can therefore fail
  with `RAFT_ILLEGAL` after the commands were applied. Decision: no
  defensive copy; this is the input-ownership policy.

## `_rxFwdAppend`

Validates and handles forwarded append requests as leader. Replies with the
appended sequence and apply results or an error.

### findings

None.

## `_rxAppendToFollower`

Handles replication and heartbeats, including term and leader changes.
Reconciles incoming history without replacing committed entries, then
advances commitment and starts applying entries. Acknowledges accepted
requests or reports why replication cannot proceed.

### findings

None.

## `onReceive`

Routes valid member messages by protocol type and adopts higher response
terms. APPEND handles replication and heartbeats; FWD_APPEND handles
forwarded commands.

### findings

None.

## `awaitEvent`

Waits for a future event value accepted by the supplied callback. Rejects if
the callback throws or shutdown begins.

### findings

None.

## `awaitLeader`

Waits until the node is leader or knows a leader, optionally requiring a
committed entry from that term. Rejects on shutdown.

### findings

None.

## `append`

Submits one valid payload to the local or remote leader. Returns its sequence
and apply result once the entry is committed and applied.

### findings

None.

## `appendBatch`

Submits a valid payload batch to the local or remote leader. Returns its
starting sequence and results once the entries are committed and applied.

### findings

None.

## `_startRaft`

Starts Raft as a follower using the persisted election state. Begins election
monitoring and announces the open state.

### findings

- **Skipped by decision: the election term is not compared with the log
  head.** `log.elec.term >= log.term` is not enforced.
- **Skipped by decision: a saved vote is not checked against membership.**
  Any vote that SQLiteLog accepts is restored: null or a nonempty string.

## `_stopRaft`

Stops Raft, cancels pending work, and announces closure.

### findings

None.

## `_closeAfterOpenFailure`

Cleans up after startup failure while preserving error reporting through
closure.

### findings

None.

## `_removeListenersAfterClose`

Removes event listeners once closure and pending fatal reporting permit it.

### findings

None.

## `_fatalClose`

Closes the node after fatal failure and emits the original errors and any
close failures before removing listeners.

### findings

None.

## `open`

Opens the log and starts Raft unless automatic startup is disabled. Cleans up
and reports the original error if startup fails.

### findings

None.

## `close`

Stops Raft and closes the log, reporting any close failure.

### findings

None.

## Dependency: `wrapError` (`src/error.js`)

Normalizes thrown values and RPC error envelopes into coded errors while
preserving available stacks.

### findings

- **Accepted limitation: exotic thrown values make normalization throw.**
  `String(err)` throws for `Object.create(null)`, and mutating a frozen
  `ErrorWithCode` throws. The consequences are stalled apply calls in
  RaftNode and a permanent outage in MonsterNode command processing; see
  MONSTER.md. The same helper runs in the send-error handler and
  `util.errorRpc`, where it can escape warning handling or prevent an error
  reply. Decision: leave unchanged as JavaScript-specific.

## Dependency: `tcpServer` (`src/tcp.js`)

Creates a server for encrypted MessagePack messages. Delivers messages and
errors to the supplied callbacks and closes failed connections.

### findings

None.

## Dependency: `DecryptingStream._drain` (`src/stream.js`)

Delivers decrypted data from complete buffered frames, respecting downstream
backpressure and reporting processing errors.

### findings

None.

## Dependency: encrypted-stream destruction (`src/stream.js`)

Releases encryption resources when either stream is destroyed. Decryption
also discards buffered input and settles any pending transform.

### findings

None.
