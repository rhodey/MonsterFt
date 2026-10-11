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

Normalizes options; validates the membership array, quorum, timeouts, batch
limits, and callbacks; and creates the SQLite log. Wraps the transport to
attach the sender ID and to report synchronous or asynchronous send failures
as warnings. Then initializes election, replication, application, and
shutdown state and the fatal-error handler.

### findings

- **Deferred: heartbeat frequency is independent of the election timeout.**
  Leaders send heartbeats every `0.3 * pingTimeout`. Followers campaign after
  `electionTimeout` plus up to 20 percent jitter, which disappears below a
  5 ms election timeout. Each value is validated independently, so a
  heartbeat interval longer than the election timeout is accepted.
  Leadership then churns. For example, `electionTimeout: 300` with
  `pingTimeout: 3000` changed leaders 9-13 times in six seconds, although
  appends still succeeded. Nodes may also use different settings. Decision:
  timeout selection is left to the user for now.
- **Deferred: an explicit `undefined` does not select a default.**
  `{ ...defaults, ...opts }` keeps it, so `electionTimeout`, `pingTimeout`,
  `appendTimeout`, `rpcMax`, `applyMax`, and SQLiteLog's `iterStepSize` throw
  `ARGUMENT_ILLEGAL`. `apply` and `quorum` do fall back to their defaults.
  Decision: valid, but not a current focus.

## `_throwIfClosing`

Throws `NODE_NOT_OPEN` once shutdown has begun.

### findings

None.

## `_emitSafe`

Calls each listener for an event separately and catches its exception, so
later listeners still run. Attempts to emit each listener failure as an
`error` event and suppresses exceptions from that fallback emission.

### findings

None.

## `_change`

Builds a description of the node's identity, membership, leadership,
followers, term, and open state, copying the membership and follower arrays.
Emits it as a `change` event.

### findings

None.

## `_stopTimers`

Clears the election timeout and heartbeat interval.

### findings

None.

## `_isQuorum`

Counts distinct supplied node IDs, defaulting to the current followers, and
includes the local node when it is a candidate or leader. Returns whether
that count reaches the configured quorum.

### findings

None.

## `_isQuorumErr`

Counts distinct failed remote node IDs and checks whether the remaining
cluster members could still form a quorum. Returns true when the failures
make quorum impossible.

### findings

None.

## `_persistElection`

Persists the current term and vote only when they differ from the log's
cached election record. Emits a fatal error and returns false on persistence
failure, otherwise returning true.

### findings

None.

## `_replyVote`

Sends a vote response unless shutdown has started, ensuring a granted vote
is persisted before sending it. Returns whether the response was handed to
the transport wrapper.

### findings

None.

## `_raceShutdown`

Races an operation against shutdown, delaying shutdown rejection until the
next immediate callback so the operation can surface a more specific
result. Removes its shutdown registration and scheduled callback when the
race settles.

### findings

None.

## `_delay`

Creates a timer-based delay that rejects when shutdown wins the race.
Clears the timer when the wait settles.

### findings

None.

## `_awaitResponse`

Registers a response callback by correlation ID and accepts only ACK or ERR
messages from the expected peer that pass any supplied validator. Resolves
with the response or rejects on cancellation or deadline failure, removing
the registration when settled.

### findings

None.

## `_sendAndAwaitResponse`

Rejects calls made after shutdown has begun. Otherwise creates an operation
deadline and registers the response waiter before sending the RPC, converts a
timeout into the supplied error, and clears the timer when the wait settles.

### findings

None.

## `_sendAndAwaitAck`

Adds operation-specific timeouts and validation for ping, replication, and
forwarded-append responses. Converts ERR replies into coded errors. When the
reply matches both the request term and current local term, it records peer
activity and, while leading, adds the peer to the follower list.

### findings

None.

## `_subscribeApply`

Creates a waiter for the application results of an inclusive sequence range,
with a result slot for each entry. Registers it with the node and removes it
when its promise settles.

### findings

None.

## `_routeApply`

Distributes a batch of application results to waiters whose sequence ranges
overlap it. Fills each result slot once and resolves a waiter when all of its
results have arrived.

### findings

- **Accepted: holes in an application result array are never routed.**
  `forEach` skips holes, and `_apply` checks only the array length. A waiter
  covering a hole never resolves, and no deadline rescues it because
  replication has already succeeded. Results filled only for non-null
  payloads leak one never-settled no-op waiter per leadership. A fully sparse
  `new Array(n)` leaves leader-local appends pending, and forwarded appends
  time out, although the commands were applied. Decision: the application
  must return one result per entry; Arrays with holes remain out of scope.

## `_rejectApplyWaiters`

Rejects every currently registered application waiter with the supplied
error.

### findings

None.

## `_apply`

Serializes application work through `_applyPrev` and reads committed entries
in batches up to the requested boundary. Calls the configured application
function with payloads, sequences, and terms, representing no-ops as null
payloads. Validates the result array against the batch's own length,
advances `_applySeq`, delivers results, and emits `apply`. Log failures,
application failures, or an incomplete read reject application waiters and
trigger fatal shutdown.

### findings

- **Accepted limitation: exotic thrown values can stall application.** The
  catch block calls `wrapError` before rejecting waiters or starting fatal
  shutdown. If the application throws a frozen `ErrorWithCode` or a
  null-prototype object, `wrapError` throws and skips both steps.
  `_applyPrev` stays rejected, so all later application is skipped. The
  node stays open with no error event, and the leader's append never
  settles. Decision: leave unchanged as a JavaScript-specific case.

## `_beginLeaderReady`

Creates and stores a new readiness waiter associated with the supplied
leadership term.

### findings

None.

## `_cancelLeaderReady`

Rejects and clears the current leadership readiness waiter if one exists.

### findings

None.

## `_replicationActive`

Checks that a replication state has not been cancelled, the node is not
closing, and it remains leader in that state's term.

### findings

None.

## `_replicationState`

Returns an existing peer replication state for the requested term or creates
a new one. Initializes its next and matched sequences, target, worker
status, outstanding RPC identifier, and waiters.

### findings

None.

## `_resolveReplicationWaiters`

Resolves replication waiters whose requested end sequence has been reached
by the peer's confirmed match index.

### findings

None.

## `_cancelReplicationState`

Marks a peer's replication state cancelled and rejects its waiters. Also
cancels its outstanding response wait when an RPC is registered. A cancelled
state remains mapped for the rest of its term.

### findings

None.

## `_cancelReplication`

Cancels leadership readiness and all peer replication states with the
supplied error, then clears the replication map.

### findings

None.

## `_reset`

Cancels replication and clears volatile election, leadership, liveness,
acknowledgement, commit, and commit-announcement state. Stops both Raft
timers.

### findings

None.

## `_toFollower`

Returns immediately once shutdown has begun. Otherwise cancels leader
replication work, switches to follower state, and records the supplied
leader. Clears prior follower and vote tracking, stops heartbeats, optionally
restarts the election timer, and optionally emits a state change.

### findings

None.

## `_advanceTerm`

Accepts only a valid term greater than the current term, clears the vote,
and transitions to follower state. Persists the new election state and emits
a change, returning whether the transition completed without shutdown.

### findings

None.

## `_pruneFollowers`

Removes followers whose last recorded response has expired according to
`pingTimeout`. Returns whether the remaining followers and local leader can
still form a quorum. If followers were removed and quorum remains, it emits
a `change` before returning.

### findings

None.

## `_startElectionTimer`

Schedules elections using a randomized delay above `electionTimeout`, and
starts a new candidacy when leader activity has been absent long enough.
Rechecks shutdown and leadership after the election attempt before arming
the next timeout.

### findings

None.

## `_startPingTimer`

Replaces the heartbeat interval with one running at 30 percent of
`pingTimeout` and immediately sends a round of heartbeats.

### findings

None.

## `_voteForSelf`

Becomes a candidate in a new term, votes for itself, clears prior leadership
tracking, and cancels replication. Persists the election state, emits the
change, and sends vote requests carrying the local log's last term and
sequence to the other nodes.

### findings

None.

## `_rxVoteRequest`

Validates a vote request, adopts a higher term when necessary, and checks
both voting eligibility and whether the candidate's log is sufficiently
current. Adopting a higher term restarts the election timer only when a
leader steps down. A granted vote records the candidate and restarts the
timer. Election state is persisted and the reply is sent before the change
is published.

### findings

None.

## `_rxVote`

Processes valid vote responses, stepping down for a higher term and counting
distinct granted votes in the current term. On reaching quorum, establishes
leadership, initializes peer replication and readiness state, starts
heartbeats, and appends the election no-op. Later granted votes can expand
the follower list.

### findings

None.

## `_pingFollowers`

Sends heartbeat APPEND messages containing the leader's log head and commit
sequence to every other node, and starts catch-up work after successful
responses. Stops the round if leadership changes during a send. Emits
warnings for failed requests, expires inactive followers, and steps down if
the remaining set cannot form a quorum.

### findings

None.

## `_checkCommit`

Finds the highest sequence replicated by a quorum using live followers'
match indexes and the local head. Advances the commit boundary only if that
entry belongs to the current term. It then resolves leadership readiness,
emits `commit`, and schedules application. Lookup failures are fatal.

### findings

None.

## `_replicationPage`

Reads and sends one bounded page of entries with its predecessor term and
sequence, stopping if the required history has been forgotten. Retries a
predecessor mismatch immediately from one sequence earlier. Delays other
retryable RPC failures, and handles cancellation or fatal log errors. On
success, advances the peer's match and next indexes, checks commitment, and
schedules resolution of satisfied waiters.

### findings

- **Open: catching up a lagging follower after a leadership change costs one
  rejected page per missing entry.** A new leader starts each peer at its own
  head plus one. A follower that is behind replies `REPL_BACKTRACK` without
  saying where its log ends. The leader then backs up by one entry, sends
  another full page of up to `rpcMax` entries, and emits a `warn`. After
  5,000 missed entries, catch-up took 5,001 rejected pages, about 4.6
  million entries shipped, and 4.2 s even in-process. The time grows by one
  round trip per entry on a real network. When the lagging node was needed
  for quorum, appends failed with `APPEND_TIMEOUT` until catch-up finished.
  Likely direction: the follower's reply includes a sequence for the leader
  to try next. This is not being implemented now.

## `_runReplication`

Sends successive replication pages while the peer's state remains active
and its match index is below the target. Stops when a page reports that
replication should not continue.

### findings

None.

## `_startReplication`

Ensures only one replication worker runs for a peer state at a time and
records additional start requests while it is busy. Clears worker state
when the run finishes and restarts if requested work remains in the same
active state.

### findings

None.

## `_catchUpFollower`

When leading a nonempty log, extends a peer's replication target to the
current head and starts replication if it has not already matched that
head. Allows the next attempt to include the head entry so the peer can
prove its log matches.

### findings

None.

## `_appendToFollower`

Requests replication of a sequence range to one peer, updating that peer's
next index and target. Returns immediately if the range is already matched;
otherwise registers a waiter with an append deadline and starts the worker.

### findings

None.

## `_appendToFollowers`

Requests replication from the current followers and waits, racing shutdown,
for enough successful peers to form a quorum within the append timeout.
Checks that the requested end is committed. It then announces the commit
with a heartbeat round, which is sent only when the commit index has
advanced since the last announcement. Otherwise it rejects with a timeout
or commitment error.

### findings

- **Accepted: early quorum failure follows membership, not selected
  followers.** Aggregation rejects early only when the failed peers make
  quorum impossible for the whole cluster. If the followers selected for an
  append are exhausted while other members are merely absent, the request
  waits for the append timeout. Decision: keep this behavior.

## `_appendToSelfAndFollowers`

Pairs each payload with the captured current term in a `{ term, entry }`
record, stores the single entry or batch locally, and registers for its
application results. Waits for both quorum replication and application
before returning the starting sequence and results. Local storage failures
are fatal, and replication failures reject the application waiter.

### findings

None.

## `_leaderAppendNoOp`

Appends an empty payload while the current leadership still needs a
current-term committed entry. Warns on failure and schedules a delayed
retry, checking the captured readiness object so retries do not carry over
into another leadership period.

### findings

None.

## `_fwdToLeader`

Sends a single payload or batch to the known leader and waits for its append
response, rejecting immediately if no leader is known. Checks the returned
result count for batches and returns the leader's sequence and results.

### findings

- **Accepted: callers must leave a submitted batch unchanged until it
  settles.** The outgoing message keeps the caller's array, and the result
  count is checked against `data.length` after the reply. The local leader
  path copies the batch into its own records immediately. Clearing or
  reusing the array while a forwarded append is pending can therefore fail
  with `RAFT_ILLEGAL` after the commands were applied. Decision: no
  defensive copy; this is the input-ownership policy.

## `_rxAppendToLeader`

Validates a forwarded append's correlation ID, term, and payload, rejecting
invalid or mismatched requests through the RPC error envelope. Replies
`NOT_LEADER` when the node is not leading. Appends valid requests locally and
to followers, then replies with their sequence and application results or
the resulting error.

### findings

None.

## `_rxAppendToFollower`

Validates incoming heartbeats and replication pages, handles term and leader
changes, and checks the predecessor against retained local history.
Appends new entries or reconciles an overlapping range by trimming only an
uncommitted conflicting suffix. Returns backtrack or forgotten-history
errors when matching cannot proceed. Matching requires equal terms and equal
payload bytes. For matched history, advances commit only through the
boundary established by the request, reads that entry's term, schedules
application, and acknowledges the request.

### findings

None.

## `onReceive`

Ignores messages from outside the cluster, messages received while closed,
and invalid RPC envelopes. Adopts higher terms from responses. Routes any
APPEND without its own `termP` as a forwarded append. Other APPENDs go to the
leader path for a current or older term while leading, and otherwise to the
follower path. Vote messages go to the vote handlers.

### findings

None.

## `awaitEvent`

Waits for a future event value accepted by the supplied callback.
Rejects if the callback throws or shutdown begins, and removes both the
listener and shutdown registration when settled.

### findings

None.

## `awaitLeader`

Waits until the node is leader or knows a leader as a follower, optionally
requiring a committed entry from that same term. Checks the current state
before subscribing to changes or commits, and cleans up its subscriptions
on success or shutdown.

### findings

None.

## `append`

Requires an open node and a nonempty Buffer, then appends locally if leader
or forwards to the known leader. Races the operation against shutdown and
returns its sequence and application result.

### findings

None.

## `appendBatch`

Requires an open node and a nonempty array of nonempty Buffers, then appends
locally if leader or forwards the batch. Races the operation against
shutdown and returns its starting sequence and application results.

### findings

None.

## `_startRaft`

Requires an open log and a node that is not closing, then resets volatile
Raft state and restores the persisted term and vote. Marks the node open as
a follower, starts the election timer, and emits a state change.

### findings

- **Skipped by decision: the election term is not compared with the log
  head.** `log.elec.term >= log.term` is not enforced; SQLiteLog validates
  each term independently.
- **Skipped by decision: a saved vote is not checked against membership.**
  Any vote that SQLiteLog accepts is restored: null or a nonempty string.

## `_stopRaft`

Marks shutdown in progress and cancels shutdown-sensitive operations,
application waiters, replication, and outstanding RPC response waits.
Resets Raft state, marks the node closed, and emits a change if it was open.

### findings

None.

## `_closeAfterOpenFailure`

Attempts to close after startup fails while temporarily deferring listener
removal. Reports cleanup errors through the fatal path, then resumes normal
listener cleanup.

### findings

None.

## `_removeListenersAfterClose`

Removes all event listeners unless cleanup is explicitly deferred or fatal
errors are still scheduled for delivery.

### findings

None.

## `_fatalClose`

Queues fatal errors and attempts to close the node immediately, recording
any close failures separately. Schedules a microtask that emits the original
errors, then the close errors, through `_emitSafe`. Every error listener
receives each error, and a listener's own exception is reported once. It
then removes listeners.

### findings

None.

## `open`

Opens the SQLite log and starts Raft when the `_autoRaft()` hook permits it.
If opening or startup fails, attempts cleanup and rethrows the original
error.

### findings

None.

## `close`

Stops Raft, attempts to close the SQLite log, and performs listener cleanup.
Rethrows any log-close error after the cleanup attempt.

### findings

None.

## Dependency: `wrapError` (`src/error.js`)

Normalizes arbitrary thrown values and RPC error envelopes into coded
errors, preserving usable stacks and reusing existing `ErrorWithCode`
instances when possible.

### findings

- **Accepted limitation: exotic thrown values make normalization throw.**
  `String(err)` throws for `Object.create(null)`, and mutating a frozen
  `ErrorWithCode` throws. The consequences are a stalled application in
  `_apply` and a permanent outage in MonsterNode command application; see
  MONSTER.md. The same helper runs in the send-error handler and
  `util.errorRpc`, where it can escape warning handling or prevent an error
  reply. Decision: leave unchanged as JavaScript-specific.

## Dependency: `tcpServer` (`src/tcp.js`)

Accepts encrypted MessagePack connections, dispatches decoded messages, and
returns the listening server. Passes connection and runtime server errors to
the supplied error callback, then closes the failed connection even if that
callback throws.

### findings

None.

## Dependency: `DecryptingStream._drain` (`src/stream.js`)

Consumes buffered encrypted frames, decrypts each complete frame, and
pushes plaintext downstream. Pauses for backpressure and completes the
current transform callback when input has been consumed or fails. A falsy
thrown value is reported as an `Error`.

### findings

None.

## Dependency: encrypted-stream destruction (`src/stream.js`)

`EncryptingStream._destroy` and `DecryptingStream._destroy` free any
initialized native secretstream state with `sodium.libsodium._free`, then
complete destruction. Decryption also clears buffered ciphertext and settles
a pending transform callback.

### findings

None.
