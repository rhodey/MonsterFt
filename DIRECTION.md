# Direction for v2 reconciliation

This document develops the reconciliation ideas in [PLAN.md](PLAN.md).
It records the direction agreed after reviewing the current write paths:
make each Proc's work for one Gate input atomic, have Gate wait for all
three Procs before delivering the next input, and use direct read-only
database access during startup. A synchronous FIFO executor provides the
processing boundary; helper names and routine implementation details can
be chosen during coding.

## Goal and boundaries

Each cluster node has ProcA, ProcB, and ProcC, with their own DB1 files.
Apply owns the single DB2 and executes user-supplied apply work for all
three Procs. Gate orders inputs and coordinates communication between
these components and the rest of the cluster. The cluster still requires
at least three nodes; the three Procs are copies within one node.

The goal is to prevent ordinary interrupted progress from leaving three
different DB1 states with no majority. Reconciliation should be able to
choose an existing majority, bring the other Proc into agreement, and
recover completed Apply work without executing it again.

The selected startup comparison remains the combined contents of:

- The `raft_election` singleton row.
- The `monsterft_meta` singleton row.
- The latest `raft_log` row, including its sequence, term, and payload,
  or an explicit empty-log value.

Choose a majority over that combined state, not separately for each table.
If no exact majority exists, stop and require repair. We are not adding a
digest of all retained history. Consequently, this comparison does not
detect differences hidden in earlier log rows, and the recovery argument
below assumes valid, consistent earlier history. It is not a proof of the
larger single-fault-tolerance goal.

## Shared database access

Gate, Apply, and all three Procs run on the same OS with access to the
same local filesystem. Each Proc writes only its own DB1, and Apply is
the only writer of DB2. Other local components may read these databases
through read-only SQLite connections.

Startup uses this permission directly: each Proc reads DB1A, DB1B, DB1C,
and the DB2 receipt independently. Gate coordinates the read and write
phases with small completion messages. It does not choose the winner on
the Procs' behalf, and no database snapshot or log rows need to travel
through reconciliation RPCs.

Runtime protocol decisions still follow the ordered Gate inputs. Read
access must not introduce independently timed observations into those
decisions. Apply replies continue to carry the receipt and results for
the particular execution; direct reads do not replace those replies.

V2 requires file-backed SQLite databases for DB1A, DB1B, DB1C, and DB2.
Drop support for `:memory:` and reject it in database configuration.
Tests and single-process examples can use temporary database files;
peer reads use read-only SQLite connections to those files.

## RPCs and transport

Inter-node `send` may lose messages. Raft and Monster keep their existing
handling of network loss and timeouts. `localSend` is expected to deliver
messages in send order for each sender/destination pair, and without loss
once the local components are ready. Messages queued ahead of a new hello
stay ahead of it, including across component replacement. This does not
require a separate queue-purging mechanism. Startup hello is the sole
retry exception. After that handshake, a missing required local reply or
completion is a fatal timeout for the local node, with no request retries.

Follow the existing RaftNode RPC convention: `msg.type` identifies the
message, requests carry a `cid`, and replies use `type: 'ack'` or
`type: 'err'` with the request's `cid`. Match replies to the expected
sender and outstanding request. Use sufficiently random CIDs to make
reuse across restarts negligible; an unmatched reply cannot complete a
request. ERR replies follow the operation's error handling; ordinary
apply rejections remain command outcomes. Replies need no ACK of their own.

Where local RPCs carry cluster messages, keep local request correlation
separate from the enclosed message's type and cid. Local envelope fields
must not make the encoded payload differ between Procs that checksum it.
A remote ERR carried through Gate keeps its existing protocol meaning.

Both `send` and `localSend` carry Buffers, including in-process transports.
Each frame contains a 32-byte SHA-256 checksum followed by its MessagePack
payload. Hash the exact payload bytes and forward them unchanged. Use
independently decodable msgpackr messages with shared-record encoding
disabled. Recipients verify the checksum before decoding; each receiving
Proc independently verifies incoming cluster frames. Checksum or decoding
failure causes fatal shutdown. See
[msgpackr's encoding options](https://github.com/kriszyp/msgpackr#options).

Independently generated Proc error stacks stay in local diagnostics and
are omitted from agreed protocol errors. Preserve their message/code
fields and preserve shared Apply or incoming error stacks.

Gate sends hello to ProcA, ProcB, ProcC, and Apply. Each attempt uses a
fresh cid and the ordinary local RPC timeout path. Hello catches timeout
rejections and retries only components that have not ACKed. ACKs for
expired attempts cannot complete newer requests. A peer that is not yet
online, or too slow to reply within the timeout, keeps Gate waiting and
retrying. Explicit startup errors still fail startup. A hello ACK means
ready to participate in coordinated startup; it does not mean
reconciliation has already completed.

An early component waits after ACKing hello. Proc watchdogs remain inactive
throughout hello and coordinated startup. Gate begins reconciliation only
after all four ACK. Hello to A/B/C uses `{ type: 'hello', cid, seed }`.
Gate generates the 16-byte seed once before hello and includes the same
seed in every retry, with fresh cryptographically random CIDs. Apply does
not need the seed; its hello remains `{ type: 'hello', cid }`. Replies
use `{ type: 'ack', cid }` with the request's cid.

Each component starts with `pastHello = false` and ACKs every hello in
that phase. Each Proc stores the seed during hello and initializes its
RNG once when leaving that phase; repeated hellos do not advance or reset
an active generator. Before processing its first subsequent startup
request, set `pastHello = true`. Any hello received after that causes fatal
shutdown without an ACK, regardless of its cid. A replacement starts in
hello and can ACK Gate's next retry. Hello never resets an active component
in place.

No startup identity field is needed. Ordered delivery keeps outstanding
hello retries ahead of the first subsequent startup request, so they
cannot arrive afterward and cause a false restart. Ordinary protocol
work is rejected before hello and reconciliation complete. Fresh CIDs
and matching to outstanding requests prevent old replies from satisfying
new requests.

Subsequent startup phases and runtime inputs require completion replies
without retries. A Proc's completion ACK means that step has finished and
its DB1 transaction has committed, not merely that the message arrived.
Completion also delimits all outputs from that step; Gate must have the
full output sequence before accepting it as complete. It need not mean
that a public append awaiting external responses has finished.

Gate owns native timers for scheduling clock inputs and guarding local
requests. Its configurable `localTimeout` defaults to `5_000` ms for hello,
startup phases, and Proc completion; `applyTimeout` defaults to `30_000`
ms for an Apply execution request, not the entire public append. These
watchdogs run independently of the input barrier and can expire while
Gate waits for completion. Hello retains its timeout-and-retry exception.

Each Proc uses a native `setTimeout` only as a watchdog for loss of Gate
communication. Start it when normal runtime begins after coordinated
startup, and rearm it on each valid Gate input using `localTimeout`. Its
expiry reports an error and fatal-closes the Proc; it never advances
protocol time or runs protocol work. It must work when Gate inputs stop,
so it cannot depend on supplied `timems`.

Apply needs no library-owned native timer; Gate times its execution
requests. This does not restrict timers in the user's apply callback.
After a fatal timeout, recovery goes through coordinated startup and
reconciliation. A missing Apply reply may follow a successful DB2 commit,
so it must not cause Apply to run the same work again during that run.

## Deployment and shutdown

For production, recommend an external supervisor that restarts failed
processes, waiting for an old OS process to exit before replacing it.
The library does not require a particular supervisor or spawn its own
processes. All components may run in a single Node.js process without a
supervisor, including for examples and tests.

The shutdown requirement is the same in either arrangement: old instances
must stop issuing messages and changing their databases before replacement
components use those databases or reconciliation reads them. Process exit
provides that boundary for separate processes. In one process, await the
components' shutdown, including completion or rollback of outstanding DB
work. A user apply callback must not resume later and commit after the new
reconciliation has captured DB2's receipt.

## One input, one durable transition

Gate delivers the same ordered input to all three Procs. Inputs include
clock ticks, cluster RPCs, public command requests, drain requests, and
Apply replies. Gate starts with `tickCount = 0n` and increments it for
every ordered input, including clock ticks. Each input carries that
count, which identifies its position in the order, not a Raft log sequence.

Immediately before fanout, Gate assigns
`timems = Math.max(previousTimems, Date.now())`. All three Procs receive
the same timestamp; equal timestamps are valid. A configurable
`tickInterval`, defaulting to `10` ms, supplies clock ticks. Keep at most
one clock tick queued and timestamp it when dispatched.

All Proc protocol deadlines use supplied `timems`: elections, heartbeats,
peer RPC timeouts, retries, and quorum waits. They run through the executor,
without native `setTimeout`, `setInterval`, or local clock reads driving
protocol work. The separate Gate-contact watchdog can only fatal-close.
Process the input and its ready continuations first, then remaining due
timers in deadline order, breaking ties by registration order. An overdue
repeating timer fires once and reschedules from current `timems`, without
catch-up bursts.

All DB1 changes caused by processing that input belong to one transaction.
An input that changes nothing in DB1 needs no write transaction. A Proc
must not declare the input complete and then continue changing protocol
state or DB1 in an unrelated promise callback.

The transaction must include all immediately enabled local work, such as:

| Work enabled by an input | Changes to group |
| --- | --- |
| Higher-term replication | Election update and log changes. |
| Conflicting log suffix | Delete the old suffix and insert its replacement. |
| Receiving committed entries | Store the entries and process any ready protocol entries. |
| Advancing commitment | Apply ready no-ops and SYNC records, including their checkpoint updates. |
| Receiving an Apply result | Copy its receipt and process any newly enabled protocol work. |
| Completing a command's protocol | Start an already-queued command when it becomes eligible. |

Ready work can append SYNC or another CMD, or write a repair fence.
Retention also runs within a controlled transaction boundary rather than
as an untracked continuation. Its deletion of older rows does not itself
change the selected startup comparison.

The existing log helpers need compatible transaction ownership.
`appendBatch()` currently starts its own transaction, so placing an outer
transaction around the current handler is not sufficient. Likewise,
wrapping only `onReceive()` misses the application and protocol work that
the current implementation schedules afterward.

## A Proc executor

A synchronous FIFO executor owned by each Proc manages runnable protocol
work, explicit pending operations, and the current input's transaction.
It also collects outgoing messages and other externally visible results
until that transaction commits.

For each input, the executor:

1. Run its handler and ready continuations, then due timers and the work
   they enable, following the ordering above.
2. Keep resulting DB1 writes in the same transaction and outgoing effects
   in a local buffer.
3. Stop when no local work is runnable without another external input.
4. Commit DB1, then make the buffered outputs available to Gate and report
   completion for this input.

**An empty runnable queue does not mean every operation has finished.**
A command may still await replication, quorum reports, or Apply. Those
waits remain recorded as pending work. Register each wait before buffering
its request. Responses arrive through Gate as later inputs, which make
the appropriate continuations runnable again.

Waiting for every promise before releasing outgoing messages would
deadlock: an append could await an ACK for a message still in its own
output buffer. The executor must finish the current processing step,
not wait for the entire append operation to settle.

Use explicit synchronous tasks and pending-operation state rather than
tracking arbitrary JavaScript promises. No task holds the DB1 transaction
open while waiting for Apply, transport, a timer, or a peer response.
Protocol-affecting completions reenter through Gate; they cannot resume
DB1 writes directly from an uncontrolled promise or timer callback.
Public API promises can still represent operations spanning many inputs.
The user callback runs in Apply, outside the Proc executors.

Local task ordering, timer processing, generated request IDs, and random
choices must be deterministic from the shared inputs and seed. Merely
giving three ordinary Node.js event loops the same tick count does not
establish that ordering. Use [`pure-rand`](https://github.com/dubzzz/pure-rand)'s
`xoroshiro128plus` for deterministic jitter and CIDs. On each start, Gate
generates a fresh 128-bit state with `crypto.randomBytes(16)`, redrawing an
all-zero state before sharing it with A/B/C in hello. Each Proc initializes
through the full-state API when leaving hello. The Procs generate matching
choices and CIDs without reusing the previous run's sequence.

On an unexpected execution or SQL failure, discard buffered success
outputs, report the original error, and stop the Proc and close its database.
Connection teardown or crash recovery discards uncommitted work; these
fatal paths need no separate `ROLLBACK`. Report cleanup errors too, and
never continue using the failed connection or report successful completion.

For a repair fence that requires shutdown, commit the fence before reporting
its error and closing. Both fatal paths report errors and stop; the fence
is an intended durable change. Keep explicit rollback where processing
continues, such as a failed CMD item's savepoint.

## Gate waits for all three

Gate may collect incoming events while an input is being processed, but
it must not deliver the next input until all three Procs report that the
current input has finished and committed. This completion report is an
ACK referring to the input request's cid and containing the ordered
output list: message bytes from A and corresponding checksums from B/C.
An empty list is valid and still requires the completion ACK.

An outgoing message may be released earlier, once Gate has the required
matching message/checksum evidence from Procs that have committed. This
preserves the distinction between releasing an agreed output and allowing
the next input to execute. With the selected A/B/C encoding roles, Gate
also needs actual message bytes; two matching checksum-only responses
cannot supply those bytes by themselves.

The completion ACK's list associates outputs with their input and order,
so missing or extra messages cannot silently become outputs of another
input. Gate compares complete ordered lists from the responding Procs
before releasing outputs.

Synchronous transport must obey the same rule. If sending an output
immediately produces a reply, Gate queues that reply. It must not reenter
a Proc's active handler or begin another transaction inside it. The same
applies to an Apply result produced immediately in the same process.

Missing required completion is a fatal timeout; failure or detected
disagreement also stops progress for this node. The first two disagreeing
outputs may still cause immediate failure, as specified in PLAN.md.
Continuing normal execution with only two Procs would give up the
recovery property described here; other cluster nodes can continue if
they retain quorum.

Completion replies must match the current outstanding input request by
cid and expected sender. A late or duplicate reply cannot count for
another request or replace a missing Proc's completion. The startup hello
exception does not apply to these processing requests; they are not
resent after a timeout.

## Why these rules help reconciliation

Assume the three Procs begin in the same intended durable state `X` and
deterministically process one input whose resulting state is `Y`.
Atomic transactions mean each DB1 contains either `X` or `Y` after an
interruption. Gate's barrier prevents any Proc from starting the next
transition to `Z` while another still contains `X`.

| Point of interruption | Possible durable states |
| --- | --- |
| Before any commit | X / X / X |
| After one commit | Y / X / X |
| After two commits | Y / Y / X |
| After all three commit | Y / Y / Y |

Each case has a majority. A transaction may insert or replace many log
rows, so this does not establish a one-row difference between DB1 files.
It establishes at most two states for one interrupted transition.

Any output released before the interruption must have been supported by
the required matching committed Procs. In particular, Apply must not run
speculatively from one Proc's uncommitted request.

This argument depends on the transaction, ordering, and completion rules
being followed. Arbitrary corruption or nondeterministic execution can
still produce no majority, in which case startup must stop.

## Apply and the DB2 receipt

Before requesting Apply, Procs commit the referenced log entry and the
checkpoint immediately preceding it. Gate releases the agreed request
only after the supporting Proc transactions have committed.

Apply executes the command once and commits its changes and receipt
together in DB2. Its reply becomes another ordered Gate input. Each Proc
copies the receipt into DB1 and processes newly ready protocol work in
that input's transaction. Only committed outputs from that transition
may authorize further Apply work.

Use the receipt captured in the reply, not a fresh DB2 query. A lagging
cluster follower can receive committed `SYNC1` and `CMD2` while its Apply
is still executing `CMD1`. On reply 1, ProcA/B can copy receipt 1, process
the ready SYNC, and commit their DB1 changes. Their matching outputs let
Gate request CMD2; Apply can finish it before ProcC handles reply 1, leaving
DB2 with receipt 2. Apply still executes commands sequentially; Gate queues
reply 2 until ProcC completes the current input. Startup direct reads avoid
this race by keeping Apply idle throughout reconciliation.

While Apply is busy, Procs can process later inputs such as elections and
replication. They must preserve protocol application order: the applied
checkpoint cannot pass the CMD whose Apply result is still outstanding.
This retains the existing one-entry-ahead receipt recovery rule even when
the log head advances further through election no-ops or replication.

DB2's receipt remains the durable evidence that Apply completed. Startup
validates its sequence and entry hash against DB1 under the existing
receipt rules; the referenced entry need not be the log head. Never
discard completed DB2 work or rerun it merely to fit a chosen DB1 state.
Sequence-zero setup retains its real entry hash and zero local digest.
Receipt recovery does not recreate old caller results or re-emit old
apply warnings.

## Startup in separate phases

Normal protocol processing and new Apply execution stay blocked during
reconciliation. Each database owner first finishes opening its database.
Receipt import and pruning remain blocked while the Procs read the
original states. Fresh metadata creation is handled before that read
phase, with all DB1 tables and initial rows created in one transaction.

A missing, unreadable, or invalid established DB1 stops startup. Automatic
majority adoption handles only structurally valid outliers; matching peers
do not authorize recreating an invalid database.

Apply must also have no unfinished work from the previous run that could
later change DB2. That work must finish or be stopped with its transaction
rolled back before capturing the receipt. Keep Apply idle and its receipt
stable until reconciliation finishes.

1. **Capture original state.** Each Proc reads the comparison state of
   all three DB1s and the DB2 receipt through SQLite. Validate the stored
   fields, then retain those original reads for the entire round. Finish
   the read transactions before proceeding to adoption.
2. **Choose the original majority.** Each Proc independently selects the
   combined winner and reports its selection and captured receipt's
   identity to Gate. No majority means failure. Gate waits for all three
   reports to agree before permitting any repair. No Proc can write early
   and change what another Proc observes during capture.
3. **Adopt that state.** The outlier reads a stable winning donor directly
   and replaces its own contents in one transaction, as described below.
   Gate waits for all three to confirm durable adoption. The winning
   databases stay unchanged until this phase finishes.
4. **Reconcile the receipt.** Each Proc validates the captured DB2 receipt
   against its adopted DB1 and performs any required import in one
   transaction. These DB1-dependent receipt checks happen after adoption;
   do not reject an outlier merely because its original DB1 differs.
   Gate waits for all three to complete this phase too. Invalid receipt
   combinations fail rather than causing Apply to run again.
5. **Start normal operation.** Restore protocol runtime state and release
   the Gate input queue. Any startup pruning must respect these same
   boundaries, not run independently between phases.

The adoption barrier matters. Starting from `X / X / Y`, importing a
receipt that changes `X` to `Z` too early can leave `Z / X / Y`. First
bringing everyone to `X` means the subsequent import can leave only `X`
or `Z`. Combining adoption and import in one transaction per Proc does
not solve this because their starting states differ.

Initialization of DB2 remains separate from DB1 creation. Established
files must not be silently reinitialized when reconciliation fails.

## Repair through local SQL reads

Use the first winning Proc in fixed A, B, C order as the donor. The
outlier reads that donor through a read-only SQLite connection and copies
its entire retained `raft_log` plus both singleton rows into its own DB1
in one transaction. Stream the rows rather than requiring the whole log
in memory. Check that the installed comparison state matches the selected
winner before commit, then refresh the outlier's runtime caches.

The destination transaction includes all replacement deletes and inserts.
A failure rolls back the replacement and stops startup. Winning Procs
do not rewrite their databases. The all-three adoption barrier keeps
donor contents stable until the copy has finished.

This copies the donor's retained range as well as its head, handling
missing pages, extra entries, and conflicting suffixes without a separate
suffix-repair protocol. It costs a full retained-log read and rewrite for
an outlier, but adds no history digest to the selected startup comparison.
Copying older rows does not establish that majority voting authenticated
their contents.

Copy logical contents through SQLite. Committed rows may still reside in
the WAL, so replacing only the main database file is not the intended
repair mechanism. See [SQLite's WAL documentation](https://sqlite.org/wal.html).
These are local reads and writes; no database transaction waits for log
pages to arrive over RPC.

## Validation targets

- Verify identical timer order, random choices, CIDs, and outputs across
  Procs. Cover input-first timeout cancellation, coalesced ticks, repeated
  timestamps, and overdue repeating timers without catch-up bursts.
- Stop Gate inputs during runtime and verify Proc watchdogs fatal-close
  without advancing protocol deadlines. Valid Gate inputs rearm the guards;
  hello and coordinated startup leave them inactive. Gate owns the Apply
  request timeout.
- Verify independent frame decoding and checksum validation, including
  nested bigint/Buffer payloads. Corrupt frames must fail fatally. Keep
  shared Apply/incoming stacks while excluding independent Proc stacks
  from agreed errors.
- Reject `:memory:` database configuration. Exercise startup and peer
  reads using temporary database files, including in a single process.
- Interrupt election updates, suffix replacement, and batched insertion
  at each write boundary; observe only complete old or new DB1 states.
- Interrupt an input that advances several checkpoints or finishes SYNC
  and starts a queued CMD; no intermediate checkpoint escapes its
  transaction.
- Delay one Proc and verify that no next input reaches either of the
  others, including when outputs have already been released. Native local
  watchdogs must still expire while Gate waits.
- Exercise synchronous and delayed transports, empty output sequences,
  and mismatched outputs. Drop local requests or replies after hello,
  including completion and Apply replies; verify fatal timeout without retries.
  Late or duplicate replies must not satisfy another request or barrier.
  Keep coverage for ordinary inter-node message loss and protocol recovery.
- Bring local components online at different times and drop hello requests
  or ACKs. Gate must keep retrying only unacknowledged components, while
  early components wait without a runtime timeout. Repeated hellos during
  that phase must ACK again, and all four must ACK before reconciliation
  begins. Explicit startup errors still fail.
- Delay a hello ACK past its timeout and verify the retry has a fresh cid
  and the same seed. Verify each Proc initializes its RNG once when
  leaving hello.
  The expired ACK must not complete the new attempt; a current ACK must
  arrive within its timeout. Ordered delivery must put hello retries
  before the subsequent startup request. After a component moves past
  hello, any hello must cause fatal shutdown without an ACK. A replacement
  can ACK the next retry, and protocol work must not execute before startup
  permits it.
- Exercise both supervised process replacement and shutdown/restart in a
  single process. Previous callbacks and transactions must not change the
  databases after reconciliation begins; no supervisor is required for
  the single-process case.
- Verify no output or successful completion escapes a failed DB1 commit,
  and no protocol write runs after completion outside another input.
- Verify intentional repair fences commit before error reporting and
  shutdown. Unexpected failures must also report and stop; close/reopen
  or crash recovery must discard uncommitted work without explicit rollback.
- Crash after DB2 commits with zero, one, two, or three receipt imports;
  completed Apply work must never execute again. Include slow Apply with
  intervening elections and no-ops.
- On a follower with committed `CMD1`, `SYNC1`, and `CMD2`, let Apply finish
  CMD2 before ProcC finishes reply 1. Verify ProcC uses receipt 1 from its
  input and Gate withholds reply 2 until that input completes.
- Interrupt both startup adoption and receipt import. Reopening should
  retain a majority for each phase under the stated assumptions. Old Apply
  work must not change the receipt after startup captures it.
- Delay one Proc's initial reads; no database may be repaired until all
  three have captured their original views. A missing phase completion
  must time out fatally without advancing to the next phase.
- Copy multi-entry differences and retained ranges with committed donor
  data still in the WAL. Interrupt destination deletes and inserts and
  verify that the whole repair commits or rolls back together.
- Verify peer DB1 and DB2 read access cannot write through those read
  interfaces. Continue rejecting startup when the selected comparison
  has no majority or receipt validation fails. Reject missing, unreadable,
  or invalid established DB1s even when two valid peers match.
