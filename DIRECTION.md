# Direction for v2 reconciliation

This document develops the reconciliation ideas in [PLAN.md](PLAN.md).
It records the direction agreed after reviewing the current write paths:
make each Proc's work for one Gate input atomic, and have Gate wait for
all three Procs before delivering the next input. The executor below is
a proposed way to implement that direction, not a finished class design.

## Goal and boundaries

Each cluster node has ProcA, ProcB, and ProcC, with their own DB1 files.
Apply owns the single DB2 and executes user-supplied apply work for all
three Procs.
Gate orders inputs and coordinates communication between these components
and the rest of the cluster. The cluster still requires at least three
nodes; the three Procs are copies within one node.

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

## One input, one durable transition

Gate delivers the same ordered input to all three Procs. Inputs include
clock ticks, cluster RPCs, public command requests, drain requests, and
Apply replies. Each input has a `tickCount` identifying its position in
that order. This is not a Raft log sequence.

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

A small executor owned by each Proc is a promising implementation. It owns
the queue of runnable protocol work and the current input's transaction.
It also collects outgoing messages and other externally visible results
until that transaction commits.

For each input, the executor would:

1. Run its handler and any local work the handler makes ready, in a
   deterministic order.
2. Keep resulting DB1 writes in the same transaction and outgoing effects
   in a local buffer.
3. Stop when no local work is runnable without another external input.
4. Commit DB1, then make the buffered outputs available to Gate and report
   completion for this input.

**An empty runnable queue does not mean every operation has finished.**
A command may still await replication, quorum reports, or Apply. Those
waits remain recorded as pending work. Their responses arrive through Gate
as later inputs, which make the appropriate continuations runnable again.

Waiting for every promise before releasing outgoing messages would
deadlock: an append could await an ACK for a message still in its own
output buffer. The executor must finish the current processing step,
not wait for the entire append operation to settle.

Prefer explicit synchronous tasks and pending-operation state over trying
to track arbitrary JavaScript promises. No task holds the DB1 transaction
open while waiting for Apply, transport, a timer, or a peer response.
Protocol-affecting completions reenter through Gate; they cannot resume
DB1 writes directly from an uncontrolled promise or timer callback.
Public API promises can still represent operations spanning many inputs.
The user callback runs in Apply, outside the Proc executors.

Local task ordering, timer processing, generated request IDs, and random
choices must be deterministic from the shared inputs and seed. Merely
giving three ordinary Node.js event loops the same tick count does not
establish that ordering.

On an unexpected execution or SQL failure, discard buffered success
outputs, attempt rollback, and stop the Proc. Do not report successful
completion or keep using runtime state derived from a failed transaction.

A protocol decision to persist a repair fence and stop is different:
commit that decision before publishing its error and closing. The current
code sometimes persists a fence and then deliberately throws. The executor
must not mistake that intended stop for a reason to roll back the fence.

## Gate waits for all three

Gate may collect incoming events while an input is being processed, but
it must not deliver the next input until all three Procs report that the
current input has finished and committed. This completion report is
required even when the input produces no outgoing RPC.

An outgoing message may be released earlier, once Gate has the required
matching message/checksum evidence from Procs that have committed. This
preserves the distinction between releasing an agreed output and allowing
the next input to execute. With the planned A/B/C encoding roles, Gate
also needs actual message bytes; two matching checksum-only responses
cannot supply those bytes by themselves.

Outputs must be associated with their input and their position within
that input's output sequence. Completion must delimit the whole sequence,
including an empty one, so missing or extra messages cannot silently
become outputs of another input. Exact message formats remain to be
specified.

Synchronous transport must obey the same rule. If sending an output
immediately produces a reply, Gate queues that reply. It must not reenter
a Proc's active handler or begin another transaction inside it. The same
applies to an Apply result produced immediately in the same process.

Missing completion, failure, or detected disagreement stops progress for
this node. The first two disagreeing outputs may still cause immediate
failure, as proposed in PLAN.md. Continuing normal execution with only
two Procs would give up the recovery property described here; other
cluster nodes can continue if they retain quorum.

Retries must not execute an input twice or allow a Proc to skip a missing
input. Completion reports must identify the current input and current
startup run, so an old or duplicate report cannot satisfy a new barrier.
The details belong with the hello and retry protocol still to be planned.

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
reconciliation. Procs communicate through Gate, without reading each
other's database files or reading DB2 themselves.

Apply must also have no unfinished work from the previous run that could
later change DB2. That work must finish or be stopped with its transaction
rolled back before capturing the receipt. Keep Apply idle and its receipt
stable until reconciliation finishes.

1. **Capture original state.** Each Proc captures its original comparison
   state once. Apply supplies its durable receipt. Keep original state
   messages unchanged for every retry throughout this reconciliation
   round; do not replace them with a repaired view.
2. **Choose the original majority.** Each Proc receives all three original
   views and independently selects the same combined winner. No majority
   means failure. Choosing the winner is not yet permission to start Raft.
3. **Adopt that state.** Repair the outlier atomically. Wait until all
   three Procs confirm durable adoption of the selected state. Keep the
   winning sources stable until adoption finishes.
4. **Reconcile the receipt.** From that common state, each Proc validates
   the shared DB2 receipt and performs any required DB1 import in one
   transaction. Wait for all three to complete this phase too. Invalid
   receipt combinations fail rather than causing Apply to run again.
5. **Start normal operation.** Restore protocol runtime state and release
   the Gate input queue. Any startup pruning must respect these same
   boundaries, not run independently between phases.

The adoption barrier matters. Starting from `X / X / Y`, importing a
receipt that changes `X` to `Z` too early can leave `Z / X / Y`. First
bringing everyone to `X` means the subsequent import can leave only `X`
or `Z`. Combining adoption and import in one transaction per Proc does
not solve this because their starting states differ.

Fresh DB1 creation should put all its tables and initial rows in one
transaction. Initialization of DB2 remains separate. Established files
must not be silently reinitialized when reconciliation fails.

## Details still to settle

- **Outlier log repair:** latest-row comparison selects the winning
  state but does not supply the missing or replacement rows. One input
  can change a whole page or suffix. We still need to choose how Procs
  transfer the necessary history and install it atomically, including
  retention boundaries. No one-row repair assumption is valid.
- **Executor interface:** choose the task and continuation API and map
  existing promise chains onto it. The rules above are requirements;
  exact class and method names are not yet decided.
- **Output and completion encoding:** define ordered message/checksum
  matching, completion records, and how the selected full-message and
  checksum roles provide the needed bytes.
- **Startup and delivery identity:** finish hello, duplicate handling,
  retransmission, and timeout rules. Delayed messages from a previous run
  must not be treated as evidence for the current run. No migration or
  protocol-version negotiation is proposed.

## Validation targets

- Interrupt election updates, suffix replacement, and batched insertion
  at each write boundary; observe only complete old or new DB1 states.
- Interrupt an input that advances several checkpoints or finishes SYNC
  and starts a queued CMD; no intermediate checkpoint escapes its
  transaction.
- Delay one Proc and verify that no next input reaches either of the
  others, including when outputs have already been released.
- Exercise synchronous and delayed transports, empty output sequences,
  duplicates, missing completion, and mismatched outputs.
- Verify no output or successful completion escapes a failed DB1 commit,
  and no protocol write runs after completion outside another input.
- Verify intentional repair fences commit before error reporting and
  shutdown, while unexpected processing failures roll back.
- Crash after DB2 commits with zero, one, two, or three receipt imports;
  completed Apply work must never execute again. Include slow Apply with
  intervening elections and no-ops.
- Interrupt both startup adoption and receipt import. Reopening should
  retain a majority for each phase under the stated assumptions. Old Apply
  work must not change the receipt after startup captures it.
- Exercise multi-entry log differences and retained history once the
  outlier repair mechanism is selected. Continue rejecting startup when
  the selected comparison has no majority or receipt validation fails.
