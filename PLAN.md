# MonsterFt
## Plan for v2

At a high level v2 is a system where there are still clusters of 3 or more
nodes but every node runs most of the library as ProcA, ProcB, and ProcC.
They communicate with the rest of the cluster through a "Gate". Gate sends
synchronized clock tick messages and forwards incoming cluster RPCs to all
three Procs. Their outbound RPCs also pass through Gate. The transaction
and reconciliation direction is developed in [DIRECTION.md](DIRECTION.md).

At a high level we are aiming for MonsterFt to become a "Single Fault Tolerant"
system or as close as possible to SFT in the sense that aerospace and defense
understand and think about SFT. I believe one of many things needed to achieve
this will be to add a checksum scheme to all RPC and to have ProcA send RPC
messages without checksum and to have ProcB and ProcC send checksums
only and in this way Gate, which is a non-double process, can combine msg plus
checksum and send together, and then the Gate that receives this passes full
msgs onto the destination ProcA, ProcB, and ProcC who independently test
the messages and checksums.

Gate starts with tickCount = 0n and increments tickCount by 1 as time moves
forward and passes tick intervals as defined by configs. Incoming RPCs,
Apply replies, and calls to Gate.append, appendBatch, and drainCmd also
become ordered inputs identified by tickCount. Gate may queue inputs, but
waits for all three Procs to finish and commit the current input before
delivering the next one. Further clock details remain to be planned.

We have introduced Gate, ProcA, ProcB, and ProcC and now it is time to also say
there is another component named "Apply". Apply is in charge of running the
user defined apply function on the SQLite DB. Previously we have had DB1 and
DB2 and user application state lives in DB2. Now we will say we have DB1A
(ProcA), DB1B (ProcB), DB1C (ProcC), and DB2 (Apply). Apply is driven by
requests from the Procs. Runtime requests and replies go through Gate.
Gate will accept a "send" function like before and also a "localSend"
function. Send is for comms between Gates in the cluster and localSend is for
local comms between Gate, ProcA, ProcB, ProcC, and Apply. Startup also uses
direct read-only database access, as described below.

In-process comms for both send and localSend should remain a valid option
because this makes writing tests way easier. Even tho the Procs are named
as such they should not in themselves spawn themselves in a child process
this should be left up to a higher level. And so it should still be possible
to write examples like example3 and example4 where all is contained in a single
JS file. Production users will be steered to writing files such as procA.js,
procB.js, and procC.js which import their Proc implementation and supply
localSend and then call start() or run() or something like this (and they
already live in a distinct nodejs proc).

For production I think by convention we can say Gate and Apply live in the same
nodejs process and the user will again give send and localSend and it should
also be easy for the user to pass the apply function that will be needed to be
given to the Apply component/class somehow. As written before the JS instance
of Gate will be the location of append, appendBatch, and drainCmd. Also certify
will maybe live on Gate too.

For production, recommend an external supervisor that restarts failed
processes and waits for each old OS process to exit before replacing it.
This is deployment guidance, not a required supervisor or process layout.
Running every component in one Node.js process without a supervisor
remains supported. In that case, await component shutdown before replacing
it or starting reconciliation against its databases. Old callbacks and
transactions must have finished or been stopped before the new components
begin using those databases.

It is envisioned that the three Procs will mostly be instances of MonsterNode
with changes made around the clock being driven by ticks over localSend coming
from Gate. Proc outputs will identify the input being processed, including
requests sent locally to Apply.

The send function is to be called with node member ID like it is now.
LocalSend destinations include "procA", "procB", "procC", "apply", and
"gate" for Proc outputs and completion reports.

This work is to include no migration path or backwards compat. This work should
include no protocol versioning or plans for that. tickCount is to help keep
messages in lock step and we can say there will be a config that says how long
Gate or Apply can wait in milliseconds before a message for tickCount Z times
out because it did not see the other message of the pair in time. Also any uses
of random numbers must be replaced with a seeded random number generator.

The inter-node send transport may lose messages; Raft and Monster retain
their existing handling of loss and timeouts. LocalSend is expected to
deliver messages in send order for each sender/destination pair, and
without loss once the local components are ready. Messages already queued
ahead of a new hello must stay ahead of it, including across component
replacement. No separate queue-purging mechanism is required.
Startup hello is the one retry exception, allowing components to come
online at different times. After hello completes, a missing required
local reply or completion is a fatal timeout for the local node. Do not
retry those requests or build a general local retransmission protocol.

Every input requires completion from all three Procs, even when it produces
no outbound RPC. Each Proc replies with an ACK only after finishing and
committing the input. Requests to Apply also require a reply within their
configured timeout. An Apply timeout does not establish whether DB2
committed; startup reconciliation recovers completed work without rerunning
it.

RPCs follow the existing RaftNode convention: messages have a type,
requests have a cid, and ACK or ERR replies carry the request's cid.
Use sufficiently random CIDs to make reuse across restarts negligible.
Match replies to the expected sender and outstanding request; an unmatched
reply cannot complete a request. Replies need no ACK of their own. A local
completion ACK covers that processing step, not necessarily the whole
public append operation. Ordinary apply rejections remain command outcomes.

## Shared database access

Gate, Apply, and the three Procs run on the same OS and have access to the
same local filesystem. Each Proc writes only its own DB1. Apply is the
only component that writes DB2. Other local components may open these
databases read-only through SQLite.

Startup uses this access to let each Proc independently read all three
DB1s and DB2's apply receipt. Runtime protocol processing still follows
ordered Gate inputs. In particular, an Apply reply supplies the fixed
receipt and results for that input; a Proc must not substitute a fresh
DB2 read that could observe a later Apply execution.

V2 requires file-backed SQLite databases for DB1A, DB1B, DB1C, and DB2.
Drop support for `:memory:` and reject it in database configuration.
Tests and single-process examples can use temporary database files.

## Reconciliation

ProcC provides a third DB1 state for startup majority selection and uses
the same checksum role as ProcB. An agreed output can be released once
Gate has matching committed message/checksum evidence and the actual
message bytes. Gate still waits for all three Procs to finish the input
before delivering the next one. If the first two outputs disagree, it can
fail immediately without waiting for the third.

Each input's DB1 writes belong to one transaction. This includes any
immediately enabled protocol work; it stops before waiting for Apply or
another external response. DIRECTION.md describes a Proc executor for
these boundaries. Starting from a common state, an interrupted input
then leaves each DB1 in either the old or new state, so a majority exists
under the stated assumptions.

Startup compares a digest of the combined raft_election singleton,
monsterft_meta singleton, and latest raft_log row, including its sequence,
term, and payload, or an explicit empty-log value. Select one combined
majority, not separate winners for each table. No exact majority means
stop and require repair. Comparing only the latest log row does not
detect differences hidden in earlier history; this limitation is accepted.

## More about localSend and send

Having a stateful transport like TCP or unix domain socket streams would be
nice because open is explicit and close is also explicit. But we want to copy
this library in to Rust sooner or later and we want the rust to target micro
controllers where a stateful transport may not be easy. So we must do
something else. On start Gate sends a "hello" RPC to each of ProcA,
ProcB, ProcC, and Apply. Each attempt uses a fresh cid and the ordinary
local RPC timeout path. Hello catches timeout rejections and tries again
for components that have not ACKed. A late ACK for an expired attempt
does not complete a newer request. A component that is not yet online,
or too slow to reply within the timeout, keeps Gate waiting and retrying.
An explicit startup error still fails startup.

A hello ACK means the component is ready to participate in coordinated
startup. Components that ACK early wait for the others; their runtime
tick timeout does not start during this wait. Gate begins reconciliation
only after all four have ACKed, and normal processing only after
reconciliation completes.

Hello uses `{ type: 'hello', cid }` and its ACK uses
`{ type: 'ack', cid }` with the request's cid. Each component starts with
`pastHello = false` and ACKs every hello while still in that phase. Set
`pastHello = true` before processing the first subsequent startup request.
From then on, any hello causes fatal shutdown without an ACK, regardless
of its cid. A replacement component starts in hello again and can ACK
Gate's next retry. Do not reset an active component in place on hello.

There is no separate startup identity field. Ordered local delivery means
outstanding hello retries arrive before the subsequent startup request,
so they cannot trigger this shutdown merely by arriving late. Components
reject ordinary protocol work before hello and reconciliation complete.
Fresh CIDs and matching to outstanding requests keep old replies from
satisfying new requests.

Also I think we can say that to give RPCs a checksum scheme we can say ProcA
will send a msgpackr encoded msg something like:

{ tickCount, type, cid .... }

And ProcB and ProcC will send a msgpackr encoded msg something like:

{ tickCount, type: 'checksum', cid, checksum: val }

And checksum I think should be calculated over the first message in its buffer
encoded form.

And then for non-local RPC that need checksum also maybe send is passed a buf
but the first 8 bytes of the buf are always a checksum and the remaining bytes
are to be decoded by msgpackr. This is another idea which needs analysis and
to have some back and forth between us maybe.

## More miscellaneous notes

For random number generators that need a seed the seed should be shared by the
local components/procs but different nodes in the cluster should use different
seeds. Choose a fresh random seed each time Gate starts, so restarting
does not repeat the generated CIDs. ProcA, ProcB, and ProcC use the shared
seed to make identical protocol choices and generate matching CIDs.
Also I think ProcA, ProcB, ProcC, Gate, Apply should expose
.onReceive(from, msg) and also .tickCount which can be read to get the latest
tickCount value as seen by that local component.

Also I no longer desire to maintain the library in a way which encourages that
the RaftNode class may be used by itself instead of MonsterNode or MonsterFt.
And so we can remove examples 1 and 2 and replace them with 3 and 4 which use
MonsterFt. Also in all the work to be done all js files in the src/ dir should
end with .v2.js so that we keep the original sources around for awhile so that
we can look at v2 and agree that everything needed from v1 is copied over. Also
there is no need to rewrite/update the doc/ dir at the start of this work, I
will ask for this to be updated later when the time is right.

For the purposes of keeping things simple we can say we are locked in on the
decision to use msgpackr as part of RPC and we want to make this change
supporting checksums but we also need to work in the direction which allows
being transport agnostic. It may be the case that the send function changes
to accept a buf instead of an obj.

Also if at any point in the implementation you encounter uncertainty about
some detail the plan did not spell out you can pause impl to ask me for
guidance.

Also updates made to certify should come last in the impl. For manual repair
from another cluster node, copy the donor's DB1A, DB1B, DB1C, and DB2 to the
target, rather than placing DB1A into all three target DB1 locations. This
is separate from majority reconciliation among Procs within one node.

## Startup reconciliation

Gate coordinates phases using local RPCs and ACK/ERR replies. A phase
ACK confirms completion of that phase; missing required replies cause a
fatal timeout. These phases begin after the retrying hello handshake has
completed. Database state and repair rows are read directly through
SQLite, without snapshot or log-transfer RPCs between Procs.

1. Open the databases and keep their contents stable. Normal processing,
   receipt import, and pruning remain blocked. Apply must have finished
   or stopped any previous work before this phase can complete.
2. Each Proc reads all three original DB1 comparison states and DB2's
   receipt, then independently chooses the majority. Retain those reads
   and the selection throughout reconciliation. Gate waits for all three
   reports to agree on the selected state and captured receipt before
   allowing any repair. Receipt checks involving DB1 wait until adoption,
   so they use the selected state rather than an outlier's original state.
3. The outlier copies the winning donor's entire retained raft_log and
   both singleton rows into its own DB1 in one transaction. Select the
   first winning Proc in fixed A, B, C order as donor. The winning DB1s
   stay unchanged until all three confirm adoption.
4. Each Proc validates the captured receipt against its adopted DB1 and
   performs any required receipt import in one transaction. Gate waits
   for all three to finish before starting normal operation.

Keep adoption and receipt import separate: starting from X/X/Y, importing
a receipt too early can produce Z/X/Y. Adopt X everywhere first, then
perform the X-to-Z import. Completed Apply work must never be rerun to
make the files agree.

Copy logical rows through SQLite rather than copying only a database
file, since committed data may still be in its WAL. This repair handles
arbitrary page or suffix differences without assuming a one-row gap.
Copying all retained rows does not add a full-history digest to the
selected comparison. DIRECTION.md describes the phases and validation
in more detail.

## Total

This total plan is not 100% ready to go just yet. We are going to enter into
planning more to search for any clarifications needed and they will be appended
to the plan. The first step tho is to analyze/investigate the idea for the
reconciliation scheme before we analyze too closely other parts of the total
plan because reconciliation I am right now least confident about.
