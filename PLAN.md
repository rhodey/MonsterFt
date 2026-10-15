# MonsterFt
## Plan for v2

At a high level v2 is a system where there are still clusters of 3 or more
nodes but every node is sort of running 90% of the library as 2 copies and
they are only allowed to communicate with the rest of the cluster through a
"Gate". The Gate is responsible for sending synchronized clock tick messages
to "ProcA" and to "ProcB" and Gate also forwards RPC comms from the cluster
to both ProcA and ProcB. Also outbound RPC from ProcA and ProcB must pass
thru the Gate to the cluster.

At a high level we are aiming for MonsterFt to become a "Single Fault Tolerant"
system or as close as possible to SFT in the sense that aerospace and defense
understand and think about SFT. I believe one of many things needed to achieve
this will be to add a checksum scheme to all RPC and to have ProcA send RPC
messages without checksum and to have ProcB send its messages as checksums
only and in this way Gate, which is a non-double process, can combine msg plus
checksum and send together, and then the Gate that receives this passes full
msgs onto the destination ProcA and ProcB who both test the msgs plus
checksums and this is being protected by their double proc nature.

Gate starts with tickCount = 0n and increments tickCount by 1 as time moves
forward and passes tick intervals as defined by configs. tickCount is also
incremented every time an incoming RPC is to be fwd to ProcA and ProcB. Gate
also has Gate.append, appendBatch, and drainCmd and each of these calls also 
increments tickCount. tickCount may need to be incremented in other cases
as we move thru planning, we will find out, its a shared clock.

We have introduced Gate, ProcA, and ProcB and now it is time to also say
there is another component named "Apply". Apply is in charge of running the
user defined apply function on the SQLite DB. Previously we have had DB1 and
DB2 and user application state lives in DB2. Now we will say we have DB1A
(ProcA) DB1B (ProcB) and DB2 (Apply). Apply is driven by comms from ProcA and
ProcB and all node-local comms including these go thru Gate. Gate will accept
a "send" function like before and it now will also accept a "localSend"
function. Send is for comms between Gates in the cluster and localSend is for
local comms between Gate, ProcA, ProcB, and Apply.

In-process comms for both send and localSend should remain a valid option
because this makes writing tests way easier. Even tho ProcA and ProcB are named
as such they should not in themselves spawn themselves in a child process
this should be left up to a higher level. And so it should still be possible
to write examples like example3 and example4 where all is contained in a single
JS file. Production users will be steered to writing files such as procA.js and
procB.js which simply import ProcA or ProcB and supply send and localSend and
then call start() or run() or something like this (and they already live in
a distinct nodejs proc).

For production I think by convention we can say Gate and Apply live in the same
nodejs process and the user will again give send and localSend and it should
also be easy for the user to pass the apply function that will be needed to be
given to the Apply component/class somehow. As written before the JS instance
of Gate will be the location of append, appendBatch, and drainCmd. Also certify
will maybe live on Gate too.

It is envisioned that ProcA and ProcB will mostly be instances of MonsterNode
with changes made around the clock being driven by ticks over localSend coming 
from Gate. Also ProcA and ProcB will probably need to do something like include
the latest tickCount in their outbound RPC and maybe also in their local RPCs
to Apply.

The send function is to be called with node member ID like it is now and the
localSend function I think can be called with: "procA", "procB", "apply", and
maybe "gate" is also a valid destination but I'm not certain there is a need
for "gate" as dest we will find out.

This work is to include no migration path or backwards compat. This work should
include no protocol versioning or plans for that. tickCount is to help keep
messages in lock step and we can say there will be a config that says how long
Gate or Apply can wait in milliseconds before a message for tickCount Z times
out because it did not see the other message of the pair in time. Also any uses
of random numbers must be replaced with a seeded random number generator.

I think components that see a tickCount lower than what they have seen before
should probably just ignore it because the transports are agreed to support
lossy sends. Also ProcA and ProcB I think will never read DB2, instead I
think it should be comms thru Gate.

Also maybe the ticks sent from Gate to ProcA and ProcB require ACK and are
timed so that the ticks become a liveliness guard. Also when ProcA and ProcB
are sending comms to Apply there should maybe also be an ACK needed and a
timeout guard here too.

## Reconciliation

On boot ProcA and ProcB may have different DB1 contents and to resolve this I
have decided that we must introduce ProcC. The introduction of ProcC should
have some obvious consequences, such as: it acts just list ProcA and ProcB
except we will say it is more like ProcB in that it will send checksums instead
of messages when its a thing where a message plus a checksum are needed. And
components which we said earlier were waiting for basically a matching pair of
messages they can continue to wait for a pair since a pair signifies 2 of 3 in
agreement which is a majority and they dont need to wait for the 3rd. If the
first 2 that arive disagree there can be fast fail and there is no need to see
if the 3rd forms a majority.

To resolve DB1 table raft_election each of ProcA, ProcB, and ProcC should read
DB1A, DB1B, DB1C and learn which state has a majority and adopt that and if
there is no majority agreement they must throw. And the same is true for table
monsterft_meta: there will be a majority or there must be some error thrown.
Use hashes/digests when comparing the multi-column values for these singleton
rows. And for table raft_log we can say that a majority needs to agree on a
hash/digest of the latest entry row.

## More about localSend and send

Having a stateful transport like TCP or unix domain socket streams would be
nice because open is explicit and close is also explicit. But we want to copy
this library in to Rust sooner or later and we want the rust to target micro
controllers where a stateful transport may not be easy. So we must do
something else. The idea will be for Gate to on start send a "hello" RPC to all
(ProcA, ProcB, ProcC, Apply) and this hello needs an ACK and a timeout and Gate
will continue to send hello to all until all have ACKd. For ProcA, ProcB,
ProcC, and Apply they must always start by waiting for hello and any other RPC 
that arrive before that will cause a fast error/close. And also if any of these
receive a hello after they have already started seeing other RPC types then
they must also fast error/close as this is a sign that the gate has restarted.
This hello scheme combined with Gate sending tick events/messages should be
enough to cause local errors to shutdown things pretty fast and to also give a
good pattern for things being able to come back online in a coordinated way. Do
not take my word for this being perfect, you need to analyze the idea and ask
questions as needed.

Also I think we can say that to give RPCs a checksum scheme we can say ProcA
will send a msgpackr encoded msg something like:

{ tickCount, type, cid .... }

And ProcB and ProcC will send a msgpackr encoded msg something like:

{ tickCount, type: 'checksum', checksum: val }

And checksum I think should be calculated over the first message in its buffer
encoded form.

And then for non-local RPC that need checksum also maybe send is passed a buf
but the first 8 bytes of the buf are always a checksum and the remaining bytes
are to be decoded by msgpackr. This is another idea which needs analysis and
to have some back and forth between us maybe.

## More miscellaneous notes

For random number generators that need a seed the seed should be shared by the
local components/procs but different nodes in the cluster should use different
seeds. Also I think ProcA, ProcB, ProcC, Gate, Apply should expose
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

Also updates made to certify should come last in the impl. And we can say that
the donor DB1A, DB1B, DB1C, and DB2 are to be copied over to the target instead
of for example DB1A being copied over to the target and put in place as 1A, 1B,
and 1C.

## Reconciliation (more)

As part of reconciliation I think its important that A, B, C each get the state
of DB1 A, B, C before agreeing on what is the majority and thus updating their
DB to match as needed. For example we dont want A seeing A, B, C and deciding
to make a change to its A and then B and C seeing A after that change. To make
this work I think we just need a small protocol that runs on boot after the
first tick event/msg is received (after the hello from gate).

A first idea is that all procs after first tick will send RPC type "reconcile"
to every other proc and wait for ACK. And they will keep sending reconcile
until all have ACKd. And reconcile should contain the latest row from SQLite
for each table: raft_election, raft_log, and monsterft_meta. And I think this
should be a winner takes all situation where a majority is chosen over all 3
tables instead of trying to choose a majority for each table individually.

And the rule for ACK should be that a proc is not allowed to ack reconcile
until it has seen RPC reconcile for every other proc and has also chosen a
winner. And then as each node gets these ACKs they are then cleared to continue
with startup/boot.

This plan overrides some things previously said about reconciliation and
importantly the direction now is to try to avoid A, B, C needing to read each
others DB thru the FS and instead using RPC to do comms.

This plan about reconciliation needs to be analyzed with care, and especially
with care given to the question: will any proc raft_log be at most 1 row/entry
ahead such that this idea to compare the latest entry is valid?

## Total

This total plan is not 100% ready to go just yet. We are going to enter into
planning more to search for any clarifications needed and they will be appended
to the plan. The first step tho is to analyze/investigate the idea for the
reconciliation scheme before we analyze too closely other parts of the total
plan because reconciliation I am right now least confident about.