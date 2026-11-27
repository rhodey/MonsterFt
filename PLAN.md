# Online repair and DB2 scrubbing

Working draft, 2026-11-27. This records the preferred direction and the remaining
questions; it is not a completed protocol specification. Experiments come before
we rely on the proposed DB2 page-equality rules.

## Scope and decisions

- Keep MonsterFt on `node:sqlite`. Build Node and its SQLite dependency with Nix,
  enabling `SQLITE_ENABLE_DBPAGE_VTAB`, `SQLITE_ENABLE_SESSION`, and
  `SQLITE_ENABLE_PREUPDATE_HOOK`. Pin the runtime, SQLite version, and relevant
  build options across members. A better-sqlite3 migration is not planned.
- Keep **WAL mode and `synchronous=FULL`**, including repair writes.
- **Backwards compatibility is out of scope.** New database schemas and wire
  formats may require fresh databases. Do not build migration or old-format
  compatibility machinery for this work.
- **Never apply patchsets or changesets.** Normal execution captures and hashes
  patchsets. Repair transfers physical database pages. Nothing in this plan
  introduces patchset/changeset application.
- The **leader is always the repair donor**. DB1 donation is verified by the
  leader's three local Procs. DB2 replacement groups require agreement from
  distinct cluster members.
- Replace logical local DB1 adoption with physical reconciliation so the local
  copies can produce matching page-transfer RPCs.
- Canonicalize the two SQLite page-1 counters for both hashing and RPC payloads.
- Move DB2's local generation into a dedicated singleton ownership table/page.
  Keep the token local and preserve the existing transaction-scoped fence.
- Add periodic committed scrub commands that compare subsets of DB2 pages
  through ordinary command-result agreement.
- A repair target closes normal operation and restarts in a repair-only mode,
  outside Raft, before discovering the leader and requesting repair.
- After its no-quorum SYNC is committed and durably applied, a node in
  `REPAIR_NO_QUORUM` should go offline. Automatic recovery from that state is
  mostly outside the initial scope.

## Snapshot identity and lifetime

Page comparisons refer to an explicit shared checkpoint, not merely to readers
opened at approximately the same wall-clock time.

DB1's applied sequence advances over CMD, SYNC, and no-op entries. DB2's receipt
sequence advances only for executed commands/setup. Record the corresponding
pair, for example DB1 checkpoint 60 and DB2 command receipt 59, with their hashes.
Prefer a completed SYNC boundary with no pending command.

The snapshot description should bind at least the repair identity, donor and
leader term, checkpoint/receipt identities, database format, page size, page
count, and normalization version. DB1 also needs an explicit retained-log range
and policy for any stored suffix beyond the chosen applied checkpoint.

Healthy members coordinate a brief barrier, open read transactions, and actually
read and validate their checkpoint/receipt before releasing the barrier. `BEGIN`
alone does not pin a deferred read snapshot. DB1 and DB2 are separate files; both
snapshots must be established and checked while the barrier holds.

Healthy members then **resume writes while retaining these read transactions**.
They keep the snapshots until the leader declares repair complete or the repair
times out. Page hashes and transferred bytes must come from those same pinned
snapshots, even if the member has since executed more commands.

Implement bounded snapshot lifetimes and cleanup for timeout, cancellation,
leader loss, and disconnected participants. Long readers retain WAL history, so
WAL growth and the repair timeout are part of the design. A failed leader must
not leave remote readers pinned indefinitely.

How the target advances from the pinned checkpoint to safe rejoining remains an
open protocol step. Healthy writes continue during copying, so installation of
that snapshot alone does not establish that the target is current. Define any
required retained Raft history, further page transfers, and ordinary command
execution explicitly. Do not fill this gap with patchset/changeset application.

## Canonical page representation

Read pages through `sqlite_dbpage`, which exposes the transaction's pager view,
including visible WAL content. Do not compare only the main database file while
ignoring its WAL.

Use one versioned canonicalization rule for comparisons, group hashes, scrub
results, and repair RPC payloads. Raw outgoing payload differences would still
fail Gate verification even if a separate comparison hash ignored them.

### Page 1

In copies of page 1 used for hashing or RPC encoding, replace these zero-based
byte ranges with zeros:

| Bytes | Field |
| --- | --- |
| 24-27 | File change counter |
| 92-95 | Version-valid-for number |

Do not normalize the rest of page 1. It contains real schema and allocation
information. Check the writing SQLite version at bytes 96-99 against the pinned
format/build policy instead of silently masking it.

Canonicalization describes comparison and transfer bytes, not an instruction to
zero live database headers. Installation must produce valid recipient headers;
SQLite can regenerate the counters on commit. Verify the resulting image using
the same canonicalization rules rather than requiring raw counter equality.

### DB2 local ownership page

The preferred new schema separates local ownership from the shared receipt:

```sql
CREATE TABLE monsterft_local_owner (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  generation BLOB NOT NULL CHECK (length(generation) = 16)
) STRICT;
```

The table name is provisional. Its intended format is one fixed-size row, no
additional indexes, and one ordinary B-tree leaf/root page. Token changes use
fixed-size updates. The shared `monsterft_meta_apply` row retains sequence,
entry hash, and local digest, but no generation.

Keep the existing ownership sequence inside the same DB2 transaction:

```text
BEGIN IMMEDIATE
check this owner's local generation
execute command and update shared receipt
COMMIT
```

A replacement claims a fresh local token. Do not reuse a shared cluster token
that could make an old owner's checks valid again. A separate shared epoch is
not required to solve this problem; existing checkpoint identifiers may already
provide the shared identity needed by repair.

Locate the ownership page through a validated schema/root mapping. Validate the
singleton, token, and expected one-page structure before treating it specially.
For hashing and RPCs, substitute a deterministic valid template page containing
a placeholder token. Generating that template through the pinned SQLite build
is preferable to implementing a general SQLite record decoder.

On installation, instantiate this page at the donor image's expected location
with the recipient's fresh local token before activation. Do not adopt the donor
token, leave a placeholder usable, or blindly preserve an old page at that page
number. A corrupted local ownership page must be repairable, not hidden by
canonicalization. An all-zero page is not a valid table-page template.

Root relocation, schema-cache handling during raw page installation, and the
one-page invariant need experiments. Every shared receipt and application page
remains fully compared. DB1 can retain its existing local generation: DB1 does
not require cross-member byte equality, and the three Procs on one node share
the local startup token.

## Physical local DB1 reconciliation and donation

The leader's local ProcA/B/C copies provide the DB1 donation agreement. Other
cluster members do not need to hold byte-identical DB1 files and are not asked
to vote on DB1 donation pages.

This relies on the existing local fault model: independent copies and enough
correct local participants. Local agreement does not itself establish current
leadership or which entries are committed; the snapshot must bind those facts
separately.

Preferred startup/reconciliation sequence:

1. Pin comparable local DB1 snapshots and compare their complete canonical page
   manifests, including page count and snapshot identity.
2. If two copies agree and the third is an outlier, physically replace the
   outlier from an agreeing copy. Use a coherent SQLite snapshot and a safe
   filesystem replacement or transactional DBPAGE installation.
3. Verify the repaired image and restore matching local state before normal
   operation and donation.
4. During donation, each Proc independently reads/verifies the selected pages
   and produces matching canonical repair output through Gate.

Current startup `stateDigest()` compares election state, metadata, and log head;
it does not certify every retained log row or page. It is insufficient by itself
to authorize a complete physical donor image. Do not combine unrelated pages
from different checkpoint snapshots.

Current Gate requires all three outgoing results to agree. Two-of-three source
selection for local reconciliation does not silently change Gate into a
two-of-three outgoing-RPC mechanism. Repair the outlier, then use the established
matching-output path. Normalize page 1 in the actual repair encoding as well as
in the local manifest.

The target installs the donated DB1 image into its local copies and claims its
own fresh local generation consistently. That intentional local change needs a
defined place in verification/activation. Installing an image must not roll
back required recipient term/vote protections. The exact rejoin rules remain
to be specified.

## DB2 grouped comparison and transfer

Let `X` be the configured number of pages per group. For an authoritative DB2
image of `N` pages, there are `Y = ceil(N / X)` groups, numbered from the first
database page, with a possibly shorter final group.

The initial flow is:

1. The repair target hashes its DB2 in groups of `X` canonical pages and sends
   the group hashes and size information to the leader.
2. The leader computes corresponding hashes from its pinned DB2 snapshot.
3. For disagreement groups, the leader asks one or more eligible healthy
   followers for hashes from the same agreed checkpoint.
4. Collect enough matching votes from distinct cluster members to satisfy the
   configured quorum. The leader contributes one vote; `Z` followers means as
   many additional agreeing members as needed, not a hardcoded single follower.
5. Send only the disagreement groups whose hashes agree with the leader and
   have quorum support. The target verifies received canonical bytes against
   the agreed hashes before installation.
6. Validate the completed target image and its DB1/DB2 checkpoint relationship
   before proceeding toward rejoining.

ProcA/B/C do not count as three independent cluster votes for DB2: they share
one local DB2 file. Do not count the repair target's potentially damaged image
as an authoritative healthy vote.

**If the leader disagrees with a quorum of healthy followers, abort repair and
mark the leader suspect.** The initial design does not let the leader relay a
follower's alternative image while remaining the donor. Define how suspicion
causes fencing/leadership change and how the target rediscovers the next donor.
If no group value has quorum support, do not choose arbitrary bytes.

Important details to resolve:

- Target and donor may have different page counts and therefore different
  values of `Y`. Define authoritative size agreement, missing/extra groups,
  growth, truncation, and partial final groups.
- Bind each hash to the database/snapshot identity, normalization version,
  page size, group start, and actual page count. Reject reordered, truncated,
  duplicated-as-votes, or stale responses.
- The proposed optimization asks followers only about groups where target and
  leader disagree. Fresh quorum evidence therefore covers transferred groups;
  it does not automatically certify every unchanged group. Specify what known
  baseline or additional validation supports final whole-image acceptance.
- Minimum transferred groups at granularity `X` is not necessarily minimum
  individual page writes. Evaluate comparing pages within received groups so
  unchanged pages need not be rewritten. Group size and any finer subdivision
  are tuning/design choices.
- Prefer minimal writes while preserving crash safety. Decide between a
  transactional in-place install, staged images, and fallback replacement when
  the target cannot expose a usable page view.
- DB1 and DB2 do not share one atomic WAL commit. A durable repair-in-progress
  state must prevent activation after partial installation or a crash; copying
  donor metadata must not accidentally clear that protection.

## Repair-only startup and completion

A node that needs ordinary automatic repair closes its normal components, then
restarts into a repair-only mode. It does not vote, run election timers,
participate in ordinary Raft replication, or execute application commands while
its image is untrusted.

It sends direct discovery RPCs to every cluster member asking who they believe
the leader is, then requests repair from the leader. Discovery needs term-aware
handling of stale/conflicting answers and bounded retries. A stale leader claim
does not authorize a snapshot merely because one member reports it.

The repair session coordinates the pinned healthy snapshots, local DB1 donation,
DB2 group verification, transfer, installation, and validation. Completion or
timeout releases all participant snapshots. Leader change, cancellation, or
interrupted transfer must have explicit cleanup/retry behavior.

Successful copying is followed by the still-to-be-specified advancement/rejoin
step. Reset the leader's per-target replication state as needed; existing docs
already record cases where donor copying alone leaves replication cancelled.
Keep the recipient fenced until its installed state and rejoin conditions have
been validated.

## Periodic DB2 scrub commands

The leader periodically commits a new internal scrub command, scheduled every
configured number of application commands or Raft sequences. The cadence and
the number of pages sampled are separate settings from repair group size `X`.
Define whether a command means a CMD batch or an individual item, and whether
scrubs themselves count toward the interval. Avoid self-triggering scrub loops.
A standalone, read-only scrub CMD batch is the preferred initial shape.

Every member computes a hash over the same subset of DB2 pages using the same
canonicalization and returns it as the scrub command's result. Normal command
agreement then detects disagreement and drives the existing SYNC/repair-state
path: a dissenting minority becomes repair candidates, while lack of any outcome
quorum follows the no-quorum path below. An empty data patchset does not make a
scrub ineffective: the returned hash participates in command-outcome agreement.

The subset must be specified by the committed command or derived identically
from agreed inputs. Independent local randomness or independently observed page
counts must not choose different subsets. Bind results to the sampled checkpoint,
page identities, sizes/counts, and normalization version. Missing selected pages
must produce a detectable result rather than silently shortening the hash input.
Define a coverage policy so repeated scrubs eventually examine the whole current
image, including growth, truncation, and the canonical ownership page. Preserve
progress across restarts and leadership changes so sampling does not repeatedly
restart at the first range.

Receipt timing is already established by [Apply's execution flow](src/apply.js):

1. Begin the DB2 write transaction, check generation, and read the previous receipt.
2. Await every application callback in the batch.
3. Extract the patchset and close the session.
4. Compute the command digest from the patchset and completed outcomes.
5. Write the new receipt, commit, and then acknowledge completion.

The receipt object allocated before the callbacks is only a JavaScript object;
it has not been written to DB2. A standalone read-only scrub uses the `db` and
transaction supplied to its execution, so it reads the previous completed
receipt and database state before its own receipt is written. There is no
unfinished stored receipt or circular result dependency to resolve, and this
ordering does not require a separate reader. The existing session-order test in
[test/apply.js](test/apply.js) also checks that receipt writing follows patchset
extraction and session closure.

Keep the initial scrub batch standalone: if it followed application writes in
the same batch, its reads would also see those uncommitted changes while the
receipt still described the previous batch. Scrubs must not use an older reader
still pinned for another node's repair. Preserve transport and local Proc
agreement for scrub outcomes and repair-trigger notifications.

## Quorum-impossible outcomes

`REPAIR_NO_QUORUM` here means no application-outcome digest can obtain the
configured quorum. It is distinct from a temporary inability to contact a Raft
quorum.

Preferred new behavior: stop admitting application work, let the corresponding
no-quorum SYNC commit, durably apply that decision, then go offline instead of
remaining online indefinitely. Define shutdown/commit-notification ordering,
especially for the leader, so it does not disappear before peers can learn the
required commitment. Behavior when the SYNC cannot commit remains an explicit
failure/timeout question.

Normal automatic quorum-backed repair of this state is mostly out of scope.
Keep it distinct from ordinary minority repair, where healthy members agree on
an authoritative image.

A possible later explicit recovery procedure chooses one cluster member as the
recovery leader/authority and has all other members copy its DB1 and DB2. This is
a deliberate choice of truth after agreement has failed, not a claim that the
chosen image was established by the ordinary quorum protocol. Selection and
cluster-reset semantics are not decided in this draft.

## Evidence and experiments still required

Prior exploratory tests provide useful evidence, not a general page-equality
proof:

- In a real three-node run at DB1 sequence 60 / DB2 receipt 59, the only
  cross-member differences were the 16-byte generations. The three local DB1
  copies matched. Equalizing generation in scratch copies made all pages match.
- After restarting a follower and continuing writes, generation and the two
  page-1 counters differed in both DB1 and DB2.
- SQLite probes with WAL and FULL showed identical pages for identical writes
  and for different checkpoint schedules in the tested workloads.
- Other probes showed equal rows and identical captured patchsets with different
  table/index bytes. Different insertion/deletion histories also changed page
  allocation. `secure_delete` and `VACUUM` were not general solutions.

Those probes used the current Node runtime plus a separate custom-SQLite reader,
or custom SQLite directly. Repeat the relevant tests using the chosen Nix-built
Node. The dedicated ownership-table design has not yet been experimentally
validated. Preserve the existing experiment agenda without introducing
patchset/changeset application.

### First: ownership-page isolation

- Create the new schema on multiple members with different local tokens; show
  exactly which raw bytes differ and that canonical hashes/RPCs match.
- Rotate tokens repeatedly, reopen connections, and update shared receipts
  across SQLite integer-encoding size boundaries. Verify that local bytes stay
  isolated and the ownership table remains one page.
- Verify stale owners cannot execute after takeover, including an active Apply
  transaction racing replacement.
- Test canonical-template installation, a damaged ownership page, changed root
  mapping, interrupted installation, and claiming the recipient token before
  activation.

### Then: DB2 physical equality and possible new rules

- Start from common physical images and execute the same ordered commands under
  the pinned runtime and settings. Compare at agreed receipt checkpoints.
- Vary restarts, checkpoint timing, batching, savepoint failures, large BLOBs,
  page splits, deletes, freelist reuse, hidden rowids, and application indexes.
- Test maintenance operations such as VACUUM, REINDEX, and ANALYZE to determine
  which must be coordinated, restricted, or treated as requiring a new baseline.
- Decide explicit page-size, auto-vacuum, secure-delete, and other relevant
  settings; do not assume Nix's defaults equal the current Node defaults.
- Evaluate the tentative rule that members must have matching canonical DB2
  pages at boot or enter repair. Such a comparison needs a common checkpoint;
  ordinary lag and differing physical history must be handled deliberately.
- Determine whether generation and page-1 normalization are sufficient under
  the resulting rules. Until demonstrated, keep this an open question.

### Then: reconciliation, repair, scrub, and lifecycle

- Repair each possible local DB1 outlier, including corruption away from the log
  head, and verify complete local manifests plus matching Gate RPCs afterward.
- Test group comparisons with missing/extra pages, partial groups, independently
  differing corruption patterns, unavailable voters, and a suspect leader.
- Verify only accepted disagreement groups are transferred and measure actual
  writes, not only transferred bytes.
- Keep healthy writers running while repair readers are pinned; test WAL growth,
  timeouts, leader loss, interrupted transfers, and cleanup.
- Interrupt installation between DB1/DB2 steps and verify restart stays in
  repair mode until a consistent image is established.
- Verify scrub selection/coverage, deterministic sampling before receipt update,
  detection of latent page corruption, and concurrent scrub/repair snapshots.
- Verify minority repair, no-quorum SYNC shutdown/dissemination, and eventual
  rejoin without the existing cancelled-replication failure.

## Suggested implementation order

1. Add the pinned Nix runtime and startup/CI capability probes for DBPAGE and
   session capture, retaining WAL/FULL.
2. Prototype the DB2 ownership page and canonicalization. Establish the tests
   and required physical-equality rules before treating page hashes as authority.
3. Implement physical local DB1 reconciliation and complete-image verification.
4. Define shared checkpoint capture, snapshot lifetime, and repair-only startup.
5. Implement grouped DB2 verification and leader-only transfer, then crash-safe
   installation and the explicit completion/rejoin path.
6. Add periodic scrub commands and integrate their outcomes with repair.
7. Change no-quorum shutdown behavior with commitment/dissemination tests. Keep
   explicit choose-one-member recovery as separate future work.

These stages are a working order, not permission to skip unresolved invariants.
As APIs change, update adjacent TypeScript declarations and consumer tests per
[test/types/README.md](test/types/README.md), as well as the function notes in
`doc/` and the operator repair documentation.

## Existing code and documented constraints

- [Storage and ownership](doc/STORAGE.md), [SQL helpers](doc/UTILS.md), and
  [Apply execution](doc/APPLY.md).
- [Gate output agreement](doc/GATE.md) and [Proc transaction boundaries](doc/PROC.md).
- [Monster outcome/SYNC handling](doc/MONSTER.md) and [Raft/retention](doc/RAFT.md).
- [Current offline repair procedure](REPAIR.md) and [certification](doc/CERTIFY.md).

Already documented issues relevant to this work include the caught automatic
rollback Apply contract, matching quorum configuration, and retention cancelling
replication across donor-copy repair. Consult those notes before treating them
as new findings. The plan records the intended future behavior; it does not
claim those issues or online repair have already been implemented.
