# Online repair and DB2 scrubbing

Working draft, 2026-11-27. This records the preferred direction and the remaining
questions; it is not a completed protocol specification. Experiments come before
we rely on the proposed DB2 page-equality rules.

The first preservation experiments are now recorded in
[doc/DB2-PAGE-EQUALITY.md](doc/DB2-PAGE-EQUALITY.md). They support ownership-page
isolation but disprove the sufficiency of owner/page-1 normalization alone:
unequal restarts can expose different unused freelist-leaf bytes. Additional
free-leaf canonicalization is now the agreed direction, conditional on reliable
snapshot-specific classification. Its corruption handling and the agreed
application rules still need implementation and validation.

Immediate work is disposable `sqlite_dbpage` write/repair experiments, including
page-write ordering, growth/truncation, and reopening repaired images. The exact
repair start/progress/completion/rejoin flow and interruption/recovery protocol
remain important open work, deliberately deferred briefly while these storage
experiments establish the installation behavior.

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
  patchsets. Repair transfers SQLite pages read through `sqlite_dbpage`.
  Nothing in this plan introduces patchset/changeset application.
- The **leader is always the repair donor**. DB1 donation is verified by the
  leader's three local Procs. DB2 replacement groups require agreement between
  the leader and **one eligible healthy follower, regardless of cluster size**.
  This is two-member corroboration, not a majority requirement in larger
  clusters. Ordinary Raft and command-outcome quorum rules are unchanged.
- Final repair acceptance does **not** require a quorum-endorsed complete DB2
  manifest. Keep groups whose hashes match the leader and obtain fresh follower
  agreement only for replacement groups.
- Compute **one hash over multiple pages per group**. Each page contributes its
  page number, normalization type, and canonical bytes to that group hash.
  Transfer normalization types with replacement pages and write every page in
  each accepted replacement group. Do not add per-page no-op/write-skipping
  optimization at the target.
- Replace logical local DB1 adoption with transactional `sqlite_dbpage`
  reconciliation. Require matching canonical local page views so the copies
  can produce matching page-transfer RPCs.
- Canonicalize the two SQLite page-1 counters in both DB1 and DB2 for hashing
  and RPC payloads. Raw equality of those counters is not required.
- Canonicalize DB2 freelist leaf pages to full zero pages for hashing and RPCs,
  provided their classification comes from validated snapshot allocation
  metadata. Preserve all allocation metadata; corruption handling remains to
  be implemented and tested.
- Move DB2's local generation into a dedicated singleton ownership table/page.
  Keep the token local and preserve the existing transaction-scoped fence.
- Application mutations must be deterministic within each command. Dependence
  on connection state inherited from earlier commands is unsupported.
- Coordinate explicit `ANALYZE` through normal Monster/Raft commands; document
  its use instead of `PRAGMA optimize`. `VACUUM` support is deferred beyond the
  initial work and is not a near-term requirement.
- Add periodic committed scrub commands that compare subsets of DB2 pages
  through ordinary command-result agreement.
- A repair target closes normal operation and restarts in a repair-only mode,
  outside Raft, before discovering the leader and requesting repair.
- Followers monitor leader heartbeats for loss of the retained history needed
  to catch up. Once catch-up through retained Raft history is impossible, install
  a durable repair-required fence and enter the repair path.
- After its no-quorum SYNC is committed and durably applied, a node in
  `REPAIR_NO_QUORUM` should go offline without waiting for final outbound RPCs.
  With ordinary 2-of-3 or 3-of-5 quorums, successor elections can carry the
  shutdown through a majority. Automatic recovery from that state is mostly
  outside the initial scope.

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

Bind queued and active repair sessions to the donor leader, term, and session
identity. On an observed term change, cancel old sessions, discard their queued
work, and release their read transactions/snapshots. Also cancel on known donor
leadership loss/change, including a local step-down without a term increment.
Reject late RPCs from cancelled sessions. Integrate this cleanup with the
existing Raft/Monster leadership-change hooks.

Retain bounded local snapshot lifetimes for completion, cancellation, timeout,
and disconnected participants: a participant may never observe the new term.
Long readers retain WAL history, so WAL growth and the repair timeout remain
part of the design. A failed leader must not leave remote readers pinned
indefinitely.

Cancellation clears transient repair-session state, not the durable
repair-required state or partial-install protection. A cancelled target remains
outside Raft and rediscovers the leader before starting a fresh repair session.

How the target advances from the pinned checkpoint to safe rejoining remains an
open protocol step. Healthy writes continue during copying, so installation of
that snapshot alone does not establish that the target is current. Define any
required retained Raft history, further page transfers, and ordinary command
execution explicitly. Do not fill this gap with patchset/changeset application.

## Canonical page representation

Read pages through `sqlite_dbpage`, which exposes the transaction's pager view,
including visible WAL content. Do not compare only the main database file while
ignoring its WAL.

For local DB1 agreement and cross-member DB2 agreement, require matching page
size, page count, normalization types, and canonicalized bytes at corresponding
page numbers in the agreed snapshot. It is equality through the `sqlite_dbpage`
read interface, not byte identity of the main database file, WAL, shared-memory
file, or filesystem layout. Different checkpoint placement and WAL bookkeeping
are permitted.

`sqlite_dbpage` still returns SQLite page bytes, including B-tree layout and free
space. It does not turn equal SQL rows into a canonical serialization. The
earlier layout/history differences were observed through this same read
interface and remain relevant. The exceptions explicitly defined below are
applied to that page view before comparison.

Use one versioned canonicalization rule per database for comparisons, group
hashes, scrub results, and repair RPC payloads. Raw outgoing payload differences
would still fail Gate verification even if a separate comparison hash ignored
them. Both databases normalize page-1 counters; DB2 also substitutes its local
ownership page and its validated freelist leaves as described below.
DB1 does not need cross-member page equality.

Each participant independently determines how to normalize its own snapshot
pages. Include that normalization type in the hash input, alongside the page
number and canonical bytes. The four types are:

| Type | Canonical bytes |
| --- | --- |
| Ordinary | Original page bytes |
| Page 1 | Original bytes with the two agreed counters normalized |
| Ownership | Valid deterministic ownership-page template |
| Free leaf | A full page of zeros |

DB1 uses only the applicable types; ownership and free-leaf substitution are DB2
rules. These types describe normalization, not every SQLite B-tree/page role.
Use a fixed, unambiguous encoding under the normalization version.

For a group starting at page `s` and containing `m` pages, compute one digest:

```text
groupHash = H(groupContext,
              (s,       type[s],       canonicalBytes[s]),
              (s + 1,   type[s + 1],   canonicalBytes[s + 1]),
              ...,
              (s + m-1, type[s + m-1], canonicalBytes[s + m-1]))
```

`groupContext` binds the format/normalization version, database and agreed
checkpoint context, page size, group start, and actual group length. Bind the
session and database page counts in the comparison protocol as well. Page tuples
are ordered by page number. For example, `X = 64` means one digest covers all
64 pages' tuples, except for a shorter final group; it does not mean 64 separate
page digests are sent.

The target's initial RPC sends group hashes and size/context information; types
are already included in those hashes and need no separate initial type list.
The leader and healthy follower derive their types independently. In particular,
the follower must not use leader-supplied classifications when computing hashes.

Replacement groups carry page numbers, normalization types, and canonical bytes.
Four types can use a packed two-bit code per page; settle the exact wire encoding
with the versioned format. The target verifies those received tuples directly
against the agreed group hash, without renormalizing them using damaged local
metadata. No preliminary allocation-map exchange is needed. Schema and allocation
metadata still travel as ordinary database pages when they need replacement.

### Page 1 in DB1 and DB2

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

Locate the ownership page through the schema in the same pinned snapshot:

```sql
SELECT rootpage FROM main.sqlite_schema
WHERE type = 'table' AND name = 'monsterft_local_owner';
```

SQLite records the table's B-tree root page here. Under the dedicated table's
one-row/one-page invariant, that root is the page containing generation. Do not
hardcode its page number or search page bytes for the token. Validate the schema,
singleton, token, and expected leaf/root structure before treating it specially.
This lookup applies to the proposed separate ownership table; the production
schema still stores generation with the shared receipt.

For hashing and RPCs, substitute a deterministic valid template page containing
a placeholder token. Generating that template through the pinned SQLite build
is preferable to implementing a general SQLite record decoder.

On installation, instantiate this page at the donor image's expected location
with the recipient's fresh local token before activation. Do not adopt the donor
token, leave a placeholder usable, or blindly preserve an old page at that page
number. A corrupted local ownership page must be repairable, not hidden by
canonicalization. An all-zero page is not a valid table-page template.

Experiments support the one-page invariant through unequal restart/claim counts,
receipt-width changes, and root relocation. Resolve the root from each pinned
snapshot; reserve the owner table against application indexes, triggers, or
schema changes. Production schema-cache handling and full startup/installation
integration still need testing. Every shared receipt and active application
page remains fully compared. DB1 can retain its existing local generation: DB1
does not require cross-member byte equality, and the three Procs on one node
share the local startup token.

### DB2 freelist leaf pages

Identical SQL and identical settings, starting from matching canonical views,
can diverge after different restart counts. The production Apply reproduction
first differs at command 135, with restarts 0/19/12: one byte on freelist leaf
page 66 differs, while receipts/outcomes match and integrity checks pass. Even
writer and fresh-reader DBPAGE views of the same committed database can differ
on these unused leaves. This affects scrub as well as repair readers.

Agreed direction: substitute page-sized zeros for **whole freelist leaf pages**
in hashes and RPC payloads, subject to reliable classification. The supported
format has no extension-reserved bytes (page-1 byte 20 is zero). Identify leaves
from explicit allocation metadata: page 1 names the first trunk and total free
page count; each trunk names its next trunk and lists leaf page numbers. Read
the graph through DBPAGE in the same pinned transaction as the pages being
hashed. No byte-pattern inference or decoding of application columns is needed.
Preserve page-1 freelist head/count and all trunk pages.

Four 320-command scenarios passed with this extra rule, including later
reuse of differing pages. Do not broaden the mask to active-page slack or
trunk bytes on this evidence. Assess the same issue for local DB1 page
comparison separately; these preservation experiments focused on DB2.

Classification must come from validated allocation metadata in the same pinned
snapshot. Bounds/cycle/count checks alone are not proof that a damaged freelist
has not misclassified an active page. A damaged target map can mask the wrong
page and make a hash over canonical bytes alone misleading. Including the
normalization type in each page's contribution prevents that ambiguity: an
ordinary zero-filled page and a free leaf normalized to zeros have different
hash inputs. Every disagreement about normalization therefore causes its group
to disagree, subject to the hash's collision properties.

Matching groups establish agreement on types and canonical bytes. Replace every
page of each differing group, including the schema/allocation pages that differ.
This removes the need for metadata-dependency tracking and additional comparison
rounds after metadata repair. Installation validation remains required. Preserve
owner-page validation before substitution and the full-copy fallback for unusable
classification metadata. Do not infer that typed hashing detects damage inside
fields intentionally excluded by the agreed normalization rules.

The typed group format is now exercised by the disposable storage-repair
experiments, including the misclassified-zero-page case. Real page installation,
reopening, and continued SQL pass the attached-target controls described below.
Production implementation, Nix Node integration, and the full repair protocol
still need validation. No final quorum-endorsed complete manifest is introduced.

The format defines the locations precisely for a valid database; the experimental
parser is not yet a proven corruption boundary. Full SQLite integrity checking
includes freelist consistency and multiply-used/missing regions, and can support
local validation. Such checks do not prove that structurally valid application
values are correct. Donor trust comes from the agreed local DB1 and two-member
DB2 fault model. If the target
cannot expose usable pages or classification metadata, use full-copy recovery
rather than requiring differential repair to handle every damaged structure.
Define an efficient validation/repair path before treating this rule as
production-ready.

Alternatives tested: zeroing free leaves before COMMIT preserved the original
canonical equality but added 6,295 page writes across three replicas in the
320-command workload. Disabling cache spilling or using FULL auto-vacuum passed
shorter stress tests, with memory/write-layout tradeoffs still to measure.
secure_delete ON alone did not solve the free-leaf failure. Keep WAL/FULL.

### Agreed application and maintenance rules

Equal final rows, patchsets, and outcomes do not establish matching physical
mutation history. Require mutation values and ordering, including intermediate
writes later overwritten, to derive from replicated inputs/state. Connection
state inherited from earlier commands, local generation, local clocks, or local
randomness must not determine those writes. In particular, depending on a
previous command's `last_insert_rowid()` or cumulative `total_changes()` is
outside the supported deterministic callback contract. The counterexamples are
evidence for that boundary, not behavior the library must make work.

An insert followed by `last_insert_rowid()` within the same command is compatible
with this rule when that insert deterministically establishes the value being
used. Explicit ordered scans are needed when their iteration drives mutations.
Document these rules; no connection-state emulation or reset mechanism is
planned to support cross-command dependencies. Any additional enforcement
remains an implementation choice.

Pin relevant SQLite settings on every open. Coordinate mutating maintenance at
agreed boundaries through Monster/Raft commands so members execute the same
operation at the same logical sequence. Do not run it independently on node
open/close. Explicit ANALYZE and REINDEX can use the ordinary Apply transaction;
pin their targets, settings, and any custom functions/collations. Coordinated
maintenance matched the tested controls, while one-sided maintenance differed.

**VACUUM support is deferred, including for the near term.** SQLite forbids it
inside an existing transaction, so it cannot be a direct call in today's ordinary
Apply callback. A future implementation would need a Raft-ordered,
library-managed out-of-transaction maintenance step, with ordering,
crash/retry/receipt semantics, snapshot coordination, and page-equality validation
defined. Retain this as a future TODO; do not add it to the initial repair work.

Identical sequence and SQL do not make bare PRAGMA optimize deterministic: it
can choose different work based on the connection's query history after a
restart. User documentation should direct applications to explicit coordinated
`ANALYZE` instead of `PRAGMA optimize`, including avoiding automatic optimize
calls on connection open/close. Appending a maintenance statement alone does not
establish its determinism. Keep these restrictions distinct from ordinary
checkpoint timing, which passed the tested controls. See the experiment report
for scope and counterexamples; these results are not a general determinism proof.

## Page installation: experimental findings and candidate

The [storage experiments](doc/DB2-PAGE-EQUALITY.md#page-writes-and-grouped-repair)
now exercise actual page replacement, growth/truncation, schema changes, and
reopening. They do not establish a universal ascending-page requirement. Direct
replacement of the connection's main database can trigger schema reprepare
against an image being changed; moving page 1 last did not solve all cases.

Preferred candidate for integration: open a dedicated connection with an
in-memory main database, ATTACH the on-disk recipient, and write through
`main.sqlite_dbpage` with its hidden `schema` column selecting that recipient.
Keep the recipient in WAL/FULL. This arrangement passed all 64 full-image
controls across four write orders, cold/warm schema, and both statement APIs.
The typed-group harness also passes its 13 scenarios in each of two attached
write orders, including expected follower-error aborts. See the report for scope.

Within a single-file installation transaction, write every page in every
accepted replacement group. Set exact size with `INSERT(N+1,NULL)` targeting
that database as the **last write before COMMIT**; subsequent writes can cancel
the pending truncation. Close the installation connection and reopen the
recipient before ordinary SQL, discarding stale schema/page interpretation.
Fresh ownership and valid-header rules still apply. Capability probes on the
Nix runtime must exercise disposable DBPAGE writes/rollback, not just reads or
compile options; SQLite's defensive mode can disable DBPAGE writes.

These are storage-level findings and a candidate recipe, not the final lifecycle
or cross-file interruption design. The grouped experiment models messages in
process and continues direct deterministic SQL with unequal restart counts;
local DB1 A/B/C reconciliation and production startup/Apply integration remain
untested. Keep those tests and the deferred lifecycle questions below.

## Physical local DB1 reconciliation and donation

The leader's local ProcA/B/C copies provide the DB1 donation agreement. Other
cluster members do not need to hold byte-identical DB1 files and are not asked
to vote on DB1 donation pages.

This relies on the existing local fault model: independent copies and enough
correct local participants. Local agreement does not itself establish current
leadership or which entries are committed; the snapshot must bind those facts
separately.

Use **transactional `sqlite_dbpage` writes** for local DB1 replacement, retaining
WAL mode and FULL. This fits the existing transactional adoption flow and avoids
coordinating filesystem replacement and all three connection lifetimes solely
to preserve identical page-1 counters. SQLite manages the destination's WAL and
transaction commit/rollback; counter differences are handled by canonicalization.

Preferred startup/reconciliation sequence:

1. Quiesce local protocol work, pin comparable DB1 snapshots, and compare their
   complete canonical page manifests, including page count and snapshot
   identity. Select an agreeing pair before replacing an outlier; keep those
   supporting snapshots pinned during copying.
2. Begin an IMMEDIATE transaction on the outlier and check its current local
   generation. Verify both supporting snapshots carry that same startup token.
3. Install the selected donor image through `sqlite_dbpage`, including exact
   page-count growth or truncation, and commit. Use valid source page bytes for
   local copying; the counter mask defines comparison/encoding, not a requirement
   to store zero counters in the live database. No logical row replay is involved.
4. Refresh affected SQLite schema/prepared-statement state and application log
   caches. Verify the committed canonical image and ownership, then complete
   receipt reconciliation consistently. Require matching complete canonical
   manifests at the common checkpoint before resuming operation and donation.
5. During donation, each Proc independently reads/verifies its selected pages
   and produces matching canonical repair output through Gate.

Current `ProtocolStore.open()` claims generation before capture, and `adopt()`
already guards its write transaction and reads both supporting donor snapshots.
Preserve those ownership checks while replacing the logical copy and summary
comparison with page installation and complete-manifest verification. All local
copies share the startup token, so this local copy must not change ownership.

Page-count handling, valid header installation, and schema/cache refresh still
need tests against the pinned SQLite build. If refreshing the destination safely
requires reopening it, allow that without requiring A/B/C to share identical
connection lifetimes.

Current startup `stateDigest()` compares election state, metadata, and log head;
it does not certify every retained log row or page. It is insufficient by itself
to authorize a complete physical donor image. Do not combine unrelated pages
from different checkpoint snapshots.

Current Gate requires all three outgoing results to agree. Two-of-three source
selection for local reconciliation does not silently change Gate into a
two-of-three outgoing-RPC mechanism. Repair the outlier, then use the established
matching-output path. Normalize page 1 in both the local manifests and the actual
DB1 donation RPC payloads, so allowed counter differences cannot break Gate
agreement.

The target verifies the received canonical DB1 bytes against the donated manifest.
Installation and fresh generation claims can change its headers; do not require
post-activation raw equality with the donor. Require canonical page equality
among the target's own A/B/C copies after startup. These intentional local
changes need a defined place in verification/activation. Installing an image
must not roll back required recipient term/vote protections. The exact remote
installation and rejoin rules remain to be specified.

## DB2 grouped comparison and transfer

Let `X` be the configured number of pages per group. For an authoritative DB2
image of `N` pages, there are `Y = ceil(N / X)` groups, numbered from the first
database page, with a possibly shorter final group.

The initial flow is:

1. The repair target computes one hash for each group of `X` pages using the
   ordered page-number/type/canonical-byte tuples above. It sends all group
   hashes and size/context information to the leader in one RPC.
2. The leader independently computes corresponding hashes from its pinned DB2
   snapshot and identifies the disagreement groups.
3. For those groups, the leader sends one RPC to one eligible healthy follower,
   containing the group identities and leader's expected hashes. The request
   binds the agreed checkpoint, normalization format, and donor image size.
4. The follower independently derives normalization types and computes group
   hashes from its own pinned snapshot. It returns the list of disagreements;
   an empty list explicitly acknowledges successful checking of the entire
   identified request, including image size. Read/classification failures or
   incomplete work are errors, never silently omitted disagreements. This uses
   exactly one other member regardless of cluster size; it does not use the
   configured Raft/application quorum size.
5. Once the follower agrees, the leader sends the replacement groups with their
   page numbers, normalization types, and canonical bytes. The target verifies
   the received tuples directly against the agreed group hashes and writes
   **every page in every accepted replacement group**. No per-page no-op check
   or write-skipping optimization is planned. Retain the valid-header and fresh
   local ownership installation rules.
6. Validate installation and the DB1/DB2 checkpoint relationship before
   proceeding toward rejoining. Do not add a final quorum vote on a complete
   image manifest.

ProcA/B/C do not count as separate DB2 corroborators: they share one local DB2
file. The required follower is another healthy member, not the repair target.
This policy assumes independent damage is unlikely to produce the same wrong
pages on two members that consider themselves healthy. It does not establish
correctness against identical damage or a shared software bug on both copies.
Requiring a larger quorum may be a later option; it is not the initial policy.
DB1 donation and ordinary Raft, SYNC, and scrub outcome agreement retain their
separate local/cluster agreement rules.

**Final acceptance does not require a quorum-endorsed complete manifest.**
Unchanged groups are accepted on target/leader hash agreement at the selected
snapshot; matching hashes cover normalization types as well as canonical bytes.
Replacement groups require leader-plus-one-follower agreement. Leader
coordination, ProcA/B/C output agreement, RPC checksums, and received-group hash
verification protect the transfer. This deliberately does not obtain fresh
follower checks for unchanged groups. That is the accepted scope of donation
verification, not an unresolved requirement for another full-image vote.

**If the selected follower disagrees with the leader, abort repair and mark the
leader suspect as a donor.** With two differing copies, this does not establish
which member is damaged. Do not relay the follower's alternative bytes or try
additional followers merely to find a matching answer. Define how suspicion
causes fencing/leadership change and how the target rediscovers the next donor.
If no eligible follower is available, wait/retry within the session timeout;
do not send uncorroborated replacement groups. Follower selection and handling
of unavailability before comparison remain implementation details.

Important details to resolve:

- Target and donor may have different page counts and therefore different
  values of `Y`. Define authoritative size agreement, missing/extra groups,
  growth, truncation, and partial final groups.
- Bind each hash to the database/snapshot identity, normalization version,
  page size, group start, and actual page count. Reject reordered, truncated,
  duplicated-as-votes, or stale responses.
- Settle the normalization-type codes and unambiguous group encoding. Include
  types in the hash input and with replacement pages; no preliminary type/map
  exchange or metadata-dependent rehash protocol is needed.
- Minimize transfers by selecting only disagreement groups at granularity `X`.
  Every page of a selected group is written, even if some pages already match.
  Tune group size without introducing per-page write skipping or finer repair
  subdivision.
- Choose a crash-safe installation mechanism: a transactional in-place install
  or staged images for differential repair.
  Full-copy replacement is the fallback when the target cannot expose a usable
  page view, as described below.
- DB1 and DB2 do not share one atomic WAL commit. A durable repair-in-progress
  state must prevent activation after partial installation or a crash; copying
  donor metadata must not accidentally clear that protection.

## Heartbeat detection of retention gaps

Followers must learn from accepted current-leader heartbeats when the leader's
retained history no longer supports their catch-up, then durably install a
repair-required fence. Use the leader's actual retained-log boundary or
equivalent explicit catch-up information; a large sequence gap or calculation
from `keepTarget`/`keepTrigger` alone does not establish which entries remain.
The comparison must account for the follower's recoverable log/checkpoint and
the predecessor needed to resume replication. Stale or rejected-leader
heartbeats must not cause this transition.

Current heartbeats advertise the leader's head sequence/term and commit sequence,
but not its retained-log boundary. Extend that information when implementing
this rule. Keep a real loss of required history distinct from the already
documented lost-ACK/cancelled-replication case: a follower that already has the
needed history must not require a database repair merely because the leader's
replication cursor is stale. Exact message fields, boundary comparisons, and the
fence transition belong to the upcoming lifecycle work.

## Repair-only startup and completion

A node that needs ordinary automatic repair closes its normal components, then
restarts into a repair-only mode. It does not vote, run election timers,
participate in ordinary Raft replication, or execute application commands while
its image is untrusted.

It sends direct discovery RPCs to every cluster member asking who they believe
the leader is, then requests repair from the leader. Discovery needs term-aware
handling of stale/conflicting answers and bounded retries. A stale leader claim
does not authorize a snapshot merely because one member reports it.

Use differential repair when the target can expose a usable canonical page view.
Opening successfully does not guarantee subsequent page reads, schema lookup,
or freelist traversal will succeed. If a DB1 copy or DB2 cannot open, or cannot
provide that usable view, fall back to full-copy recovery of the affected image.
The target need not produce hashes for an unreadable image. Readable corruption
normally produces disagreement groups. Normalization types in the hash input
also expose mismatched classifications whose canonical bytes happen to match.

For full-copy recovery, close all affected connections and discard/replace the
unusable database files, handling their old WAL/SHM files as part of the same
recovery. Obtain complete images from the leader's pinned checkpoint: DB1 uses
the leader's local A/B/C agreement, and every supplied DB2 group requires the
leader plus one healthy follower. Preserve the durable repair-only marker and
required local identity/term/vote protections outside anything being replaced;
an interrupted rebuild must never restart as a fresh participating member.
Restore a consistent DB1/DB2 checkpoint pair and claim fresh local ownership
before activation. The exact staging/install mechanism remains to be specified.
This fallback does not change the normal transactional DBPAGE method for local
DB1 reconciliation.

The repair session coordinates the pinned healthy snapshots, local DB1 donation,
DB2 group verification, transfer, installation, and validation. Completion or
timeout releases all participant snapshots. Apply the term/leadership-change
cancellation rules above to queued and active sessions. Cancellation or an
aborted transfer releases local resources and keeps the target fenced while it
rediscovers the donor and retries; late replies cannot reactivate an old session.

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
ordered page-number/normalization-type/canonical-byte encoding as repair, and
returns it as the scrub command's result. Normal command
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
remaining online indefinitely. No final outbound RPC, output drain, or
commit-notification acknowledgement is required before shutdown.

Reuse the existing protocol: `MonsterEngine.decide()` constructs and appends
the SYNC; `RaftEngine.checkCommit()` establishes commitment from quorum
replication in the current term. `applySync()` writes the `REPAIR_NO_QUORUM`
repair state and the applied checkpoint; the enclosing Proc DB1 transaction
must commit before shutdown. The early client no-quorum error precedes the SYNC
append and is not a shutdown trigger. Preserve the durable fence across restart.
The existing terminal-input behavior, which commits before suppressing outputs,
is compatible with this rule; do not require those final outputs to be released.

The intended outcome is that a majority durably applies the fence, not that
every member learns the final commit position. For an odd-sized cluster with
the ordinary majority quorum, successor elections provide this progression:

- With three nodes A/B/C, A can commit the SYNC on A+B, apply it, and stop without
  informing B of commitment. B can win the next election with C. Its normal
  current-term election no-op, replicated to B+C, establishes commitment of the
  inherited prefix. B then applies the SYNC and stops. A+B are fenced; C may
  remain unaware of commitment and is unable to form a quorum alone.
- With five nodes, after the first shutdown four remain, and after the second
  shutdown three remain. Each surviving set can still elect a leader and commit
  its election no-op with a quorum of three. After the third node applies the
  SYNC and stops, a majority is fenced; the remaining two cannot form a quorum.

[Raft's leader-completeness rule](https://raft.github.io/raft.pdf) preserves the
committed SYNC in successor leaders. MonsterFt already appends the election
no-op and waits for its commitment/application before admitting new leader
command work. A successor
must establish commitment normally; merely finding a SYNC in its log is not
permission to apply it. Commit notifications that do arrive can hasten shutdown,
but the progression does not depend on notifications from a departing leader.

This progression assumes the remaining nodes can communicate, finish pending
application work, and make normal Raft progress. It covers ordinary 2-of-3 and
3-of-5 quorums (generally `N = 2f + 1`, quorum `f + 1`). Do not claim the same
majority-fencing guarantee for even-sized membership or a larger configured
quorum: for example, with 3-of-3, the first shutdown prevents another election.
Their shutdown policy remains to be specified if those configurations are in
scope. Behavior when the original SYNC cannot commit remains a failure/timeout
question. No separate shutdown-consensus protocol is introduced.

Automatic repair of this state is mostly out of scope.
Keep it distinct from ordinary minority repair, where healthy members agree on
an authoritative image.

A possible later explicit recovery procedure chooses one cluster member as the
recovery leader/authority and has all other members copy its DB1 and DB2. This is
a deliberate choice of truth after agreement has failed, not a claim that the
chosen image was established by the ordinary quorum protocol. Selection and
cluster-reset semantics are not decided in this draft.

## Evidence and experiments still required

The new [DB2 preservation report](doc/DB2-PAGE-EQUALITY.md) and
[reproducible scripts/results](experiments/db2-page-equality/README.md) include:

- Ownership isolation through 256 checkpoints with restart counts 0/37/5.
- Actual Apply execution with 0/48/4 restarts, plus the separate restart-only
  freelist counterexample reproduced on Node SQLite 3.54.0 and SQLite 3.54.1.
- SIGKILL before commit and after commit/before simulated ACK, preserving
  canonical equality through recovery/replay or receipt-based duplicate skip.
- Free-leaf canonicalization and physical-zeroing alternatives, plus failing
  intermediate-write and local-maintenance cases and their controls.
- Actual DBPAGE installation/order tests and typed grouped repair: the attached
  controller passes schema changes, growth/shrinkage, corrupt-target fallback,
  reopening, and continued deterministic SQL with unequal restart counts.

All page comparisons use DBPAGE read transactions, with WAL/FULL. These are
prototype/schema, Apply-path, and storage-repair experiments, not a complete
Raft repair test. Modeled message batches are not a transport test.
The future Nix Node runtime and production ownership integration remain untested.

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

The probes use the current Node runtime plus a separate custom-SQLite reader,
or custom SQLite directly. Repeat the relevant tests using the chosen Nix-built
Node. Preserve the remaining experiment agenda without introducing
patchset/changeset application.

### Ownership-page isolation and integration

- Retain the passing ownership/schema probes and integrate the new table into
  production startup/Apply, then repeat unequal restart/claim counts and receipt
  integer-width boundaries on the pinned runtime. Verify actual RPC encoding.
- Verify stale owners cannot execute after takeover, including an active Apply
  transaction racing replacement.
- Test canonical-template installation, a damaged ownership page, changed root
  mapping, interrupted installation, and claiming the recipient token before
  activation.

### Then: DB2 canonical page-view equality and possible new rules

- Start from common physical images and execute the same ordered commands under
  the pinned runtime and settings. Compare at agreed receipt checkpoints.
- Vary restarts, checkpoint timing, batching, savepoint failures, large BLOBs,
  page splits, deletes, freelist reuse, hidden rowids, and application indexes.
- Validate coordinated ANALYZE and REINDEX under the pinned settings and document
  the supported command contract. Preserve the existing VACUUM experiment results
  for future work; VACUUM implementation/testing is not a near-term requirement.
- Decide explicit page-size, auto-vacuum, secure-delete, and other relevant
  settings; do not assume Nix's defaults equal the current Node defaults.
- Evaluate the tentative rule that members must have matching canonical DB2
  pages at boot or enter repair. Such a comparison needs a common checkpoint;
  ordinary lag and differing physical history must be handled deliberately.
- Generation and page-1 normalization alone are disproven for the tested
  rollback workload. Validate the agreed additional free-leaf rule against
  corrupted classification metadata, leaf-to-trunk reuse, metadata-changing
  repairs, and further deterministic workloads before settling the format.
- Document the agreed application mutation/maintenance contract and determine
  which restrictions need enforcement rather than documentation alone. Keep
  cross-command connection-state dependencies explicitly unsupported.

### Then: reconciliation, repair, scrub, and lifecycle

- Repair each possible local DB1 outlier, including corruption away from the log
  head, through transactional `sqlite_dbpage` replacement. Verify page-count
  growth/truncation, rollback on interruption, ownership checks, and schema/log
  cache refresh. Compare complete canonical manifests after commit, receipt
  reconciliation, and continued ordered inputs, plus matching Gate RPCs despite
  differing raw page-1 counters.
- Test group comparisons with missing/extra pages, partial groups, independently
  differing corruption patterns, an unavailable follower, and leader/follower
  disagreement. Verify one-follower corroboration in both three- and five-member
  clusters, abort on disagreement without seeking a matching third member,
  and retain ordinary Raft/scrub quorum rules. Verify unchanged groups require
  no fresh follower check and final acceptance adds no quorum-endorsed complete
  manifest.
- Verify one digest covers all ordered page tuples in each multi-page group,
  all target group hashes fit the single submission flow, and the follower
  checks the requested groups in one request. An empty disagreement list must
  acknowledge complete successful checking; omitted/error groups must not pass.
- Test unopenable images and failures during page reads, schema lookup, or
  freelist traversal. Verify full-copy fallback, stale WAL/SHM handling, persistent
  repair fencing across replacement/crash, and corroboration of every full-copy
  DB2 group. Corrupt otherwise parseable allocation/owner mappings and verify
  differing normalization types cause immediate group disagreement even when
  canonical bytes match. Include an ordinary zero-filled donor page misclassified
  as a free leaf on the target, with garbage hidden by target normalization.
  Verify replacement resolves the corruption without metadata-dependent rehash
  rounds and received typed bytes are hashed without target-side renormalization.
- Verify only accepted disagreement groups are transferred and every page in
  each such group is installed, including pages that already match. Measure
  actual writes as well as transferred bytes; do not add no-op write skipping.
- Keep healthy writers running while repair readers are pinned; test WAL growth,
  timeouts, term changes, same-term donor step-down, interrupted transfers, and
  cleanup. Verify late old-session RPCs are rejected and cancellation preserves
  the target's durable repair/install protection.
- Interrupt installation between DB1/DB2 steps and verify restart stays in
  repair mode until a consistent image is established.
- Verify scrub selection/coverage, deterministic sampling before receipt update,
  detection of latent page corruption, and concurrent scrub/repair snapshots.
- Verify minority repair and eventual rejoin without the existing
  cancelled-replication failure.
- Test heartbeat-driven retention-gap detection and durable fencing, including
  the exact retained-boundary/predecessor cases, stale heartbeats, and a follower
  whose data is current despite a lost ACK. Do not mistake the latter for a
  genuine need to replace its databases.
- Test no-quorum SYNC shutdown after durable application with all final outgoing
  messages suppressed. In 2-of-3 and 3-of-5 clusters, verify successor elections
  and election no-op commitment fence a majority even when remaining followers
  initially do not know the SYNC committed. Test fence persistence across restart;
  do not shut down on the early client error or an uncommitted SYNC.

## Suggested implementation order

The current next step is the storage experiment pass described above; settling
the full repair lifecycle and interruption protocol follows that pass. The list
below remains the broader implementation direction.

1. Add the pinned Nix runtime and startup/CI capability probes for DBPAGE and
   session capture, retaining WAL/FULL.
2. Extend the DB2 ownership prototype with the agreed free-page rule, typed
   multi-page group hashes, and application/maintenance constraints. Test the
   corruption cases and establish the required page-view equality rules before
   treating page hashes as authority.
3. Implement transactional local DB1 page reconciliation and complete-image
   verification.
4. Define shared checkpoint capture, snapshot lifetime, and repair-only startup.
5. Implement grouped DB2 verification and leader-only transfer, then crash-safe
   installation and the explicit completion/rejoin path.
6. Add periodic scrub commands and integrate their outcomes with repair.
7. Change no-quorum shutdown behavior with durable-fence and successor-election
   tests. Keep explicit choose-one-member recovery as separate future work.

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
