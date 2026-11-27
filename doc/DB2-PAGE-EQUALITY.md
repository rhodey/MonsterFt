# DB2 page equality experiments, 2026-11-27

The proposed ownership page works in the tested workloads, including unequal
restart counts. **Ownership-page substitution and the two page-1 counter masks
alone are insufficient.** Identical deterministic commands can leave different
bytes in unused freelist leaf pages after savepoint rollback. This reproduces
through production `MonsterApply._execute`, not just a standalone SQLite model.

The strongest candidate is to canonicalize those unused leaf pages too, while
fully comparing the metadata that identifies them. Application mutation history
also needs a stronger contract than equal final rows/patchsets. These are
experimental recommendations, not implemented production changes.

## Scope and reproducibility

Sources, commands, and recorded JSON are in
[experiments/db2-page-equality](../experiments/db2-page-equality/README.md).
All comparisons read `sqlite_dbpage` inside actual read transactions. WAL mode
and `synchronous=FULL` remain enabled. No changeset or patchset is ever applied.
Preservation tests verify matching canonical page views, then execute common
command sequences with independent restart schedules and fresh local tokens.

Two runtime layers were tested:

- Node v26.13.0, built-in SQLite 3.54.0, using the actual production
  `MonsterApply._execute` for two suites. Experimental startup/schema glue moves
  generation into the proposed owner table; production source is unchanged.
- A separately compiled SQLite 3.54.1 with DBPAGE, SESSION, and PREUPDATE_HOOK,
  used for broader C API probes and as the external page reader for Node files.
  The source is the workspace's better-sqlite3 amalgamation; a build helper and
  recorded source hash make this reproducible without changing that project.

The Node suites cover callback/savepoint/session/receipt/commit behavior, not
the production startup handshake, `onReceive` replay path, full Raft cluster,
or future repair RPC implementation. The crash suite models that ordering in
the C API. SIGKILL tests process death, not failed fsync or power loss. The
future pinned Nix Node build has not been built or tested here.

The later storage-repair experiments below use the custom SQLite library for
both reads and writes. They exercise real page installation with in-process
stand-ins for message batches. They do not implement repair lifecycle,
interruption recovery, paired DB1/DB2 snapshots, production Apply replay, or Raft.

## Results

| Experiment | Unequal restart counts | Result |
| --- | --- | --- |
| Owner isolation, 256 checkpoints | 0 / 37 / 5 | Only page 1 and owner page differ; no observed token leakage |
| Production Apply, 280 batches plus receipt-width boundaries | 0 / 48 / 4 | All 36 sampled page comparisons match; 40 rejected items per replica |
| Production Apply, rollback stress, no restarts | 0 / 0 / 0 | All 160 comparisons match |
| Same rollback stress, only restart schedules differ | 0 / 19 / 12 at failure | At command 135, page 66 byte 3 differs; all 159-page images have matching receipts/outcomes and pass integrity checking |
| SIGKILL before commit and after commit/before simulated ACK | Persistent A; three crashes each for B/C | Recovery/replay or duplicate-skip preserves equality through 1,427 pages |
| Additional free-leaf canonicalization, four configurations | 0 / 45 / 29 | All 1,280 comparisons match; later reuse of differing pages also matches |
| Physically zero free leaves before commit | 0 / 45 / 29 | All 320 comparisons match, at extra scan/write cost |

The passing general Apply workload includes table/index splits, overflow BLOBs,
deletes/reuse, TEXT primary keys, WITHOUT ROWID tables, AUTOINCREMENT, failed
savepoints, different cache sizes/checkpoint schedules, and a long reader held
across other writes and writer restarts. Passing it is useful evidence, not a
general determinism proof.

### Ownership isolation

[ownership-results.json](../experiments/db2-page-equality/ownership-results.json)
records 256 three-replica comparisons, owner-claim counts 256/293/261, and receipt
integer-width boundaries through SQLite's signed 64-bit maximum. Raw differences
remained on page 1 and the owner page. Historical tokens were scanned for
outside the owner page; none were found.

A valid template followed by a fresh local claim within the same transaction
worked; rolling that transaction back retained the old owner. A replacement
claim was blocked while another writer held its transaction; afterward, the old
owner failed its token check. Malformed owner rows, extra indexes/triggers, and
damaged page headers were rejected before canonicalization.

The owner root is not a permanent page number. A coordinated FULL-auto-vacuum
DROP moved it from page 4 to 3 while preserving equality. A one-sided VACUUM
moved another root from 3 to 2 and broke equality; performing the same VACUUM on
both images restored equality in that case. Resolve and validate the owner
table from each pinned snapshot. Reserve its schema against application changes.

### Free-page counterexample

[apply-freelist-results.json](../experiments/db2-page-equality/apply-freelist-results.json)
is the production Apply reproduction. All replicas use the same cache size
(2,000 pages), cache spilling enabled, secure_delete OFF, WAL/FULL, common SQL,
and common starting canonical pages. Only B's and C's restarts differ: before
every seventh and eleventh command, respectively. There is no checkpoint skew
or extra pinned reader in this reproduction.

Commands insert/update/delete deterministic values. Every fifth command also
tries a large update, hits the same uniqueness violation, and rolls that item
back using the ordinary Apply savepoint behavior. At command 135, with restart
counts 0/19/12, page 66 differs at byte 3. That page is a freelist leaf in all
three snapshots. The complete production receipts and outcomes still match.
No-restart control passes 160 commands. All integrity checks report `ok`.

[lifecycle-results.json](../experiments/db2-page-equality/lifecycle-results.json)
reproduces the same result on SQLite 3.54.1 and separates restart-only,
checkpoint-only, and reader-only controls. Only restart-only fails in these
equal-setting controls. `secure_delete=ON` still fails at command 135. Differing
cache/spill configurations can expose differences earlier, at command 20.

There is also a connection-view issue: at the same committed receipt, a writer
can return `00000041...` for page 66 while a fresh reader of that same database
returns `00000000...`. Every observed writer/reader difference is on a free
leaf. This matters because scrub executes on the Apply connection while repair
can hold a separate reader.

SQLite's file format assigns no information to freelist leaf contents; trunk
pages carry the allocation links. This supports treating leaf contents as
undefined representation rather than repairable application data. The observed
WAL behavior is measured here, not inferred from rollback-journal guarantees.
[SQLite file format](https://sqlite.org/fileformat.html#the_freelist)

### Options for free pages

| Option | Measured evidence | Cost or remaining question |
| --- | --- | --- |
| Canonicalize freelist leaves to page-sized zeros | 4 × 320 checkpoints passed; secure_delete ON/OFF and equal/mixed cache profiles | Requires reliable snapshot-specific classification; recommended candidate |
| Zero nonzero free leaves through DBPAGE before COMMIT | 320 checkpoints passed without the extra mask | 6,295 page writes / 25,784,320 bytes and 157,854 scanned pages across three replicas |
| Disable cache spilling on every writer | 160-command stress case passed | Larger dirty transactions consume more memory; limited evidence, not a general fix |
| FULL auto-vacuum | 160-command stress case passed | Changes allocation/moves pages at commit; extra work needs measurement |
| secure_delete ON alone | Still fails at command 135 | Does not solve the free-leaf case |

[freelist-results.json](../experiments/db2-page-equality/freelist-results.json)
keeps page-1 freelist head/count and **all trunk bytes** in the comparison. Only
structurally validated leaf contents are substituted. Previously differing free pages later
became active and matched raw bytes: 1, 1, 181, and 174 reused pages in the four
scenarios. Scanning without zeroing is a control and still diverges, so the
physical-zeroing result is not merely an observer/cache effect. Reported writes
are DBPAGE writes in the harness, not measured device I/O or WAL amplification.

The experimental parser checks bounds, cycles, duplicate entries, and page
counts. That alone is not a production corruption proof: an otherwise plausible
damaged freelist can misclassify an active page. Preserve and validate all
allocation metadata; never mask it. If repairing metadata changes page
classification, a hash over canonical bytes alone can miss a disagreement that
was hidden by the original classification. The agreed [repair plan](../PLAN.md)
addresses this by including each page's normalization type with its page number
and canonical bytes in one hash over multiple pages. Replacement groups carry
those types, and the target writes every page in each accepted group. This
replaces the earlier metadata-dependent rehash proposal and does not require a
new quorum vote on a complete final manifest. The preservation experiments above
did not test typed repair; the storage experiments below now cover selected
corruption and installation cases.

No tests established a need to mask unused trunk tails or slack inside active
B-tree pages. Do not silently broaden the exception to those areas.

### Connection-local state can escape through intermediate writes

[connection-results.json](../experiments/db2-page-equality/connection-results.json)
contains nine Node SQLite cases. Baselines match even after B restarts and
claims a new owner. The same next SQL can then produce different intermediate
values from `last_insert_rowid()` or `total_changes()`. For example:

```sql
UPDATE items SET v = replace(hex(zeroblob(32)), '0',
                            char(65 + last_insert_rowid() % 26)) WHERE id=25;
UPDATE items SET v = 'x' WHERE id=25;
```

The persistent connection remembers rowid 777; the reopened one has zero.
Final rows, captured patchsets, and outcomes match, but 58 stale application-page
bytes differ. Repeating B's restart seven times has the same problem. SQLite
documents last-insert state as belonging to the connection.
[SQLite last-insert-rowid](https://sqlite.org/c3ref/last_insert_rowid.html)

secure_delete ON eliminates that specific residual-byte example. It does not
fix different intermediate allocation: choosing a temporary overflow value's
size from that local state, then overwriting it with the same final value,
leaves 15 versus 11 total pages even with secure_delete ON.

Recommended contract: **every mutation value and mutation order, including
temporary writes that are later overwritten, must derive from common replicated
inputs/state.** Equal final rows/patchsets are weaker. Cross-command connection
counters, local tokens, clocks, randomness, and unordered mutation-driving scans
cannot determine those writes. A last-insert value established by the same
deterministic command is a different case; explicit values or INSERT RETURNING
make that dependency clearer. Literal-intermediate and ordinary automatic-rowid
controls passed. `changes()` also matched under the tested claim/receipt pattern;
this is not a blanket prohibition on every connection function.

### Maintenance and ordering

After identical indexed reads and growth from 200 to 4,200 rows, restarting only
one connection made identical bare `PRAGMA optimize` calls differ: one updated
sqlite_stat1, the other did nothing. SQLite makes that decision partly from
connection query history. Explicit ANALYZE and `optimize(0x10002)` matched in the
controls; those are candidates for coordinated maintenance, not blanket
determinism guarantees. [SQLite optimize rules](https://sqlite.org/pragma.html#pragma_optimize)

The optimize case produced **different patchsets** (46 versus zero bytes), so
normal command agreement detects this case when it occurs inside a command.
It is distinct from the latent free-leaf and intermediate-write examples above.

Independent VACUUM, REINDEX, and ANALYZE changed page views. Applying the same
maintenance to both replicas matched in the tested controls. An unordered
INSERT SELECT with deliberately differing `reverse_unordered_selects` settings
changed hidden rowids/page layout; adding ORDER BY on a unique source key fixed
that case. Pin relevant connection settings on every reopen, and coordinate
maintenance at explicit replicated boundaries. Bare optimize on local open/close
is incompatible with this goal. VACUUM needs its own lifecycle boundary because
it cannot run within the ordinary Apply transaction.

## Page writes and grouped repair

[page-write-order-results.json](../experiments/db2-page-equality/page-write-order-results.json)
records 312 full-image installation cases: 246 successful installs and 66
negative controls. Successful installs matched canonical pages after reopening,
passed integrity checking, and preserved equality after identical subsequent
DML/DDL and growth. The donor reader stayed pinned while its writer advanced.

There is no general rule established here that earlier pages must be written
before later pages. The important distinction was the connection/schema used
to perform the writes:

- Direct writes into the connection's main database can fail when replacing
  schema/root layouts. Ascending order can fail at page 2 after writing page 1;
  moving page 1 last can move the failure to the subsequent truncation statement.
  Both per-statement preparation and a reused prepared INSERT reproduce this.
- An in-memory main database controlling an attached on-disk target passed all
  **64 attached-target controls**: ascending, descending, page-1-last, and shuffled
  orders; reused or separately prepared statements; cold or warmed target schema;
  four target shapes. These controls use cache size 5 with spilling enabled.
- Close and reopen the target before ordinary application SQL. Cached schemas
  can remain stale even after a successful page copy; some same-connection
  queries reported missing tables while fresh connections worked.
- A transaction covering the page installation matters. Per-page autocommit
  failed in smaller/empty-target controls after committing page 1.

The successful controller writes through the main database's virtual table,
using its hidden `schema` column to select the target:

```sql
ATTACH DATABASE 'disposable-target.db' AS repair;
PRAGMA repair.journal_mode = WAL;
PRAGMA repair.synchronous = FULL;
BEGIN IMMEDIATE;
-- Repeat for every page in the accepted replacement groups:
INSERT INTO main.sqlite_dbpage(pgno, data, schema)
VALUES(?1, ?2, 'repair');
-- Last write before COMMIT; ?1 here is donor page count + 1:
INSERT INTO main.sqlite_dbpage(pgno, data, schema)
VALUES(?1, NULL, 'repair');
COMMIT;
```

The main database in this example is `:memory:`; the attached target remains
WAL/FULL. The recorded EXPLAIN output shows the INSERT's schema-cookie check
addresses the unchanged main database. Direct-main writes instead check the
schema being replaced. This and the source's page-copy behavior explain why
schema refresh/reprepare is relevant; this is evidence for the tested SQLite
build, not a promise about every future version. Close the controller and reopen
the target after commit. Do not substitute `repair.sqlite_dbpage` for the tested
`main.sqlite_dbpage` plus `schema='repair'` arrangement.

**Truncation must be last.** SQLite specifies that `INSERT(N+1,NULL)` truncates
to N pages only when used as the last write before COMMIT; later writes and
ROLLBACK TO can cancel it. A 108-page target restored from a 44-page donor still
occupied 108 main-file pages without final truncation, although DBPAGE exposed
44 pages. Final truncation reduced it to 44. Equality is still defined through
DBPAGE, but exact installation size must not depend on a header hiding an old
physical tail. [SQLite DBPAGE documentation](https://sqlite.org/dbpage.html)

[grouped-repair-results.json](../experiments/db2-page-equality/grouped-repair-results.json)
records 104 cases across eight installer variants. The two attached-target
variants each pass all **13 scenarios**: 11 completed repairs and two expected
aborts for follower disagreement/read failure. Each completed repair passes
integrity checking and canonical equality after reopening, then 30 further
deterministic command checkpoints with restart counts **0/10/6**. Across those
two variants that is 660 continued equality checkpoints. Cases cover:

- Ordinary application and index corruption.
- The active-zero page falsely classified as free: byte-only hashing misses its
  group's disagreement, while the typed hash detects it immediately. Replacing
  two four-page groups fixes the damaged allocation metadata and hidden bytes.
- Growth from 79 to 283 pages, shrinkage from 283 to 79, ownership root moving
  from page 3 to page 2, and partial final groups.
- Full-copy fallback for damaged owner pages, freelist loops, schema headers,
  and a file header that produces `file is not a database`.
- A pinned donor/witness snapshot remaining usable while their writers advance.

Every selected replacement page is written. `pagesWritten` counts explicit
DBPAGE replacement calls, not physical device/WAL writes or a separate local
ownership claim. One hash submission and one follower-check request are modeled
as batches of Python values; there is no transport or RPC serialization test.
Fresh ownership pages are generated as valid pages; a fresh token is checked
before subsequent commands. Continued commands use a simplified receipt and
direct deterministic SQL, not production Apply or an implemented rejoin path.

The other variants retain useful negative controls: ordinary direct-main writes
fail on several schema/shrink/full-copy cases; `writable_schema=ON` fixes only
some. Aligning the schema cookie before direct writes also passed all 13 cases,
but the attached controller avoids that additional header manipulation and is
the preferred candidate for integration. None of this defines the deferred
cross-file crash/interruption protocol. Nix Node binding integration and local
DB1 A/B/C reconciliation still require tests.

## Recommended next step

Carry the attached-target installation candidate and the typed-group corruption
regressions into the selected Nix Node runtime before integrating production
repair. Preserve ownership validation, free-leaf canonicalization, the agreed
application contract, and the same hash format for scrub and repair. Test local
DB1 reconciliation and the real startup/ownership path next. The full repair
lifecycle and interruption design remain temporarily deferred at the user's
request; these storage results do not settle them.

This evidence establishes specific failures and promising remedies. It does
not prove equality for arbitrary SQL, all resource failures, all SQLite builds,
or every permitted application extension.
