# PLAN3: A transactional, error-correcting SQLite VFS

## 1. Deliverable and scope

Write this design to a new root-level `PLAN3.md`. Preserve `PLAN.md` and
`PLAN2.md` as earlier alternatives. This step changes documentation only.

The first implementation will be a **standalone Rust VFS**, tested independently
before integration into MonsterFt. Initially target Linux and macOS.

The goal is to reduce how often bit flips require MonsterFt's offline repair,
balancing protection against CPU, I/O, storage, and implementation complexity.
MonsterFt's existing detection and agreement mechanisms remain important; this
VFS does not need to correct every possible failure. Concentrate correction on
long-lived contents and use cheaper detection for short-lived contents.

Use:

```text
Outer SQLite: TRUNCATE
  -> bounded VFS buffers, checksums, and error correction
    -> inner SQLite: TRUNCATE + synchronous=FULL
      -> ordinary SQLite OS VFS
```

Support multiple outer connections and processes using SQLite’s ordinary locking
rules. Accept rollback-mode reader/writer blocking. Exclude transactions
requiring a super-journal to commit multiple persistent attached databases
atomically; read-only attachments and ordinary temporary work remain supported.

Do not require `temp_store=MEMORY`. Checksum disk-backed temporary files, without
parity or a durability guarantee. Their backing-store policy is discussed below.

## 2. Storage and protection

### Backing files and records

Use one backing directory per persistent outer database:

```text
<database-path>/
  lock           Ordinary SQLite locking, without application data
  store          Inner SQLite database
  store-journal  Inner SQLite rollback journal
```

The outer main database and its journal are logical files stored together in
`store`. They share each atomic inner flush. Their existence, lengths, contents,
checksums, and any applicable parity are stored as SQL records.

| Contents | Protection | Persistence |
| --- | --- | --- |
| Main database pages | Checksums plus modest error correction | Durable |
| Small persistent metadata and page-one header | Checksums plus error correction | Durable |
| Outer rollback journal contents | Checksums, without parity | Durable, committed atomically with the main database |
| Disposable temporary files and subjournals | Checksums, without parity | Separate storage without durability syncs |
| Inner SQLite structure and active journal | SQLite's existing mechanisms | Accepted remaining exposure |

A damaged journal or temporary record fails verification rather than supplying
incorrect bytes. Such failures may still require recovery or MonsterFt repair.
The outer main journal is temporary in purpose, but remains essential recovery
state until its transaction finishes. Omitting parity does not weaken its
durability or separate its flush from the main database.

Keep outer locking separate from the inner database’s locks. Never replace or
remove the lock anchor while connections remain open.

Use integer-keyed records with independently protected file identity and block
number. Verify those identities against the requested location. Protect logical
lengths and file-existence metadata as well as data.

Use a compact rowid-table layout for protected records:

```sql
CREATE TABLE records (
  id INTEGER PRIMARY KEY,
  record BLOB NOT NULL
) STRICT;
```

Keep the record's identity, contents, parity, and checksum copies together in
that BLOB rather than separate SQL columns. The integer key locates the record;
verify the independently protected identity inside it against the requested
location. SQLite's key and row-lookup structure remain outside our protection.

With the default settings, an ordinary main-page record is **4,240 bytes**:

| Contents | Bytes |
| --- | ---: |
| File identity and block number, two fixed-width 64-bit values | 16 |
| Outer page payload | 4,096 |
| Reed–Solomon parity | 32 |
| Three copies of the data checksum | 48 |
| Three copies of the parity checksum | 48 |
| Total | 4,240 |

The data checksum and Reed–Solomon encoding cover the identity and page payload
together. Use a fixed binary encoding with no additional per-record SQL fields
or serialization headers for these ordinary page records. Persist format and
codec configuration in protected store metadata.

The 4,240-byte size applies to ordinary main pages with the default settings.
Page one's additional header protection stays in its BLOB and makes that record
larger. Metadata and zero-range records have their own smaller payloads; journal
and temporary records omit parity and its checksum copies. Keep these records
compact too, without padding them to the ordinary main-page size.

- Main blocks contain one configured SQLite page.
- Journal and temporary-file blocks contain ordinary byte ranges of the same
  size, identified by logical file and byte-block number. Do not parse
  rollback-journal records. Journal headers, stored page numbers, page images,
  and SQLite's own checksums are all covered as bytes; a journal page image can
  span protection blocks.
- Remove the old WAL-specific 24-byte prefix.
- Truncation removes obsolete records and clears any retained partial block’s
  unused suffix so extension cannot resurrect old bytes.

Represent genuine gaps with **protected zero ranges**, not a stored zero block
for every page. A missing data record below EOF is zero only when a verified
range explicitly covers it; otherwise it is corruption. Protect each range's
file identity and bounds as metadata. Large holes require range records, not
work proportional to the number of absent pages.

Writing into a hole splits or removes its range and installs the data record
atomically with any length change. Truncation clips or removes ranges and data;
merge adjacent zero ranges. Data records and zero ranges must cover the logical
file without gaps or overlaps.

Starting defaults, configurable before creation:

| Setting | Default |
| --- | --- |
| Outer page/block size | 4,096 bytes |
| Inner SQLite page size | 4,096 bytes |
| Parity per main-database page | 32 bytes |
| VFS dirty-buffer budget | 64 MiB per VFS instance |

The buffer budget bounds persistent main/journal dirty overlays and any
additional VFS buffering selected for temporary files. SQLite caches and bounded
operation scratch space are separate. This does not require a separate RAM-first
temporary-file cache. Validate configuration on reopening. Changing page/codec
settings requires rebuilding the store.

### Checksums and correction

Carry forward the agreed rules:

- Canonical 16-byte XXH3-128 checksums for data and, where present, separately
  for parity.
- Three copies of each expected checksum; two agreeing copies establish its
  value.
- Include record identity and meaningful metadata in the protected data.
- Return verified data unchanged even if parity is damaged.
- Accept reconstructed data only when it matches the pre-existing expected
  checksum.
- Never reinterpret unrecoverable contents as zeros, EOF, or an older record.

Use the existing systematic Reed–Solomon parameters: little-endian 16-bit
symbols, `GF(2^16)`, polynomial `0x1100b`, field generator 2, first root 0, root
step 1, and shortened codewords.

Start with 32 parity bytes per ordinary main page, enough to correct eight
unknown damaged 16-bit symbols. Keep the amount configurable; 64 bytes corrects
16 such symbols. Accept that larger damage may require donor repair rather than
paying the previous 1,024-byte default's cost on every page. Keep the same codec
and checksum design, without adding fragments or another correction algorithm.
The independently protected header and small persistent metadata retain their
own protection; the ordinary-page default does not remove it.

Use a Rust wrapper around libfec’s integer API, supporting both unknown errors
and known erasures. Pin the audited source revision and compile only the required
Reed–Solomon implementation. Keep its original notices, LGPL license, source,
and build materials. LGPL library use is compatible with the project’s
`AGPL-3.0-only` license under [GNU’s compatibility guidance](https://www.gnu.org/licenses/gpl-faq.en.html#AllCompatibility).

Page one retains its original header bytes once, within its ordinary payload.
Store additional protection for its first 100 bytes in the same row, using the
previously selected 100 parity bytes. Initial header reads need not validate the
page body. A verified full page may also supply the header if its separate
protection is unusable.

## 3. Batching and SQLite lifecycle

### Buffered operations

Maintain a private dirty overlay shared by an outer writer’s main and journal
handles. Reads and size queries observe its pending changes. Other connections
read committed backing records under outer SQLite’s locking rules.

Full replacements do not read old payloads. Partial replacements first verify
the bytes being preserved, repairing them where correction is available.

Calculate and retain expected checksums when accepting buffered contents. Verify
them before subsequent reads, partial modifications, or persistence. For
records with error correction, generate parity from verified contents during
flushing. Do not calculate a fresh checksum at flush time that silently accepts
intervening buffer corruption. Journal contents need no parity encoding.

Flush related changes in **one short inner transaction** when:

- Buffer pressure requires space.
- The main journal is truncated to zero.
- The main journal is deleted during initialization or recovery.
- `COMMIT_PHASETWO` arrives with changes still pending.

A flush includes the corresponding main/journal records and logical metadata,
regardless of their different protection levels. Never flush the persistent
main journal independently through a disposable temporary store.
Release dirty buffers only after inner commit succeeds. Large writes must be
processed without exceeding the buffer budget apart from bounded scratch space.

An ordinary transaction fitting the buffer should normally need one inner
commit. Larger transactions may require several pressure flushes. Late main-file
shrinkage may require another flush at `COMMIT_PHASETWO`.
If the journal is finalized before any pressure flush, its discarded buffered
contents need not be written to backing storage.

### Durability and recovery

This is a SQLite-aware batching VFS. Intermediate outer main/journal sync
requests do not each force a separate inner commit. Preserve the recovery
invariant:

- Intermediate durable batches retain the journal information required to undo
  persisted unfinished database changes.
- Finalization persists the completed database together with journal
  invalidation.
- Finalization must not report success until its inner transaction is durable.

Rollback can also finalize the journal. Flush the resulting restored state; do
not interpret journal finalization as proof that the application committed.

Support journal deletion during startup recovery: SQLite may recover a hot
journal before the requested TRUNCATE mode takes effect. `COMMIT_PHASETWO`
handles remaining changes before normal unlock; an empty buffer adds no
transaction or sync. [SQLite transaction finalization](https://github.com/sqlite/sqlite/blob/version-3.54.0/src/pager.c#L2040)

On failed persistence, report the error and stop using the affected write
context. Do not automatically flush failed or unfinished buffers during close.
Preserve committed backing state and any durable outer journal for reopening.

The inner rollback journal holds old inner pages for undo; it is not a staging
file containing all newly written outer records. After a pressure flush commits,
the outer journal can remain necessary in `store` while the inner journal is
empty. An interrupted inner flush still depends on the unprotected inner journal
for recovery. These are separate exposure windows; the accepted inner limitation
does not make protection of outer records pointless.

### Modes and temporary files

Configure and verify TRUNCATE/FULL for persistent outer and inner connections.
Reject explicit unsupported persistent journal modes and super-journal commits.
Allow ordinary startup recovery callbacks.

Temporary databases and subjournals retain SQLite’s normal temporary-file
behavior, including their different journal settings. Persistent main-database
TRUNCATE does not force all SQLite temporary work into that mode. Treat these
as private disposable byte files: support ordinary reads, writes, truncation,
and deletion without a separate transaction-finalization state machine. Only
the persistent main/journal pair needs the batching rules above.

Checksum temporary contents without parity. Keep their backing storage separate
from the durable store, avoid durability syncs, and discard their contents when
finished rather than flushing them for recovery. They need no recovery guarantee
after process termination. Preserve bounded memory use.

Start with a separate disposable inner SQLite database using `synchronous=OFF`.
Send temporary-file writes directly to that store, without additional VFS memory
buffering, a small-file cache, or a lazy spill policy. SQLite retains its normal
page cache. Removing fsync and parity work should help, but SQL execution,
B-tree updates, journaling, and filesystem writes still have costs; equivalence
to ordinary filesystem writes has not been established.
[SQLite synchronous settings](https://www.sqlite.org/pragma.html#pragma_synchronous)

Memory buffering remains an optional later optimization. The VFS's existing
persistent-file buffering code may make it inexpensive to add for temporary
files too. Measure large sorts and savepoint-heavy workloads before deciding
whether that additional buffering is worthwhile.

Disable unchecked mapped reads so verification cannot be bypassed. Preserve
exact short-read, EOF, truncation, and filesystem-error behavior.

## 4. Concurrency, repair, and limitations

Finish inner reads before returning from VFS reads. Never hold an inner write
transaction while acquiring or upgrading an outer database lock. Do not retain
clean-record caches across connections without invalidation.

Returning corrected bytes remains a read operation. Persist queued repairs at
the next ordinary writable flush, following these rules:

1. Queue record identities, not corrected bytes saved from an earlier read.
2. Reread and verify the current record inside the inner write transaction.
   Reconstruct against its current expected checksum; skip it if already healthy.
3. Normal buffered changes take precedence. Never overwrite a newer replacement
   or undo truncation, deletion, or replacement with a hole to satisfy an old
   repair request.

Read-only connections do not persist repairs; no background repair thread is
required initially.

Record these limitations explicitly:

- TRUNCATE loses WAL’s reader/writer concurrency and performs more synchronization
  work. Batching reduces the number of inner transactions, not each transaction’s
  own sync requirements.
- Inner SQLite’s **active journal and structural bytes** remain unprotected,
  including B-tree structure, row lookup, and overflow links. Damage may prevent
  retrieval before correction can run; complete detection of such damage is not
  promised.
- Parity, checksum copies, inner page allocation, and two journaling layers add
  storage and write overhead. The 4 KiB inner-page default accepts overflow
  pointers as part of the unprotected structure. Avoiding those pointers does
  not justify nearly doubling storage in the measured simple layout below.
- Journal and temporary contents have detection without correction. Their
  shorter lifetime motivates this tradeoff, but damage can still interrupt
  operations or recovery and require offline repair.
- Checksums establish integrity, not freshness. A valid older backing-store
  image is not detected merely by checking its records.
- Correction is bounded. Data damaged before its expected checksum is
  established, or beyond available redundancy, is outside the repair guarantee.

Keep each backing directory together for offline copying and repair. Persist
newly created directories/files as required before acknowledging durable
initialization. The inner SQLite always uses an explicitly selected ordinary
VFS, avoiding recursion.

### Measurements informing the defaults

An isolated native libfec encoder benchmark on the development machine, with
4 KiB inputs, reused codec/buffers, and no I/O, SQL, or checksum work, measured:

| Parity bytes | Unknown damaged 16-bit symbols correctable | Encoding throughput |
| --- | ---: | ---: |
| 32 | 8 | About 88 MiB/s |
| 64 | 16 | About 54 MiB/s |
| 1,024 | 256 | About 3.4 MiB/s |

These are encoder-only measurements, not completed-VFS throughput. They support
the modest, configurable parity default and avoiding parity work for temporary
contents.

A SQLite layout probe used 20,000 rows containing an `INTEGER PRIMARY KEY` and
a 4,096-byte BLOB. With 4 KiB inner pages, BLOB bytes were about 88.9% of the
file. That particular overflow-page layout had 481 unused bytes, not a universal
434-byte padding area. [SQLite file format](https://www.sqlite.org/fileformat2.html#b_tree_pages)

Physical storage averaged about 4,610 bytes per row with 4 KiB pages, versus
8,202 with 8 KiB pages. A follow-up test with 100,000 rows confirmed that increasing
the BLOB from 4,096 to 4,240 bytes did not increase page count in this layout.

The follow-up used SQLite 3.51.0, 4 KiB pages, and no auto-vacuum. With one million
compact 4,240-byte BLOB records, **99.542%** of the committed inner database was
BLOB contents or genuinely unused bytes: 91.988% BLOBs and 7.554% unused space.
This count excludes freeblock headers and other live SQLite bookkeeping from
unused space. With separate SQL columns, the corresponding result was 99.292%
for 100,000 rows. Compact records after deletion, refill, and updates measured
99.535% for 100,000 rows. These results support the compact layout selected above.

Fault tests of unused overflow tails, unallocated gaps, and freeblock bodies
passed 392 mutation cases, including subsequent writes and integrity checks.
All 64 control mutations in freeblock headers were detected by integrity checks.
SQLite's reported unused space includes those headers, so it must be adjusted
before making this calculation. [SQLite DBSTAT](https://www.sqlite.org/dbstat.html)

These are storage-layout measurements, not a demonstrated VFS recovery rate.
They exclude the active inner journal and do not establish correction of inner
SQLite structure. Key sizes, other record types, and workload affect the result;
repeat the measurements and fault tests against the implemented store and codec.

## 5. Implementation stages and validation

Implement and validate in this order:

1. **Codec:** independent reference vectors; data/parity damage; checksum-copy
   disagreement; decoder miscorrection rejection; header-only recovery.
2. **Protected record store:** identity and length validation, missing records,
   full replacement, partial writes, protected zero ranges, huge sparse gaps,
   truncation, and extension.
3. **VFS and batching:** reads through overlays, memory limits, grouped pressure
   flushes, journal finalization, late shrink, temporary files, and mode
   restrictions.
4. **Recovery and concurrency:** crash injection, multiple connections/processes,
   locking, read-only access, and queued repair.
5. **SQLite integration tests and performance measurements:** public SQLite tests
   within the supported configuration, targeted fault tests, sync counts,
   latency, and memory use.

Crash tests must cover before/during/after pressure flushes and finalization,
inner commit failures, outer rollback, savepoints, startup journal deletion,
and shrinking databases. Every acknowledged outer commit must survive;
unfinished work must remain recoverable.

Exercise disk temporary files, sorting, large statements, savepoint-heavy work,
VACUUM, multiple processes, and rejected super-journal transactions. Measure
temporary-file performance separately, including the `synchronous=OFF`
store. Verify journal and temporary checksum failures return errors, and main
page corruption produces corrected bytes or an explicit error within the stated
protection boundary. Confirm that reduced journal protection never separates its
durability from the main database.

Use sqlitesuperfs as the concrete batching precedent and retain its reported
public SQLite test-suite success. Validate this implementation independently,
especially its different backing engine and error-correction layer.
