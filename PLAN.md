# PLAN6: A directly mapped, error-correcting SQLite VFS

## 1. Direction and scope

Build a standalone Rust VFS, initially for Linux and macOS, that adds checksums
and error correction to ordinary files. Use outer SQLite WAL mode with
`synchronous=FULL`. Preserve PLAN.md through PLAN5.md as earlier alternatives.
This is a design and prototype direction, not a claim that recovery is proven.

```text
Outer SQLite: WAL + synchronous=FULL
  -> direct logical-block / physical-slot mapping
     checksums and error correction
  -> ordinary filesystem I/O
```

SQLite already owns transactions, WAL recovery, checkpoints, snapshots, and
locking. Use those mechanisms before adding another protocol. This design has
no inner SQLite database, staging database, internal WAL, or VFS COMMIT records.
It does not need to recognize SQL COMMIT or commit main-file and WAL changes
atomically together. Honor SQLite's write order and synchronization requests.

Minimize additional synchronization and implementation complexity. Successful
writes are visible through the filesystem before `xSync`; the OS may buffer them.
Support multiple connections, threads, and processes through ordinary SQLite
locking and shared memory. Initially require read/write main-database and WAL
opens and writable shared memory; reject their read-only fallbacks, immutable
access, and no-lock configurations. Scope these checks to those files, allowing
SQLite's read-only inspection of the initialization rollback journal. Read
transactions and long-held snapshots on read/write connections remain supported.
Do not require `temp_store=MEMORY`.

Assume the supported OS/filesystem honors its documented successful I/O and
full-synchronization calls. Our responsibility is to preserve those contracts
through the transformed layout. A successful `xSync` establishes the durability
barrier for the required preceding contents and file-size changes. Fault tests
exercise partial writes, failed calls, and permitted persistence ordering;
they are not a separate qualification of individual devices/controllers.
An unacknowledged commit may recover as either the old or the new transaction,
depending on what persisted; failure does not always imply rollback.

## 2. Files and arithmetic mapping

Keep the usual main database, `-wal`, and rebuildable `-shm` file relationships.
Each protected logical file has one ordinary backing file containing:

```text
fixed format/configuration prefix
protected slot 0
protected slot 1
...
final slot, which may contain a partial payload
```

Use these settings for persistent main-database and WAL files:

| Setting | Default | Allowed values |
| --- | --- | --- |
| Logical block size `B` | 4,096 bytes | Powers of two from 512 through 65,536; main-database page size must equal `B`. |
| Parity bytes `R` | 32 | Even values from 32 through 1,024. |
| Physical alignment `Q` | 4,096 bytes | Powers of two from 512 through 4,096, subject to the filesystem requirements below. |
| File-prefix size `H` | 4,096 bytes | Fixed for this format version. |

Each ordinary protected slot contains 32 bytes of metadata. Entirely zero slots
use the separate zero-block representation described below.

| Field | Bytes | Meaning |
| --- | ---: | --- |
| File identity | 16 | Must match the file prefix. |
| Logical block offset | 8 | Must equal the offset derived from the slot number. |
| Initialized payload length | 4 | Length of the initialized prefix, from zero through `B`. |
| Flags/reserved | 4 | Zero for this format version. |

Use fixed-width little-endian integers. Store metadata, three data-checksum
copies, parity, three parity-checksum copies, alignment padding, then payload.
The two groups of checksum copies occupy 96 bytes in total. Derive the physical
slot size `A` and its prefix size `P`:

```text
A                   = roundUp(B + 32 + R + 96, Q)
P                   = A - B
block number        = floor(logical offset / B)
offset within block = logical offset % B
physical slot start = H + block number * A
```

The defaults produce an 8 KiB physical slot for a 4 KiB payload. Accept this
initial space and I/O cost to keep slots physically aligned. Recognizing zero
blocks does not depend on allocation boundaries or hole reporting. WAL protection
groups are aligned logical byte ranges of `B` bytes, rather than one group per
24-byte frame header plus page. A WAL frame can cross groups; this layout
deliberately matches SQLite's sector boundaries.

Require the supported storage's write-damage units to fit within aligned
`Q`-byte regions. Both `H` and `A` are multiples of `Q`. Hole recognition adds no
allocation-unit requirement. Reject unsupported storage/configuration
combinations: choosing a number does not create a filesystem guarantee.
Logical `B`, physical `A`, and physical alignment are different quantities.
Do not advertise atomic writes or powersafe
overwrite merely because an underlying VFS advertises them.

The file prefix contains a fixed-format record: 128 bytes of immutable magic,
version, file kind, identity, settings, and reserved fields; 32 parity bytes;
and 96 checksum-copy bytes. The remaining prefix bytes are padding. Its codec
and layout are fixed independently of configurable slot settings, allowing the
header to be verified/repaired before those settings are used. This is our file
header, not special handling of SQLite page one.

Validate settings on reopening; changing them requires rebuilding the file.
Do not make a repeatedly overwritten global length field the sole authority
for accessing every previously committed block.

Check all offset/length arithmetic before I/O. For signed 64-bit offsets, use
the conservative limit below, further restricted by the OS/filesystem:

```text
maxBlocks        = floor((INT64_MAX - H) / A)
maxLogicalLength = maxBlocks * B
```

The largest ordinary codeword in the allowed range has
`(B + 32 + R) / 2 = 33,296` symbols, below the field's 65,535-symbol limit.
An arithmetic probe checked all 15,904 setting combinations, including alignment,
prefix capacity, EOF round trips, and maximum offsets. This checks the format
math, not the filesystem's crash or concurrency guarantees.

### Exact length and holes

Prefer deriving logical EOF from native physical EOF. Put the slot's protection
and alignment space before its payload. Let that prefix occupy `P = A - B`
bytes. For logical length `L = q * B + r`, where `0 <= r < B`:

```text
physical length = H + q * A + (r > 0 ? P + r : 0)
```

The final partial payload is not physically padded to `B` bytes. The protected
initialized-length field describes the payload prefix, not total file length;
the remainder of the logical block is zero. Checksums and error correction cover
the metadata and full zero-expanded payload. Length queries reverse the physical
EOF mapping; protection/padding is never exposed as logical data. Incomplete
final prefixes and malformed physical lengths require explicit recovery/error
handling.

A write of 20 bytes at offset 100 within an untouched block initializes 120
bytes: 100 zeros followed by the supplied contents. A distant later append can
leave this partial block unchanged as an interior block; its protected length
still declares the zero suffix.

Define an entirely zero physical slot as a logical zero block, regardless of
whether the filesystem allocated it. Inspect the whole slot, including metadata,
checksums, parity, padding, and payload; a zero header alone is insufficient.
For a valid final partial slot, inspect every byte present within the established
physical EOF and treat only its absent payload suffix as canonical zeros.
Finish short reads or report failure; never turn an I/O error or unexpected EOF
into zeros and then accept those invented bytes as a zero slot. The file-format
prefix still requires normal verification and has no all-zero exception.

All other slots undergo normal verification and repair. Partial writes into a
zero slot start from a zero payload and produce an ordinary protected record.
Require efficient sparse allocation and zero-filled gaps, but no `SEEK_HOLE`,
`SEEK_DATA`, or extent queries for correctness. Do not write a protected record
for every absent block or add a presence bitmap/range index. Dense copies remain
valid, although they consume more space. Extension after truncation must never
resurrect discarded bytes.

Accept two limitations of this representation. Complete erasure of a populated
slot into zeros cannot be distinguished from a legitimate zero block. A bit flip
in a materialized zero slot makes it invalid, but the bare zero representation
has no stored checksum/parity with which to repair it automatically. Large sparse
gaps are not expected to dominate normal MonsterFt database/WAL workloads; still
support them efficiently for general SQLite behavior and tests.

The filesystem remains responsible for its own allocation, inode, namespace,
and file-length integrity. The protection layer does not promise to repair
damage that prevents the filesystem from locating the backing contents.

### Sparse-file portability

Linux and macOS allow conservative hole reporting that treats holes as data.
The selected zero-slot representation does not depend on those reports or on
untouched slots remaining physically unallocated.

A local APFS probe wrote 8 KiB at offsets 0, 16 KiB, and 48 KiB. Before `fsync`,
24 KiB was allocated and the gaps were reported as holes. After `fsync`, all
56 KiB was allocated and reported as data, although the untouched gaps still
read as zeros. Larger tested alignments did not resolve this behavior. This is
harmless under the zero-slot rule. APFS, XFS, and ext4 remain intended targets;
validate our locking, damage-boundary mapping, and durability implementation
against their documented contracts. Keep the selected alignment for now;
portable hole handling does not by itself justify packing physical slots more
tightly.

## 3. Protection and ordinary I/O

Carry forward the agreed codec direction:

- Canonical 16-byte XXH3-128 checksums for data and separately for parity.
- Three copies of each expected checksum; require two agreeing copies.
- Include file/block identity and meaningful record metadata in protection.
- Return verified data unchanged when only parity is damaged.
- Accept corrected bytes only if they match the pre-existing expected checksum.
- Start with 32 parity bytes per ordinary block, configurable before creation.

Use systematic shortened Reed-Solomon coding with little-endian 16-bit symbols,
`GF(2^16)`, polynomial `0x1100b`, field generator 2, first root 0, and root step 1.
Retain the libfec integer-API direction, pinned source, and required license and
build materials. Thirty-two parity bytes correct eight unknown damaged symbols.
There is no recursive checksum-of-checksum scheme.

Complete replacements calculate protection directly from incoming bytes.
Partial replacements first verify or repair the preserved contents, or recognize
a zero slot, then merge the update and calculate protection for the resulting
block. Never give damaged preserved bytes a fresh valid checksum. Use bounded
per-operation scratch space.

Prefer one buffered physical read of a complete protected slot and one buffered
write of its replacement. Complete necessary short-I/O handling before reporting
success. Disable unchecked mapped reads and translate size/offset-related file
controls so they cannot bypass the layout.

`xSync` must make all preceding required contents and protection durable before
succeeding. Forward the appropriate native synchronization, including required
file/directory lifecycle barriers. Do not suppress outer syncs or add a sync
for every block write. Multi-block writes need not become internally atomic.

### Page one

Treat page one exactly like an ordinary protected page. Add no independent
100-byte header checksum, parity, alternate copy, or verification bypass.
A short initial header read verifies/repairs its containing block normally.
If that block is unrecoverable, opening fails. Accept this limitation even when
SQLite might otherwise have recovered page one from its WAL. Revisit only if
later experience justifies special handling.

### Concurrent reads

`xSectorSize` describes crash damage, not runtime synchronization. A reader and
writer can access different logical bytes that share one protection block.
The read must not combine old payload with newly written protection.

Prefer a single buffered `pread` for the slot's payload and protection together,
and a single buffered `pwrite` for its assembled replacement, at the calculated
physical offset. Do not split payload and protection into separate calls. These
calls avoid a shared seek position, but their names alone do not guarantee a
coherent whole-slot read, crash atomicity, or an atomic read-modify-write pair.

Use one short advisory byte-range guard per backing file on all supported
filesystems. XFS has corresponding buffered read/write I/O locks; ext4 does not
provide the equivalent whole-call exclusion, and no sufficient public APFS
guarantee was established. Single-call I/O also cannot protect a retry sequence.
Our explicit guard removes the reason to prefer XFS for this behavior; XFS,
ext4, and APFS remain intended targets without a filesystem preference.

Reserve byte `H - 1` in the existing prefix as the guard's lock location; locking
does not change its stored contents. Use open-file-description byte-range locks
(`F_OFD_SETLK`/`F_OFD_SETLKW`): shared for reads and size queries, exclusive for
the complete read-modify-write or truncate operation, including short-I/O
handling. This range is separate from SQLite's main-file lock bytes. Add no
lockfile, persistent lock metadata, or extra synchronization-to-disk calls.

The VFS supplies a process-local mutex for threads sharing the guard descriptor;
OFD locks on that same descriptor do not exclude one another. When wrapping a
native VFS, keep the guard descriptor shared by this VFS's local handles for
the inode, and close it only after their last delegated SQLite close. Closing
an extra descriptor earlier could release SQLite's process-owned locks.

Acquire the mutex and I/O guard only for the physical operation and release both
before returning. Never acquire SQLite protocol locks or call back into SQLite
while holding this guard. No guard is held across a SQL transaction or reader
snapshot. SQLite's locks still govern logical transactions and writer ownership.
Do not bypass verification or the guard with mapped or direct I/O. Tests should
confirm this implementation, including a reader of a duplicate WAL frame while
the next append updates their shared block, rather than depend on undocumented
filesystem coherence.

Returning corrected bytes is a read operation. Do not persist repairs inside
`xRead`. Ordinary replacements rewrite protection; any later repair-writeback
feature must reread current contents and coordinate with writers. No background
repair worker is required for the initial prototype.

## 4. Let SQLite protect the write boundary

Report `xSectorSize = B`, a supported power of two, and disable powersafe
overwrite for the transformed files. Report other capabilities truthfully.
With outer FULL synchronization, SQLite pads WAL commits using duplicate frames
and synchronizes through a logical sector boundary before writing the remaining
duplicate-frame suffix.

A SQLite 3.54.0 trace with `B = 4096` and powersafe overwrite disabled showed:

```text
WAL header:       [0, 32), then header sync
commit frame:     [32, 4152)
duplicate prefix: [4152, 8192), then sync
duplicate suffix: [8192, 8272)
next commit:      starts at 8272
```

Later appends can rewrite the block beginning at 8192 without rewriting the
earlier complete durable commit. This validates SQLite's padding behavior, not
the correctness of our transformed VFS. Keep physical slot damage within the
logical boundary we report, and test crashes around the post-sync suffix.

## 5. WAL recovery: repair first, then retain the prefix

During recovery, verify and repair WAL blocks in logical order. At the first
unrecoverable block, retain the preceding verified prefix and discard that block
and everything after it. Do not scan ahead for later intact frames or commit
evidence to decide whether the suffix may be discarded. SQLite selects the last
valid commit in the retained bytes.

This is the selected availability policy, not proof that the discarded suffix
was an unfinished append. Damage in the middle of a WAL can cause later intact,
previously acknowledged transactions to be discarded too. Accept this broader
loss in exchange for simpler recovery that does not reject the suffix merely
because later transaction material survived. Checksum validity does not prove
durability or freshness.

SQLite still interprets WAL frame salts, checksums, and commit markers, including
stale frames left by WAL reuse. Do not build a separate tail classifier. The
file format and WAL header must first be established; if they cannot be verified
or repaired, fail rather than assume an empty WAL.

### Prefer a block-aligned cutoff over fabricated bytes

If the first unrecoverable block is `K`, discard the suffix at logical
offset `K * B`, corresponding to physical offset `H + K * A`. Retained blocks
must verify or be recognized zero blocks. SQLite's normal recovery ignores any
incomplete final frame and selects the preceding valid commit. Tail removal
itself does not require a partial-block protection rewrite.

Establish and apply the cutoff before reporting WAL size to SQLite's recovery
scan. Truncating only after that scan has captured the old larger length can
cause a short-read error instead of successful recovery.

### Recovery entry and exclusion

Authorize destructive cutoff at these two VFS entry points. Associate a WAL
handle with its main-database wrapper through
`sqlite3_database_file_object(zWalName)` during WAL opening.

For normal shared-memory recovery:

1. Intercept an exclusive `xShmLock` acquisition whose range includes
   `WAL_RECOVER_LOCK` (index 2), and acquire the delegated lock first.
2. Scan the WAL before returning success. SQLite's WAL-size query follows this
   acquisition; its write/checkpoint exclusion is already in place.
3. If cutoff is needed, attempt a nonblocking main-file EXCLUSIVE upgrade.
   Other WAL users hold SHARED locks there. If blocked, restore the prior lock
   level, including any PENDING lock left by the attempt, and return `BUSY`
   without changing the WAL.
4. With exclusion established, apply and fully synchronize the cutoff. Keep the
   temporary main EXCLUSIVE lock until SQLite releases its recovery-lock range,
   after rebuilding the index, then restore the previous main lock level.
5. If preparation fails before returning acquisition success, undo the newly
   acquired SHM range and any temporary main-lock upgrade ourselves. SQLite
   will not release a lock acquisition that was reported as failed.

For a newly opened WAL whose main handle already holds EXCLUSIVE:

1. At `xOpen(SQLITE_OPEN_WAL)`, confirm the associated main wrapper's actual
   delegated EXCLUSIVE lock. Exclusive-mode startup acquires it before WAL open.
2. Open the backing WAL read/write, rejecting read-only fallback. Validate its
   format and scan existing WAL contents before returning the handle. A new
   logically empty WAL has no old SQLite WAL header to validate.
3. Apply and fully synchronize any cutoff while that main lock remains held.
4. Return the handle; SQLite builds its private WAL index from the resulting
   bytes. On failure, clean up the new WAL resources and return the error.
   Do not release or downgrade the main lock: SQLite owns it in this path.

The source audit found one call to `walIndexRecover`, inside `walIndexReadHdr`.
That reader is used by transactions and checkpoints. These are two authorized
cutoff entry points, not a claim that SQLite only performs recovery at startup.
Later locking-mode transitions and last-close checkpointing can suppress
`xShmLock` callbacks in exclusive mode. Those paths keep strict ordinary reads:
if damaged storage needs cutoff, return an error and require closing/reopening
through the entry points above. Do not infer recovery from every WAL `xFileSize`
call or every main-file EXCLUSIVE lock.

Reject read-only/unreliable SHM fallback as well as read-only main/WAL opens;
otherwise SQLite can use another private recovery path. `PERSIST_WAL` changes
close-time retention, not these entry rules. Check this callback sequence when
changing supported SQLite versions. SQLite rebuilds its own index; do not edit
or delete SHM to perform the cutoff. Never wait for main-file exclusion while
holding recovery locks that another SHARED owner may itself be waiting for.

Keep exclusion until all changed backing files have been fully synchronized.
For a cutoff of an existing WAL, truncate and fully sync that WAL; sync any other
file actually modified. Do not sync an unchanged main database or the rebuildable
`-shm` file. Creation, deletion, or rename may also require synchronizing the
containing directory. Use the platform's native/full-sync semantics. Failure to
persist the changes is recovery failure, not successful cleanup.

Do not simply hide a corrupt suffix from one reader while leaving it to poison
future partial writes. Make the accepted cutoff consistent across owners and
future appends. Test interruption of tail removal and its durability ordering.

An alternative recovery-only signal is a deliberately invalid frame, such as
one with page number zero, returned successfully so SQLite stops its scan.
Arbitrary garbage is not a guaranteed checksum failure. Prefer the block-aligned
cutoff because it also removes the corrupt storage from subsequent write paths.

Normal live WAL reads must return verified/repaired contents, a recognized zero
block, or an error. SQLite does not repeat its recovery frame-checksum validation
on every indexed page read. Never apply a global unchecked-byte fallback. Genuine operating-system
I/O failures remain errors, as does unrecoverable main-database damage; the
prefix-discard policy applies to WAL integrity failures during recovery only.

## 6. Truncation, initialization, and temporary files

Removing whole slots needs only physical truncation. For a partial ordinary
protected slot, hold the exclusive I/O guard and:

1. Verify or repair its retained contents.
2. Set initialized length to the smaller of its old initialized length and the
   new partial payload length. Calculate protection over the retained prefix
   and canonical zero suffix.
3. Rewrite through the slot's old physically present payload extent, zeroing
   the discarded suffix without extending old EOF. Do not leave stale physical
   bytes hidden behind the new length field; the decoder must not need an
   unverified length to choose which payload bytes to validate.
4. Truncate to the mapped new EOF. Report success only after both operations
   succeed, then release the guard.

An entirely zero slot needs no protection replacement. Successful native
truncation discards removed bytes; extension supplies zeros. A failed operation
may have changed the file. Report the error and disable further ordinary work
through the affected handle; do not assume that SQLite will stop immediately.

SQLite's actual operations limit the affected area:

- Main-file checkpoint truncations are page-aligned; with `B = page size`, they
  remove whole slots.
- WAL TRUNCATE checkpoints truncate to zero.
- WAL size-limit trimming can be nonaligned, but retains all current frames,
  including padding, and affects only a slot at or after the FULL-sync boundary.

For the selected FULL-sync, page-size=`B`, and disabled powersafe-overwrite
configuration, SQLite's source gives:

```text
E = end of original commit frame
S = ceil(E / B) * B               // FULL-sync boundary
D >= S                           // end after any duplicate-frame padding
T = max(journal_size_limit, D)    // truncate target
floor(T / B) * B >= S             // start of affected partial slot
```

Thus the earlier complete durable commit is outside that slot. This is a
source-backed deduction; tests confirm the callback implementation and damage
boundary mapping. The expected interrupted states are:

| Persisted state | Expected result |
| --- | --- |
| Old record and old EOF | Unchanged valid representation. |
| New record and old EOF | Valid rewritten slot with a zero suffix; old file length remains. |
| New record and new EOF | Completed trim. |
| Old/torn record and new EOF | Repair the final slot if possible; otherwise WAL recovery discards it, beyond the earlier durable prefix. |

Call order alone does not impose persistence order before synchronization.
These rules allow either order without adding an intermediate fsync.

Test failures between truncation and protection replacement, including continued
live access: SQLite's WAL size-limit helper can ignore a truncate error. The VFS
must not let an unsuccessful update become unchecked successful reads afterward.

For the first truncation tests:

- Trace logical/physical truncate offsets, protection writes, and sync boundaries
  for shrinking main databases, zero-length WAL resets, and reused WALs trimmed
  by `journal_size_limit`. Vary limits around block boundaries.
- For partial WAL trimming, confirm the derived boundary relationship against
  actual callbacks, leaving the earlier complete durable commit intact.
- Inject interruption before, between, and during the truncate/protection
  operations, and before/after the next sync. Test ordinary I/O errors with the
  process still alive as well as crash images with partially persisted writes.
- Check exact EOF and retained bytes after successful truncation, then extend
  again to ensure removed bytes cannot reappear. After interrupted trimming,
  test recovery followed by new appends, not just successful opening.

Process termination alone does not simulate power loss because dirty OS-cache
contents can survive it. Include controlled torn-write/reordering images. Keep
these tests separate from later bit rot invoking the accepted prefix-loss policy:
an ordinary interrupted trim must not destroy an earlier acknowledged commit.

### Malformed physical EOF

Let `F` be physical file length. Reject `F < H` as an incomplete file-format
prefix. Otherwise validate that prefix, then calculate:

```text
q = floor((F - H) / A)
t = (F - H) % A
```

- `t = 0`: whole-slot EOF, with logical length `q * B`.
- `P < t < A`: partial payload of `t - P` bytes; verify/repair the slot normally.
- `0 < t <= P`: incomplete final slot prefix, without a complete logical payload.

During authorized WAL recovery, an incomplete final slot can be discarded at
`H + q * A` once an earlier usable SQLite WAL header is established. An invalid
payload-bearing slot follows the same repair-then-cutoff policy. Main-database
malformed storage and unrecoverable file headers remain errors. Do not infer
EOF from a failed native read or fill an unexpected short read with invented
zeros. This retains the possibility of rejecting an interrupted main-file write
rather than promising every crash image will reopen.

### Initialization rollback journal

Allow SQLite's ordinary fresh-database transition into WAL. Route its
`SQLITE_OPEN_MAIN_JOURNAL` file through the underlying ordinary VFS as unchanged
filesystem bytes, without our prefix, checksums, or parity. This is a third
handling case alongside protected main/WAL files and checksum-only temporary
contents. The main database remains protected during initialization.

The initialization journal is short-lived but must survive interrupted setup
until SQLite finishes recovery. Preserve native reads, writes, syncs, and
deletion; do not treat it as disposable or delete it merely on close. Select
this handling using SQLite's file-type flags, without a separate initialization
state machine. Verify WAL/FULL before application use.

Allow `SQLITE_OPEN_READONLY` for this journal: `hasHotJournal()` can inspect its
first byte before deciding whether recovery is needed. Native passthrough also
lets SQLite inspect an incomplete journal without our whole-block verification
preventing that decision. Read-only main/WAL access remains unsupported; no
blanket rejection of read-only file opens is needed.

### Temporary files

Use checksum-only content slots for `SQLITE_OPEN_TEMP_DB`,
`SQLITE_OPEN_TEMP_JOURNAL`, `SQLITE_OPEN_TRANSIENT_DB`, and
`SQLITE_OPEN_SUBJOURNAL`. Preserve SQLite's private ownership and delete-on-close
behavior. Different temporary journal settings require only the usual file
operations, with no separate transaction or recovery protocol.

Reuse the fixed file prefix unchanged, including its small amount of parity;
its file kind identifies the temporary format. Keep the same `B`, `H`, and `Q`,
32-byte slot metadata, and three 16-byte copies of the data checksum. The
checksum covers metadata and the full zero-expanded payload. Omit content-slot
parity and parity-checksum copies; store metadata, data-checksum copies,
alignment padding, then payload:

```text
Atemp = roundUp(B + 32 + 48, Q)
Ptemp = Atemp - B
```

Reuse the offset, EOF, initialized-length, zero-slot, partial-write, and
truncation rules with `Atemp` and `Ptemp`. This includes sparse gaps and their
accepted zero-slot limitations. At the defaults, slots still occupy 8 KiB for
4 KiB of payload; omitting parity saves computation rather than default slot
space. Full replacements need no old-content read; partial replacements must
verify preserved contents. Failed verification returns an error without a
repair or prefix-discard fallback.

Write directly through the filesystem with bounded operation scratch space.
Honor requested syncs but add none per write. Introduce no inner SQLite or
memory-only temporary-storage requirement. These files need no recovery
guarantee after their owner exits.

## 7. Prototype and validation

Build the smallest direct implementation before choosing it over PLAN3/PLAN5:

1. Codec and mapping: identities, checksum copies, parity, complete replacement,
   partial updates, exact length, sparse gaps, and bounded memory. Include huge
   gaps, allocated zero slots, dense copies, final partial slots, partial writes
   into zero blocks, and truncate/re-extension. Verify that damaged zero slots
   and read failures are not silently converted into logical zeros; separately
   demonstrate the accepted complete-erasure limitation.
2. SQLite sector behavior: actual WAL writes, sync boundaries, duplicate frames,
   reset/reuse, and truthful device flags.
3. Crash recovery: torn writes before/after sync, block-aligned prefix cutoff,
   and interior damage followed by intact commits that are still discarded.
   Include interrupted truncation, unrecoverable headers, and failed page one.
4. Concurrent access: whole-slot reads/writes, size changes, short I/O, multiple
   processes, checkpoints, long outer snapshots, and rejection of unsupported
   read-only/immutable/no-lock access. Cover both cutoff entry points, late
   exclusive-mode failures, lock cleanup, and guard-descriptor lifetime.
5. Integration and cost: SQLite tests within supported settings, temporary work,
   initialization, write amplification, parity CPU, padding, and actual syncs.
   Cover checksum-only temporary slots, sparse/truncate behavior, corruption
   errors, and cleanup. Verify initialization-journal passthrough, its read-only
   probe, and interrupted initialization followed by recovery and WAL startup.

Distinguish expected loss under the accepted WAL-prefix policy from violating
an acknowledged durability barrier during an ordinary later write. Include
recovery followed by further appends; opening successfully alone is insufficient.
Do not promise every interrupted write is recoverable before these tests pass.

Keep the remaining work focused on implementing and checking these contracts:
I/O guards, sparse and exact EOF/truncation behavior, damage-boundary mapping,
and recovery entry-point integration.
No source implementation is authorized by this documentation update.

## References

- [SQLite powersafe overwrite and sector damage boundaries](https://sqlite.org/psow.html)
- [VFS I/O methods and capabilities](https://sqlite.org/c3ref/io_methods.html)
- [SQLite VFS file-type and access flags](https://sqlite.org/c3ref/vfs.html)
- [SQLite temporary files and rollback-journal lifetime](https://sqlite.org/tempfiles.html)
- [SQLite WAL writes, recovery, and truncation](https://github.com/sqlite/sqlite/blob/version-3.54.0/src/wal.c)
- [SQLite pager and exclusive-mode WAL opening](https://github.com/sqlite/sqlite/blob/version-3.54.0/src/pager.c)
- [Main-database handle associated with a WAL filename](https://sqlite.org/c3ref/database_file_object.html)
- [SQLite WAL locks and recovery](https://sqlite.org/walformat.html)
- [Linux hole queries and their limitations](https://man7.org/linux/man-pages/man2/lseek.2.html)
- [Apple hole-query contract](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/lseek.2)
- [Linux extent and cached-data handling for hole queries](https://github.com/torvalds/linux/blob/v6.12/fs/iomap/seek.c)
- [Linux file-lock participation](https://man7.org/linux/man-pages/man2/flock.2.html)
- [Linux open-file-description locks](https://man7.org/linux/man-pages/man2/F_OFD_SETLK.2const.html)
- [Apple open-file-description locks](https://github.com/apple-oss-distributions/xnu/blob/xnu-10002.81.5/bsd/man/man2/fcntl.2)
- [File and directory synchronization](https://man7.org/linux/man-pages/man2/fsync.2.html)
- [Linux truncation and extension](https://man7.org/linux/man-pages/man2/truncate.2.html)
- [Apple truncation and extension](https://github.com/apple-oss-distributions/xnu/blob/main/bsd/man/man2/truncate.2)
- [XFS buffered I/O locking](https://github.com/torvalds/linux/blob/v6.12/fs/xfs/xfs_file.c)
- [ext4 buffered I/O](https://github.com/torvalds/linux/blob/v6.12/fs/ext4/file.c)
- [PLAN3: transactional TRUNCATE alternative](PLAN3.md)
- [PLAN5: shared staging and durable-store alternative](PLAN5.md)
