# TODO: MonsterVfs

MonsterVfs is the SQLite VFS, in its own repository, that adds checksums and
Reed-Solomon parity to the database and WAL. This file lists what MonsterFt must
change to run on it. Nothing here is done yet.

Everything below was checked against MonsterVfs, on macOS with Node 26.13, unless a
line says otherwise. Stock Node cannot load the extension (item 1), so the VFS
behavior was checked from C against the same SQLite.

## Blocking: MonsterFt cannot run on it until these are done

### 1. Get a SQLite the VFS accepts

Node 26.13 bundles the exact SQLite the VFS pins (3.54.0, source ID `be8d059e…`),
but without `SQLITE_ENABLE_DBPAGE_VTAB`, so loading fails:

```
ERR_LOAD_SQLITE_EXTENSION: error during initialization:
monstervfs requires SQLITE_ENABLE_DBPAGE_VTAB
```

- Choose a custom Node build or another binding. Node's `node_shared_sqlite` build
  variable suggests `--shared-sqlite` can link our own SQLite; this is untested.
- Keep `SQLITE_ENABLE_SESSION` and `SQLITE_ENABLE_PREUPDATE_HOOK`, which
  [src/apply.js:144](src/apply.js#L144) needs, plus `SQLITE_THREADSAFE=1`.
- `engines` in [package.json](package.json) says `>=24.15`. The VFS accepts one exact
  SQLite source ID, so that range is too wide.

### 2. Load the extension and select the VFS at every open

Add one helper and route every open through it. Load once per process, before the
first database open:

```js
const boot = new DatabaseSync(':memory:', { allowExtension: true })
boot.enableLoadExtension(true)
boot.loadExtension(libraryPath, 'sqlite3_monstervfs_init')
boot.close()
```

Then select the VFS with a URI. `node:sqlite` accepted `file:` strings and `URL`
objects in testing, but `vfs=` itself is untested from Node. Build it with
`pathToFileURL` so that paths are escaped.

- Open sites: [src/log.js:309](src/log.js#L309),
  [src/storage.js:60](src/storage.js#L60), [src/storage.js:144](src/storage.js#L144),
  [src/certify.js:45](src/certify.js#L45).
- 17 test files and [example3/server.js](example3/server.js) also open databases
  directly.
- Plain SQLite files will not open: they fail with `SQLITE_IOERR_DATA`. There is no
  in-place migration, which matches the policy in [doc/STORAGE.md](doc/STORAGE.md).
- The existing `journal_mode = WAL` and `synchronous = FULL` pragmas stay as they are.
  Leave `page_size` alone: the default of 4096 matches the VFS default `mv_block`.
- A file with no room for content and no usable header counts as an unfinished
  creation, because a crash can leave one. A read/write open silently replaces it. A
  read-only open matches native: an empty file reads as empty, and any other such file
  fails with `SQLITE_NOTADB` (26), which is what the `unreadable` case in
  [test/storage.js:223](test/storage.js#L223) relies on.
- `readOnly: true` needs no change, so `readDatabase`, `checkLocals`, `capture` and
  `adopt` keep working. A read-only connection behaves as on the native VFS and never
  changes the database or its WAL.

### 3. Set a busy timeout

Every connection runs at `timeout: 0`. Add a timeout at each open from item 2.

- **At runtime**, a repair commit briefly holds the WAL write lock. With `timeout: 0`
  a `BEGIN IMMEDIATE` or `log.append` then fails with `SQLITE_BUSY`.
  `appendEntry` at [src/raft.js:261](src/raft.js#L261) has no `try`, so that reaches
  `_fatal`. One repaired page must not stop a Proc.
- **At startup**, several processes read all four databases at once, and the first
  reader after a crash runs recovery, which now verifies every WAL frame. Expect
  `SQLITE_BUSY`, `SQLITE_BUSY_RECOVERY` and `SQLITE_PROTOCOL` here.
- `node:sqlite` blocks the thread while it waits, so a Proc stops handling input for
  that long. Keep the runtime value well under Gate and election timeouts, and use a
  longer one for startup reads.
- Leave `busy_timeout = 0` in [src/certify.js:48](src/certify.js#L48). Certification
  runs with the node stopped and should fail fast if it is not.

## Failures MonsterFt must handle

### 4. Keep storage errors out of application outcomes

**Fix this first.** It does not need the VFS: a native I/O error takes the same path
today.

[src/apply.js:156](src/apply.js#L156) catches anything thrown inside `apply()`. If the
transaction is still open, it rolls the item back, records it as `rejected`, feeds
that into `local_digest`, and commits. An `IOERR_DATA` from one damaged page therefore
becomes a committed "the application rejected this command". The node's state then
differs from its peers, and the outcome it reports is wrong. The digest mismatch should
catch this later, but by then the node needs a full repair for what was a local read
failure.

- After `wrapError`, check `err.sqlCode & 0xff`. Rethrow to the fatal path for storage
  classes: `SQLITE_NOMEM` 7, `SQLITE_IOERR` 10, `SQLITE_CORRUPT` 11, `SQLITE_FULL` 13,
  `SQLITE_CANTOPEN` 14, `SQLITE_PROTOCOL` 15, `SQLITE_NOTADB` 26.
- The existing `!db.isTransaction` check does not cover this, because an I/O error
  does not always end the transaction.
- An application that catches the error and throws its own loses `sqlCode`. Cover this
  in the README guidance already tracked as open in [doc/APPLY.md](doc/APPLY.md).

`BEGIN IMMEDIATE` at [src/apply.js:254](src/apply.js#L254) already keeps `SQLITE_BUSY`
out of `apply()`.

### 5. Handle `MONSTERVFS_CORRUPT_BACKFILLED` (19723)

WAL recovery found a damaged frame that a checkpoint may already have copied into
main. Cutting there could keep half of a transaction, so the VFS refuses and changes
nothing. Retrying can never succeed.

It appears on the first read after a crash, so in `readDatabase`, `log.open` and
`openApplyDatabase`. Today it would become a generic `Err.SQLITE_ERROR` with
`sqlCode` 19723, the startup would fail, and the Supervisor's `_loop` would retry
until it is stopped.

- Add a distinct error code, and stop restarting when it appears.
- Repair follows [REPAIR.md](REPAIR.md), for DB1 and DB2 alike. The VFS knows only its
  own database, so rebuilding one database from another is not its job.

### 6. Handle `SQLITE_IOERR_DATA` (8202)

A slot failed verification and parity could not correct it. Other pages stay readable.
Reading that page fails every time, so a Proc that needs it would die and restart
forever. Route it to repair, as in item 5.

The exception is the scrub in item 8, which should record it and move on to the next
page.

### 7. Know that a WAL cutoff can remove committed rows

If a damaged frame is above the checkpoint watermark, recovery truncates the WAL at
that frame. That removes every later transaction, including acknowledged ones, and
reads then succeed with no error. This is the chosen availability policy.

A read-only connection cannot cut. It stops reading at the damage and leaves the file
alone, and the next writer's recovery makes the cut. `readDatabase` is usually the
first reader after a crash, so expect `wal_cutoffs` from it before any cut has happened.

- **DB1:** one regressed copy loses the `capture()` majority and is replaced by
  `adopt()`. Two copies losing the same suffix would be believed.
- **DB2:** `reconcileReceipt` rejects a regressed receipt. Confirm that this ends in
  repair, and not in a restart loop.
- `stateDigest` covers only election, metadata and the log head. Because a cut removes
  a suffix, the log head does catch it.

Node cannot read `sqlite3_log`, so read these through item 9. They are per process and
reset on restart, which is accepted: MonsterFt notices a regressed node without them.

## Should add

### 8. Scrub cold pages

The VFS only checks pages that get read, so MonsterFt must read the rest. Any read
that reaches a damaged page queues its repair.

```sql
SELECT data FROM sqlite_dbpage WHERE schema = 'main' AND pgno = ?;
```

- Run a few pages at a time on a timer, using a separate short-lived connection, so
  that SQLite's page cache does not answer.
- Run it in the process that owns the database, while its main connection is open. The
  last application close in a process cancels pending repairs.
- On `SQLITE_IOERR_DATA`, record the page and continue.
- This needs item 1, because `sqlite_dbpage` is what stock Node lacks.

### 9. Report `PRAGMA monstervfs_maintenance`

It returns one row holding JSON, per process. Emit these as `warn` events:

| Field | Meaning |
|---|---|
| `main_damage`, `wal_damage` | Damage found that physical files still hold |
| `pending_pages`, `committed_replacements` | Repair queue and repairs committed |
| `blocked` | Exclusive locking is stopping repairs |
| `queue_full` | Repairs were dropped; a later read queues them again |
| `prefix_pending`, `prefix_degraded` | A file header copy needs repair |
| `zero_slot_reads` | All-zero main slots were read as holes. MonsterFt sets WAL before its first table, so only an erased slot should cause this |
| `wal_cutoffs`, `wal_cutoff_bytes`, `wal_last_cutoff` | Item 7. An ordinary crash can cause these |
| `wal_cutoff_commits` | Finished commits certainly lost. An ordinary crash never raises it |
| `wal_refusals` | Item 5 |
| `last_error` | Newest maintenance failure |

## Docs and tests

### 10. Update REPAIR.md and the README

- **[REPAIR.md](REPAIR.md) step 4:** say to remove the recipient's `-wal` and `-shm`,
  and to copy a donor `-wal` if one exists. A stale WAL beside a donor main is not
  detected.
- **Storage:** REPAIR.md says a cluster "uses approx 3x storage". Each 4 KiB page now
  takes an 8 KiB slot, so the files double.
- **Cost:** on macOS/APFS with real full syncs on both sides, a small commit cost
  1.06x native because the sync dominates. Otherwise cost follows pages written, not
  rows: about 60 us per page against 9 to 16 us native. A transaction of 2,000 rows of
  4 KiB (2,250 pages) took 4 to 6x, the same rows at 128 bytes (71 pages) 2.4x, and a
  checkpoint 6 to 8x. Most of that is Reed-Solomon encoding. Linux is not measured yet.
  State this next to any throughput numbers, and re-check `wal_autocheckpoint`, which is
  at the default of 1,000 pages.
- **Certify:** `locking_mode = EXCLUSIVE` in [src/certify.js:49](src/certify.js#L49)
  works and recovers correctly, but it blocks repairs while held.
- **Shutdown:** `monstervfs_shutdown()` must run before `sqlite3_shutdown()` and is a
  C call. Check whether our Node build needs it. A process that just exits only loses
  queued repairs, which later reads queue again.
- **Tests:** run the whole suite on the VFS, and add tests for items 4 to 7.

## Error codes

`node:sqlite` throws `err.code === 'ERR_SQLITE_ERROR'` with the extended code in
`err.errcode`, and `wrapError` keeps it as `err.sqlCode`. Match the full code first:
`19723 & 0xff` is also 11.

| Code | Name | Meaning | Action |
|---|---|---|---|
| 5 | `SQLITE_BUSY` | Lock held, often by a repair commit | The busy timeout covers it |
| 261 | `SQLITE_BUSY_RECOVERY` | Another connection is recovering | The busy timeout covers it |
| 517 | `SQLITE_BUSY_SNAPSHOT` | Snapshot went stale before a write | Restart the transaction |
| 15 | `SQLITE_PROTOCOL` | Lost a WAL lock race repeatedly | Retry the operation |
| 8202 | `SQLITE_IOERR_DATA` | Slot is unrecoverable, or the file is not in this format | Repair; a scrub continues |
| 19723 | `MONSTERVFS_CORRUPT_BACKFILLED` | Recovery refused, nothing changed | Repair; never retry |
| 11 | `SQLITE_CORRUPT` | SQLite itself found bad content | Repair |
| 778 | `SQLITE_IOERR_WRITE` | Write failed, handle still usable | Fatal is fine; a reopen works |
| 13 | `SQLITE_FULL` | Disk full, handle still usable | Free space, then reopen |
| 1034 | `SQLITE_IOERR_FSYNC` | Sync failed, handle stopped for good | Close and reopen |
| 1546 | `SQLITE_IOERR_TRUNCATE` | Truncate failed, handle stopped for good | Close and reopen |
| 5386 | `SQLITE_IOERR_SHMMAP` | Shared memory unusable, handle stopped for good | Close and reopen; check `-shm` permissions |
| 8 | `SQLITE_READONLY` | A write on a read-only connection or under `query_only` | Programming error |
| 14 | `SQLITE_CANTOPEN` | Missing file on a read-only open, `nolock` set, or `mv_*` differs from the file | Programming error, or handle the missing file |
| 26 | `SQLITE_NOTADB` | Read-only open of a file too short to hold a header | Unfinished creation; repair or recreate |
| 21 | `SQLITE_MISUSE` | Illegal `mv_*` value | Programming error |
| 1 | `SQLITE_ERROR` | Rejected PRAGMA: not WAL/FULL, wrong page size, or mmap | Programming error |

A failed extension load has no `errcode`. Check for
`err.code === 'ERR_LOAD_SQLITE_EXTENSION'`.

The VFS also logs through `sqlite3_log`: `SQLITE_WARNING` (28) for a refusal or a cut
that lost a finished commit, and `SQLITE_NOTICE_RECOVER_WAL` (283) for any other cut.
`node:sqlite` cannot read this log, so use item 9.
