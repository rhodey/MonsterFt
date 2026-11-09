# TODO: MonsterVfs integration

MonsterFt does not yet use MonsterVfs. Its `ARCH.md` and `tools/README.md` own the
VFS contract and build instructions; keep this list focused on integration.

1. **Qualify a SQLite host.** Match the VFS's pinned SQLite version/source ID and
   enable DBPAGE, SESSION, PREUPDATE_HOOK, and threaded connections. The previously
   tested stock Node lacked DBPAGE. Choose and verify a Node build/binding, then
   align our runtime requirements with it.

2. **Centralize database opens.** Cover [log](src/log.js),
   [storage](src/storage.js), [certification](src/certify.js), and test/example
   readers. Load the extension once per process using its default entry point;
   select `monstervfs_copy` or `monstervfs_parity` for each protected database.
   Verify Node's URI/VFS selection. Use fresh protected files, 4096-byte pages,
   and WAL/FULL; check that selecting WAL succeeds. Reserved bytes are automatic.
   Ordinary connection teardown needs no explicit VFS shutdown call.

3. **Preserve startup ordering.** Keep disposable read-only preflight, then serial
   writable owner initialization, then capture/adoption/reconciliation. Readers
   must not retain unfinished main handles across initialization. Resolve the
   torn-prefix case where preflight returns `SQLITE_NOTADB` but writable open
   could resume creation, without allowing recreation beside established state.
   Keep previous owners stopped before replacement and normal readers read-only.

4. **Handle lock contention.** Add bounded busy timeouts for maintenance writes
   and recovery. Synchronous Node waits block input handling: choose values with
   Gate/election deadlines in mind and qualify startup separately. Certification
   remains offline with `busy_timeout=0`.

5. **Keep storage failures out of CMD outcomes.** In [Apply](src/apply.js), fatal
   storage errors must escape before recording a rejected item or digest, even
   when the transaction remains open. Preserve `sqlCode`; classify NOMEM, IOERR,
   CORRUPT, FULL, CANTOPEN, PROTOCOL, and NOTADB separately from application errors.
   Document preserving/rethrowing database errors and issuing no further SQL
   after transaction loss; see [the existing finding](doc/APPLY.md).

6. **Route unrecoverable storage to offline repair.** Avoid endless Supervisor
   restarts for `MONSTERVFS_CORRUPT_BACKFILLED` (19723), unrecoverable
   `SQLITE_IOERR_DATA` (8202), or persistent corruption/read failures. Match
   extended `sqlCode` before primary classes. Keep transient contention and
   recoverable operation failures distinct. Qualify DB1 majority adoption and
   DB2 receipt reconciliation after WAL suffix loss; acknowledged commits can
   be lost, and the VFS's documented double-fault limit can expose mixed state.

7. **Add maintenance visibility and cold-page scrubbing.** Read process-local
   `PRAGMA monstervfs_maintenance` for damage, pending/blocked repairs, cutoff,
   refusal, and sync-failure diagnostics; counters are incomplete and reset.
   Scrub through fresh short-lived writable connections using `sqlite_dbpage`,
   in small batches while the owner stays open. Read-only reads never queue
   repair. Record unrecoverable pages and continue the scrub.

8. **Document and qualify integration.** Update [REPAIR.md](REPAIR.md): stop owners,
   remove stale recipient WAL/SHM, and copy donor main plus any required WAL
   together. Account for roughly 2x VFS storage plus overhead; measure performance
   on the chosen build instead of reusing historical numbers. Run the full suite
   on both codecs, including interrupted initialization, busy handling, Apply
   I/O failures, recovery cutoffs/refusals, failed checkpoints, repair, and close.
