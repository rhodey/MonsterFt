# MonsterFt repair

Use this procedure when a node reports `REPAIR_QUORUM_IMPOSSIBLE` or
`REPAIR_OUTSIDE_AGREEMENT`, or when history can no longer catch it up due
to retention. Each node owns four databases: `${databasePath}.a`,
`${databasePath}.b`, `${databasePath}.c`, and `${databasePath}2`. The first
three belong to ProcA/B/C; the last belongs to Apply.

1. Choose the donor whose state you accept. A successful drain closes the donor:

   ```js
   await donor.drainCmd()
   ```
2. Certify the donor's base path:

   ```js
   MonsterFt.certify(databasePath)
   ```

   Certification rejects an undrained state by default. If an operator
   determines that this needs to be bypassed, it may be:

   ```js
   MonsterFt.certify(databasePath, true)
   ```
3. Stop every component of each node to be repaired and await shutdown.
4. Copy all four donor databases and their SQLite sidecars to the corresponding
   target paths, replacing the stopped target's files and stale sidecars. Copy
   DB1A, DB1B, and DB1C individually; do not duplicate DB1A into all three paths.
5. Construct fresh components with their original node IDs and configuration,
   then await `open()` on each Gate. Startup reconciles its local copies and
   imports any committed Apply receipt without executing that work again.


## Copyright
LOCK HOST, INC. 2026. GNU AGPLv3.

Commercial license contact: hello@lock.host.
