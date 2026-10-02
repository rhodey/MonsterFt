# MonsterFt repair

Use this procedure when a node reports `REPAIR_QUORUM_IMPOSSIBLE` or
`REPAIR_OUTSIDE_AGREEMENT`, or when history can no longer catch it up due
to KEEP. Each node owns a DB pair: the constructor `databasePath` and the
literal `${databasePath}2`.

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
4. Stop every node to be repaired.
5. For each target: remove `-wal` and `-shm` and SQLite db files. Then copy
   all from the donor to the target.
7. Construct fresh `MonsterFt` instances with their original constructor
   arguments, then call `open()` normally.


## Copyright
LOCK HOST, INC. 2026. GNU AGPLv3.

Commercial license contact: hello@lock.host.
