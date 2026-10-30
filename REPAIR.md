# MonsterFt repair

Use this procedure when a node reports `REPAIR_NO_QUORUM` or
`REPAIR_OUTSIDE`, or when history can no longer catch it up due
to retention. Each node owns 4 databases: `${databasePath}1a`,
`${databasePath}1b`, `${databasePath}1c`, and `${databasePath}2`. The first
3 belong to ProcA/B/C; the last belongs to Apply.

The Raft log lives in 1a, 1b, 1c and the ["Retention"](README.md#retention)
section of README explains how log size can be kept within limits. The result
is that a 3 node MonsterFt cluster only uses approx 3x storage (approx the
size of 3 Apply DBs).

1. Choose the donor whose state you accept, then drain it:

   ```js
   await supervisor.drainCmd() // (donor)
   ```

2. Certify the donor's databasePath:

   ```js
   import { certify } from 'monsterft'

   certify(databasePath) // (donor)
   ```

   Certification rejects an undrained state by default. If an operator
   determines that this needs to be bypassed, it may be:

   ```js
   certify(databasePath, true) // (donor)
   ```

3. Close each node to be repaired through its Supervisor:

   ```js
   await supervisor.close() // (recipient)
   ```

4. Copy all four donor databases to the recipient paths, replacing the
   recipient nodes's files.

5. Create a fresh Supervisor for each node, using original node IDs
   and constructor args.

6. Start/open Supervisors as before.

## Copyright
LOCK HOST, INC. 2026. GNU AGPLv3.

Commercial license contact: hello@lock.host.
