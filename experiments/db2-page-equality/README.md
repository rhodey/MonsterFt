# Reproducing DB2 page-equality experiments

Read [the findings](../../doc/DB2-PAGE-EQUALITY.md) and [PLAN.md](../../PLAN.md).
Production modules are unchanged. Experiments capture/hash patchsets; none apply
them. All database files are disposable and retained in the reported temporary
directories for inspection. Recorded `*-results.json` files are observations,
including intentional counterexamples, not a claim every comparison matched.

Requirements: project Node dependencies, Node with `node:sqlite` sessions,
Python 3, a C compiler, and a local SQLite amalgamation. The checked-in results
use Node 26.13.0 / SQLite 3.54.0 plus an external DBPAGE-enabled SQLite 3.54.1
reader. The Nix Node runtime remains untested.

Build a separate experimental library (macOS example; use `.so` on Linux):

```sh
python3 experiments/db2-page-equality/build_sqlite.py --output /tmp/monsterft-db2-repro/libsqlite3.dylib
export SQLITE_PROBE_LIB=/tmp/monsterft-db2-repro/libsqlite3.dylib
```

The helper defaults to the sibling better-sqlite3 checkout's amalgamation. Supply
`--source /path/to/sqlite3.c` otherwise. It records the source SHA-256 and compiler
command beside the library. No library or database binary is stored in this repo.

From the MonsterFt root:

```sh
python3 experiments/db2-page-equality/ownership.py
node experiments/db2-page-equality/apply-lifecycle.mjs
python3 experiments/db2-page-equality/lifecycle.py
node experiments/db2-page-equality/apply-freelist.mjs
node experiments/db2-page-equality/connection-state.mjs
python3 experiments/db2-page-equality/crash.py
python3 experiments/db2-page-equality/freelist.py
python3 experiments/db2-page-equality/page-write-order.py
python3 experiments/db2-page-equality/grouped-repair.py
```

Scripts overwrite their named result JSON by default. Python scripts and
connection-state.mjs accept `--output PATH`; the two Apply scripts take an
optional positional output path. Do not use Python `-O`: assertions validate
the experimental assumptions. Some scripts assert a known counterexample, so
a runtime change can require reviewing an assertion rather than treating it as
a production regression. `freelist.py` reuses the lifecycle workload and records
mitigation comparisons rather than hiding mismatches.

`sqlite_probe.py snapshot DATABASE [--pages]` captures a pinned raw/canonical
page view, validating the owner singleton and substituting a generated valid
template. `--pages` includes base64 raw bytes. This common reader applies only
the original owner/page-1 rules; `freelist.py` separately experiments with the
additional free-leaf rule. Its validators are experimental, not a production
corruption boundary.

`apply-lifecycle.mjs` and `apply-freelist.mjs` invoke actual
`MonsterApply._execute` against the proposed schema, with experimental
startup/ownership wiring. They do not run a Raft cluster. `crash.py` uses
pipe-synchronized SIGKILL of its own disposable child processes before commit
and after commit/before simulated ACK. It does not simulate failed storage or
power loss.

`page-write-order.py` compares full-image writes in ascending, descending,
page-1-last, and shuffled order. It retains failing direct-main and autocommit
controls, and tests a dedicated in-memory main connection with an attached
recipient. It records same-connection versus reopened behavior, physical size
after truncation, and continued DML/DDL equality. The donor stays pinned while
its writer advances.

`grouped-repair.py` models the typed multi-page hash submission and follower
check with Python values, then writes real replacement pages and reopens the
target. Its default matrix retains failing installer variants. The attached
variants must pass all repair/expected-abort cases, including wrong free-page
classification, schema/root changes, growth/shrinkage, and full-copy fallback.
Each completed repair continues for 30 equality checkpoints with unequal
restart counts. `pagesWritten` counts replacement calls, not device writes or
separate ownership claims. No real RPCs, Raft, or production Apply are involved.

To run only the preferred installation candidate:

```sh
python3 experiments/db2-page-equality/grouped-repair.py --profiles attached_ascending attached_page1_last --output /tmp/monsterft-grouped-attached-results.json
```

These new suites intentionally leave repair lifecycle, interruption/recovery,
and rejoin design for later. Both use the custom SQLite library for reads and
writes; repeat the relevant cases on the planned Nix Node runtime. Do not treat
the expected failures in the recorded comparison matrix as successful repairs.
