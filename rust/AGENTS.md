# Working on MonsterFt Rust

Start with [README.md](README.md) for building and running the examples. This is
an early Rust port of MonsterFt for Linux and macOS. The [JavaScript implementation](../src/)
and its [review guides](../doc/) are the behavioral reference for protocol changes. Clusters
start with fresh Rust databases; JS/Rust interoperability, migration, and
MonsterVfs integration are outside the current scope.

## Architecture

Each component owns its mutable state on a current-thread Tokio loop. Listeners
run serially on that thread and need neither `Send` nor `Sync`. `example1/` puts
DB owners on separate threads in one process; `example2/` and `example3/` run four compiled
children, with Gate and Supervisor in the parent.

- `gate` orders inputs, supplies a shared protocol clock, checks three-Proc
  agreement, and exposes command results and events.
- `proc` owns one DB1 copy and its transaction/publication boundary. `engine`,
  `raft`, `monster`, and `executor` implement synchronous protocol execution and
  deterministic continuations/timers. `startup`, `storage`, `log`, and `certify`
  handle durable state, reconciliation, and offline repair certification.
- `apply` is the sole DB2 writer. It runs sync or async application callbacks and
  commits application changes together with their receipt.
- `supervisor` replaces failed attempts only after old children exit and cleanup
  succeeds. A lifetime pipe signals parent loss; a blocked child still requires
  host/supervisor termination. It never replays commands.
- `net` carries unchanged frames: TCP between nodes, shmem-ipc on Linux and
  iceoryx2 on macOS locally. Send callbacks synchronously enqueue; they must not
  block or recursively invoke receivers. TCP may drop frames; Raft handles its
  retries. Local transport failure stops the attempt.

## Rules to preserve

- Each Proc processes an ordered input, continuations, and timers in one guarded
  DB1 transaction, committing before publication. A supplies payloads; B/C supply
  complementary checksum halves. Gate uses their assembled checksum and validates
  the entire output list before releasing anything or dispatching the next input.
- Keep protocol time, randomness, iteration, and encoding deterministic. Preserve
  checked sequence arithmetic and capture-time event values. Compare final state
  once per input for `change`; application results must also be deterministic.
- Startup adoption requires independent majority verification by each Proc.
  Preserve generation guards, receipt reconciliation, and WAL/FULL durability.
  Committed Apply work must not execute again after interrupted acknowledgment.
- Keep one unresolved CMD at a time. Pair SYNC with the next queued CMD as separate
  log entries in one append transaction; apply SYNC first and preserve drain,
  retention, and repair fences.
- Apply callbacks must stay inside the library transaction. After an SQLite error
  ends it, propagate the error without further SQL. Ordinary item failures roll
  back their savepoint; application changes and the Session-derived receipt
  commit atomically. Local backtraces must not enter agreement digests.
- Unexpected failures stop the owner without publishing uncommitted outputs.
  Intentional fatal repair commits its fence, then stops without a completion.
  A live `Code::RepairNoQuorum` notification remains nonfatal. Diagnostics stay with their owner;
  malformed local traffic is fatal, while remote corruption is warned/discarded
  with existing protocol-specific ERR responses preserved.
- Gate admits operations when called. Dropping a result future stops waiting,
  not execution. Timeouts do not undo effects or make retries safe. Preserve the
  JS limitations around duplicate forwarding and retention/backtracking.

## Development

Keep test bodies under `test/`, registered explicitly in `Cargo.toml`. Follow
[test/README.md](test/README.md) for checks and coverage, and
[bench/README.md](bench/README.md) for benchmark commands and results. Use focused
regressions while editing, then the full suite and all three examples for runtime
changes. Keep dependencies pinned and README introductory. Prefer deterministic
fault tests over sleeps or wall-clock performance thresholds.
