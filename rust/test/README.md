# Tests

Tests live here and are registered in `Cargo.toml`, outside implementation files.
See [../README.md](../README.md) for build prerequisites. From the crate root:

```sh
cargo test --locked --test protocol
cargo test --locked --all-targets
cargo fmt --all --check
cargo clippy --locked --all-targets -- -D warnings
python3 test/supervised_fault_smoke.py
```

The first command runs focused protocol regressions; choose another Cargo test
target for other changes. The smoke test builds example2, kills/suspends Apply
children, and checks recovery and cleanup. It prints the retained log directory.
CI also runs `./example1/entry.sh`, `./example2/entry.sh`, and
`./example3/entry.sh` twice each to exercise fresh and retained databases on
Linux and macOS.

Coverage includes encoding, elections/replication, paired SYNC/CMD, three-Proc
agreement, storage/reconciliation, Apply receipts, retention/certification, API
cancellation, transports, supervision, and the HTTP example’s immutable reports
and frozen observations. `support/protocol.rs` uses real Procs
and databases with simulated remote replies and Apply receipts; real callbacks
and owner loops are covered separately. Ignored subprocess fixtures are invoked
by their parent tests; IPC tests need sockets/shared-memory access.

This is a partial port of the JS suite. Priorities for further coverage include
interrupted certification/adoption, DB2 commit failure on a paired CMD, and more
backtracking/election edge cases. Give tests isolated databases and IPC resources,
and use deterministic clocks and fault injection rather than timing thresholds.
