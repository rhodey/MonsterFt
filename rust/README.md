# MonsterFt

A Rust port of MonsterFt: replicated SQLite application execution, with three
independent local protocol owners checking their outputs before publication.
This port is new and is not yet production ready.

Gate, Proc A/B/C, Apply, and Supervisor are explicit components. Run them together
for a demonstration, or supervise four compiled database-owner processes with
shared-memory IPC and TCP between cluster nodes. Application callbacks can be
synchronous or asynchronous.

Build with Rust 1.94 or newer, a C compiler, and libclang. SQLite is bundled with
its Session extension enabled.
Run the following commands from `rust/`.

```sh
cargo test --all-targets
./example1/entry.sh
./example2/entry.sh
./example3/entry.sh
```

[Example3](example3/README.md) demonstrates async HTTP side effects with a quorum-aware fact service.

Examples keep their databases under their respective `data/` directories. Both
Linux and macOS are supported. See [AGENTS.md](AGENTS.md) for architecture and
contributor guidance, and [test/README.md](test/README.md) for validation commands.

See [bench/README.md](bench/README.md) for benchmark results and run instructions.

Licensed under AGPL-3.0-only.
