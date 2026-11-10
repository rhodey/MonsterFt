# MonsterFt

MonsterFt for Rust lives here. The Rust crate is given version `0.5.0` to start even though it follows JS `0.8.x`.

MonsterFt is architected around 5 processes: Gate, Proc A/B/C, Apply and with `send` and `localSend` for IPC.

This design lends itself to a very literal JS-to-Rust port with one current-thread Tokio loop per process.

IPC on linux uses [shmem-ipc](https://github.com/diwic/shmem-ipc). IPC on macOS uses [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2).

## Tests

```sh
cargo test --all-targets
./example1/entry.sh
./example2/entry.sh
./example3/entry.sh
```

## Docs

+ [AGENTS.md](AGENTS.md)
+ [test/README.md](test/README.md)
+ [bench/README.md](bench/README.md)

## License

LOCK HOST, INC. 2026. AGPL-3.0-only.
