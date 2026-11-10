# MonsterFt

MonsterFt for Rust lives here. The Rust crate is given version `0.5.0` to start even though it follows JS `0.8.x`.

MonsterFt is architected around 5 processes: Gate, Proc A/B/C, Apply and with `send` and `localSend` for IPC.

This design lends itself to a very literal JS-to-Rust port with one current-thread Tokio loop per process.

IPC on linux uses [shmem-ipc](https://github.com/diwic/shmem-ipc). IPC on macOS uses [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2).

## Tests

[test/README.md](test/README.md)

```sh
cargo test --all-targets
./example1/entry.sh
./example2/entry.sh
./example3/entry.sh
```

## Bench

[bench/README.md](bench/README.md)

### Latency (p95)

| Batch size | Same-AZ | Multi-AZ |
| ---: | ---: | ---: |
| 1 | 1.660 ms | 2.543 ms |
| 256 | 3.485 ms | 4.890 ms |
| 512 | 5.652 ms | 7.295 ms |
| 1024 | 9.838 ms | 11.369 ms |

### Throughput (CMD/s)

| Batch size | Same-AZ | Multi-AZ |
| ---: | ---: | ---: |
| 1 | 1,082 | 545 |
| 256 | 84,293 | 72,596 |
| 512 | 98,447 | 81,113 |
| 1024 | 108,323 | 96,999 |

## License

LOCK HOST, INC. 2026. AGPL-3.0-only.
