# MonsterFt

MonsterFt for Rust lives here. The Rust crate is given version `0.5.0` to start even though it follows JS `0.8.x`.

MonsterFt is architected around 5 processes: Gate, Proc A/B/C, Apply and `send` and `localSend` for IPC.

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

[bench/README.md](bench/README.md) (complete)

#### Latency (p95)

| Batch size | Same-AZ | Multi-AZ |
| ---: | ---: | ---: |
| 1 | 1.660ms | 2.543ms |
| 256 | 3.485ms | 4.890ms |
| 512 | 5.652ms | 7.295ms |
| 1024 | 9.838ms | 11.369ms |

#### Throughput

| Batch size | Same-AZ | Multi-AZ |
| ---: | ---: | ---: |
| 1 | 1,082/s | 545/s |
| 256 | 84,293/s | 72,596/s |
| 512 | 98,447/s | 81,113/s |
| 1024 | 108,323/s | 96,999/s |

## License

LOCK HOST, INC. 2026. AGPL-3.0-only.
