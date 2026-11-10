# Benchmarks

Each CMD increments a SQLite counter. The elected leader submits the workload;
results and database agreement are verified after timing.

## Run simulated

Run from the Rust crate directory with Python 3.9+ and the usual Rust build
prerequisites. This starts a local three-node cluster and builds release binaries:

```sh
python3 bench/run.py --profiles lan,az --batch-size 256 --windows 1,32 \
  --warmup 5 --seconds 30 --repeats 1 --artifacts bench/results/local-256
```

Use `--profiles baseline` to run without added network latency.

## Run remotes

Deploy the same source and release binary to three Linux machines with Python
3.9+ and `vmstat`. Build on the target architecture:

```sh
cargo build --locked --release --example monsterft-bench
python3 bench/remote.py \
  --hosts HOST1,HOST2,HOST3 --private-hosts PRIVATE_IP1,PRIVATE_IP2,PRIVATE_IP3 \
  --user SSH_USER --identity /path/to/key --known-hosts /path/to/known_hosts \
  --remote-source /path/to/rust-crate \
  --binary /path/to/rust-crate/target/release/examples/monsterft-bench \
  --remote-root /mnt/monsterft/bench/run-1 --artifacts bench/results/remote-256 \
  --profile same-az --batch-size 256 --windows 1 \
  --warmup 5 --seconds 30 --repeats 1
```

Omit `--user` to use your SSH configuration or current username.

Use `--windows 32 --seconds 20` for throughput, and `--profile multi-az` when
hosts span availability zones. Choose fresh artifact directories for each run;
`bench/results/` is ignored by Git. Both runners provide `--help`.

## Results

November 10, 2026: three AWS `i8g.2xlarge` hosts (8 cores, 64 GiB RAM each),
local NVMe with ext4/noatime, SQLite WAL/FULL, and private TCP. Same-AZ uses
`us-east-1b`; multi-AZ uses `us-east-1b`, `us-east-1c`, and `us-east-1d`.
The same AZ B host leads each trial and submits commands locally.

One measured run per configuration after a 5-second warmup: 30 seconds for
latency and 20 seconds for throughput, plus draining outstanding work.
All trials passed the result, final SYNC, and database-agreement checks.

### Batch latency

One outstanding batch. Times are milliseconds until the **whole batch** returns.

| CMDs per batch | Same-AZ p50 | Same-AZ p95 | Same-AZ p99 | Multi-AZ p50 | Multi-AZ p95 | Multi-AZ p99 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.420 | 1.660 | 1.858 | 2.394 | 2.543 | 2.706 |
| 16 | 1.415 | 1.534 | 1.599 | 2.534 | 2.705 | 3.834 |
| 32 | 1.442 | 1.768 | 1.850 | 2.706 | 2.858 | 3.102 |
| 64 | 1.645 | 1.772 | 1.866 | 2.517 | 3.448 | 4.149 |
| 128 | 1.868 | 2.344 | 2.737 | 2.920 | 3.856 | 4.345 |
| 256 | 2.435 | 3.485 | 3.596 | 4.660 | 4.890 | 5.063 |
| 512 | 5.490 | 5.652 | 7.044 | 6.960 | 7.295 | 7.843 |
| 1024 | 9.604 | 9.838 | 12.018 | 11.051 | 11.369 | 12.315 |

### Throughput

32 outstanding batches. CMD/s counts individual commands, including the time to
drain submitted batches. A backlog exercises paired SYNC/CMD replication.

| CMDs per batch | Same-AZ CMD/s | Multi-AZ CMD/s |
| ---: | ---: | ---: |
| 1 | 1,082 | 545 |
| 16 | 15,551 | 8,375 |
| 32 | 30,643 | 16,669 |
| 64 | 51,879 | 32,118 |
| 128 | 80,978 | 56,975 |
| 256 | 84,293 | 72,596 |
| 512 | 98,447 | 81,113 |
| 1024 | 108,323 | 96,999 |

### Private-network RTT

A separate 256-byte TCP_NODELAY echo probe: 1,000 samples after warmup, including
Python processing and scheduling overhead. `b1`–`b3` are AZ B hosts (`b3` leads);
`c` and `d` are the AZ C and AZ D hosts.

| Deployment | Pair | p50 RTT, ms | p95 RTT, ms |
| --- | --- | ---: | ---: |
| same-az | b1–b2 | 0.095 | 0.108 |
| same-az | b1–b3 | 0.133 | 0.137 |
| same-az | b2–b3 | 0.262 | 0.265 |
| multi-az | c–d | 0.619 | 0.682 |
| multi-az | c–b3 | 0.966 | 0.970 |
| multi-az | d–b3 | 0.511 | 0.548 |
