# HTTP side effects

From the crate root, run `./example3/entry.sh`. It builds and starts a fact server
and three supervised nodes. Node 1 submits a POST command, then a GET command;
the launcher stops all processes when the demonstration finishes.

Every node reports the same fact under one request ID. The server uses SHA-256
and counts distinct member reports, requiring two to agree. Repeated reports are
idempotent; conflicting reports from the same member are rejected. Each GET has
an observation ID whose first result is frozen, including a pending result.
This gives every Apply callback the same response despite different arrival times.

`entry.rs` constructs Supervisor, using example2's shared transport setup.
The child scripts show Proc/Apply ownership and IPC; `apply.rs` runs an async HTTP
callback. `server.rs` and `facts.rs` implement the separate persistent fact service.
HTTP crates are example/test dependencies, not library dependencies.

Data persists under `example3/data/`. Override it with `MONSTERFT_DATA_DIR`;
`MONSTERFT_BASE_PORT` changes the cluster ports (default 30500), and
`MONSTERFT_HTTP_PORT` changes the HTTP port (default 43000). The launcher supplies
`MONSTERFT_FACT_URL` to every node and Apply child.
