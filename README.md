# MonsterFt

MonsterFt is a fixed-membership replicated SQLite state machine. For each
command (request), every available node executes the application and
captures its database change as an [SQLite Session Extension](https://sqlite.org/sessionintro.html)
patchset. The command succeeds when a configured quorum agrees on the database
change and the return value.

MonsterFt is built on top of standard, unmodified Raft. Raft orders commands;
MonsterFt adds SQLite agreement above it. Why? [Cosmic Ray Bit-flips](https://en.wikipedia.org/wiki/Single-event_upset)
plus faulty hard drives.

## In-process example

Create at least three nodes with the same membership IDs:

```js
import { Gate, MonsterProc, MonsterApply } from 'monsterft'

const nodeIds = ['1', '2', '3']
const nodesById = new Map()

function createNode(id) {
  const send = (to, frame) => {
    return nodesById.get(to).onReceive(id, frame)
  }

  const apply = (db, data, term, seq, index) => {
    if (seq === 0n) {
      db.exec(`
        CREATE TABLE IF NOT EXISTS counters (
          name TEXT PRIMARY KEY NOT NULL,
          value INTEGER NOT NULL
        ) STRICT
      `)
      return
    }

    const command = JSON.parse(data.toString())
    if (command.type !== 'increment') {
      throw new Error(`unknown command: ${command.type}`)
    }

    db.prepare(`
      INSERT INTO counters (name, value) VALUES (?, 1)
      ON CONFLICT (name) DO UPDATE SET value = value + 1
    `).run(command.name)

    return db.prepare(
      'SELECT value FROM counters WHERE name = ?',
    ).get(command.name).value
  }

  const databasePath = `/tmp/node${id}.db`
  const opts = {}
  const processes = new Map()
  const localSend = (from) => (to, frame) => {
    return processes.get(to).onReceive(from, frame)
  }

  const gate = new Gate(id, nodeIds, send, localSend('gate'), opts)
  processes.set('gate', gate)
  for (const role of ['procA', 'procB', 'procC']) {
    processes.set(role, new MonsterProc(
      role, id, nodeIds, localSend(role), databasePath, opts,
    ))
  }
  processes.set('apply', new MonsterApply(
    id, localSend('apply'), databasePath, { apply },
  ))

  nodesById.set(id, gate)
  return gate
}

const nodes = nodeIds.map(createNode)
await Promise.all(nodes.map((node) => node.open()))
await Promise.all(nodes.map((node) => node.awaitLeader()))

const command = Buffer.from(JSON.stringify({
  type: 'increment',
  name: 'count',
}))
const [cmdSeq, value] = await nodes[0].append(command)
console.log('count =', value)
```

`send(to, frame)` carries Buffer frames and may be synchronous or async. A real
deployment delivers each frame to `target.onReceive(senderId, frame)`. Await
`open()` and `close()` on Gate, which coordinates the other components;
shutdown waits for outstanding Apply work. Reopening uses fresh component
instances.

The example constructs Gate, ProcA/B/C, and Apply in one process and connects
them through `localSend`. Each Proc has its own protocol database; Apply alone
writes the application database. The files are `${databasePath}1a`,
`${databasePath}1b`, `${databasePath}1c`, and `${databasePath}2`.
V2 requires file-backed databases and a fresh layout; there is no migration
from earlier releases.

### Application and patchset rules

`apply(db, data, term, seq, index)` runs inside an SQLite transaction and
may be synchronous or async. When `seq === 0n` create the application
schema and return, MonsterFt ignores seq 0 result.

MonsterFt uses SQLite's Session extension, so its standard rules apply:

- Give every replicated table a declared, non-NULL primary key.
- Create schema during the sequence-zero callback; patchsets capture row
  changes, not schema changes.
- Keep replicated state in the `main` database. Temporary,
  virtual, and attached tables are not captured.
- Keep triggers and foreign-key behavior deterministic across nodes.

Nodes must produce matching patchsets and outcomes. Return a
MessagePack-encodable value or throw an error. When `apply` coordinates
external work, use a protocol that gives every node the same stable
observation; see [FETCH.md](FETCH.md).

### Commands

```js
const [cmdSeq, value] = await node.append(buffer)
const [cmdSeq, outcomes] = await node.appendBatch(buffers)
```

Commands (buffers) may be appended through any node. `appendBatch()`
returns one `Promise.allSettled()`-style outcome for each command, in the
same order. A command with error is rolled back without discarding
successful commands; singular `append()` throws its application error
instead.

Useful public state includes `.id`, `.state`, and `.isOpen`.

Gate exposes agreed protocol events including `change`, `commit`, `cmd`, and
`sync`. Each component emits its own `warn` and `error` diagnostics; listen on
Apply and every Proc as well as Gate. Gate does not collect their diagnostics.
An ordinary rejected command item warns on Apply and remains a command result.

### Retention

In addition to integrity MonsterFt can be configured to limit growth of the
Raft log to target limits. Each MonsterFt command adds two log entries: one
`CMD` and one `SYNC`. Raft also comes with post-election no-op entries but
in general `keepTarget` of 1,000 will retain 500 commands.

Pass the same retention options to each `MonsterProc` through `opts` above:

```js
const opts = {
  keepTarget: 1_000,
  keepTrigger: 1_500,
}
```

Supply both options or neither. The author chose to not implement Raft "Log
Compaction" and to not implement Raft membership changes because both were
deemed too complicated to justify what they bring. In the case of Log
Compaction it also does not play well with storage-constrained systems. Readers
who consult the [Raft PDF](https://raft.github.io/raft.pdf) will agree that
both Log Compaction and membership changes are extensions outside of core.

### Repair

A node requires repair when an error with code `REPAIR_QUORUM_IMPOSSIBLE` or
`REPAIR_OUTSIDE_AGREEMENT` is thrown or emit. Respectively these represent a
command for which no quorum can agree (most likely your app failed to use
SQLite deterministically) and a command in which only a minority of nodes
disagree. A node also requires repair if it falls behind retention.

The MonsterFt repair protocol is an "offline" procedure to be carried out by
an operator. [REPAIR.md](REPAIR.md) documents the procedure, in summary its
an SQLite database copy from a designated healthy donor to an unhealthy
node/nodes.

### Examples

- [example3.js](example3.js): in-process MonsterFt cluster.
- [example4.js](example4.js): MonsterFt with HTTP side effects.

### Protocol copies and deployment

Each cluster node runs three protocol copies with separate SQLite logs. Gate
supplies identical ordered inputs and logical time, compares their committed
outputs, and passes agreed requests to the cluster or the single Apply owner.
Frames carry SHA-256 checksums. Startup reconciles the local protocol copies
by majority before recovering completed Apply work.

For separate processes, construct each component in its intended process and
replace the in-process `localSend` routing with a `localSend(to, frame)` transport,
such as Unix-domain sockets. Gate takes both cluster `send` and `localSend`;
the Procs and Apply take only `localSend`. Use the same node ID, membership,
base database path, and protocol options for the corresponding components.
Local transports deliver Buffers to `.onReceive(from, frame)` using destinations
`gate`, `procA`, `procB`, `procC`, and `apply`. They must preserve send order and
deliver without loss once ready. Gate retries startup hello; later local
timeouts stop the component that detects them. Gate's configurable
`applyTimeout` defaults to 5,000 ms per Apply execution request. Gate and Apply
can share a process while each Proc runs separately.

An unexpected component failure reports locally and stops that component;
Gate and runtime Procs detect missing progress through timeouts. Gate failure
or failed `open()` does not coordinate cleanup. Explicitly call `Gate.close()`
or stop the owning processes before restarting; Gate emits `close` only after
coordinated shutdown succeeds.

The library does not spawn OS processes. For production, use a supervisor and
wait for old database owners to exit before replacing them. All components
share a local filesystem. The in-process arrangement remains useful for tests
and examples; standalone RaftNode is no longer a supported public API.

### Roadmap

The v2 architecture moves toward tolerating faults in protocol execution while
running user apply work only once per cluster node. It is not a claim of full
single-fault tolerance: Gate and Apply remain individual components, and startup
majority comparison does not authenticate every older log row. The
[review guides](doc/) describe the components and their known limitations.

### Collaborations

The work is licensed `AGPL-3.0-only` to encourage collaborations while
leaving open a path for negotiating commercial licenses and support
packages.

I incorporated [Lock Host, Inc](https://lock.host/) for some works years
ago and they led to this. Write to hello@lock.host for commercial
licenses and other inquiries. Opening GitHub issues is also encouraged.

### Install

This software is on NPM with version `0.7.0` for educational purposes. It
may be the case that I cut a `1.0.0` release before releasing what is on
the roadmap as `2.0.0` but I don't feel pressured to commit to this now.

```sh
npm install monsterft
```

### Copyright

LOCK HOST, INC. 2026.
