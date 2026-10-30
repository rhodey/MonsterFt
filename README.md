# MonsterFt

MonsterFt is a fixed-membership replicated SQLite state machine. For each
command (request), every available node executes the application and
captures its database change as an [SQLite Session Extension](https://sqlite.org/sessionintro.html)
patchset. The command succeeds when a configured quorum agrees on the database
change and the return value.

MonsterFt is built on top of standard, unmodified Raft. Raft orders commands;
MonsterFt adds SQLite agreement above it. Why? [Cosmic Ray Bit-flips](https://en.wikipedia.org/wiki/Single-event_upset)
plus faulty hard drives.

## Minimal example

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

  const local = new Map()
  const localSend = (from) => (to, frame) => {
    return local.get(to).onReceive(from, frame)
  }

  const gate = new Gate(id, nodeIds, send, localSend('gate'))
  local.set('gate', gate)

  const databasePath = `/tmp/node${id}.db`
  for (const role of ['procA', 'procB', 'procC']) {
    local.set(role, new MonsterProc(
      role, id, nodeIds, localSend(role), databasePath,
    ))
  }
  local.set('apply', new MonsterApply(
    id, localSend('apply'), databasePath, apply,
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

What's going on here is important and also easy to miss: this is a 3 node
Raft cluster but every node is made up of a "gate", three protocol engines,
and one "apply" worker. These are to be run as five linux processes and
this architecture maximizes correctness. The three protocol engines per node
turn "strong leader" Raft into "strong correct leader" in the presence of
cosmic bit flips.

`send(to, frame)` and `localSend(to, frame)` allow MonsterFt to be fully
transport-agnostic. send is allowed to be lossy and unordered but localSend
is required to be reliable and ordered. frame is `Buffer` for both. See
[example2/](example2/) for a demo using TCP between nodes and Unix Domain
Sockets between local processes.

### Application and patchset rules

`apply(db, data, term, seq, index)` runs inside an SQLite transaction and
may be synchronous or async. When `seq === 0n` create the application
schema and return, MonsterFt ignores seq 0 result.

MonsterFt uses [SQLite's Session extension](https://sqlite.org/sessionintro.html),
so its standard rules apply:

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

Commands (buffers) may be appended through any node (gate). `appendBatch()`
returns one `Promise.allSettled()`-style outcome for each command, in the
same order. A command with error is rolled back without discarding
successful commands; singular `append()` throws its application error
instead.

Useful public state includes `.id`, `.state`, and `.isOpen`.

Public events include `change`, `warn`, and `error`.

### Retention

MonsterFt can be configured to limit growth of the Raft log to target limits.
Each MonsterFt command adds two log entries: one `CMD` and one `SYNC`. Raft
also comes with post-election no-op entries but in general `keepTarget` of
1,000 will retain 500 commands.

```js
const opts = {
  keepTarget: 1_000,
  keepTrigger: 1_500,
}
const proc = new MonsterProc(
  'procA', id, nodeIds, localSend, databasePath, opts
)
```

This author chose to not implement Raft "Log Compaction" and to not implement
Raft membership changes because both were deemed too complicated to justify
what they bring. Readers who consult the [Raft PDF](https://raft.github.io/raft.pdf)
will agree that both Log Compaction and membership changes are extensions
outside of core.

### Repair

A node requires repair when an error with code `REPAIR_NO_QUORUM` or
`REPAIR_OUTSIDE` is thrown or emit. Respectively these represent a
command for which no quorum can agree (most likely your app failed to use
SQLite deterministically) and a command in which only a minority of nodes
disagree. A node also requires repair if it falls behind retention.

The MonsterFt repair protocol is an "offline" procedure to be carried out by
an operator. [REPAIR.md](REPAIR.md) documents the procedure, in summary its
SQLite database copies from a healthy donor to an unhealthy node/nodes.

### Examples

- [example1/](example1/): in-process MonsterFt cluster.
- [example2/](example2/): supervised child processes, Unix sockets, and TCP.
- [example3/](example3/): supervised MonsterFt with HTTP side effects.

### Roadmap

MonsterFt has **big** ambitions on Earth and in orbit. A thorough reading of
[DRAM Errors in the Wild: A Large-Scale Field Study](https://research.google/pubs/dram-errors-in-the-wild-a-large-scale-field-study/)
will reveal that even server-grade ECC RAM has an error rate of 1 error
per 100 server-years (Earth). Thus a 100 server fleet is 1 error per 1 year.
What we're aiming for is on the order of 1 error per 100 Billion server-years
and we think our current direction can get us there.

MonsterFt has these users in mind: space, aerospace, defense, fintech, ai
(autonomy). `0.8.0` saw many big changes, next up on path to 100 Billion:
upgrades to repairs. Soon after: Rust implementation.

### Collaborations

The work is licensed `AGPL-3.0-only` to encourage collaborations while
leaving open a path for negotiating commercial licenses and support
packages. I incorporated [Lock Host, Inc](https://lock.host/) for some works
years ago and led to this. Write to hello@lock.host for commercial licenses
and other inquiries. Opening GitHub issues is also encouraged.

### Install

This software is on NPM with version `0.8.0` for educational purposes. The
primary purpose of this README is introductory, not exhaustive. `1.0.0` will
aim for exhaustive doc.

```sh
npm install monsterft
```

### Copyright

LOCK HOST, INC. 2026.
