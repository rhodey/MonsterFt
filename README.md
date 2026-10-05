# MonsterFt

MonsterFt is a fixed-membership replicated SQLite state machine. For each
command (request), every available node executes the application and
captures its database change as an [SQLite Session Extension](https://sqlite.org/sessionintro.html)
patchset. The command succeeds when a configured quorum agrees on the database
change and the result.

MonsterFt is built on top of standard, unmodified Raft. Raft orders commands;
MonsterFt adds SQLite agreement above it. Why? [Cosmic Ray Bit-flips](https://en.wikipedia.org/wiki/Single-event_upset)
plus faulty hard drives.

## Minimal example

Create at least three nodes with the same membership IDs:

```js
import { MonsterFt } from 'monsterft'

const nodeIds = ['1', '2', '3']
const nodesById = new Map()

function createNode(id) {
  const send = (to, message) => {
    return nodesById.get(to).onReceive(id, message)
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

  const node = new MonsterFt(
    id,
    nodeIds,
    send,
    `/tmp/node${id}.db`,
    { apply },
  )
  nodesById.set(id, node)
  return node
}

const nodes = nodeIds.map(createNode)
nodes.forEach((node) => node.open())
await Promise.all(nodes.map((node) => node.awaitLeader(true)))

const command = Buffer.from(JSON.stringify({
  type: 'increment',
  name: 'count',
}))
const [cmdSeq, value] = await nodes[0].append(command)
console.log('count =', value)
```

`send(to, message)` may be synchronous or async. A real deployment uses
its transport to deliver the message to `target.onReceive(senderId, message)`.
`open()` and `close()` are synchronous.

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

Public events include `change`, `warn`, and `error`.

### Retention

In addition to integrity MonsterFt can be configured to limit growth of the
Raft log to target limits. Each MonsterFt command adds two log entries: one
`CMD` and one `SYNC`. Raft also comes with post-election no-op entries but
in general `keepTarget` of 1,000 will retain 500 commands.

```js
const node = new MonsterFt(id, nodeIds, send, databasePath, {
  apply,
  keepTarget: 1_000,
  keepTrigger: 1_500,
})
```

Supply both options or neither. The author chose to not implement Raft "Log
Compaction" and to not implement Raft membership changes because both were
deemed too complicated to justify what they bring. In the case of Log
Compaction it also does not play well with storage-constrained systems. Readers
who consult the [Raft PDF](https://raft.github.io/raft.pdf) will agree that
both log compaction and membership changes are extensions outside of core.

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

- [example1.js](example1.js): in-process Raft cluster.
- [example2.js](example2.js): Raft over encrypted TCP.
- [example3.js](example3.js): in-process MonsterFt cluster.
- [example4.js](example4.js): MonsterFt with HTTP side effects.

### Roadmap

Without MonsterFt your application uses maybe 2GB of RAM and maybe 100GB of
HDD. This is the surface area in which things can go wrong even if you are
using Raft. With MonsterFt: the surface area vulnerable to bad things is on
the order of 100 bytes. I'm using approximate language here because this
repo is not MonsterFt in its final form and what is planned is looking
like it can be truly fault tolerant in the sense that aerospace and the
defense industry understand.

Raft is a "strong leader" protocol and this means that if the leader node
tells a follower to truncate log to X and append Y the follower is going to
do this. Raft needs faultless HDD and we know how to do this with things
like RAID but we do not have faultless CPU and RAM see: ECC RAM stats on
[lock.host](https://lock.host/) and understand also that bit-flips increase
with altitude and orbit and beyond.

The planned work keeps Raft and SQLite and all MonsterFt protocol messages
while borrowing an architecture from SpaceX: a minimum 3 node cluster
remains but every node is running two copies of the app stack and
these copies are forced to agree before they can RPC with the cluster.

Actually you could ditch MonsterFt and do Raft core with this architecture
and do pretty well but you would not see some benefits and you would need
6X compute and 6X storage while MonsterFt I'm pretty sure can deliver on
approx 3X compute and approx 3X storage.

### Collaborations

The work is licensed `AGPL-3.0-only` to encourage collaborations while
leaving open a path for negotiating commercial licenses and support
packages.

I incorporated [Lock Host, Inc](https://lock.host/) for some works years
ago and they led to this. Write to hello@lock.host for commercial
licenses and other inquiries. Opening GitHub issues is also encouraged.

### Install

This software is on NPM with version `0.5.0` for educational purposes. It
may be the case that I cut a `1.0.0` release before releasing what is on
the roadmap as `2.0.0` but I don't feel pressured to commit to this now.

```sh
npm install monsterft
```

### Copyright

LOCK HOST, INC. 2026.
