import test from 'tape'
import { cluster, until, roles } from './v2-helper.js'
import * as Err from '../src/error.v2.js'

for (const delay of [0, 2]) {
  test(`v2 retained leaders reject an empty follower without closing (${delay} ms transport)`, async (t) => {
    const c = cluster({ delay, opts: {
      keepTarget: 2, keepTrigger: 5, electionTimeout: 300, appendTimeout: 2_000,
    } })
    try {
      c.blocked.add('c')
      await Promise.all(c.gates().map((gate) => gate.open()))
      await c.nodes.get('c').gate.close()
      const leader = await until(() => c.leader(['c']), 'leader without empty follower')
      await until(() => ['a', 'b'].every((id) => c.nodes.get(id).procA.engine.appliedSeq >= 0n),
        'setup on the active pair')
      for (let i = 0; i < 8; i++) { await leader.append(Buffer.from(`retained${i}`)) }
      await c.settled()
      for (const id of ['a', 'b']) {
        t.ok(c.nodes.get(id).procA.log.begin > 0n, 'healthy member has pruned genesis')
      }

      // Restart the retained pair so replication begins at its current head.
      await Promise.all(['a', 'b'].map((id) => c.nodes.get(id).gate.close()))
      for (const id of ['a', 'b']) { c.makeNode(id) }
      await Promise.all(['a', 'b'].map((id) => c.nodes.get(id).gate.open()))
      await until(() => c.leader(['c']), 'leader with retained history')
      const before = c.history.length
      const empty = c.makeNode('c')
      c.blocked.delete('c')
      await empty.gate.open()
      await until(() => c.warnings.some(({ error }) => error.code === Err.REPL_FORGOT) ||
        c.errors.length > 0, 'retention rejection')
      t.ok(c.history.slice(before).some(({ from, message }) => from === 'c' &&
        message.code === Err.REPL_BACKTRACK && message.logSeq === -1n),
      'empty follower supplies the genesis backtracking hint')
      t.ok(c.warnings.some(({ error }) => error.code === Err.REPL_FORGOT),
        'missing retained history is reported as REPL_FORGOT')
      t.equal(c.errors.length, 0, 'healthy history is not reported as corrupt')
      t.ok(['a', 'b'].every((id) => c.nodes.get(id).gate.isOpen), 'both healthy members stay open')
      if (c.errors.length === 0) {
        const current = await until(() => c.leader(['c']), 'healthy leader')
        t.equal((await current.append(Buffer.from('continued')))[1], 'continued',
          'the healthy quorum continues accepting commands')
        await until(() => ['a', 'b'].every((id) =>
          c.nodes.get(id).apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 9n),
        'continued command on healthy members')
        t.equal(c.errors.length, 0, 'continued replication remains healthy')
      }
    } finally { await c.close() }
    t.end()
  })
}

const cases = [
  { name: 'byte limit with synchronous transport', delay: 0, rpcMax: 1024, rpcMaxBytes: 2048, size: 1024 },
  { name: 'byte limit with delayed transport', delay: 2, rpcMax: 1024, rpcMaxBytes: 2048, size: 1024 },
  { name: 'record limit', delay: 0, rpcMax: 3, rpcMaxBytes: 16_384, size: 32 },
  { name: 'one oversized record', delay: 0, rpcMax: 1024, rpcMaxBytes: 512, size: 1024 },
]

for (const { name, delay, rpcMax, rpcMaxBytes, size } of cases) {
  test(`v2 replication bounds SQLite payload reads by ${name}`, async (t) => {
    const c = cluster({ delay, opts: { rpcMax, rpcMaxBytes, electionTimeout: 300, appendTimeout: 1_000 } })
    try {
      await c.open()
      const leader = c.leader()
      const laggerId = c.ids.find((id) => id !== leader.id)
      c.blocked.add(laggerId)
      await c.nodes.get(laggerId).gate.close()
      const values = Array.from({ length: 12 }, (_, i) => Buffer.alloc(size, 65 + i))
      for (const value of values) { await leader.append(value) }
      await c.settled()

      const reads = []
      for (const members of c.nodes.values()) {
        if (members.gate.id === laggerId) { continue }
        for (const role of roles) {
          const statement = members[role].log._statements.range
          const all = statement.all.bind(statement)
          statement.all = (...args) => {
            const rows = all(...args)
            reads.push({ rows: rows.length, bytes: rows.reduce((sum, row) => sum + row.entry.byteLength, 0) })
            return rows
          }
        }
      }
      const before = c.history.length
      const returned = c.makeNode(laggerId)
      c.blocked.delete(laggerId)
      await returned.gate.open()
      await until(() => returned.apply.db.prepare('SELECT count(*) AS n FROM items').get().n === 12n,
        'lagging follower catches up')
      const pages = c.history.slice(before).filter(({ to, message }) =>
        to === laggerId && message.type === 'append' && message.data).map(({ message }) => ({
        rows: message.data.length,
        bytes: message.data.reduce((sum, record) => sum + record.entry.length, 0),
      }))
      t.ok(pages.length > 1, 'catch-up spans multiple pages')
      t.ok(pages.every((page) => page.rows <= rpcMax), 'outgoing pages obey the record limit')
      t.ok(pages.every((page) => page.bytes <= rpcMaxBytes || page.rows === 1),
        'outgoing pages obey the byte limit except for one oversized record')
      t.ok(reads.length > 0, 'actual SQLite payload reads were observed')
      t.ok(reads.every((read) => read.rows <= rpcMax), 'payload reads obey the record limit')
      t.ok(reads.every((read) => read.bytes <= rpcMaxBytes || read.rows === 1),
        'payload reads obey the byte limit except for one oversized record')
      if (rpcMax === 3) {
        t.ok(pages.some((page) => page.rows === rpcMax), 'fitting records fill the count-limited page')
      }
      if (size > rpcMaxBytes) {
        t.ok(pages.some((page) => page.bytes > rpcMaxBytes && page.rows === 1),
          'an oversized record can still make progress')
      }
      const rows = returned.apply.db.prepare('SELECT value FROM items ORDER BY id').all()
      t.deepEqual(rows.map((row) => Buffer.from(row.value)), values, 'every payload arrives unchanged and in order')
      t.equal(c.errors.length, 0)
    } finally { await c.close() }
    t.end()
  })
}
