import test from 'tape'
import { cluster, until, pause, roles } from './v2-helper.js'
import * as Err from '../src/error.v2.js'

for (const delay of [0, 2]) {
  test(`v2 heartbeats continue while a data ACK is missing (${delay} ms transport)`, async (t) => {
    let armed = false
    let leaderId
    let followerId
    let lostCid = null
    let dropped = 0
    const c = cluster({ delay, opts: { electionTimeout: 200, appendTimeout: 1_200 },
      transport({ from, to, message }) {
        if (armed && lostCid === null && from === leaderId && to === followerId &&
            message.type === 'append' && message.data?.some((record) => record.entry.length > 0)) {
          lostCid = message.cid
        }
        if (dropped === 0 && from === followerId && to === leaderId &&
            message.type === 'ack' && message.cid === lostCid) {
          dropped++
          return false
        }
      },
    })
    try {
      await c.open()
      const leader = c.leader()
      leaderId = leader.id
      followerId = c.ids.find((id) => id !== leaderId)
      const term = leader.term
      const before = c.history.length
      armed = true
      t.equal((await leader.append(Buffer.from('first')))[1], 'first',
        'the other follower lets the command succeed despite one lost ACK')
      await until(() => dropped === 1, 'dropped data ACK')
      await pause(500)
      const traffic = c.history.slice(before)
      const heartbeats = traffic.filter(({ from, to, message }) => from === leaderId &&
        to === followerId && message.type === 'append' && message.data === undefined)
      t.equal(dropped, 1, 'exactly one data ACK was dropped')
      t.ok(heartbeats.length >= 2, 'heartbeats reach the follower before the data request times out')
      t.notOk(traffic.some(({ from, message }) => from === followerId && message.type === 'vote_request'),
        'the responsive follower does not start an election')
      t.ok(c.gates().every((gate) => gate.term === term), 'all members retain their election term')
      t.equal(leader.state, 'leader', 'the original leader remains in place')
      await until(() => c.nodes.get(followerId).apply.db
        .prepare('SELECT count(*) AS n FROM items').get().n === 1n, 'command reaches the affected follower')
      if (leader.state === 'leader' && leader.term === term) {
        t.equal((await leader.append(Buffer.from('second')))[1], 'second', 'continued commands succeed')
        await until(() => [...c.nodes.values()].every((members) => members.apply.db
          .prepare('SELECT count(*) AS n FROM items').get().n === 2n), 'both commands on every node')
        for (const members of c.nodes.values()) {
          const values = members.apply.db.prepare('SELECT value FROM items ORDER BY id').all()
          t.deepEqual(values.map(({ value }) => Buffer.from(value).toString()), ['first', 'second'],
            `${members.gate.id} applies each command once and in order`)
        }
        t.ok(c.warnings.some(({ id, error }) => id === leaderId &&
          error.code === Err.APPEND_TIMEOUT && error.message === 'append timeout'),
        'the lost data ACK still produces an append timeout')
      } else { t.fail('continued commands require no leadership change') }
      t.equal(c.errors.length, 0, 'the dropped ACK causes no fatal errors')
    } finally { await c.close() }
    t.end()
  })

  test(`v2 a lost heartbeat ACK does not serialize later heartbeats (${delay} ms transport)`, async (t) => {
    let armed = false
    let leaderId
    let followerId
    let lostCid = null
    let dropped = 0
    const c = cluster({ delay, opts: { electionTimeout: 200, appendTimeout: 1_200 },
      transport({ from, to, message }) {
        if (armed && lostCid === null && from === leaderId && to === followerId &&
            message.type === 'append' && message.data === undefined) {
          lostCid = message.cid
        }
        if (dropped === 0 && from === followerId && to === leaderId &&
            message.type === 'ack' && message.cid === lostCid) {
          dropped++
          return false
        }
      },
    })
    try {
      await c.open()
      const leader = c.leader()
      leaderId = leader.id
      followerId = c.ids.find((id) => id !== leaderId)
      const term = leader.term
      const before = c.history.length
      armed = true
      await until(() => dropped === 1, 'dropped heartbeat ACK')
      await pause(500)
      const traffic = c.history.slice(before)
      const heartbeats = traffic.filter(({ from, to, message }) => from === leaderId &&
        to === followerId && message.type === 'append' && message.data === undefined)
      t.equal(dropped, 1, 'exactly one heartbeat ACK was dropped')
      t.ok(heartbeats.some(({ message }) => message.cid !== lostCid),
        'fresh heartbeat requests are sent before the missing response times out')
      t.notOk(traffic.some(({ from, message }) => from === followerId && message.type === 'vote_request'),
        'the follower never needs a replacement leader')
      t.ok(c.gates().every((gate) => gate.term === term), 'the cluster keeps its term')
      t.equal(leader.state, 'leader', 'the leader stays ready')
      const timeouts = c.warnings.filter(({ id, error }) =>
        id === leaderId && error.code === Err.HEARTBEAT_TIMEOUT)
      t.deepEqual(timeouts.map(({ role }) => role).sort(), roles,
        'each Proc reports the lost heartbeat ACK locally')
      t.equal(timeouts[0]?.error.message, 'heartbeat timeout', 'the warning identifies the heartbeat')
      t.notOk(c.warnings.some(({ error }) => error.code === Err.APPEND_TIMEOUT),
        'heartbeat loss is not reported as an append timeout')
      if (leader.state === 'leader' && leader.term === term) {
        t.equal((await leader.append(Buffer.from('after-heartbeat-loss')))[1], 'after-heartbeat-loss',
          'commands remain available')
      } else { t.fail('commands require no replacement leader') }
      t.equal(c.errors.length, 0, 'the missing heartbeat response is not fatal')
    } finally { await c.close() }
    t.end()
  })

  test(`v2 retention cancellation leaves heartbeats active (${delay} ms transport)`, async (t) => {
    const c = cluster({ delay, opts: {
      keepTarget: 2, keepTrigger: 5, electionTimeout: 200, appendTimeout: 1_200,
    } })
    try {
      c.blocked.add('c')
      await Promise.all(c.gates().map((gate) => gate.open()))
      await c.nodes.get('c').gate.close()
      const initial = await until(() => c.leader(['c']), 'leader without the empty follower')
      await until(() => ['a', 'b'].every((id) => c.nodes.get(id).procA.engine.appliedSeq >= 0n),
        'setup on the healthy pair')
      for (let i = 0; i < 8; i++) { await initial.append(Buffer.from(`retained-${i}`)) }
      await c.settled()
      t.ok(['a', 'b'].every((id) => c.nodes.get(id).procA.log.begin > 0n),
        'both healthy members have pruned the empty follower\'s missing history')

      // Begin replication from the retained head when the empty follower returns.
      await Promise.all(['a', 'b'].map((id) => c.nodes.get(id).gate.close()))
      for (const id of ['a', 'b']) { c.makeNode(id) }
      await Promise.all(['a', 'b'].map((id) => c.nodes.get(id).gate.open()))
      const leader = await until(() => c.leader(['c']), 'retained leader')
      const term = leader.term
      const empty = c.makeNode('c')
      c.blocked.delete('c')
      await empty.gate.open()
      await until(() => c.warnings.some(({ id, error }) => id === leader.id && error.code === Err.REPL_FORGOT),
        'retention cancels data replication')
      const before = c.history.length
      await pause(500)
      const traffic = c.history.slice(before)
      const appends = traffic.filter(({ from, to, message }) => from === leader.id &&
        to === 'c' && message.type === 'append')
      t.ok(appends.length >= 2, 'heartbeats continue after retention cancels replication')
      t.ok(appends.every(({ message }) => message.data === undefined),
        'canceled data replication is not restarted by heartbeats')
      t.notOk(traffic.some(({ from, message }) => from === 'c' && message.type === 'vote_request'),
        'the connected empty follower does not campaign')
      t.ok(c.gates().every((gate) => gate.term === term), 'the healthy pair keeps a stable term')
      t.equal(leader.state, 'leader', 'the retained leader remains available')
      t.equal(empty.procA.log.seq, -1n, 'heartbeats do not invent the missing log')
      t.equal(empty.procA.engine.appliedSeq, -1n, 'the empty follower does not apply unavailable history')
      if (leader.state === 'leader' && leader.term === term) {
        t.equal((await leader.append(Buffer.from('continued')))[1], 'continued',
          'the healthy pair continues accepting commands')
        await until(() => ['a', 'b'].every((id) => c.nodes.get(id).apply.db
          .prepare('SELECT count(*) AS n FROM items').get().n === 9n), 'continued command on the healthy pair')
      } else { t.fail('continued commands require no leadership change') }
      t.equal(empty.procA.log.seq, -1n, 'the additional command does not revive canceled data replication')
      t.equal(c.errors.length, 0, 'manual-repair requirement does not close healthy nodes')
    } finally { await c.close() }
    t.end()
  })
}
