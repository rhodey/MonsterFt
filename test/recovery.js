import test from 'tape'
import { cluster, until, pause, roles } from './helper.js'
import { decodeFrame } from '../src/wire.js'
import { readMeta, readApply } from '../src/utils.js'
import { readState, stateDigest } from '../src/storage.js'

for (const imports of [0, 1, 2, 3]) {
  test(`recovery after DB2 commit and ${imports} committed DB1 imports`, async (t) => {
    let target = null
    let armed = false
    let inputCid = null
    let captured = false
    const c = cluster({
      local({ id, from, to, frame }) {
        if (!armed || id !== target) { return }
        const msg = decodeFrame(frame)
        if (from === 'gate' && msg.type === 'input' && msg.kind === 'apply' && msg.reply.receipt.seq > 0n) {
          inputCid = msg.cid
          captured = true
          if (roles.indexOf(to) >= imports) { return false }
        }
        // Hold all completion evidence so no later operation escapes this step.
        if (to === 'gate' && roles.includes(from) && msg.cid === inputCid) { return false }
      },
    })
    try {
      await c.open()
      target = c.ids.find((id) => id !== c.leader().id)
      armed = true
      const command = c.leader().append(Buffer.from('committed once'))
      command.catch(() => {})
      await until(() => captured, 'interrupted receipt input')
      const members = c.nodes.get(target)
      const receipt = readApply(members.apply.db)
      t.ok(receipt.seq > 0n, 'DB2 receipt committed')
      t.equal(roles.filter((role) => readMeta(members[role].db).applied_seq === receipt.seq).length,
        imports, 'only selected imports reached DB1')
      await Promise.all(c.gates().map((gate) => gate.close()))
      armed = false
      const before = c.calls.filter((call) => call.id === target && call.seq === receipt.seq).length
      c.ids.forEach(c.makeNode)
      await c.open()
      await c.settled()
      const reopened = c.nodes.get(target)
      t.equal(c.calls.filter((call) => call.id === target && call.seq === receipt.seq).length,
        before, 'completed Apply work is not repeated')
      t.equal(reopened.apply.db.prepare('SELECT count(*) AS n FROM items').get().n, 1n)
      const states = roles.map((role) => stateDigest(readState(reopened[role].db)))
      t.ok(states.every((state) => state.equals(states[0])), 'reconciliation restores matching copies')
      t.equal((await c.leader().append(Buffer.from('continued')))[1], 'continued')
    } finally { await c.close() }
    t.end()
  })
}

test('delayed third Proc imports its fixed receipt after Apply has completed the next command', async (t) => {
  let target = null
  let releaseApply = null
  let slowSeq = null
  let held = null
  let holdReply = false
  const c = cluster({
    apply(id, db, buffer, term, seq) {
      if (seq === 0n) {
        db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB) STRICT')
        return null
      }
      const run = () => {
        db.prepare('INSERT INTO items (value) VALUES (?)').run(buffer)
        return buffer.toString()
      }
      if (id === target && buffer.toString() === 'one') {
        slowSeq = seq
        return new Promise((resolve) => { releaseApply = () => resolve(run()) })
      }
      return run()
    },
    local({ id, from, to, frame, deliver }) {
      if (!holdReply || id !== target || from !== 'gate' || to !== 'procC') { return }
      const msg = decodeFrame(frame)
      if (msg.type === 'input' && msg.kind === 'apply' && msg.reply.receipt.seq === slowSeq) {
        held = deliver
        return false
      }
    },
  })
  try {
    await c.open()
    target = c.ids.find((id) => id !== c.leader().id)
    const leader = c.leader()
    t.equal((await leader.append(Buffer.from('one')))[1], 'one')
    await until(() => releaseApply !== null, 'slow follower Apply')
    const second = await leader.append(Buffer.from('two'))
    const members = c.nodes.get(target)
    await until(() => members.procA.engine.commitSeq >= second[0], 'second CMD committed behind slow Apply')
    holdReply = true
    releaseApply()
    await until(() => held !== null && readApply(members.apply.db).seq === second[0], 'second Apply before third import')
    t.ok(readMeta(members.procC.db).applied_seq < slowSeq, 'third Proc has not imported receipt one')
    t.equal(readApply(members.apply.db).seq, second[0], 'DB2 already holds receipt two')
    const tick = members.procA.tickCount
    await pause(10)
    t.equal(members.procA.tickCount, tick, 'Gate withholds later inputs until third completion')
    holdReply = false
    held()
    await c.settled()
    t.equal(members.apply.db.prepare('SELECT count(*) AS n FROM items').get().n, 2n)
    t.equal(c.calls.filter((call) => call.id === target && call.seq > 0n).length, 2,
      'each command executed once')
    t.equal(c.errors.length, 0)
    const states = roles.map((role) => stateDigest(readState(members[role].db)))
    t.ok(states.every((state) => state.equals(states[0])), 'all three copies agree after queued receipt two')
  } finally {
    releaseApply?.()
    held?.()
    await c.close()
  }
  t.end()
})
