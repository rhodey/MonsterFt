import test from 'tape'
import { cluster, roles, until } from './helper.js'
import { decodeFrame, framePayload, checksum } from '../src/wire.js'
import { readApply } from '../src/utils.js'
import * as Err from '../src/error.js'

const malformedPayload = Buffer.from([0xdb, 0, 0, 0, 10, 65])

for (const localDelay of [0, 2]) {
  test(`malformed remote MessagePack warns and completes with ${localDelay ? 'delayed' : 'synchronous'} local transport`, async (t) => {
    let target = null
    const received = []
    const completed = []
    const malformed = framePayload(malformedPayload, checksum(malformedPayload))
    const c = cluster({
      localDelay,
      delay: localDelay,
      local({ id, from, to, frame }) {
        if (id !== target) { return }
        const message = decodeFrame(frame)
        if (from === 'gate' && message.type === 'cluster_message' &&
            message.frame.equals(malformed)) {
          received.push({ to, message })
        } else if (message.type === 'ack' && received.some((input) =>
          input.to === from && input.message.cid === message.cid)) {
          completed.push({ from, message })
        }
      },
    })
    try {
      await c.open()
      await c.settled()
      const gate = c.leader()
      target = gate.id
      const node = c.nodes.get(target)
      const from = c.ids.find((id) => id !== target)
      const warnings = []
      for (const role of roles) {
        node[role].on('warn', (error) => {
          if (error.code === Err.FRAME_CORRUPT) {
            warnings.push({ role, error, transaction: node[role].db.isTransaction })
          }
        })
      }
      const heartbeatStart = c.history.length
      gate.onReceive(from, malformed)
      await until(() => completed.length === 3 && warnings.length === 3, 'malformed input completion')
      t.deepEqual(received.map(({ to }) => to), roles, 'all three Procs receive the remote input')
      t.ok(received.every(({ message }) => message.frame.equals(malformed)),
        'fanout preserves the exact remote bytes')
      t.deepEqual(completed.map(({ from }) => from), roles, 'all three Procs acknowledge the input')
      t.ok(completed.every(({ from, message }) => message.tickCount ===
        received.find(({ to }) => to === from).message.tickCount), 'completion advances the ordered input')
      t.deepEqual(warnings.map(({ role, error }) => [role, error.code]),
        roles.map((role) => [role, Err.FRAME_CORRUPT]), 'each Proc warns once locally')
      t.ok(warnings.every(({ error }) => error.message.startsWith(`cluster frame from ${from}: message decode `)),
        'warnings identify the remote sender and decoding failure')
      t.ok(warnings.every(({ transaction }) => !transaction), 'warnings follow committed input processing')
      t.notOk(c.warnings.some(({ role }) => role === 'gate'), 'Gate does not mirror Proc diagnostics')
      t.ok(gate.isOpen && roles.every((role) => node[role].isOpen), 'Gate and Procs remain open')
      await until(() => c.history.slice(heartbeatStart).some(({ from, message }) =>
        from === target && message.type === 'append' && message.data === undefined), 'later heartbeat')
      t.equal((await gate.append(Buffer.from('after malformed MessagePack')))[1], 'after malformed MessagePack')
      await c.settled()
      t.deepEqual(c.errors, [], 'decoding failure causes no error, timeout, or output disagreement')
      t.equal(c.calls.filter(({ seq }) => seq > 0n).length, 3, 'every node applies the later command once')
    } finally { await c.close() }
    t.end()
  })
}

for (const role of ['gate', 'procA', 'apply']) {
  test(`malformed local MessagePack remains fatal at ${role}`, async (t) => {
    const c = cluster()
    try {
      const node = c.nodes.get('a')
      await node.gate.open()
      const malformed = framePayload(malformedPayload, checksum(malformedPayload))
      await node[role].onReceive(role === 'gate' ? 'procA' : 'gate', malformed)
      const diagnostic = c.errors.find((item) => item.role === role)
      t.equal(diagnostic?.error.code, Err.FRAME_CORRUPT, 'receiver reports the local decoding failure')
      t.notOk(node[role].isOpen, 'receiver stops')
      if (role !== 'gate') {
        await until(() => node[role].db === null, 'failed owner cleanup')
        t.equal(node[role].db, null, 'receiver closes its own database')
      }
      t.equal(c.warnings.length, 0, 'local decoding failure is not downgraded to a warning')
      t.equal(c.calls.length, 0, 'invalid local frame cannot invoke Apply')
    } finally { await c.close() }
    t.end()
  })
}

for (const localDelay of [0, 2]) {
  test(`a bad remote checksum warns and drops before ${localDelay ? 'delayed' : 'synchronous'} local fanout`, async (t) => {
    const received = []
    const corrupt = framePayload(malformedPayload, checksum(malformedPayload))
    corrupt[0] ^= 1
    const c = cluster({
      localDelay,
      delay: localDelay,
      local({ from, frame }) {
        if (from !== 'gate') { return }
        const message = decodeFrame(frame)
        if (message.type === 'cluster_message' && message.frame.equals(corrupt)) {
          received.push(message)
        }
      },
    })
    try {
      await c.open()
      await c.settled()
      const gate = c.leader()
      const node = c.nodes.get(gate.id)
      const from = c.ids.find((id) => id !== gate.id)
      const warnings = []
      gate.on('warn', (error) => warnings.push(error))
      gate.onReceive(from, corrupt)
      t.equal(warnings.length, 1, 'Gate warns once immediately')
      t.equal(warnings[0].code, Err.FRAME_CORRUPT)
      t.equal(warnings[0].message, `cluster frame from ${from}: frame checksum differs`)
      t.ok(gate.isOpen, 'the receiving Gate remains open')
      t.ok(roles.every((role) => node[role].isOpen), 'all local Procs remain open')
      t.equal((await gate.append(Buffer.from('after corrupt frame')))[1], 'after corrupt frame')
      await c.settled()
      t.equal(received.length, 0, 'corrupt bytes are never dispatched to the Procs')
      t.deepEqual(c.errors, [], 'corruption does not produce a terminal diagnostic')
      t.equal(c.calls.filter(({ seq }) => seq > 0n).length, 3, 'every node applies the later command')
    } finally { await c.close() }
    t.end()
  })
}

test('failed startup reports locally and leaves remaining owners for explicit cleanup', async (t) => {
  const messages = []
  const c = cluster({
    opts: { localTimeout: 300 },
    local({ id, from, frame }) {
      if (id === 'a') { messages.push({ from, msg: decodeFrame(frame) }) }
    },
  })
  try {
    const node = c.nodes.get('a')
    const original = new Err.ErrorWithCode('cannot open DB1', Err.SQLITE_ERROR, 14)
    node.procB.store.open = () => { throw original }
    let closed = false
    node.gate.on('close', () => { closed = true })
    const rejection = await node.gate.open().then(() => null, (err) => err)
    t.equal(rejection.code, Err.LOCAL_TIMEOUT, 'startup caller gets the missing ACK timeout')
    t.deepEqual(c.errors.map(({ role, error }) => [role, error.code]),
      [['procB', Err.SQLITE_ERROR], ['gate', Err.LOCAL_TIMEOUT]])
    t.equal(c.errors[0].error, original, 'the failing Proc preserves its original diagnostic')
    t.notOk(messages.some(({ from, msg }) => from === 'procB' && msg.type === 'err'),
      'unexpected startup failure does not send ERR')
    t.notOk(messages.some(({ from, msg }) => from === 'gate' && msg.type === 'close'),
      'failed open does not request owner cleanup')
    t.notOk(closed)
    t.ok(node.procA.db, 'the earlier-opened owner remains open after startup fails')

    await node.gate.close()
    t.ok(closed, 'explicit close stops all owners')
    t.equal(node.procA.db, null)
    c.makeNode('a')
    await c.open()
    t.equal((await c.leader().append(Buffer.from('after startup failure')))[1], 'after startup failure')
    await c.settled()
  } finally { await c.close() }
  t.end()
})

for (const committed of [false, true]) {
  test(`Apply failure ${committed ? 'after' : 'before'} commit stays local and recovers after explicit shutdown`, async (t) => {
    let target = null
    let armed = false
    let injected = false
    const messages = []
    const failure = new Error('injected execution commit failure')
    const c = cluster({
      opts: { applyTimeout: 80, localTimeout: 1_000 },
      local({ id, from, to, frame, members }) {
        if (!armed || id !== target) { return }
        const envelope = decodeFrame(frame)
        const msg = envelope.to === 'apply' ? envelope.message : envelope
        messages.push({ from, to, msg })
        if (!injected && from === 'gate' && to === 'apply' && msg.type === 'execute' && msg.seq > 0n) {
          injected = true
          const db = members.apply.db
          const exec = db.exec.bind(db)
          db.exec = (sql) => {
            if (sql === 'COMMIT') {
              if (committed) { exec(sql) }
              throw failure
            }
            return exec(sql)
          }
        }
      },
    })
    try {
      await c.open()
      await c.settled()
      target = c.leader().id
      armed = true
      const node = c.nodes.get(target)
      let closed = false
      node.gate.on('close', () => { closed = true })
      const rejection = await node.gate.append(Buffer.from('survives recovery')).then(
        () => null, (err) => err,
      )
      t.equal(rejection.code, Err.APPLY_TIMEOUT, 'Gate detects missing execution completion')
      t.equal(node.apply.db, null, 'Apply closes its own failed execution')
      const diagnostics = c.errors.filter(({ id }) => id === target)
      t.deepEqual(diagnostics.map(({ role, error }) => [role, error.code]),
        [['apply', Err.APPLY_ERROR], ['gate', Err.APPLY_TIMEOUT]],
        'each owner reports its own diagnostic without mirroring')
      t.equal(diagnostics[0].error.stack, failure.stack, 'Apply preserves the original trace')
      t.notOk(messages.some(({ from, msg }) => from === 'apply' && msg.type === 'err'),
        'fatal execution never attempts to transmit ERR')
      t.notOk(messages.some(({ from, msg }) => from === 'gate' && msg.type === 'close'),
        'Gate fatal shutdown does not coordinate component close')
      t.notOk(closed, 'fatal Gate shutdown does not claim all owners have closed')
      t.ok(roles.every((role) => node[role].db !== null), 'Proc owners remain until explicitly stopped')
      t.equal(c.warnings.length, 0, 'infrastructure failures do not become item warnings')

      await Promise.all(c.gates().map((gate) => gate.close()))
      t.ok(closed, 'explicit close completes coordinated shutdown after failure')
      t.ok(roles.every((role) => node[role].db === null))
      armed = false
      c.ids.forEach(c.makeNode)
      await c.open()
      await c.settled()
      const reopened = c.nodes.get(target)
      t.equal(c.calls.filter((call) => call.id === target && call.seq > 0n).length,
        committed ? 1 : 2, 'only uncommitted Apply work executes again')
      t.deepEqual(reopened.apply.db.prepare('SELECT value FROM items').all()
        .map(({ value }) => Buffer.from(value).toString()), ['survives recovery'],
      'recovery preserves exactly one committed effect')
      t.ok(readApply(reopened.apply.db).seq > 0n)
      t.equal((await c.leader().append(Buffer.from('continued')))[1], 'continued')
      await c.settled()
    } finally { await c.close() }
    t.end()
  })
}
