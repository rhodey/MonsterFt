import test from 'tape'
import { cluster, roles, until } from './helper.js'
import { decodeFrame, framePayload } from '../src/wire.js'
import { readApply } from '../src/utils.js'
import * as Err from '../src/error.js'

for (const localDelay of [0, 2]) {
  test(`malformed remote MessagePack is diagnosed by each Proc with ${localDelay ? 'delayed' : 'synchronous'} local transport`, async (t) => {
    const received = []
    const c = cluster({
      opts: { tickInterval: 1_000, localTimeout: 100 },
      localDelay,
      local({ id, from, to, frame }) {
        if (id !== 'a' || from !== 'gate') { return }
        const message = decodeFrame(frame)
        if (message.type === 'input' && message.kind === 'rpc') {
          received.push({ to, frame: message.frame })
        }
      },
    })
    try {
      const node = c.nodes.get('a')
      await node.gate.open()
      const malformed = framePayload(Buffer.from([0xdb, 0, 0, 0, 10, 65]))
      node.gate.onReceive('b', malformed)
      t.equal(c.errors.length, 0, 'Gate accepts the checksum without parsing the payload')
      await until(() => c.errors.some(({ role }) => role === 'gate'), 'Gate completion timeout')
      t.deepEqual(received.map(({ to }) => to), roles, 'all three Procs receive the remote input')
      t.ok(received.every(({ frame }) => frame.equals(malformed)), 'fanout preserves the exact remote bytes')
      t.deepEqual(c.errors.map(({ role, error }) => [role, error.code]), [
        ...roles.map((role) => [role, Err.FRAME_CORRUPT]),
        ['gate', Err.LOCAL_TIMEOUT],
      ], 'each Proc reports its own decode failure before Gate reports missing completion')
      t.ok(roles.every((role) => node[role].db === null), 'all failing Procs close their own stores')
      t.notOk(node.gate.isOpen)
      t.notEqual(node.apply.db, null, 'Gate does not implicitly close the remaining owner')
      t.equal(c.calls.length, 0, 'malformed input never invokes Apply')
    } finally { await c.close() }
    t.end()
  })
}

test('a bad remote checksum stops Gate before local fanout', async (t) => {
  const received = []
  const c = cluster({
    opts: { tickInterval: 1_000, localTimeout: 100 },
    local({ id, from, frame }) {
      if (id !== 'a' || from !== 'gate') { return }
      const message = decodeFrame(frame)
      if (message.type === 'input' && message.kind === 'rpc') { received.push(message) }
    },
  })
  try {
    const node = c.nodes.get('a')
    await node.gate.open()
    const corrupt = framePayload(Buffer.from([0xdb, 0, 0, 0, 10, 65]))
    corrupt[0] ^= 1
    node.gate.onReceive('b', corrupt)
    t.deepEqual(c.errors.map(({ role, error }) => [role, error.code]),
      [['gate', Err.FRAME_CORRUPT]], 'Gate reports checksum corruption immediately')
    t.equal(c.errors[0].error.message, 'frame checksum differs')
    t.notOk(node.gate.isOpen)
    t.equal(received.length, 0, 'corrupt bytes are never dispatched to the Procs')
    t.ok(roles.every((role) => node[role].isOpen), 'Proc owners remain open until explicit cleanup')
    t.equal(c.calls.length, 0)
  } finally { await c.close() }
  t.end()
})

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
