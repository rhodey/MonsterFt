import test from 'tape'
import { cluster, roles } from './helper.js'
import { encodeFrame } from '../src/wire.js'
import * as Err from '../src/error.js'

const owners = [...roles, 'apply']

for (const localDelay of [0, 2]) {
  test(`wrong local senders warn without processing with ${localDelay ? 'delayed' : 'synchronous'} transport`, async (t) => {
    let sends = 0
    const c = cluster({ localDelay, delay: localDelay, local() { sends++ } })
    const node = c.nodes.get('a')
    const warningMessage = (role) => `${role === 'apply' ? 'Apply' : 'Proc'} sender a is illegal, expected gate`
    try {
      for (const role of owners) {
        await node[role].onReceive('a', Buffer.alloc(0))
      }
      t.equal(sends, 0, 'wrong sender receives no reply before hello')
      t.equal(c.calls.length, 0, 'invalid local traffic invokes no apply callback')
      t.deepEqual(c.warnings.map(({ role, error }) => [role, error.code, error.message]),
        owners.map((role) => [role, Err.RPC_ILLEGAL, warningMessage(role)]),
        'all owners warn before hello with expected and received sender')
      t.deepEqual(c.errors, [], 'wrong sender bytes are not decoded or treated as fatal')

      await c.open()
      await c.settled()
      const before = c.warnings.length
      const wrongClose = encodeFrame({ type: 'close', cid: 'wrong-sender-close' })
      for (const role of owners) {
        await node[role].onReceive('a', wrongClose)
        t.ok(node[role].isOpen, `${role} does not process the wrong sender's close request`)
      }
      t.deepEqual(c.warnings.slice(before).map(({ role, error }) => [role, error.code, error.message]),
        owners.map((role) => [role, Err.RPC_ILLEGAL, warningMessage(role)]),
        'each active owner reports one local warning')
      t.equal((await c.leader().append(Buffer.from('after wrong sender')))[1], 'after wrong sender',
        'normal command processing continues after misrouted traffic')
      await c.settled()
      t.deepEqual(c.errors, [], 'owners remain healthy')
      t.equal(c.calls.filter(({ seq }) => seq > 0n).length, 3, 'the command executes once per node')

      await c.close()
      const warnings = c.warnings.length
      const sent = sends
      for (const role of owners) { await node[role].onReceive('a', Buffer.alloc(0)) }
      t.equal(c.warnings.length, warnings, 'stopped owners ignore wrong senders quietly')
      t.equal(sends, sent, 'stopped owners send no reply')
    } finally { await c.close() }
    t.end()
  })
}

for (const role of owners) {
  for (const throws of [false, true]) {
    test(`${role} wrong-sender warning listener may ${throws ? 'throw' : 'close'}`, async (t) => {
      let ownerSends = 0
      const c = cluster({ local({ from }) { if (from === role) { ownerSends++ } } })
      const node = c.nodes.get('a')
      const owner = node[role]
      const original = new Err.ErrorWithCode('wrong sender listener failed', Err.SQLITE_ERROR, 10)
      const events = []
      try {
        await node.gate.open()
        let laterWarnings = 0
        owner.on('warn', () => {
          if (throws) { throw original }
          owner.close()
        })
        owner.on('warn', () => { laterWarnings++ })
        owner.on('error', (err) => {
          t.notOk(owner.isOpen, 'owner stops before reporting the listener failure')
          t.equal(err, original, 'original listener diagnostic is preserved')
          events.push('error')
        })
        owner.on('close', () => { events.push('close') })
        const before = ownerSends
        await owner.onReceive('a', encodeFrame({ type: 'close', cid: 'misrouted' }))
        await owner.close()
        t.equal(ownerSends, before, 'warning handling sends no reply')
        t.notOk(owner.isOpen, 'listener stops the owner')
        t.equal(laterWarnings, 1, 'later warning listeners still run')
        t.deepEqual(events, throws ? ['error', 'close'] : ['close'], 'diagnostic and cleanup ordering remains intact')
        t.equal(c.calls.length, 0, 'no apply callback runs')
        const warnings = c.warnings.length
        await owner.onReceive('a', Buffer.alloc(0))
        t.equal(c.warnings.length, warnings, 'later wrong-sender traffic is quiet')
      } finally { await c.close() }
      t.end()
    })
  }
}

test('invalid local frames from Gate remain fatal for every owner', async (t) => {
  const c = cluster()
  try {
    const node = c.nodes.get('a')
    for (const role of owners) {
      const owner = node[role]
      await owner.onReceive('gate', Buffer.alloc(0))
      await owner.close()
      const errors = c.errors.filter((item) => item.role === role)
      t.equal(errors.length, 1, `${role} reports one fatal failure`)
      t.equal(errors[0].error.code, Err.FRAME_CORRUPT, `${role} retains the frame-corruption code`)
      t.notOk(owner.isOpen, `${role} is stopped`)
    }
    t.deepEqual(c.warnings, [], 'correctly routed malformed frames are never warnings')
  } finally { await c.close() }
  t.end()
})
