import test from 'tape'
import { pack, unpack } from 'msgpackr'
import { certify } from '../src/certify.js'
import { readApply, readMeta } from '../src/utils.js'
import { readDatabase, readState, stateDigest } from '../src/storage.js'
import { decodeFrame } from '../src/wire.js'
import { cluster, until, roles } from './helper.js'
import { protocol, append, record, cmd, messages } from './fixtures/protocol.js'

const digest = Buffer.alloc(16, 7)
const page = (reply) => messages(reply, 'append', 'b').find((message) => message.data)
const ack = (f, message) => f.rpc({
  type: 'ack', cid: message.cid, term: message.term, seq: message.seqP + 1n,
})
const sync = (cmdSeq) => pack({
  type: 'sync', cmdSeq, quorum: true, digest, agree: ['a', 'b'], disagree: [],
})

function pending(f) {
  const reply = f.rpc(append({
    seqP: 0n, termP: 1n, commitSeq: 1n, data: [record(1n, cmd())],
  }))
  f.complete(messages(reply, 'execute', 'apply')[0], [[0, 'ok']], digest)
}

test('a failed second paired insert rolls back SYNC and releases no outputs', (t) => {
  const f = protocol().leader()
  try {
    const first = f.input('append', { clientId: 'first', items: [Buffer.from('one')] })
    f.complete(messages(ack(f, page(first)), 'execute', 'apply')[0], [[0, 'one']], digest)
    f.input('append', { clientId: 'second', items: [Buffer.from('two')] })
    const before = f.state()
    const rows = f.rows()
    for (const proc of f.procs) {
      proc.db.exec(`CREATE TRIGGER reject_second BEFORE INSERT ON raft_log WHEN NEW.seq = 3
        BEGIN SELECT RAISE(ABORT, 'paired CMD insert failed'); END`)
    }
    const sent = []
    f.onSend = (proc, reply) => sent.push({ role: proc.role, reply })
    t.equal(f.rpc({ type: 'monster_outcome', cmdSeq: 1n, digest }), undefined,
      'failed input sends no completion')
    t.equal(sent.length, 0, 'neither the prior caller result nor replication is published')
    t.deepEqual(f.rows(), rows, 'the first insert is rolled back with the failed second insert')
    t.deepEqual(f.state(), before, 'checkpoint and pending command remain unchanged')
    t.equal(readApply(f.db2).seq, 1n, 'the completed first Apply is retained')
    for (const proc of f.procs) {
      t.notOk(proc.isOpen, `${proc.role} stops`)
      t.match(proc._fatalError.message, /paired CMD insert failed/, 'original SQL failure is reported')
    }
  } finally { f.close() }
  t.end()
})

for (const imports of [0, 1, 2, 3]) {
  test(`paired CMD recovery after DB2 commit and ${imports} DB1 imports`, async (t) => {
    let target = null
    let holdReports = false
    let secondSeq = null
    let inputCid = null
    let captured = false
    const reports = []
    const c = cluster({
      transport({ message, deliver }) {
        if (holdReports && message.type === 'monster_outcome') {
          reports.push(deliver)
          return false
        }
      },
      local({ id, from, to, frame }) {
        if (id !== target || secondSeq === null) { return }
        const message = decodeFrame(frame)
        if (from === 'gate' && message.type === 'apply_result' && message.reply.receipt.seq === secondSeq) {
          inputCid = message.cid
          captured = true
          if (roles.indexOf(to) >= imports) { return false }
        }
        if (to === 'gate' && roles.includes(from) && message.cid === inputCid) { return false }
      },
    })
    try {
      await c.open()
      const leader = c.leader()
      target = c.ids.find((id) => id !== leader.id)
      holdReports = true
      const first = leader.append(Buffer.from('one'))
      first.catch(() => {})
      const members = c.nodes.get(leader.id)
      await until(() => members.procA.engine.pendingCommand !== null, 'first command awaits agreement')
      const firstSeq = members.procA.engine.pendingCommand.cmdSeq
      secondSeq = firstSeq + 2n
      const second = leader.append(Buffer.from('two'))
      second.catch(() => {})
      await until(() => roles.every((role) => members[role].engine.commands.length === 1),
        'next command is queued before the decision')
      holdReports = false
      for (const deliver of reports.splice(0)) { deliver() }
      await until(() => captured, 'paired command receipt import is interrupted')
      t.ok(c.history.some(({ from, message }) => from === leader.id &&
        message.data?.length === 2 && message.seqP === firstSeq &&
        message.data.map(({ entry }) => unpack(entry).type).join(',') === 'sync,cmd'),
      'the leader replicated the two ordinary records together')
      const interrupted = c.nodes.get(target)
      t.equal(readApply(interrupted.apply.db).seq, secondSeq, 'paired command committed in DB2')
      for (const [index, role] of roles.entries()) {
        const meta = readMeta(interrupted[role].db)
        t.equal(meta.applied_seq, index < imports ? secondSeq : secondSeq - 1n,
          `${role} retains the CMD receipt or its preceding SYNC checkpoint`)
        t.equal(meta.pending_cmd_seq, index < imports ? secondSeq : null,
          `${role} has only the appropriate unresolved command`)
      }
      t.equal((await first)[1], 'one')
      t.equal((await second)[1], 'two', 'healthy quorum completes the paired command')
      await Promise.all(c.gates().map((gate) => gate.close()))
      secondSeq = null
      c.ids.forEach(c.makeNode)
      await c.open()
      await c.settled()
      const reopened = c.nodes.get(target)
      t.equal(c.calls.filter((call) => call.id === target && call.buffer?.toString() === 'two').length,
        1, 'reconciliation does not execute the committed paired command again')
      t.deepEqual(reopened.apply.db.prepare('SELECT value FROM items ORDER BY id').all()
        .map(({ value }) => Buffer.from(value).toString()), ['one', 'two'])
      const states = roles.map((role) => stateDigest(readState(reopened[role].db)))
      t.ok(states.every((state) => state.equals(states[0])), 'all three durable checkpoints converge')
      t.equal((await c.leader().append(Buffer.from('continued')))[1], 'continued')
    } finally {
      holdReports = false
      secondSeq = null
      await c.close()
    }
    t.end()
  })
}

test('failed paired CMD commit preserves its SYNC checkpoint and retries only unfinished Apply', async (t) => {
  let target = null
  let holdReports = false
  let injected = false
  const reports = []
  const c = cluster({
    transport({ message, deliver }) {
      if (holdReports && message.type === 'monster_outcome') {
        reports.push(deliver)
        return false
      }
    },
    local({ id, from, to, frame, members }) {
      if (injected || id !== target || from !== 'gate' || to !== 'apply') { return }
      const message = decodeFrame(frame).message
      if (message?.type !== 'execute' || message.items?.[0]?.toString() !== 'two') { return }
      injected = true
      const exec = members.apply.db.exec.bind(members.apply.db)
      members.apply.db.exec = (sql) => {
        if (sql === 'COMMIT') { throw new Error('paired DB2 commit failed') }
        return exec(sql)
      }
    },
  })
  try {
    await c.open()
    const leader = c.leader()
    target = c.ids.find((id) => id !== leader.id)
    holdReports = true
    const first = leader.append(Buffer.from('one'))
    first.catch(() => {})
    await until(() => c.nodes.get(leader.id).procA.engine.pendingCommand !== null,
      'first command awaits agreement')
    const second = leader.append(Buffer.from('two'))
    second.catch(() => {})
    await until(() => roles.every((role) => c.nodes.get(leader.id)[role].engine.commands.length === 1),
      'next command is queued before the decision')
    holdReports = false
    for (const deliver of reports.splice(0)) { deliver() }
    const [firstSeq] = await first
    const [secondSeq] = await second
    await until(() => injected && c.nodes.get(target).apply.db === null, 'failed Apply closes DB2')
    for (const role of roles) {
      const meta = readMeta(c.nodes.get(target)[role].db)
      t.equal(meta.applied_seq, secondSeq - 1n, `${role} retains the completed SYNC`)
      t.equal(meta.pending_cmd_seq, null, `${role} has not imported the failed CMD`)
    }
    const before = readDatabase(`${c.directory}/${target}2`, (db) => ({
      receipt: readApply(db), count: db.prepare('SELECT count(*) AS n FROM items').get().n,
    }))
    t.equal(before.receipt.seq, firstSeq, 'failed commit rolls back the receipt')
    t.equal(before.count, 1n, 'failed commit rolls back the application write')
    t.ok(c.errors.some(({ id, role, error }) => id === target && role === 'apply' &&
      error.message.includes('paired DB2 commit failed')), 'Apply reports the injected failure locally')
    await Promise.all(c.gates().map((gate) => gate.close()))
    c.ids.forEach(c.makeNode)
    await c.open()
    await c.settled()
    const db = c.nodes.get(target).apply.db
    t.deepEqual(db.prepare('SELECT value FROM items ORDER BY id').all()
      .map(({ value }) => Buffer.from(value).toString()), ['one', 'two'],
    'only the unfinished command is committed on reopening')
    t.equal(c.calls.filter((call) => call.id === target && call.buffer?.toString() === 'one').length, 1)
    t.equal(c.calls.filter((call) => call.id === target && call.buffer?.toString() === 'two').length, 2,
      'one rolled-back attempt and one successful retry')
  } finally {
    holdReports = false
    await c.close()
  }
  t.end()
})

test('drain between paired records leaves a certifiable SYNC checkpoint and restartable CMD', (t) => {
  const f = protocol().setup()
  try {
    pending(f)
    f.input('drain', { clientId: 'drain' })
    const reply = f.rpc(append({
      seqP: 1n, termP: 1n, commitSeq: 3n,
      data: [record(1n, sync(1n)), record(1n, cmd([Buffer.from('next')]))],
    }))
    t.equal(messages(reply, 'execute', 'apply').length, 0, 'drain does not start the paired command')
    t.deepEqual(messages(reply, 'ack', 'gate').map(({ clientId }) => clientId), ['drain'])
    t.equal(f.state().meta.applied_seq, 2n, 'SYNC is durably applied')
    t.equal(f.state().meta.pending_cmd_seq, null)
    t.equal(readApply(f.db2).seq, 1n, 'DB2 still identifies the already completed command')
    f.stop()
    t.equal(certify(f.base), 2n, 'an unexecuted later CMD is not unresolved Apply work')
    f.reopen()
    t.equal(f.proc.engine.pendingApply, null, 'restart does not replay the old command')
    const committed = f.rpc(append({ seqP: 3n, termP: 1n, commitSeq: 3n }))
    const execute = messages(committed, 'execute', 'apply')[0]
    t.equal(execute.seq, 3n, 'reaffirmed commitment resumes at the paired CMD')
    t.deepEqual(execute.items, [Buffer.from('next')])
    f.complete(execute, [[0, 'next']], digest)
    t.equal(readApply(f.db2).seq, 3n)
  } finally { f.close() }
  t.end()
})

test('sustained paired records retain bounded history and preserve receipt recovery', (t) => {
  const f = protocol({ keepTarget: 3, keepTrigger: 6 }).setup()
  try {
    pending(f)
    for (let index = 0; index < 12; index++) {
      const head = f.proc.log.seq
      const reply = f.rpc(append({
        seqP: head, termP: 1n, commitSeq: head + 2n,
        data: [record(1n, sync(head)), record(1n, cmd())],
      }))
      const execute = messages(reply, 'execute', 'apply')[0]
      t.equal(execute.seq, head + 2n)
      t.equal(f.state().meta.applied_seq, head + 1n, 'each SYNC advances before Apply')
      f.complete(execute, [[0, 'ok']], digest)
      t.ok(f.rows().length <= 6, 'continuous pairing still permits pruning')
    }
    t.ok(f.proc.log.begin > 0n, 'completed history was removed')
    const receipt = readApply(f.db2)
    f.reopen()
    t.equal(f.proc.engine.pendingApply, null, 'restart needs no repeated Apply')
    t.equal(f.proc.engine.pendingCommand.cmdSeq, receipt.seq, 'latest unresolved CMD survives')
    t.equal(readApply(f.db2).seq, receipt.seq)
    const head = f.proc.log.seq
    f.rpc(append({ seqP: head, termP: 1n, commitSeq: head + 1n,
      data: [record(1n, sync(head))] }))
    t.equal(f.proc.engine.pendingCommand, null, 'final standalone SYNC completes the chain')
  } finally { f.close() }
  t.end()
})
