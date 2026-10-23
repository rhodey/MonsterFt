import test from 'tape'
import { fileURLToPath } from 'node:url'
import { Supervisor } from '../src/supervisor.js'
import { Gate } from '../src/gate.js'
import * as Err from '../src/error.js'
import * as wire from '../src/wire.js'
import { until, pause } from './helper.js'

const roles = ['procA', 'procB', 'procC', 'apply']
const idle = new URL('./fixtures/supervisor-idle.js', import.meta.url)
const deferred = () => {
  let resolve
  let reject
  const promise = new Promise((yes, no) => { resolve = yes; reject = no })
  return { promise, resolve, reject }
}
const exited = (child) => child.exitCode !== null || child.signalCode !== null

function harness(options = {}) {
  const runs = []
  const errors = []
  const opens = []
  let closes = 0
  const supervisor = new Supervisor(async () => {
    const number = runs.length
    await options.before?.(number)
    const run = {
      number, requests: [], diagnostics: [], children: null,
      cleaned: 0, stopped: 0, gateClosed: 0, transportFailed: deferred(),
    }
    let gate
    const reply = (from, message) => gate.onReceive(from, wire.encodeFrame(message))
    const localSend = (to, frame) => {
      const decoded = wire.decodeFrame(frame)
      const message = decoded.to === 'apply' ? decoded.message : decoded
      run.requests.push({ to, message })
      if (options.local?.(run, to, message, reply)) { return }
      if (message.type === 'capture') {
        reply(to, {
          type: 'ack', cid: message.cid,
          selection: { state: Buffer.alloc(16), receipt: Buffer.alloc(16), donor: 'procA' },
        })
      } else if (message.type === 'input') {
        let outputs = []
        if (message.kind === 'append') {
          outputs = [wire.encodePayload({
            to: 'client', message: {
              type: 'ack', cid: message.requestId, cmdSeq: 1n,
              results: message.items.map((item) => [0, item.toString()]),
            },
          })]
        } else if (message.kind === 'drain') {
          outputs = [wire.encodePayload({
            to: 'client', message: { type: 'ack', cid: message.requestId },
          })]
        }
        reply(to, {
          type: 'ack', cid: message.cid, tickCount: message.tickCount,
          outputs: to === 'procA' ? outputs : outputs.map(wire.checksum),
        })
      } else {
        reply(to, { type: 'ack', cid: message.cid })
      }
    }
    gate = new Gate('a', ['a', 'b', 'c'], () => {}, localSend, {
      tickInterval: 10_000, localTimeout: 100, applyTimeout: 100,
    })
    gate.on('error', (error) => run.diagnostics.push(error))
    gate.on('close', () => { run.gateClosed += 1 })
    run.gate = gate
    runs.push(run)
    return {
      gate,
      children: Object.fromEntries(roles.map((role, index) => [
        role, options.child?.(run, role) ?? {
          file: index % 2 ? fileURLToPath(idle) : idle,
        },
      ])),
      ready: async (children) => {
        run.children = children
        await options.ready?.(run)
      },
      failed: run.transportFailed.promise,
      stop: () => {
        run.stopped += 1
        run.gateStoppedAtStop = !gate.isOpen
        run.ownersAliveAtStop = run.children !== null &&
          [...run.children.values()].some((child) => !exited(child))
        options.stop?.(run)
      },
      cleanup: async () => {
        run.cleaned += 1
        run.exitedAtCleanup = run.children === null ||
          [...run.children.values()].every(exited)
        await options.cleanup?.(run)
      },
    }
  }, {
    restartDelay: 5,
    startupTimeout: 1_000,
    ...options.opts,
  })
  supervisor.on('error', (error) => errors.push(error))
  supervisor.on('open', (gate) => opens.push(gate))
  supervisor.on('close', () => { closes += 1 })
  return { supervisor, runs, errors, opens, get closes() { return closes } }
}

test('Supervisor validates options without starting children', (t) => {
  for (const create of [null, {}, 'factory']) {
    t.throws(() => new Supervisor(create), (err) => err.code === Err.ARGUMENT_ILLEGAL)
  }
  for (const opts of [
    { restartDelay: -1 }, { restartDelay: 0.5 }, { restartDelay: Infinity },
    { startupTimeout: 0 }, { startupTimeout: -1 }, { startupTimeout: NaN },
  ]) {
    t.throws(() => new Supervisor(() => {}, opts),
      (err) => err.code === Err.ARGUMENT_ILLEGAL)
  }
  t.doesNotThrow(() => new Supervisor(() => {}, { restartDelay: 0 }))
  t.end()
})

test('Supervisor launches four children with a transport-independent Gate and force-closes', async (t) => {
  const h = harness()
  try {
    t.equal(h.supervisor.gate, null)
    t.equal(h.supervisor.isStopped, false, 'new supervisor can start')
    const opening = h.supervisor.open()
    t.equal(h.supervisor.isStopped, false, 'startup is not stopped')
    t.equal(await opening, undefined)
    const run = h.runs[0]
    t.equal(h.supervisor.gate, run.gate)
    t.deepEqual(h.opens, [run.gate])
    t.ok(run.children instanceof Map)
    t.deepEqual([...run.children.keys()], roles)
    const children = [...run.children.values()]
    t.equal(new Set(children.map((child) => child.pid)).size, 4)
    t.ok(children.every((child) => child.pid !== process.pid && !exited(child)))
    t.deepEqual(await run.gate.append(Buffer.from('one')), [1n, 'one'],
      'synchronous host-supplied localSend works without sockets')
    t.equal(await h.supervisor.open(), undefined)
    t.equal(h.runs.length, 1, 'repeated open retains the live attempt')
    const closing = h.supervisor.close()
    t.equal(h.supervisor.isStopped, true, 'close disables retries immediately')
    t.equal(run.gate.isOpen, false, 'close stops command admission before returning')
    await Promise.all([closing, h.supervisor.close()])
    t.ok(children.every((child) => child.signalCode === 'SIGKILL'),
      'forced shutdown kills and awaits every child')
    t.ok(run.exitedAtCleanup, 'cleanup runs after all owners exit')
    t.ok(run.gateStoppedAtStop, 'Gate stops before the transport stop hook')
    t.ok(run.ownersAliveAtStop, 'transport stops before owners are killed')
    t.equal(run.stopped, 1, 'transport stop hook runs once')
    t.equal(run.cleaned, 1)
    t.equal(run.gateClosed, 0, 'forced shutdown is not coordinated Gate close')
    t.equal(run.requests.filter(({ message }) => message.type === 'close').length, 0)
    t.equal(h.closes, 1)
    t.equal(h.supervisor.gate, null)
    t.equal(h.errors.length, 0)
    const reopened = await h.supervisor.open().catch((err) => err)
    t.equal(reopened.code, Err.NODE_NOT_OPEN, 'shutdown is terminal')
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor reaps the old group before restarting after a child exits', async (t) => {
  let stoppedBeforeReport = false
  const h = harness({
    before(number) {
      if (number > 0) {
        const old = h.runs[number - 1]
        t.ok([...old.children.values()].every(exited), 'all previous children exited before setup')
        t.equal(old.cleaned, 1, 'transport cleanup finished before setup')
      }
    },
  })
  h.supervisor.on('error', () => {
    stoppedBeforeReport = h.supervisor.gate?.isOpen === false
    t.equal(h.supervisor.isStopped, false, 'ordinary child failure still permits restart')
  })
  try {
    await h.supervisor.open()
    const first = h.runs[0]
    first.children.get('procB').kill('SIGKILL')
    await until(() => h.opens.length === 2, 'replacement group startup')
    t.notEqual(h.supervisor.gate, first.gate, 'replacement gets a fresh Gate')
    t.ok([...first.children.values()].every(exited))
    t.ok(h.errors.some((err) => err.code === Err.SUPERVISOR_ERROR),
      'unexpected child exit is a Supervisor diagnostic')
    t.ok(stoppedBeforeReport, 'failure closes command admission before error listeners run')
    first.gate.emit('error', new Error('late error from old Gate'))
    await pause(20)
    t.equal(h.runs.length, 2, 'old Gate callbacks cannot stop a replacement')
    t.deepEqual(await h.supervisor.gate.append(Buffer.from('continued')), [1n, 'continued'])
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor restarts a fatal Gate without mirroring its diagnostic', async (t) => {
  const h = harness()
  try {
    await h.supervisor.open()
    const run = h.runs[0]
    run.gate.onReceive('procA', Buffer.alloc(17))
    await until(() => h.opens.length === 2, 'replacement after Gate fatal')
    t.equal(run.diagnostics.at(-1).code, Err.FRAME_CORRUPT)
    t.ok([...run.children.values()].every(exited))
    t.notOk(h.errors.some((err) => err.code === Err.FRAME_CORRUPT),
      'Gate remains the owner of the original fatal diagnostic')
  } finally { await h.supervisor.close() }
  t.end()
})

test('a corrupt remote frame does not restart Supervisor or reap healthy owners', async (t) => {
  const h = harness()
  try {
    await h.supervisor.open()
    const run = h.runs[0]
    const warnings = []
    run.gate.on('warn', (error) => warnings.push(error))
    run.gate.onReceive('b', Buffer.alloc(17))
    t.equal(warnings.length, 1)
    t.equal(warnings[0].code, Err.FRAME_CORRUPT)
    t.equal(warnings[0].message, 'cluster frame from b: frame checksum differs')
    t.deepEqual(await run.gate.append(Buffer.from('continued')), [1n, 'continued'])
    await pause(20)
    t.equal(h.supervisor.gate, run.gate, 'the same Gate continues serving commands')
    t.equal(h.runs.length, 1, 'no replacement is started')
    t.ok([...run.children.values()].every((child) => !exited(child)), 'all original owners remain alive')
    t.equal(run.stopped, 0)
    t.equal(run.cleaned, 0)
    t.notOk(run.requests.some(({ message }) => message.kind === 'rpc'), 'corrupt input is not fanned out')
    t.deepEqual(run.diagnostics, [])
    t.deepEqual(h.errors, [])
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor replaces a failed transport without mirroring its diagnostic or replaying commands', async (t) => {
  const h = harness({
    local(run, to, message) { return run.number === 0 && message.kind === 'append' },
  })
  try {
    await h.supervisor.open()
    const first = h.runs[0]
    const pending = first.gate.append(Buffer.from('uncertain')).catch((err) => err)
    await until(() => first.requests.some(({ message }) => message.kind === 'append'),
      'command awaiting transport progress')
    first.transportFailed.reject(new Error('host transport failed'))
    t.equal((await pending).code, Err.NODE_NOT_OPEN)
    await until(() => h.opens.length === 2, 'replacement after transport failure')
    t.ok([...first.children.values()].every(exited), 'old owners have all exited')
    t.ok(first.gateStoppedAtStop, 'Gate stops before transport cancellation')
    t.ok(first.ownersAliveAtStop, 'transport cancellation precedes child termination')
    t.equal(first.stopped, 1)
    t.equal(first.cleaned, 1)
    t.ok(first.exitedAtCleanup)
    t.deepEqual(h.errors, [], 'the transport owns its diagnostic')
    t.notOk(h.runs[1].requests.some(({ message }) => message.kind === 'append'),
      'transport restart does not replay the pending command')
    t.deepEqual(await h.supervisor.gate.append(Buffer.from('continued')), [1n, 'continued'])
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor observes transport failure throughout startup and ignores late readiness', async (t) => {
  for (const phase of ['ready', 'hello']) {
    const waiting = deferred()
    const h = harness({
      ready(run) {
        if (phase !== 'ready' || run.number !== 0) { return }
        run.transportFailed.reject(new Error('transport failed before readiness'))
        return waiting.promise
      },
      local(run, to, message) {
        if (phase !== 'hello' || run.number !== 0 || message.type !== 'hello') { return }
        run.transportFailed.reject(new Error('transport failed while opening Gate'))
        return true
      },
    })
    try {
      await h.supervisor.open()
      const first = h.runs[0]
      t.equal(h.runs.length, 2, `${phase}: failed transport is replaced`)
      t.equal(h.opens.length, 1)
      if (phase === 'ready') { t.equal(first.requests.length, 0, 'failed readiness cannot start Gate') }
      else { t.ok(first.requests.some(({ message }) => message.type === 'hello')) }
      t.equal(first.stopped, 1)
      t.ok(first.exitedAtCleanup)
      t.deepEqual(h.errors, [], 'startup transport diagnostic is not forwarded')
      t.deepEqual(first.diagnostics, [], 'stopping Gate does not invent another diagnostic')
      waiting.resolve()
      await pause(20)
      if (phase === 'ready') {
        t.equal(first.requests.length, 0, 'late readiness cannot start an obsolete Gate')
      }
      t.equal(h.runs.length, 2)
    } finally { await h.supervisor.close() }
  }
  t.end()
})

test('Supervisor ignores obsolete failure promises and cancellation diagnostics', async (t) => {
  const h = harness({
    stop(run) {
      if (run.number > 0) { run.transportFailed.reject(new Error('intentional transport cancellation')) }
    },
  })
  try {
    await h.supervisor.open()
    const first = h.runs[0]
    first.children.get('procA').kill('SIGKILL')
    await until(() => h.opens.length === 2, 'replacement before old failure promise rejects')
    const errorCount = h.errors.length
    first.transportFailed.reject(new Error('obsolete transport failed'))
    await pause(20)
    t.equal(h.runs.length, 2)
    t.ok(h.supervisor.gate.isOpen)
    t.equal(h.errors.length, errorCount, 'superseded failure causes no diagnostic')
    await h.supervisor.close()
    t.equal(h.errors.length, errorCount, 'shutdown failure promise cannot restart or report')
    t.equal(h.runs[1].stopped, 1)
    t.equal(h.closes, 1)
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor stop hook failure still reaps owners and cleans up without replacement', async (t) => {
  const failure = new Error('transport cancellation failed')
  const h = harness({ stop() { throw failure } })
  try {
    await h.supervisor.open()
    const first = h.runs[0]
    first.transportFailed.reject(new Error('transport stopped working'))
    await until(() => h.supervisor.isStopped, 'stop hook failure terminates supervision')
    const result = await h.supervisor.close().catch((err) => err)
    t.equal(result.message, failure.message)
    t.equal(result.stack, failure.stack)
    t.equal(result.code, Err.SUPERVISOR_ERROR)
    t.ok([...first.children.values()].every(exited), 'throwing stop cannot leave children running')
    t.ok(first.exitedAtCleanup, 'cleanup still follows owner exit')
    t.equal(first.cleaned, 1, 'cleanup runs despite stop hook failure')
    t.equal(first.stopped, 1)
    t.equal(h.closes, 0, 'failed cancellation cannot advertise successful cleanup')
    t.equal(await h.supervisor.close().catch((err) => err), result,
      'later close retains the original failure')
    await pause(20)
    t.equal(h.runs.length, 1, 'failed stop prevents replacement')
    t.equal(h.errors.length, 1)
    t.equal(h.errors[0], result)
  } finally { await h.supervisor.close().catch(() => {}) }
  t.end()
})

test('Gate listener failures restart its group while Supervisor diagnostics remain nonterminal', async (t) => {
  const h = harness({
    local(run, to, message, reply) {
      if (run.number !== 0 || message.kind !== 'append') { return false }
      const outputs = [wire.encodePayload({
        to: 'event', message: { type: 'change', status: {
          state: 'leader', leader: 'a', term: 1n, seq: 1n,
          commitSeq: 1n, commitTerm: 1n, followers: ['b', 'c'],
        } },
      }), wire.encodePayload({
        to: 'client', message: {
          type: 'ack', cid: message.requestId, cmdSeq: 1n, results: [[0, 'must not escape']],
        },
      })]
      reply(to, {
        type: 'ack', cid: message.cid, tickCount: message.tickCount,
        outputs: to === 'procA' ? outputs : outputs.map(wire.checksum),
      })
      return true
    },
  })
  const supervisorFailure = new Error('Supervisor listener failed')
  h.supervisor.once('open', () => { throw supervisorFailure })
  try {
    await h.supervisor.open()
    const original = h.runs[0]
    t.equal(h.errors[0].message, supervisorFailure.message)
    t.ok(original.gate.isOpen, 'Supervisor listener diagnostic does not stop Gate')
    t.equal(h.runs.length, 1)
    const gateFailure = new Error('Gate listener failed')
    let stoppedBeforeError = false
    original.gate.on('error', () => { stoppedBeforeError = !original.gate.isOpen })
    original.gate.once('change', () => { throw gateFailure })
    const failure = await original.gate.append(Buffer.from('stopped')).catch((err) => err)
    t.equal(failure.message, gateFailure.message, 'queued success cannot escape after listener failure')
    t.equal(failure.stack, gateFailure.stack)
    t.ok(stoppedBeforeError)
    await until(() => h.opens.length === 2, 'replacement after Gate listener failed')
    t.ok([...original.children.values()].every(exited))
    t.equal(h.errors.length, 1, 'Gate retains its diagnostic without Supervisor forwarding')
    t.deepEqual(await h.supervisor.gate.append(Buffer.from('continued')), [1n, 'continued'])
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor retries a failed factory and startup failure until first success', async (t) => {
  let factories = 0
  const h = harness({
    before() {
      factories += 1
      if (factories === 1) { throw new Error('setup unavailable') }
    },
    ready(run) {
      if (run.number === 0) { throw new Error('transport unavailable') }
    },
  })
  try {
    const first = h.supervisor.open()
    const second = h.supervisor.open()
    await Promise.all([first, second])
    t.equal(factories, 3)
    t.equal(h.runs.length, 2)
    t.equal(h.opens.length, 1, 'failed attempts never publish open')
    t.equal(h.runs[0].cleaned, 1)
    t.ok(h.runs[0].exitedAtCleanup)
    t.ok(h.errors.length >= 2)
    t.ok(h.errors.every((error) => error.code === Err.SUPERVISOR_ERROR))
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor startup deadline covers readiness and Gate opening', async (t) => {
  for (const phase of ['ready', 'hello']) {
    const waiting = deferred()
    const h = harness({
      opts: { startupTimeout: 40 },
      ready(run) {
        if (phase === 'ready' && run.number === 0) { return waiting.promise }
      },
      local(run, to, message) {
        return phase === 'hello' && run.number === 0 && message.type === 'hello'
      },
    })
    try {
      await h.supervisor.open()
      t.equal(h.runs.length, 2, `${phase}: second attempt opens`)
      t.equal(h.opens.length, 1)
      t.ok(h.runs[0].exitedAtCleanup)
      t.ok(h.errors.some((err) =>
        err.code === Err.SUPERVISOR_ERROR && /timeout/i.test(err.message)))
      t.equal(h.runs[0].requests.filter(({ message }) => message.type === 'close').length, 0)
      const errors = h.errors.length
      waiting.resolve()
      await pause(10)
      t.equal(h.errors.length, errors, 'late readiness cannot fail a replacement')
      t.equal(h.runs.length, 2)
      if (phase === 'ready') {
        t.equal(h.runs[0].requests.length, 0, 'late readiness cannot open the old Gate')
      }
    } finally { await h.supervisor.close() }
  }
  t.end()
})

test('Supervisor handles synchronous spawn failure and child exit during startup', async (t) => {
  for (const failure of ['spawn', 'exit']) {
    const h = harness({
      child(run, role) {
        if (run.number !== 0 || role !== 'procB') { return }
        return failure === 'spawn'
          ? { file: '\0' }
          : { file: idle, args: ['exit', '7'] }
      },
      ready(run) {
        if (run.number === 0) { return new Promise(() => {}) }
      },
    })
    try {
      await h.supervisor.open()
      t.equal(h.runs.length, 2, `${failure}: failed startup is replaced`)
      t.equal(h.opens.length, 1)
      t.equal(h.runs[0].cleaned, 1)
      t.ok(h.errors.some((err) => err.code === Err.SUPERVISOR_ERROR))
      if (failure === 'exit') {
        t.equal(h.runs[0].children.get('procB').exitCode, 7)
        t.ok([...h.runs[0].children.values()].every(exited))
      }
    } finally { await h.supervisor.close() }
  }
  t.end()
})

test('Supervisor cancels pending readiness and ignores its eventual rejection', async (t) => {
  const waiting = deferred()
  const h = harness({ ready: () => waiting.promise })
  try {
    const opening = h.supervisor.open().catch((err) => err)
    await until(() => h.runs[0]?.children, 'children launched')
    await h.supervisor.close()
    t.equal((await opening).code, Err.NODE_NOT_OPEN)
    t.ok(h.runs[0].exitedAtCleanup)
    waiting.reject(new Error('obsolete ready failed'))
    await pause(20)
    t.equal(h.runs.length, 1)
    t.equal(h.errors.length, 0, 'late readiness cannot add a diagnostic or retry')
    t.equal(h.closes, 1)
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor closes a factory result that arrives after shutdown was requested', async (t) => {
  const waiting = deferred()
  const h = harness({ before: () => waiting.promise })
  const opening = h.supervisor.open().catch((err) => err)
  await pause(5)
  const closing = h.supervisor.close()
  waiting.resolve()
  try {
    await closing
    t.equal((await opening).code, Err.NODE_NOT_OPEN)
    t.equal(h.opens.length, 0)
    t.equal(h.runs[0].cleaned, 1, 'late factory transport is disposed of')
    t.equal(h.runs[0].children, null, 'late factory does not launch children')
    t.equal(h.runs[0].gate.isOpen, false)
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor close cancels restart delay and does not replace failed owners', async (t) => {
  const h = harness({ opts: { restartDelay: 200 } })
  try {
    await h.supervisor.open()
    h.runs[0].children.get('apply').kill('SIGKILL')
    await until(() => h.runs[0].cleaned === 1, 'old group cleanup')
    t.equal(h.supervisor.gate, null)
    t.equal(h.supervisor.isStopped, false, 'waiting to restart is not permanently stopped')
    await h.supervisor.close()
    await pause(220)
    t.equal(h.runs.length, 1)
    t.equal(h.closes, 1)
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor cleanup failure prevents replacement and is never retried', async (t) => {
  const failure = new Error('transport cleanup failed')
  const h = harness({
    cleanup() { throw failure },
  })
  let diagnostic
  let closing
  h.supervisor.on('error', (err) => {
    if (err.message !== failure.message) { return }
    diagnostic = err
    t.equal(h.supervisor.isStopped, true, 'terminal state precedes cleanup diagnostic')
    closing = h.supervisor.close()
    closing.catch(() => {})
  })
  try {
    await h.supervisor.open()
    h.runs[0].children.get('procC').kill('SIGKILL')
    await until(() => h.runs[0].cleaned > 0, 'failing transport cleanup')
    await pause(30)
    t.equal(h.runs.length, 1, 'failed cleanup prohibits automatic replacement')
    t.ok([...h.runs[0].children.values()].every(exited))
    t.equal(h.supervisor.close(), closing, 'reentrant close shares the cached operation')
    const error = await closing.catch((err) => err)
    t.equal(error, diagnostic, 'close retains the terminal cleanup diagnostic')
    t.equal(error.code, Err.SUPERVISOR_ERROR)
    t.match(error.message, /transport cleanup failed/)
    t.equal(h.closes, 0, 'failed cleanup cannot advertise successful close')
    t.equal(await h.supervisor.close().catch((err) => err), error,
      'later close returns the same failure')
    t.equal(h.runs[0].cleaned, 1, 'cleanup is attempted only once')
    t.equal(h.runs.length, 1)
  } finally { await h.supervisor.close().catch(() => {}) }
  t.end()
})

test('Supervisor rejects initial open if failed startup cannot clean up', async (t) => {
  const h = harness({
    ready() { throw new Error('readiness failed') },
    cleanup() { throw new Error('cannot remove transport') },
  })
  try {
    const failure = await h.supervisor.open().catch((err) => err)
    t.equal(failure.code, Err.SUPERVISOR_ERROR)
    t.match(failure.message, /cannot remove transport/)
    t.equal(h.runs.length, 1)
    t.equal(h.opens.length, 0)
    t.ok(h.runs[0].exitedAtCleanup)
    t.equal(h.supervisor.isStopped, true)
    t.equal(await h.supervisor.close().catch((err) => err), failure)
    t.equal(h.runs[0].cleaned, 1, 'close cannot repeat failed startup cleanup')
    t.equal(h.runs.length, 1)
    t.equal(h.closes, 0)
  } finally { await h.supervisor.close().catch(() => {}) }
  t.end()
})

test('Supervisor caches explicit close failure while supervision also awaits cleanup', async (t) => {
  const cleanup = deferred()
  const failure = new Error('delayed cleanup failure')
  const h = harness({ cleanup: () => cleanup.promise })
  let diagnostic
  h.supervisor.on('error', (err) => {
    diagnostic = err
    t.equal(h.supervisor.isStopped, true, 'cleanup error observes permanent shutdown')
  })
  try {
    await h.supervisor.open()
    const closing = h.supervisor.close()
    closing.catch(() => {})
    t.equal(h.supervisor.close(), closing)
    await until(() => h.runs[0].cleaned === 1, 'transport cleanup started')
    t.ok(h.runs[0].exitedAtCleanup)
    t.equal(h.supervisor.isStopped, true, 'stopped does not claim cleanup succeeded')
    cleanup.reject(failure)
    const error = await closing.catch((err) => err)
    await until(() => diagnostic !== undefined, 'terminal cleanup diagnostic')
    t.equal(error, diagnostic)
    t.equal(error.stack, failure.stack)
    t.equal(await h.supervisor.close().catch((err) => err), error)
    t.equal(h.runs[0].cleaned, 1)
    t.equal(h.closes, 0)
  } finally {
    cleanup.resolve()
    await h.supervisor.close().catch(() => {})
  }
  t.end()
})

test('Supervisor never retries failed owner termination or replaces an unconfirmed owner', async (t) => {
  const h = harness()
  const failure = new Error('cannot kill owner')
  let owner
  let kill
  let attempts = 0
  try {
    await h.supervisor.open()
    const run = h.runs[0]
    owner = run.children.get('procA')
    kill = owner.kill.bind(owner)
    owner.kill = () => { attempts += 1; throw failure }
    run.children.get('procC').kill('SIGKILL')
    await until(() => h.supervisor.isStopped, 'terminal owner cleanup failure')
    t.notOk(exited(owner), 'failed termination leaves ownership unconfirmed')
    t.ok([...run.children.values()].filter((child) => child !== owner).every(exited),
      'remaining children are killed and reaped before failure returns')
    const error = await h.supervisor.close().catch((err) => err)
    t.equal(error.code, Err.SUPERVISOR_ERROR)
    t.equal(error.stack, failure.stack)
    t.equal(await h.supervisor.close().catch((err) => err), error)
    t.equal(attempts, 1, 'explicit close cannot retry failed termination')
    t.equal(run.cleaned, 0, 'transport cleanup cannot run before every owner exits')
    t.equal(h.closes, 0)
    await pause(20)
    t.equal(h.runs.length, 1, 'no replacement can open databases still owned by a child')
  } finally {
    if (owner && !exited(owner)) {
      const done = new Promise((resolve) => owner.once('exit', resolve))
      kill('SIGKILL')
      await done
    }
    await h.supervisor.close().catch(() => {})
  }
  t.end()
})

test('Supervisor explicit drain coordinates Gate close before reaping children', async (t) => {
  const h = harness()
  try {
    await h.supervisor.open()
    const run = h.runs[0]
    const draining = h.supervisor.drainCmd()
    t.equal(h.supervisor.isStopped, true, 'drain disables retries before cleanup')
    const append = run.gate.append(Buffer.from('too late')).catch((err) => err)
    const batch = run.gate.appendBatch([Buffer.from('too late')]).catch((err) => err)
    t.equal((await append).code, Err.DRAINING, 'drain closes single-command admission immediately')
    t.equal((await batch).code, Err.DRAINING, 'drain closes batch admission immediately')
    await draining
    t.notOk(run.requests.some(({ message }) => message.kind === 'append'),
      'neither command reaches the local owners')
    t.equal(run.requests.filter(({ message }) => message.kind === 'drain').length, 3)
    t.equal(run.requests.filter(({ message }) => message.type === 'close').length, 4)
    t.equal(run.gateClosed, 1)
    t.ok(run.exitedAtCleanup)
    t.equal(h.closes, 1)
    t.equal(h.runs.length, 1)
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor draining from an open listener rejects appends from later listeners', async (t) => {
  const h = harness()
  let draining
  let append
  h.supervisor.on('open', () => { draining = h.supervisor.drainCmd() })
  h.supervisor.on('open', (gate) => {
    append = gate.append(Buffer.from('listener command')).catch((err) => err)
  })
  try {
    await h.supervisor.open()
    t.equal((await append).code, Err.DRAINING,
      'a later listener sees draining in the same event emission')
    await draining
    t.notOk(h.runs[0].requests.some(({ message }) => message.kind === 'append'))
    t.equal(h.runs[0].requests.filter(({ message }) => message.kind === 'drain').length, 3)
    t.equal(h.closes, 1)
  } finally { await h.supervisor.close() }
  t.end()
})

test('Supervisor retains a drain failure after forced cleanup and does not replay commands', async (t) => {
  const h = harness({
    local(run, to, message, reply) {
      if (message.kind !== 'drain') { return false }
      const output = wire.encodePayload({
        to: 'client', message: {
          type: 'err', cid: message.requestId,
          error: { msg: 'cannot drain', code: Err.NOT_LEADER, stack: 'original drain trace' },
        },
      })
      reply(to, {
        type: 'ack', cid: message.cid, tickCount: message.tickCount,
        outputs: to === 'procA' ? [output] : [wire.checksum(output)],
      })
      return true
    },
  })
  try {
    await h.supervisor.open()
    const run = h.runs[0]
    const failure = await h.supervisor.drainCmd().catch((err) => err)
    t.equal(failure.code, Err.NOT_LEADER)
    t.equal(failure.message, 'cannot drain')
    t.equal(failure.stack, 'original drain trace')
    t.ok(run.exitedAtCleanup)
    t.equal(h.runs.length, 1)
    t.equal(run.requests.filter(({ message }) => message.type === 'close').length, 0)
  } finally { await h.supervisor.close() }

  const pending = harness({
    local(run, to, message) { return run.number === 0 && message.kind === 'append' },
  })
  try {
    await pending.supervisor.open()
    const run = pending.runs[0]
    const result = run.gate.append(Buffer.from('uncertain')).catch((err) => err)
    await until(() => run.requests.some(({ message }) => message.kind === 'append'), 'pending command')
    run.children.get('apply').kill('SIGKILL')
    t.equal((await result).code, Err.NODE_NOT_OPEN)
    await until(() => pending.opens.length === 2, 'replacement after pending command')
    t.notOk(pending.runs[1].requests.some(({ message }) => message.kind === 'append'),
      'Supervisor cannot replay a caller command onto its replacement')
  } finally { await pending.supervisor.close() }
  t.end()
})
