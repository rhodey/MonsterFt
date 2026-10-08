import crypto from 'node:crypto'
import { spawnSync } from 'node:child_process'
import test from 'tape'
import { MonsterFt, SQLiteLog } from '../src/index.js'
import { APPLY_ERROR, ErrorWithCode, MONSTER_ILLEGAL } from '../src/error.js'
import { databasePath, sleep } from './util.js'

const ids = ['1', '2', '3']
let fixtureId = 0
const openNodes = (nodes) => nodes.forEach((node) => node.open())
const closeNodesQuietly = (nodes) => {
  for (const node of nodes) {
    try { node.close() } catch {}
  }
}

const withTimeout = (promise, name, ms=8_000) => {
  let timer = null
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} timeout`)), ms)
  })
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer))
}

const waitFor = async (fn, name, ms=8_000) => {
  const end = Date.now() + ms
  while (!fn()) {
    if (Date.now() >= end) { throw new Error(`${name} timeout`) }
    await sleep(5)
  }
}

const nextSync = (node) => new Promise((resolve) => node.once('sync', resolve))

const failCommandLookup = (node, failure) => {
  node._monsterRunDbGetCmd = async () => { throw failure }
}

const makeFixture = (t, name) => {
  const unique = `${++fixtureId}-${process.pid}-${name}`
  const paths = new Map(ids.map((id) => [
    id,
    databasePath(`monster-fatal-${unique}-${id}`),
  ]))
  const current = new Map()
  const warnings = []

  const nodes = ids.map((id) => {
    const send = (to, msg) => {
      const target = current.get(to)
      if (!target) { return }
      return target.onReceive(id, msg)
    }
    const node = new MonsterFt(id, ids, send, paths.get(id), {
      electionTimeout: 60_000,
      pingTimeout: 500,
      appendTimeout: 3_000,
      quorum: 2,
      apply: () => null,
    })
    node.on('warn', (err) => warnings.push({ id, err }))
    current.set(id, node)
    return node
  })

  t.teardown(() => {
    closeNodesQuietly(nodes)
    for (const file of paths.values()) {
      new SQLiteLog(file).del()
      new SQLiteLog(`${file}2`).del()
    }
  })

  return { nodes, warnings }
}

const elect = async (nodes) => {
  openNodes(nodes)
  const leader = nodes[0]
  leader._voteForSelf()
  await waitFor(() => leader.state === 'leader', 'leader election')
  await withTimeout(
    Promise.all(nodes.map((node) => node.awaitLeader(true))),
    'leader commit',
  )
  await leader._monsterLeaderSync
  return leader
}

const expectFatalClose = async (t, target, source, msg, failure, context) => {
  const fatals = []
  const message = failure.message
  target.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => target.once('error', resolve))
  source.send(target.id, msg)
  const err = await withTimeout(errored, `${context} fatal close`)
  t.deepEqual(fatals, [failure], `${context} enters the Raft fatal path exactly once`)
  t.equal(err, failure, `${context} reports the original local failure`)
  t.equal(err.message, message,
    `${context} preserves the original failure message`)
  t.notOk(target.isOpen, `${context} closes the affected receiver`)
}

test('uncaught application errors retain their trace without an anonymous script header', (t) => {
  const moduleUrl = new URL('../src/index.js', import.meta.url).href
  for (const mode of ['startup', 'local', 'forwarded']) {
    const source = `
      import { MonsterFt } from ${JSON.stringify(moduleUrl)}
      const mode = ${JSON.stringify(mode)}
      const ids = ['1', '2', '3']
      const byId = new Map()
      const nodes = ids.map((id) => {
        const node = new MonsterFt(id, ids,
          (to, msg) => byId.get(to).onReceive(id, msg), ':memory:', {
            electionTimeout: 60000,
            apply: function applicationApply(db, data, term, seq) {
              if (mode !== 'startup' && seq === 0n) { return }
              throw new TypeError('original application failure')
            },
          })
        byId.set(id, node)
        return node
      })
      nodes.forEach((node) => node.open())
      nodes[0]._voteForSelf()
      if (mode !== 'startup') {
        await Promise.all(nodes.map((node) => node.awaitLeader(true)))
        const caller = nodes[mode === 'local' ? 0 : 1]
        try {
          await caller.append(Buffer.from('fail'))
        } catch (err) {
          queueMicrotask(() => caller.emit('error', err))
        }
      }
    `
    const result = spawnSync(process.execPath, ['--input-type=module', '-e', source], {
      encoding: 'utf8', timeout: 8000,
    })
    t.error(result.error, `${mode} child completes without a timeout`)
    t.equal(result.status, 1, `${mode} still exits on an unhandled error`)
    t.match(result.stderr, /TypeError: original application failure/,
      `${mode} preserves the original error header`)
    t.match(result.stderr, /at MonsterFt\.applicationApply/,
      `${mode} preserves the application frame`)
    t.notOk(/<anonymous_script>:0|\n{3}/.test(result.stderr),
      `${mode} has no anonymous header or extra blank lines`)
  }
  t.end()
})

for (const asynchronous of [false, true]) {
  const mode = asynchronous ? 'async' : 'sync'
  test(`MonsterFt preserves ${mode} sequence-zero errors in fatal and error events`, async (t) => {
    const fixture = makeFixture(t, `schema-stack-${mode}`)
    const node = fixture.nodes[0]
    let original = null
    let fatal = null
    const schemaApply = () => {
      original = new TypeError('schema application failure')
      throw original
    }
    node._monsterUserApply = asynchronous
      ? async () => {
          await Promise.resolve()
          schemaApply()
        }
      : schemaApply
    node.on('fatal', (err) => { fatal = err })
    const errored = new Promise((resolve) => node.once('error', resolve))
    openNodes(fixture.nodes)
    node._voteForSelf()
    const err = await withTimeout(errored, `${mode} schema error event`)
    t.ok(err instanceof ErrorWithCode, 'emits a normalized error')
    t.equal(err.code, APPLY_ERROR, 'the sequence-zero failure retains its fatal code')
    t.equal(err.message, '(apply) schema application failure', 'the contextual message is preserved')
    t.equal(err.stack, original.stack, 'the emitted error keeps the original trace')
    t.match(err.stack, /schemaApply/, 'the trace identifies the application callback')
    t.equal(err, fatal, 'fatal and error events report the same error')
    t.notOk(node.isOpen, 'the initialization failure still closes the node')
  })
}

test('MonsterFt treats an illegal repair state as fatal', async (t) => {
  const fixture = makeFixture(t, 'repair-state')
  const leader = await elect(fixture.nodes)
  const fatals = []
  leader.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => leader.once('error', resolve))
  let thrown = null

  try {
    leader._monsterRepairError(0)
  } catch (err) {
    thrown = err
  }

  const reported = await withTimeout(errored, 'illegal repair state fatal')
  t.ok(thrown instanceof ErrorWithCode,
    'the illegal state throws ErrorWithCode')
  t.equal(thrown.message, 'repair state is illegal',
    'the illegal state throws the invariant error')
  t.equal(thrown.code, MONSTER_ILLEGAL,
    'the illegal state has the Monster invariant code')
  t.equal(thrown.sqlCode, null,
    'the illegal state has no SQLite error code')
  t.deepEqual(fatals, [thrown],
    'the invariant error enters the fatal path exactly once')
  t.equal(reported, thrown, 'the fatal path reports the same error')
  t.equal(leader.isOpen, false, 'the illegal state closes the node')
})

test('MonsterFt leader sync command lookup failure is fatal once',
  async (t) => {
    const fixture = makeFixture(t, 'leader-sync-command')
    const leader = fixture.nodes[0]
    const failure = new Error('injected leader sync command lookup failure')
    const message = failure.message
    const diagnostics = []
    leader.on('fatal', (err) => diagnostics.push({ type: 'fatal', err }))
    leader.on('warn', (err) => diagnostics.push({ type: 'warn', err }))
    const errored = new Promise((resolve) => {
      leader.on('error', (err) => {
        diagnostics.push({ type: 'error', err })
        resolve(err)
      })
    })
    failCommandLookup(leader, failure)

    openNodes(fixture.nodes)
    leader._voteForSelf()
    const reported = await withTimeout(
      errored,
      'leader sync command lookup fatal',
    )
    let rejected = null
    try {
      await leader._monsterLeaderSync
    } catch (err) {
      rejected = err
    }

    t.equal(rejected, failure,
      'leader synchronization rejects with the lookup failure')
    t.equal(reported, failure,
      'the fatal path reports the original lookup failure')
    t.equal(failure.message, message,
      'leader synchronization preserves the failure message')
    t.deepEqual(diagnostics.map(({ type }) => type), ['fatal', 'error'],
      'leader synchronization has one fatal diagnostic owner')
    t.ok(diagnostics.every(({ err }) => err === failure),
      'every diagnostic preserves the original failure')
    t.equal(leader.isOpen, false,
      'the lookup failure closes the leader')
  })

test('MonsterFt OUTCOME DB FIFO failure fatally closes its receiver',
  async (t) => {
    const fixture = makeFixture(t, 'outcome')
    const leader = await elect(fixture.nodes)
    const source = fixture.nodes.find((node) => node !== leader)
    const warningCount = fixture.warnings.length
    const failure = new Error('injected outcome DB FIFO failure')

    leader._monsterRunDb = async () => { throw failure }
    await expectFatalClose(t, leader, source, {
      type: 'monster_outcome',
      cmdSeq: 0n,
      digest: Buffer.alloc(32),
    }, failure, 'OUTCOME FIFO')

    t.deepEqual(
      fixture.warnings.slice(warningCount).filter(({ id }) => id === source.id),
      [],
      'the one-way receiver failure is not reported as a sender transport warning',
    )
  })

test('MonsterFt OUTCOME_REQUEST pending lookup failure fatally closes its receiver',
  async (t) => {
    const fixture = makeFixture(t, 'outcome-request')
    const leader = await elect(fixture.nodes)
    const target = fixture.nodes.find((node) => node !== leader)
    const warningCount = fixture.warnings.length
    const failure = new Error('injected outcome request pending lookup failure')

    failCommandLookup(target, failure)
    await expectFatalClose(t, target, leader, {
      type: 'monster_outcome_request',
      cmdSeq: 0n,
    }, failure, 'OUTCOME_REQUEST CMD 0 lookup')

    t.deepEqual(fixture.warnings.slice(warningCount).filter(({ id }) => id === leader.id), [],
      'the one-way receiver failure is not reported as a sender transport warning')
  })

test('MonsterFt hashes one command envelope only at CMD', async (t) => {
  const fixture = makeFixture(t, 'digest-count')
  const leader = await elect(fixture.nodes)
  const createHash = crypto.createHash
  let hashes = 0
  let syncSeq = null

  crypto.createHash = (...args) => {
    hashes++
    return createHash(...args)
  }
  try {
    const synced = nextSync(leader)
    await leader.append(Buffer.from('digest count'))
    syncSeq = (await withTimeout(synced, 'agreed SYNC event')).syncSeq
    await waitFor(() => {
      return fixture.nodes.every((node) => node._applySeq >= syncSeq)
    }, 'agreed SYNC apply')
  } finally {
    crypto.createHash = createHash
  }

  t.equal(hashes, ids.length * 3,
    'each replica hashes one command envelope plus its CMD and SYNC checkpoints')
})
