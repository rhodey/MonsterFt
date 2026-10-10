import test from 'tape'
import {
  APPEND_TIMEOUT,
  REPL_BACKTRACK,
  ErrorWithCode,
  ARGUMENT_ILLEGAL,
  LOG_NOT_OPEN,
  NO_LEADER,
  NODE_NOT_OPEN,
  TERM_DIFF,
  NOT_COMMIT,
  PING_TIMEOUT,
  RAFT_ILLEGAL,
  RPC_ILLEGAL,
  SEND_ERROR,
  SQLITE_ERROR,
} from '../src/error.js'
import { RaftNode as ProductionRaftNode } from '../src/index.js'
import { ACK_OPERATION } from '../src/node.js'
import { databasePath } from './util.js'

let injectedLogId = 0
class TestRaftNode extends ProductionRaftNode {
  constructor(id, nodes, send, log, opts) {
    const file = typeof log?.path === 'string' && log.path.length > 0
      ? log.path
      : databasePath(`08-injected-log-${process.pid}-${++injectedLogId}`)
    super(id, nodes, send, file, opts)
    this.log = log
  }
}
const RaftNode = TestRaftNode

class DeferredRaftNode extends RaftNode {
  _autoRaft() {
    return false
  }
}

const ids = ['1', '2', '3']

const rejects = async (t, promise, pattern, name) => {
  try {
    await promise
    t.fail(name)
  } catch (err) {
    t.ok(pattern.test(err.message), name)
  }
}

const rejection = async (promise) => {
  try {
    await promise
    return null
  } catch (err) {
    return err
  }
}

const thrown = (fn) => {
  try {
    fn()
    return null
  } catch (err) {
    return err
  }
}

const lifecycleLog = () => ({
  calls: [],
  isOpen: false,
  seq: -1n,
  head: null,
  term: -1n,
  elec: { term: 0n, votedFor: null },
  open() {
    this.calls.push('open')
    this.isOpen = true
  },
  close() {
    this.calls.push('close')
    this.isOpen = false
  },
  election(currentTerm, votedFor) {
    this.elec.term = currentTerm
    this.elec.votedFor = votedFor
    return [currentTerm, votedFor]
  },
})

test('numeric quorum configuration', (t) => {
  const ids4 = ['1', '2', '3', '4']
  const send = () => {}
  const log = {}

  const node = new RaftNode('1', ids4, send, log)
  t.equal(node.quorum, 3, 'defaults to a majority')
  t.equal(node.opts.quorum, 3, 'resolved option is numeric')
  t.equal(new RaftNode('1', ids4, send, log, { quorum: 3 }).quorum, 3,
    'accepts an explicit majority')
  t.equal(new RaftNode('1', ids4, send, log, { quorum: 4 }).quorum, 4,
    'accepts an explicit quorum')
  t.equal(new RaftNode('1', ids4, send, log, { rpcMax: 1 }).opts.rpcMax, 1n,
    'normalizes a valid RPC page size')
  t.equal(new RaftNode('1', ids4, send, log, { applyMax: 1 }).opts.applyMax, 1,
    'accepts a valid apply batch size')
  t.ok(new RaftNode('1', ids, send, log, { quorum: 3 })._isQuorumErr(['2']),
    'one failure makes an all-node quorum impossible')
  t.end()
})

test('node configuration requires a viable fixed cluster', (t) => {
  const send = () => {}
  const log = {}
  const throwsInvalidArgument = (create, pattern, label) => {
    const err = thrown(create)
    t.ok(err instanceof ErrorWithCode, `${label} uses ErrorWithCode`)
    t.equal(err?.code, ARGUMENT_ILLEGAL, `${label} has ARGUMENT_ILLEGAL code`)
    t.equal(err?.sqlCode, null, `${label} has no SQLite code`)
    t.match(err?.message, pattern, label)
  }

  for (const invalid of [1, 1n, '', Buffer.from('1'), ['1'], { id: '1' },
    new String('1')]) {
    throwsInvalidArgument(() => new RaftNode(invalid, ids, send, log),
      /id must be non-empty string/, `rejects local id ${String(invalid)}`)
  }
  for (const invalid of [1, 1n, '', Buffer.from('2'), ['2'], { id: '2' },
    new String('2')]) {
    throwsInvalidArgument(
      () => new RaftNode('1', ['1', invalid, '3'], send, log),
      /nodes must be non-empty string array/,
      `rejects member id ${String(invalid)}`)
  }
  for (const invalid of [null, undefined]) {
    throwsInvalidArgument(
      () => new RaftNode('1', invalid, send, log),
      /nodes must be non-empty string array/,
      `rejects nodes ${String(invalid)}`)
  }
  throwsInvalidArgument(() => new RaftNode('1', ['1', '2'], send, log),
    /nodes must contain at least 3 ids/,
    'requires at least three unique node ids')
  throwsInvalidArgument(() => new RaftNode('1', ['1', '1', '2'], send, log),
    /nodes must contain at least 3 ids/,
    'counts unique node ids')
  throwsInvalidArgument(() => new RaftNode('4', ids, send, log),
    /nodes must contain its id/,
    'requires the local node id in the cluster')
  for (const quorum of [2, 2.5, NaN, '3']) {
    throwsInvalidArgument(
      () => new RaftNode('1', ['1', '2', '3', '4'], send, log, { quorum }),
      /quorum must be int >=/, `rejects invalid quorum ${quorum}`)
  }
  throwsInvalidArgument(() => new RaftNode('1', ids, send, log, { quorum: 4 }),
    /quorum must be <= nodes length/, 'quorum cannot exceed the cluster size')
  for (const rpcMax of [0, -1, 1.5, NaN, '1', 1n]) {
    throwsInvalidArgument(() => new RaftNode('1', ids, send, log, { rpcMax }),
      /rpcMax must be int > 0/, `rejects invalid rpcMax ${rpcMax}`)
  }
  for (const applyMax of [0, -1, 1.5, NaN, '1', 1n]) {
    throwsInvalidArgument(() => new RaftNode('1', ids, send, log, { applyMax }),
      /applyMax must be int > 0/, `rejects invalid applyMax ${applyMax}`)
  }
  for (const invalid of [undefined, null, false, 1, 'callback', {}]) {
    throwsInvalidArgument(() => new RaftNode('1', ids, invalid, log),
      /send must be a function/, `rejects invalid send ${String(invalid)}`)
  }
  for (const apply of [false, 1, 'callback', {}]) {
    throwsInvalidArgument(() => new RaftNode('1', ids, send, log, { apply }),
      /apply must be a function/, `rejects invalid apply ${String(apply)}`)
  }
  for (const apply of [undefined, null]) {
    const node = new RaftNode('1', ids, send, log, { apply })
    t.deepEqual(node.opts.apply(node, [Buffer.from('value')]), [null],
      `apply ${String(apply)} retains the default application callback`)
  }
  t.end()
})

test('node timeouts require positive integers', (t) => {
  for (const name of ['electionTimeout', 'pingTimeout', 'appendTimeout']) {
    for (const value of [undefined, null, false, 0, -1, 1.5, NaN, Infinity,
      '100', 100n]) {
      const err = thrown(() => new RaftNode('1', ids, () => {}, {}, { [name]: value }))
      t.equal(err?.code, ARGUMENT_ILLEGAL, `${name} rejects ${String(value)}`)
      t.match(err?.message ?? '', new RegExp(name), 'error identifies the option')
    }
    for (const value of [1, 2_147_483_648, Number.MAX_SAFE_INTEGER]) {
      const node = new RaftNode('1', ids, () => {}, {}, { [name]: value })
      t.equal(node.opts[name], value, `${name} accepts positive integer ${value}`)
    }
  }
  t.end()
})

test('RaftNode has no read operation', (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog())
  t.equal(node.read, undefined, 'read is absent from the public API')
  t.end()
})

test('send normalizes synchronous and asynchronous transport errors',
  async (t) => {
    const syncError = new Error('sync failure')
    const asyncError = new Error('async failure')
    let calls = 0
    const node = new RaftNode('1', ids, () => {
      if (++calls === 1) { throw syncError }
      return Promise.reject(asyncError)
    }, lifecycleLog())
    const warnings = []
    node.on('warn', (err) => warnings.push(err))
    node._open = true

    node.send('2', { type: 'probe' })
    node.send('2', { type: 'probe' })
    await new Promise((resolve) => setImmediate(resolve))

    t.equal(warnings.length, 2, 'reports both transport errors')
    for (const [index, message] of ['sync failure', 'async failure'].entries()) {
      const warning = warnings[index]
      t.ok(warning instanceof ErrorWithCode,
        `${message} uses ErrorWithCode`)
      t.equal(warning.code, SEND_ERROR, `${message} has SEND_ERROR code`)
      t.equal(warning.sqlCode, null, `${message} has no SQLite code`)
      t.equal(warning.message, `(send) ${message}`,
        `${message} has send context`)
    }
    t.notEqual(warnings[0], syncError, 'normalizes the synchronous error')
    t.notEqual(warnings[1], asyncError, 'normalizes the asynchronous error')
    node.close()
  })

test('election persistence returns true when current or written', (t) => {
  const log = lifecycleLog()
  const election = log.election.bind(log)
  let writes = 0
  log.election = (...args) => {
    writes++
    return election(...args)
  }
  const node = new RaftNode('1', ids, () => {}, log)
  node.term = 0n
  node._votedFor = null

  t.equal(node._persistElection(), true, 'current election state succeeds')
  t.equal(writes, 0, 'current election state requires no write')

  node.term = 1n
  node._votedFor = '1'
  t.equal(node._persistElection(), true, 'written election state succeeds')
  t.equal(writes, 1, 'changed election state is written once')
  t.deepEqual(log.elec, { term: 1n, votedFor: '1' },
    'publishes the written state')
  t.end()
})

test('vote persistence completes before change and transport callbacks', async (t) => {
  const log = lifecycleLog()
  const order = []
  const election = log.election.bind(log)
  log.election = (...args) => {
    order.push('persist')
    return election(...args)
  }
  let node = null
  const send = (to, msg) => {
    if (msg.type !== 'vote') { return }
    t.deepEqual(log.elec, { term: 1n, votedFor: '2' },
      'send observes the persisted vote')
    order.push('send')
  }
  node = new RaftNode('1', ids, send, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node._stopTimers()

  const changed = () => {
    t.deepEqual(log.elec, { term: 1n, votedFor: '2' },
      'change observes the persisted vote')
    order.push('change')
  }
  node.on('change', changed)

  await node.onReceive('2', {
    type: 'vote_request',
    term: 1n,
    termP: -1n,
    seqP: -1n,
  })

  t.deepEqual(order, ['persist', 'send', 'change'],
    'persists and sends the vote before publishing the change')
  node.removeListener('change', changed)
  node.close()
})

test('a vote response precedes a change listener that closes the node', async (t) => {
  const log = lifecycleLog()
  let persisted = false
  const election = log.election.bind(log)
  log.election = (...args) => {
    const result = election(...args)
    persisted = true
    return result
  }
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node._stopTimers()
  node.on('change', (change) => {
    if (!change.open || change.term !== 1n) { return }
    t.ok(persisted, 'listener observes the durable election state')
    node.close()
  })

  await node.onReceive('2', {
    type: 'vote_request',
    term: 1n,
    termP: -1n,
    seqP: -1n,
  })

  t.notOk(node.isOpen, 'listener leaves the node closed')
  t.notOk(log.isOpen, 'listener closes storage before returning')
  t.deepEqual(sent, [['2', {
    type: 'vote', term: 1n, voteGranted: true, from: '1',
  }]], 'sends the durable vote before the change listener closes the node')
})

test('plain Raft open starts the protocol automatically', (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const changes = []
  node.on('change', (change) => changes.push(change.open))

  t.equal(node._autoRaft(), true, 'Raft starts automatically by default')
  node.open()
  t.deepEqual(log.calls, ['open'], 'opens DB1 once')
  t.ok(log.isOpen, 'DB1 remains open')
  t.ok(node.isOpen, 'publishes the active Raft node')
  t.equal(node.state, 'follower', 'starts as a follower')
  t.equal(node.term, log.elec.term, 'loads the durable election term')
  t.equal(node._votedFor, log.elec.votedFor, 'loads the durable vote')
  t.ok(node._electionTimer, 'starts the election timer')
  t.deepEqual(changes, [true], 'publishes one open change')

  const timer = node._electionTimer
  node._startRaft()
  t.equal(node._electionTimer, timer, 'repeated start does not replace the timer')
  t.deepEqual(changes, [true], 'repeated start does not publish again')
  node.close()
  t.end()
})

test('deferred Raft open keeps DB1 inactive until explicit start', (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new DeferredRaftNode('1', ids,
    (to, msg) => sent.push([to, msg]), log, {
      electionTimeout: 60_000,
      pingTimeout: 60_000,
    })
  const changes = []
  node.on('change', (change) => changes.push(change.open))

  node.open()
  t.deepEqual(log.calls, ['open'], 'opens DB1')
  t.ok(log.isOpen, 'DB1 is open before Raft starts')
  t.notOk(node.isOpen, 'Raft is not yet active')
  t.equal(node.state, null, 'does not initialize protocol state')
  t.equal(node.term, null, 'does not publish an election term')
  t.equal(node._electionTimer, undefined, 'does not start an election timer')
  t.deepEqual(changes, [], 'does not publish an open change')
  node.send('2', { type: 'probe' })
  t.deepEqual(sent, [], 'transport remains inactive')

  node._startRaft()
  t.ok(node.isOpen, 'explicit start activates Raft')
  t.equal(node.state, 'follower', 'explicit start initializes follower state')
  t.equal(node.term, log.elec.term, 'explicit start loads the durable term')
  t.ok(node._electionTimer, 'explicit start creates the election timer')
  t.deepEqual(changes, [true], 'explicit start publishes one open change')

  const timer = node._electionTimer
  node._startRaft()
  t.equal(node._electionTimer, timer, 'idempotent start retains the timer')
  t.deepEqual(changes, [true], 'idempotent start does not republish')
  node.close()
  t.end()
})

test('Raft cannot start with a closed log', (t) => {
  const node = new DeferredRaftNode('1', ids, () => {}, lifecycleLog(), {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })

  const err = thrown(() => node._startRaft())
  t.ok(err instanceof ErrorWithCode, 'uses ErrorWithCode')
  t.equal(err?.message, 'log not open', 'identifies the closed log')
  t.equal(err?.code, LOG_NOT_OPEN, 'uses LOG_NOT_OPEN')
  t.equal(err?.sqlCode, null, 'has no SQLite code')
  t.end()
})

test('close before deferred start closes DB1 and rejects later start or open', (t) => {
  const log = lifecycleLog()
  const node = new DeferredRaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const changes = []
  node.on('change', (change) => changes.push(change.open))

  node.open()
  node.close()
  t.deepEqual(log.calls, ['open', 'close'], 'closes the storage-only DB1 handle once')
  t.notOk(log.isOpen, 'DB1 is closed')
  t.notOk(node.isOpen, 'Raft remains inactive')
  t.deepEqual(changes, [], 'never publishes protocol state')
  t.throws(() => node._startRaft(), /node not open/,
    'cannot start after close')
  t.throws(() => node.open(), /node not open/,
    'cannot reopen the same instance')
  t.deepEqual(log.calls, ['open', 'close'], 'rejected calls do not touch DB1')
  t.end()
})

test('failed open requires a fresh node instance', (t) => {
  const openFailure = new Error('Raft policy failed')
  class FailingRaftNode extends RaftNode {
    _autoRaft() {
      throw openFailure
    }
  }
  const log = lifecycleLog()
  const node = new FailingRaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })

  const failed = thrown(() => node.open())
  t.equal(failed, openFailure, 'preserves the original open error')
  t.deepEqual(log.calls, ['open', 'close'], 'makes one immediate DB1 cleanup attempt')
  t.notOk(log.isOpen, 'releases DB1')
  t.notOk(node.isOpen, 'does not leave Raft active')
  t.equal(node._closing, true, 'prevents reuse of the failed instance')
  t.throws(() => node.open(), /node not open/,
    'the failed instance cannot retry open')
  t.deepEqual(log.calls, ['open', 'close'], 'rejected retry does not touch DB1')

  const replacementLog = lifecycleLog()
  const replacement = new RaftNode('1', ids, () => {}, replacementLog, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  replacement.open()
  t.ok(replacement.isOpen, 'the fresh instance starts Raft')
  replacement.close()
  t.end()
})

test('failed open cleanup is attempted once until caller invokes close again', async (t) => {
  const openFailure = new Error('Raft policy failed')
  const cleanupFailure = new Error('DB1 close failed')
  class FailingRaftNode extends RaftNode {
    _autoRaft() {
      throw openFailure
    }
  }
  const log = lifecycleLog()
  const closeLog = log.close.bind(log)
  let closeCalls = 0
  log.close = () => {
    closeCalls++
    if (closeCalls === 1) {
      log.calls.push('close')
      throw cleanupFailure
    }
    closeLog()
  }
  const node = new FailingRaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))

  const failed = thrown(() => node.open())
  t.equal(failed, openFailure, 'preserves the original open failure')
  t.deepEqual(log.calls, ['open', 'close'],
    'attempts DB1 cleanup once before rejecting')
  t.equal(closeCalls, 1, 'performs exactly one immediate close attempt')
  t.ok(log.isOpen, 'retains the handle after close failure')
  t.equal(node._closing, true, 'failed open establishes the shutdown barrier')
  t.deepEqual(fatals, [cleanupFailure], 'reports cleanup failure through fatal')
  t.deepEqual(errors, [], 'defers public error reporting')
  t.ok(node.eventNames().includes('error'),
    'failed-open cleanup retains listeners until fatal reporting')

  t.equal(await errored, cleanupFailure, 'reports the cleanup failure publicly')
  await new Promise((resolve) => setImmediate(resolve))
  t.deepEqual(node.eventNames(), [],
    'failed-open cleanup removes listeners after fatal reporting')
  t.equal(closeCalls, 1, 'does not schedule or recursively retry cleanup')
  t.throws(() => node.open(), /node not open/,
    'later open remains rejected')
  t.equal(closeCalls, 1, 'rejected open does not retry cleanup')

  node.close()
  t.equal(closeCalls, 2, 'explicit close makes one additional attempt')
  t.notOk(log.isOpen, 'explicit retry releases DB1')
  t.deepEqual(log.calls, ['open', 'close', 'close'],
    'no cleanup attempts occur between the two caller actions')
})

test('open change listener can close without duplicate cleanup', async (t) => {
  const log = lifecycleLog()
  const closeLog = log.close.bind(log)
  let closeCalls = 0
  log.close = () => {
    closeCalls++
    closeLog()
  }
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const changes = []
  const storageAtClosedChange = []
  node.on('change', (change) => {
    changes.push(change.open)
    if (!change.open) {
      storageAtClosedChange.push(log.isOpen)
      return
    }
    if (change.open) { node.close() }
  })

  t.throws(() => node.open(), /node not open/,
    'open throws instead of succeeding after listener close')
  t.deepEqual(changes, [true, false], 'publishes open followed by terminal close')
  t.deepEqual(storageAtClosedChange, [true],
    'closed publication happens before physical DB1 cleanup')
  t.deepEqual(log.calls, ['open', 'close'], 'storage is closed before open throws')
  t.equal(closeCalls, 1, 'open does not invoke close a second time')
  t.notOk(node.isOpen, 'node is closed before control returns')
  t.notOk(log.isOpen, 'listener-triggered close returns after DB1 cleanup')
  await new Promise((resolve) => setImmediate(resolve))
  t.equal(closeCalls, 1, 'does not schedule a later cleanup attempt')
})

test('_stopRaft resets protocol state before one closed publication', (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const changes = []
  const storageAtClosedChange = []
  node.on('change', (change) => {
    changes.push(change)
    if (!change.open) { storageAtClosedChange.push(log.isOpen) }
  })

  node.open()
  node.state = 'leader'
  node.leader = node.id
  node.followers = ['2']
  node.term = 4n
  node._votedFor = node.id
  node._pingms = 123
  node._pongs.set('2', 123)
  node._votes = ['2']
  node._commitSeq = 4n
  node._commitTerm = 3n

  const stopTimers = node._stopTimers.bind(node)
  let stopTimerCalls = 0
  node._stopTimers = () => {
    stopTimerCalls++
    stopTimers()
  }

  node._stopRaft()
  t.ok(node._closing, 'establishes the terminal shutdown barrier')
  t.ok(node._shutdownError instanceof ErrorWithCode,
    'shutdown uses ErrorWithCode')
  t.equal(node._shutdownError.message, 'node not open',
    'shutdown has the not-open message')
  t.equal(node._shutdownError.code, NODE_NOT_OPEN,
    'shutdown has the NODE_NOT_OPEN code')
  t.equal(node._shutdownError.sqlCode, null,
    'shutdown has no SQLite code')
  t.notOk(node.isOpen, 'makes the protocol unavailable')
  t.equal(node.state, null, 'resets the protocol role')
  t.equal(node.leader, null, 'resets the known leader')
  t.deepEqual(node.followers, [], 'resets follower state')
  t.equal(node.term, null, 'resets the volatile term')
  t.equal(node._votedFor, null, 'resets the volatile vote')
  t.equal(node._pingms, 0, 'resets ping state')
  t.equal(node._pongs.size, 0, 'resets pong state')
  t.deepEqual(node._votes, [], 'resets vote collection')
  t.equal(node._commitSeq, -1n, 'resets the volatile commit index')
  t.equal(node._commitTerm, -1n, 'resets the volatile commit term')
  t.equal(stopTimerCalls, 1, 'timer cleanup comes from the reset')
  t.deepEqual(changes.map((change) => change.open), [true, false],
    'publishes exactly one open and one closed state')
  t.equal(changes[1].state, null,
    'the closed publication observes reset protocol state')
  t.deepEqual(storageAtClosedChange, [true],
    'the closed publication leaves DB1 physically open')
  t.deepEqual(log.calls, ['open'],
    'protocol shutdown does not perform storage cleanup')

  node._stopRaft()
  t.equal(stopTimerCalls, 1, 'repeated shutdown does not reset twice')
  t.equal(changes.length, 2, 'repeated shutdown does not republish closed state')
  t.deepEqual(log.calls, ['open'],
    'repeated protocol shutdown still leaves DB1 alone')

  node.close()
  t.deepEqual(log.calls, ['open', 'close'],
    'close makes the one physical DB1 close attempt')
  t.notOk(log.isOpen, 'DB1 is closed before close returns')
  t.equal(stopTimerCalls, 1,
    'close reuses the completed protocol shutdown transition')
  t.equal(changes.length, 2, 'close does not republish closed state')
  node.close()
  t.deepEqual(log.calls, ['open', 'close'],
    'repeated close skips an already released DB1 handle')
  t.end()
})

test('_throwIfClosing reports a coded not-open precondition', (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog())
  node.close()

  const err = thrown(() => node._throwIfClosing())
  t.ok(err instanceof ErrorWithCode, 'uses ErrorWithCode')
  t.equal(err?.message, 'node not open', 'uses the not-open message')
  t.equal(err?.code, NODE_NOT_OPEN, 'has the NODE_NOT_OPEN code')
  t.equal(err?.sqlCode, null, 'has no SQLite code')
  t.notEqual(err, node._shutdownError,
    'does not expose the terminal cancellation as a precondition error')
  t.end()
})

test('node lifecycle calls are idempotent', (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const changes = []
  node.on('change', (change) => changes.push(change))

  node.open()
  node.open()
  t.deepEqual(log.calls, ['open'], 'opens the log once')
  t.equal(changes.length, 1, 'emits one open change')
  t.ok(node.isOpen, 'node is open')

  node._commitSeq = 4n
  node.close()
  t.equal(node._closing, true, 'marks the node as terminally closing')
  t.notOk(node.isOpen, 'node is unavailable when close returns')
  node.close()
  t.equal(node._closing, true, 'retains the terminal closing fence after success')
  t.deepEqual(log.calls, ['open', 'close'], 'closes the log once')
  t.equal(changes.length, 2, 'emits one close change')
  t.notOk(node.isOpen, 'node is closed')
  t.equal(node._commitSeq, -1n, 'resets the volatile commit index')
  t.equal(node._commitTerm, -1n, 'resets the volatile commit term')
  t.deepEqual(node.eventNames(), [], 'close removes all node listeners')
  t.end()
})

test('awaitEvent rejects every value thrown by its callback', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog(), {
    electionTimeout: 60_000,
  })
  t.teardown(() => node.close())
  node.open()
  const failure = new Error('callback failed')
  const stack = failure.stack
  const coded = new ErrorWithCode('coded callback failure', ARGUMENT_ILLEGAL)

  for (const reason of [undefined, null, false, 0, 0n, '', NaN, failure, coded]) {
    const label = typeof reason + ':' + String(reason)
    const waiting = node.awaitEvent('custom-event', () => { throw reason })
    const outcome = waiting.then(
      (value) => ({ status: 'fulfilled', value }),
      (error) => ({ status: 'rejected', error }),
    )
    node.emit('custom-event', 'value')
    const result = await outcome

    t.equal(result.status, 'rejected', `${label}: callback failure rejects`)
    t.ok(Object.is(result.error, reason), `${label}: preserves the exact thrown value`)
    t.equal(node.listenerCount('custom-event'), 0, `${label}: removes the event listener`)
    t.equal(node._shutdownWaiters.size, 0, `${label}: removes the shutdown registration`)
  }
  t.equal(failure.stack, stack, 'the callback error retains its original stack')
})

test('awaitEvent filters events and returns the accepted value unchanged', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog(), {
    electionTimeout: 60_000,
  })
  t.teardown(() => node.close())
  node.open()

  for (const value of [false, null, undefined, { accepted: true }]) {
    const seen = []
    const waiting = node.awaitEvent('custom-event', (event) => {
      seen.push(event)
      return event !== 'skip'
    })
    node.emit('custom-event', 'skip')
    t.equal(node.listenerCount('custom-event'), 1, 'an unmatched event leaves the wait active')
    node.emit('custom-event', value)
    t.equal(await waiting, value, 'returns the exact accepted value, including undefined')
    t.equal(node.listenerCount('custom-event'), 0, 'success removes the event listener')
    t.equal(node._shutdownWaiters.size, 0, 'success removes the shutdown registration')
    node.emit('custom-event', 'later')
    t.deepEqual(seen, ['skip', value], 'later events do not call the completed callback')
  }
})

test('awaitEvent rejects if its callback closes the node before returning a match', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog(), {
    electionTimeout: 60_000,
  })
  t.teardown(() => node.close())
  node.open()
  const waiting = node.awaitEvent('custom-event', () => {
    node.close()
    return true
  })
  node.emit('custom-event', 'value')

  t.equal(await rejection(waiting), node._shutdownError,
    'shutdown rejection takes precedence over the later match')
  t.equal(node.listenerCount('custom-event'), 0, 'shutdown removes the event listener')
  t.equal(node._shutdownWaiters.size, 0, 'shutdown removes the registration')
})

test('close rejects and detaches pending public waiters without waiting for them', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()

  const waiters = [
    ['leader', rejection(node.awaitLeader(false))],
    ['committed leader', rejection(node.awaitLeader(true))],
    ['custom event', rejection(node.awaitEvent('custom-event', () => true))],
  ]
  t.equal(node.listenerCount('change'), 1, 'plain leader waiter owns one change listener')
  t.equal(node.listenerCount('commit'), 1, 'commit leader waiter owns one commit listener')
  t.equal(node.listenerCount('custom-event'), 1, 'event waiter owns one custom listener')

  node.close()

  const waiterPending = Symbol('waiter pending')
  for (const [name, waiter] of waiters) {
    const err = await Promise.race([
      waiter,
      new Promise((resolve) => setImmediate(() => resolve(waiterPending))),
    ])
    t.notEqual(err, waiterPending, `${name} waiter settles during close`)
    t.equal(err?.message, 'node not open', `${name} waiter rejects on close`)
  }
  t.equal(node.listenerCount('change'), 0, 'close removes the leader change listener')
  t.equal(node.listenerCount('commit'), 0, 'close removes the leader commit listener')
  t.equal(node.listenerCount('custom-event'), 0, 'close removes the custom event listener')
})

test('shutdown clears quorum deadlines with only one available follower', async (t) => {
  const nodes = new Map()
  const errors = []
  const timers = new Set()
  const appendTimeout = 5_000
  const setTimeout = global.setTimeout
  const clearTimeout = global.clearTimeout
  let blocked = false

  try {
    for (const id of ['1', '2']) {
      const node = new ProductionRaftNode(id, ids, (to, msg) => {
        if (blocked && msg.type === 'append' && msg.data) { return }
        return nodes.get(to)?.onReceive(id, msg)
      }, ':memory:', {
        electionTimeout: id === '1' ? 20 : 60_000,
        pingTimeout: 60_000,
        appendTimeout,
      })
      node.on('error', (err) => errors.push(err))
      nodes.set(id, node)
    }
    for (const node of nodes.values()) { node.open() }
    await Promise.all([...nodes.values()].map((node) => node.awaitLeader(true)))
    await new Promise(setImmediate)
    const leader = nodes.get('1')
    t.equal(leader.state, 'leader', 'elects the first node normally')
    t.deepEqual(leader.followers, ['2'], 'only one of the configured peers is available')

    global.setTimeout = (fn, ms, ...args) => {
      const timer = setTimeout(fn, ms, ...args)
      if (ms === appendTimeout) { timers.add(timer) }
      return timer
    }
    global.clearTimeout = (timer) => {
      timers.delete(timer)
      clearTimeout(timer)
    }

    const [seq] = await leader.append(Buffer.from('completed'))
    await new Promise(setImmediate)
    t.equal(seq, 1n, 'normal appends still complete with the available quorum')
    t.equal(timers.size, 0, 'successful append clears its deadlines')
    t.equal(leader._shutdownWaiters.size, 0, 'successful append removes shutdown registrations')

    blocked = true
    const command = rejection(leader.append(Buffer.from('blocked')))
    await new Promise(setImmediate)
    t.ok(timers.size > 0, 'blocked replication has active deadlines')
    for (const node of nodes.values()) { node.close() }
    const err = await command
    await new Promise(setImmediate)
    t.equal(err?.code, NODE_NOT_OPEN, 'public append rejects on shutdown')
    t.equal(timers.size, 0, 'shutdown clears every append deadline without waiting for timeout')
    t.equal(leader._shutdownWaiters.size, 0, 'shutdown removes all operation registrations')
    t.equal(leader._acks.size, 0, 'shutdown removes all response waiters')
    t.deepEqual(errors, [], 'shutdown produces no fatal errors')
  } finally {
    for (const node of nodes.values()) { node.close() }
    for (const timer of timers) { clearTimeout(timer) }
    global.setTimeout = setTimeout
    global.clearTimeout = clearTimeout
  }
})

test('shutdown races retain only active operations', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog())
  let release = null
  const work = new Promise((resolve) => { release = resolve })
  const raced = node._raceShutdown(work)

  t.equal(node._shutdownWaiters.size, 1,
    'an active operation registers one removable shutdown waiter')
  release('done')
  t.equal(await raced, 'done', 'the operation returns its result')
  t.equal(node._shutdownWaiters.size, 0,
    'a completed operation removes its shutdown waiter')

  const pending = node._raceShutdown(new Promise(() => {}))
  t.equal(node._shutdownWaiters.size, 1,
    'a pending operation remains interruptible')
  node.close()
  const err = await rejection(pending)
  t.equal(err, node._shutdownError,
    'close rejects the pending operation with the shared shutdown error')
  t.equal(err?.message, 'node not open', 'shutdown has the not-open message')
  t.equal(err?.code, NODE_NOT_OPEN, 'shutdown has the NODE_NOT_OPEN code')
  t.equal(node._shutdownWaiters.size, 0,
    'an interrupted operation removes its shutdown waiter')

  const late = node._raceShutdown(new Promise(() => {}))
  const lateErr = await rejection(late)
  t.equal(lateErr, node._shutdownError,
    'an operation registered after close observes the shutdown error')
  t.equal(node._shutdownWaiters.size, 0,
    'a post-close operation leaves no shutdown waiter')

  const closing = new RaftNode('1', ids, () => {}, lifecycleLog())
  closing._closing = true
  const fallbackErr = await rejection(
    closing._raceShutdown(new Promise(() => {})),
  )
  t.ok(fallbackErr instanceof ErrorWithCode,
    'the defensive shutdown fallback uses ErrorWithCode')
  t.equal(fallbackErr?.message, 'node not open',
    'the defensive shutdown fallback has the not-open message')
  t.equal(fallbackErr?.code, NODE_NOT_OPEN,
    'the defensive shutdown fallback has the NODE_NOT_OPEN code')
  t.equal(fallbackErr?.sqlCode, null,
    'the defensive shutdown fallback has no SQLite code')
})

test('close before first open makes later open calls fail', (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })

  node.close()
  const first = thrown(() => node.open())
  const second = thrown(() => node.open())

  t.match(first?.message ?? '', /node not open/,
    'close before open permanently rejects the first open')
  t.match(second?.message ?? '', /node not open/,
    'the terminal rejection remains stable')
  t.equal(second?.message, first?.message, 'repeated opens report the same terminal state')
  t.deepEqual(log.calls, [], 'the closed instance never opens its log')
  t.notOk(node.isOpen, 'the node remains unavailable')

  if (node.isOpen) { node.close() }
  t.end()
})

test('open remains permanently rejected after a successful close', (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })

  node.open()
  node.close()
  const first = thrown(() => node.open())
  const second = thrown(() => node.open())

  t.match(first?.message ?? '', /node not open/,
    'the closed instance cannot be reopened')
  t.match(second?.message ?? '', /node not open/,
    'later opens remain rejected')
  t.equal(second?.message, first?.message, 'the terminal rejection is stable')
  t.deepEqual(log.calls, ['open', 'close'], 'rejected opens do not touch storage')
  t.notOk(node.isOpen, 'successful close is terminal')

  if (node.isOpen) { node.close() }
  t.end()
})

test('fatal errors close synchronously and report after the closed change', async (t) => {
  const first = new Error('first fatal')
  const second = new Error('second fatal')
  const log = lifecycleLog()
  let closeCalls = 0
  log.close = () => {
    log.calls.push('close')
    closeCalls++
    log.isOpen = false
  }
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const errors = []
  const fatals = []
  const timeline = []
  let resolveDrained = null
  const drained = new Promise((resolve) => { resolveDrained = resolve })
  node.on('change', (change) => timeline.push(change.open ? 'open' : 'closed'))
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => {
    errors.push(err)
    timeline.push(`error:${err.message}`)
    if (errors.length === 2) { resolveDrained() }
  })

  node.open()
  timeline.length = 0

  node.emit('fatal', first)
  node.emit('fatal', second)
  t.equal(closeCalls, 1, 'fatal handling closes storage immediately and once')
  t.notOk(log.isOpen, 'fatal handling releases storage before returning')
  t.deepEqual(timeline, ['closed'], 'closed change is synchronous')
  t.deepEqual(errors, [], 'public error reporting is deferred')
  t.deepEqual(fatals, [first, second],
    'fatal listeners receive every source error before cleanup')
  t.ok(node.eventNames().includes('error'),
    'fatal close retains listeners while public errors are pending')

  await drained
  t.deepEqual(errors, [first, second], 'source errors retain occurrence order')
  t.deepEqual(timeline, ['closed', 'error:first fatal', 'error:second fatal'],
    'fatal errors are re-emitted after the closed-state change')
  t.deepEqual(node.eventNames(), [],
    'fatal close removes listeners after public error reporting')
})

for (const throwingListener of [false, true]) {
  const mode = throwingListener ? 'a throwing error listener' : 'no error listener'
  test(`fatal reporting finishes cleanup with ${mode}`, async (t) => {
    const first = new Error('first fatal')
    const second = new Error('second fatal')
    const closeFailure = new Error('close failed')
    const log = lifecycleLog()
    const closeLog = log.close.bind(log)
    log.close = () => {
      closeLog()
      throw closeFailure
    }
    const node = new RaftNode('1', ids, () => {}, log, {
      electionTimeout: 60_000,
    })
    const errors = []
    if (throwingListener) {
      node.on('error', (err) => {
        errors.push(err)
        throw new Error('error listener failed')
      })
    }
    node.open()
    node.emit('fatal', first)
    node.emit('fatal', second)
    t.notOk(node.isOpen, 'fatal handling still closes the node synchronously')

    await Promise.resolve()
    if (throwingListener) {
      t.deepEqual(errors, [first, second, closeFailure],
        'listener exceptions do not prevent later reports or get re-emitted')
    }
    t.deepEqual(node._fatalErrors, [], 'drains the fatal error queue')
    t.deepEqual(node._fatalCloseErrors, [], 'drains the close error queue')
    t.equal(node._fatalScheduled, false, 'finishes the scheduled reporting pass')
    t.deepEqual(node.eventNames(), [], 'removes listeners after reporting')
  })
}

test('close failure remains terminal while allowing storage cleanup retry', (t) => {
  const closeFailure = new Error('close failed')
  const log = lifecycleLog()
  let closeCalls = 0
  log.close = () => {
    log.calls.push('close')
    closeCalls++
    if (closeCalls === 1) { throw closeFailure }
    log.isOpen = false
  }
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })

  node.open()
  const failed = thrown(() => node.close())
  t.equal(failed, closeFailure, 'the first close preserves its storage failure')
  t.equal(node._closing, true, 'retains the terminal closing fence after failure')
  t.ok(log.isOpen, 'failed close retains the open storage handle')
  t.notOk(node.isOpen, 'failed close still makes the node unavailable')
  t.deepEqual(node.eventNames(), [],
    'failed close still removes all node listeners')

  const beforeRetry = thrown(() => node.open())
  t.match(beforeRetry?.message ?? '', /node not open/,
    'open is forbidden while terminal storage cleanup is incomplete')

  node.close()
  t.equal(node._closing, true, 'retains the terminal fence after a successful retry')
  t.equal(closeCalls, 2, 'an idempotent close retries physical storage cleanup')
  t.notOk(log.isOpen, 'the close retry releases storage')
  const afterRetry = thrown(() => node.open())
  t.match(afterRetry?.message ?? '', /node not open/,
    'successful cleanup does not make the closed instance reusable')
  t.notOk(node.isOpen, 'the closed instance remains unavailable after cleanup')

  if (node.isOpen) { node.close() }
  t.end()
})

test('received responses advance only valid newer terms', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'leader'
  node.leader = node.id

  await node.onReceive('2', { type: 'ack', term: null, cid: 'invalid-term' })
  await node.onReceive('2', {
    type: 'ack', term: 9_223_372_036_854_775_808n, cid: 'oversized-term',
  })
  await node.onReceive('2', { type: 'ack', term: 0n, cid: 'current-term' })
  t.equal(node.term, 0n, 'invalid and current terms are ignored')
  t.equal(node.state, 'leader', 'current term does not step down')

  await node.onReceive('2', { type: 'ack', term: 1n, cid: 'newer-term' })
  t.equal(node.term, 1n, 'newer term is accepted')
  t.equal(log.elec.term, 1n, 'newer term is persisted')
  t.equal(node.state, 'follower', 'newer term steps down')

  node.close()
})

test('closed nodes discard messages before term handling', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.close()

  await node.onReceive('2', {
    type: 'ack', term: 1n, cid: 'closed-newer-term',
  })

  t.equal(node.term, null, 'the closed node does not advance its volatile term')
  t.equal(log.elec.term, 0n, 'the closed node does not persist the received term')
})

test('received response callback failure is fatal and consumed', async (t) => {
  const failure = new Error('response callback failed')
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  const fatals = []
  node.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))
  node.open()
  node._acks.set('failed-response', (from, msg, err) => {
    if (err) {
      node._acks.delete('failed-response')
      return
    }
    throw failure
  })

  await node.onReceive('2', {
    type: 'ack', term: node.term, cid: 'failed-response',
  })
  t.equal(await errored, failure, 'reports the callback failure')
  t.deepEqual(fatals, [failure], 'emits the callback failure once as fatal')
  t.notOk(node.isOpen, 'the callback failure terminally closes the node')
})

test('messages from unknown node ids are ignored', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'candidate'
  node.term = 1n
  node._votedFor = '1'

  await node.onReceive('999', { type: 'vote', term: 1n, voteGranted: true })
  await node.onReceive('999', { type: 'ack', term: 2n })

  t.equal(node.state, 'candidate', 'unknown vote cannot elect the candidate')
  t.deepEqual(node._votes, [], 'unknown vote is not counted')
  t.equal(node.term, 1n, 'unknown response cannot advance the term')
  node.close()
})

test('malformed message values and append payloads are ignored or rejected', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()

  for (const msg of [null, undefined, 1, 'message', [], Buffer.alloc(0)]) {
    await node.onReceive('2', msg)
  }
  t.equal(node.term, 0n, 'non-message values do not change Raft state')
  t.equal(sent.length, 0, 'non-message values produce no replies')

  await node.onReceive('2', {
    type: 'append', cid: 'short-entry', term: 0n,
    termP: -1n, seqP: -1n, commitSeq: -1n,
    data: [Buffer.alloc(7)],
  })
  t.equal(node.leader, null, 'invalid AppendEntries does not install a leader')
  t.equal(sent.length, 1, 'invalid AppendEntries with a CID receives one reply')
  t.equal(sent[0][1].type, 'err', 'invalid AppendEntries receives ERR')

  await node.onReceive('2', {
    type: 'append', cid: 'overflow-entry', term: 0n,
    termP: 0n, seqP: 9_223_372_036_854_775_807n, commitSeq: -1n,
    data: [Buffer.alloc(8)],
  })
  t.equal(sent.length, 2, 'overflowing AppendEntries receives one reply')
  t.equal(sent[1][1].type, 'err', 'overflowing AppendEntries receives ERR')
  t.equal(sent[1][1].msg, 'append end seq is illegal',
    'overflow is rejected before reaching the log')
  t.ok(node.isOpen, 'overflowing AppendEntries does not close the node')

  node.state = 'leader'
  node.leader = node.id
  await node.onReceive('2', {
    type: 'append', cid: 'invalid-forward', term: 0n, data: { nope: true },
  })
  t.equal(sent.length, 3, 'invalid forwarded append receives one reply')
  t.equal(sent[2][1].type, 'err', 'invalid forwarded append receives ERR')

  for (const [index, label] of [
    [0, 'invalid AppendEntries'],
    [1, 'overflowing AppendEntries'],
    [2, 'invalid forwarded append'],
  ]) {
    t.equal(sent[index][1].code, RPC_ILLEGAL,
      `${label} uses RPC_ILLEGAL`)
    t.equal(sent[index][1].sqlCode, null,
      `${label} has no SQLite code`)
    t.deepEqual(Object.keys(sent[index][1]).sort(),
      ['cid', 'code', 'from', 'msg', 'sqlCode', 'stack', 'term', 'type'],
      `${label} uses the common RPC error envelope`)
  }

  node.close()
})

test('Raft RPC handlers report different terms consistently', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids,
    (to, msg) => sent.push([to, msg]), log, {
      electionTimeout: 60_000,
      pingTimeout: 60_000,
    })
  node.open()
  node.state = 'leader'
  node.leader = node.id
  node.term = 2n

  await node.onReceive('2', {
    type: 'append', cid: 'append-term-diff', term: 1n,
    data: Buffer.from('value'),
  })
  node.state = 'follower'
  node.leader = '2'
  await node.onReceive('2', {
    type: 'append', cid: 'entries-term-diff', term: 1n,
    termP: -1n, seqP: -1n, commitSeq: -1n,
  })
  node.state = 'leader'
  node.leader = node.id
  node._rxAppendToLeader({
    type: 'append', cid: 'append-term-greater', term: 3n,
    data: Buffer.from('value'),
  }, '2')
  t.equal(sent.length, 3, 'each different-term RPC receives one response')
  for (const [index, label, message] of [
    [0, 'forwarded append', 'term is lesser'],
    [1, 'AppendEntries', 'term is lesser'],
    [2, 'misrouted higher-term append', 'higher term append routed to leader'],
  ]) {
    t.equal(sent[index][1].type, 'err', `${label} receives ERR`)
    t.equal(sent[index][1].msg, message,
      `${label} retains its specific term diagnostic`)
    t.equal(sent[index][1].code, TERM_DIFF,
      `${label} uses TERM_DIFF`)
    t.equal(sent[index][1].sqlCode, null,
      `${label} has no SQLite code`)
  }
  node.close()
})

test('forwarded Raft handlers transmit normalized SQLite metadata', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids,
    (to, msg) => sent.push([to, msg]), log, {
      electionTimeout: 60_000,
      pingTimeout: 60_000,
    })
  node.open()
  node.state = 'leader'
  node.leader = node.id

  const sqlite = new Error('constraint failed')
  sqlite.code = 'ERR_SQLITE_ERROR'
  sqlite.errcode = 19
  node._appendToSelfAndFollowers = () => Promise.reject(sqlite)

  await node.onReceive('2', {
    type: 'append', term: node.term, cid: 'sqlite-forward',
    data: Buffer.from('value'),
  })
  await new Promise((resolve) => setImmediate(resolve))

  t.equal(sent.length, 1, 'sends one forwarded failure')
  t.deepEqual(sent[0], ['2', {
    type: 'err',
    term: node.term,
    cid: 'sqlite-forward',
    msg: 'constraint failed',
    code: SQLITE_ERROR,
    sqlCode: 19,
    stack: sqlite.stack,
    from: '1',
  }], 'transmits normalized codes and the original SQLite trace')

  node.close()
})

test('candidate waits for a majority before becoming leader', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node._leaderAppendNoOp = () => {}

  node._voteForSelf()
  t.equal(node.state, 'candidate', 'self vote alone is not a majority')

  await node.onReceive('2', {
    type: 'vote',
    term: node.term,
    voteGranted: 'true',
  })
  t.equal(node.state, 'candidate', 'a non-boolean vote is ignored')

  await node.onReceive('2', {
    type: 'vote',
    term: node.term,
    voteGranted: true,
  })
  t.equal(node.state, 'leader', 'one peer vote plus self forms a majority')
  node.close()
})

test('current-term commit stops a scheduled leader no-op retry', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog(), {
    pingTimeout: 100,
  })
  node._open = true
  node.state = 'leader'
  node.leader = node.id
  node.term = 0n
  const leaderReady = node._beginLeaderReady(node.term)
  const warnings = []
  let attempts = 0
  let rejectAppend = null
  node.on('warn', (err) => warnings.push(err))
  node._appendToSelfAndFollowers = () => {
    attempts++
    return new Promise((_, reject) => { rejectAppend = reject })
  }

  node._leaderAppendNoOp()
  rejectAppend(new Error('leader no-op operation timed out'))
  await new Promise((resolve) => setImmediate(resolve))
  t.equal(warnings.length, 1, 'the failed attempt is reported')
  t.equal(node._shutdownWaiters.size, 1, 'the retry delay is active')

  node._commitTerm = node.term
  leaderReady.resolve(0n)
  await new Promise((resolve) => setTimeout(resolve, 30))

  t.equal(attempts, 1, 'the committed readiness entry prevents another no-op')
  t.equal(node._shutdownWaiters.size, 0, 'the completed retry delay detaches')
  node.close()
})

test('leader no-op retries stay with their original leadership', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog(), {
    electionTimeout: 60_000,
  })
  t.teardown(() => node.close())
  const attempts = []
  const retries = []
  let rejectAppend = null
  node._appendToSelfAndFollowers = () => {
    attempts.push(node.term)
    return new Promise((_, reject) => { rejectAppend = reject })
  }
  node._delay = () => new Promise((resolve) => retries.push(resolve))
  node._startPingTimer = () => {}
  node.open()

  node._voteForSelf()
  await node.onReceive('2', { type: 'vote', term: node.term, voteGranted: true })
  const originalReady = node._leaderReady
  rejectAppend(new Error('first-term no-op failed'))
  await new Promise((resolve) => setImmediate(resolve))
  t.equal(retries.length, 1, 'the first leadership schedules a retry')

  node._advanceTerm(2n)
  node._voteForSelf()
  await node.onReceive('2', { type: 'vote', term: node.term, voteGranted: true })
  t.notEqual(node._leaderReady, originalReady, 'the new leadership has its own readiness')
  t.deepEqual(attempts, [1n, 3n], 'each election starts one no-op')

  retries.shift()()
  await new Promise((resolve) => setImmediate(resolve))
  t.deepEqual(attempts, [1n, 3n], 'the old retry does not append in the new term')

  rejectAppend(new Error('current-term no-op failed'))
  await new Promise((resolve) => setImmediate(resolve))
  t.equal(retries.length, 1, 'the current leadership still schedules its own retry')
  retries.shift()()
  await new Promise((resolve) => setImmediate(resolve))
  t.deepEqual(attempts, [1n, 3n, 3n], 'the current leadership can retry its no-op')
})

test('awaitLeader with commit waits past a granted vote', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()

  let resolved = false
  const waiting = node.awaitLeader(true).then(() => { resolved = true })
  await node.onReceive('2', {
    type: 'vote_request',
    term: 1n,
    termP: -1n,
    seqP: -1n,
  })
  await Promise.resolve()

  t.equal(node.leader, null, 'granted vote does not establish a leader')
  t.notOk(resolved, 'granted vote alone does not satisfy commit readiness')
  t.equal(node.listenerCount('change'), 0, 'commit readiness does not subscribe to changes')
  node._commitTerm = node.term - 1n
  node.emit('commit', 0n)
  await Promise.resolve()
  t.notOk(resolved, 'a stale-term commit does not satisfy commit readiness')
  log.term = node.term
  await Promise.resolve()
  t.notOk(resolved, 'an uncommitted current-term head does not satisfy commit readiness')
  await node.onReceive('2', {
    type: 'append',
    term: node.term,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'establish-leader',
  })
  t.equal(node.leader, '2', 'AppendEntries establishes the leader')
  node._commitTerm = node.term
  node.emit('commit', 0n)
  await waiting
  t.ok(resolved, 'commit satisfies readiness')
  t.equal(node.listenerCount('commit'), 0, 'commit readiness removes its listener')
  await node.awaitLeader(true)
  t.pass('a current-term commit satisfies later readiness calls')
  node.close()
})

test('same-term AppendEntries discovers its leader directly', async (t) => {
  const log = lifecycleLog()
  log.seq = 5n
  log.term = 1n
  log.elec.term = 2n
  log.elec.votedFor = '1'
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'candidate'
  node.leader = null

  await node.onReceive('2', {
    type: 'append',
    term: 2n,
    termP: 1n,
    seqP: 4n,
    commitSeq: -1n,
    cid: 'discover',
  })

  t.equal(node.state, 'follower', 'candidate becomes a follower')
  t.equal(node.leader, '2', 'records the AppendEntries sender as leader')
  t.equal(node._votedFor, '1', 'retains its same-term vote')
  t.equal(log.elec.votedFor, '1', 'keeps the persisted vote unchanged')
  t.equal(sent.length, 1, 'sends one response')
  t.equal(sent[0][1].type, 'ack', 'responds through AppendEntries rather than RequestVote')

  node.close()
})

test('higher-term AppendEntries persists its term and continues', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'candidate'
  let electionTimers = 0
  const startElectionTimer = node._startElectionTimer.bind(node)
  node._startElectionTimer = () => {
    electionTimers++
    startElectionTimer()
  }
  await node.onReceive('2', {
    type: 'append',
    term: 1n,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'higher-term',
  })

  t.equal(node.term, 1n, 'advances to the leader term')
  t.equal(node.state, 'follower', 'steps down to follower')
  t.equal(node.leader, '2', 'records the higher-term leader')
  t.ok(node._pingms > 0, 'records leader contact')
  t.equal(electionTimers, 1, 'starts the follower election timer')
  t.equal(log.elec.term, 1n, 'persists the leader term')
  t.equal(sent.length, 1, 'responds to the first higher-term AppendEntries')
  t.equal(sent[0][1].type, 'ack', 'processes the RPC instead of requiring a retry')

  node.close()
})

test('higher-term AppendEntries stops when election persistence fails', async (t) => {
  const failure = new Error('append election write failed')
  const log = lifecycleLog()
  let appends = 0
  log.election = () => { throw failure }
  log.appendBatch = () => { appends++ }
  const sent = []
  const fatals = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))
  node.open()

  const entry = Buffer.alloc(8)
  entry.writeBigUInt64LE(1n)
  await node.onReceive('2', {
    type: 'append',
    term: 1n,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'failed-higher-term',
    data: [entry],
  })

  t.equal(await errored, failure, 'reports the election persistence failure')
  t.deepEqual(fatals, [failure], 'emits the original failure once as fatal')
  t.equal(appends, 0, 'does not mutate the log after election persistence fails')
  t.deepEqual(sent, [], 'does not ACK or reject the AppendEntries request')
  t.notOk(node.isOpen, 'terminally closes the node')
})

test('leader processes higher-term AppendEntries through the follower path', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'leader'
  node.leader = '1'

  await node.onReceive('2', {
    type: 'append',
    term: 1n,
    termP: -1n,
    seqP: -1n,
    commitSeq: -1n,
    cid: 'higher-leader',
  })

  t.equal(node.state, 'follower', 'steps down to follower')
  t.equal(node.leader, '2', 'records the higher-term AppendEntries sender')
  t.equal(log.elec.term, 1n, 'persists the higher term')
  t.equal(sent.length, 1, 'responds to the first RPC')
  t.equal(sent[0][1].type, 'ack', 'processes the RPC through follower handling')
  node.close()
})

test('correlated send validates responses and preserves timeout errors', async (t) => {
  const log = lifecycleLog()
  let node = null
  const registered = []
  const validated = []
  let sends = 0
  node = new RaftNode('1', ids, (to, msg) => {
    sends++
    registered.push(node._acks.has(msg.cid))
    node.onReceive(to, {
      type: 'ack',
      term: node.term,
      cid: msg.cid,
      accepted: false,
    })
    if (msg.cid === 'response-timeout') { return }
    return node.onReceive(to, {
      type: 'ack',
      term: node.term,
      cid: msg.cid,
      accepted: true,
    })
  }, log, {
    electionTimeout: 60_000,
  })
  node.open()

  const originalSetTimeout = global.setTimeout
  const delays = []
  global.setTimeout = (fn, ms, ...args) => {
    delays.push(ms)
    return originalSetTimeout(fn, 0, ...args)
  }
  let response = null
  let timeout = null
  const responseTimeout = new ErrorWithCode('ping timeout', PING_TIMEOUT)
  const timeoutErr = new ErrorWithCode('append timeout', APPEND_TIMEOUT)
  const validate = (msg) => {
    validated.push(msg.accepted)
    return msg.accepted === true
  }
  try {
    response = await node._sendAndAwaitResponse('2', {
      type: 'append',
      term: node.term,
      cid: 'synchronous-response',
    }, 37, responseTimeout, validate)
    timeout = await rejection(node._sendAndAwaitResponse('2', {
      type: 'append',
      term: node.term,
      cid: 'response-timeout',
    }, 41, timeoutErr, validate))
  } finally {
    global.setTimeout = originalSetTimeout
  }

  t.deepEqual(registered, [true, true],
    'registers each CID before calling the transport')
  t.equal(sends, 2, 'sends each correlated message exactly once')
  t.deepEqual(delays, [37, 41],
    'uses each configured timeout without rounding')
  t.deepEqual(validated, [false, true, false],
    'ignores responses rejected by the validator')
  t.equal(response.type, 'ack', 'returns the raw correlated response')
  t.equal(response.accepted, true, 'returns the response accepted by validation')
  t.equal(timeout, timeoutErr, 'rejects with the exact supplied timeout error')
  t.ok(timeout instanceof ErrorWithCode, 'timeout uses ErrorWithCode')
  t.equal(timeout.code, APPEND_TIMEOUT, 'timeout preserves its error code')
  t.equal(timeout.message, 'append timeout', 'timeout preserves its message')
  t.equal(timeout.sqlCode, null, 'timeout has no SQLite code')
  t.equal(node._acks.size, 0, 'removes the settled CID waiter')

  node.close()
})

test('correlated send rejects after close without registering work', async (t) => {
  let sends = 0
  const node = new RaftNode('1', ids, () => { sends++ }, lifecycleLog())
  node.open()
  node.close()

  const setTimeout = global.setTimeout
  let timers = 0
  global.setTimeout = (fn, ms, ...args) => {
    timers++
    return setTimeout(fn, 0, ...args)
  }
  let work = null
  let err = null
  try {
    t.doesNotThrow(() => {
      work = node._sendAndAwaitResponse('2', { cid: 'closed-request' }, 10,
        new ErrorWithCode('append timeout', APPEND_TIMEOUT))
    }, 'shutdown rejection does not throw synchronously')
    t.ok(work instanceof Promise, 'returns a promise after shutdown')
    t.equal(node._acks.size, 0, 'does not register a response waiter')
    err = await rejection(work)
  } finally {
    global.setTimeout = setTimeout
  }
  t.equal(err?.code, NODE_NOT_OPEN, 'reports shutdown instead of a timeout')
  t.equal(timers, 0, 'does not schedule a timer')
  t.equal(sends, 0, 'does not call the transport')
})

test('synchronous transport shutdown prevents later correlated waits', async (t) => {
  const sent = []
  let node = null
  node = new RaftNode('1', ids, (to) => {
    sent.push(to)
    throw new Error('transport failed')
  }, lifecycleLog())
  node.open()
  node.on('warn', () => node.close())

  const work = ['2', '3'].map((to) => node._sendAndAwaitAck(to, {
    type: 'append', term: 0n, termP: -1n, seqP: -1n,
    commitSeq: -1n, cid: `close-during-send-${to}`,
  }, ACK_OPERATION.PING))
  t.notOk(node.isOpen, 'the first send closes the node synchronously')
  t.equal(node._acks.size, 0, 'later sends leave no response registrations')
  const errors = await Promise.all(work.map(rejection))
  t.ok(errors.every((err) => err?.code === NODE_NOT_OPEN),
    'both requests reject as closed, without synchronous throws')
  t.deepEqual(sent, ['2'], 'only the first request reaches the transport')
})

test('ACK operation descriptors select timeout policy and error', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 31,
    appendTimeout: 37,
  })
  node.open()

  const originalSetTimeout = global.setTimeout
  const delays = []
  const errors = []
  const cases = [
    [ACK_OPERATION.PING, 'append', 31, PING_TIMEOUT, 'ping timeout'],
    [ACK_OPERATION.APPEND, 'append', 37, APPEND_TIMEOUT, 'append timeout'],
  ]
  global.setTimeout = (fn, ms, ...args) => {
    delays.push(ms)
    return originalSetTimeout(fn, 0, ...args)
  }
  try {
    for (const [operation, type, delay, code, message] of cases) {
      const err = await rejection(node._sendAndAwaitAck('2', {
        type,
        term: node.term,
        cid: `descriptor-${message}-${delay}`,
      }, operation))
      errors.push(err)
      t.ok(err instanceof ErrorWithCode, `${message} uses ErrorWithCode`)
      t.equal(err.code, code, `${message} uses its operation code`)
      t.equal(err.message, message, `${message} uses its operation message`)
      t.equal(err.sqlCode, null, `${message} has no SQLite code`)
    }
  } finally {
    global.setTimeout = originalSetTimeout
  }

  t.deepEqual(delays, [31, 37],
    'descriptors select ping and append timeout options')
  t.equal(new Set(errors).size, cases.length,
    'each timeout receives a fresh error instance')
  t.ok(Object.isFrozen(ACK_OPERATION), 'descriptor enum is frozen')
  t.ok(cases.every(([operation]) => Object.isFrozen(operation)),
    'each operation descriptor is frozen')
  t.equal(node._acks.size, 0, 'removes every timed-out CID waiter')

  node.close()
})

test('leader discovers a returning follower from its ACK', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'leader'
  node.leader = node.id
  node.followers = ['2']

  const ack = node._sendAndAwaitAck(
    '3', {
      type: 'append', term: node.term, termP: -1n, seqP: -1n,
      commitSeq: -1n, cid: 'returning',
    }, ACK_OPERATION.PING
  )
  t.equal(sent.length, 1, 'sends the returning-follower probe once')
  await node.onReceive('3', {
    type: 'ack', term: node.term, cid: 'returning', seq: 0n,
  })
  await ack

  t.deepEqual(node.followers, ['2', '3'], 'adds the responding node as a follower')
  t.ok(node._pongs.has('3'), 'records the follower heartbeat')

  node.close()
})

test('ACK post-processing reports the not-open precondition', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()

  const cid = 'ack-before-close'
  const pending = node._sendAndAwaitAck('2', {
    type: 'append', term: node.term, termP: -1n, seqP: -1n,
    commitSeq: -1n, cid,
  }, ACK_OPERATION.PING)
  node.onReceive('2', { type: 'ack', term: node.term, cid, seq: 0n })
  node.close()

  const err = await rejection(pending)
  t.ok(err instanceof ErrorWithCode,
    'an ACK awaiting post-processing receives ErrorWithCode')
  t.equal(err?.message, 'node not open',
    'an ACK awaiting post-processing receives the not-open message')
  t.equal(err?.code, NODE_NOT_OPEN,
    'an ACK awaiting post-processing receives NODE_NOT_OPEN')
  t.equal(err?.sqlCode, null,
    'an ACK awaiting post-processing has no SQLite code')
})

test('malformed and lower-term correlated ACKs are ignored', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'leader'
  node.leader = node.id
  node.followers = ['2']
  node.term = 1n

  const cid = 'guarded-ack'
  const pending = node._sendAndAwaitAck('3', {
    type: 'append', term: node.term, termP: -1n, seqP: -1n,
    commitSeq: -1n, cid,
  }, ACK_OPERATION.PING)

  await node.onReceive('3', { type: 'ack', term: node.term, cid })
  t.equal(node._acks.has(cid), true, 'invalid ACK leaves its waiter active')
  t.notOk(node._pongs.has('3'), 'invalid ACK does not refresh liveness')
  t.deepEqual(node.followers, ['2'], 'invalid ACK does not restore a follower')

  await node.onReceive('3', { type: 'ack', term: null, cid, seq: 0n })
  t.equal(node._acks.has(cid), true,
    'ACK with an invalid term leaves its waiter active')

  await node.onReceive('3', { type: 'ack', term: node.term - 1n, cid, seq: 0n })
  t.equal(node._acks.has(cid), true, 'lower-term ACK leaves its waiter active')
  t.notOk(node._pongs.has('3'), 'lower-term ACK does not refresh liveness')
  t.deepEqual(node.followers, ['2'], 'lower-term ACK does not restore a follower')

  await node.onReceive('3', { type: 'ack', term: node.term, cid, seq: 0n })
  await pending
  t.equal(node._acks.has(cid), false, 'valid ACK settles and removes the waiter')
  t.ok(node._pongs.has('3'), 'valid ACK refreshes liveness')
  t.deepEqual(node.followers, ['2', '3'], 'valid ACK restores the follower')

  node.close()
})

test('forwarded result survives a term change before ACK post-processing', async (t) => {
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push(msg), lifecycleLog(), {
    electionTimeout: 60_000,
  })
  t.teardown(() => node.close())
  node.open()
  node.leader = '2'

  const pending = node.append(Buffer.from('command'))
  node.onReceive('2', {
    type: 'ack', term: 0n, cid: sent[0].cid, seq: 4n, results: 'completed',
  })
  node._advanceTerm(1n)

  t.deepEqual(await pending, [4n, 'completed'], 'returns the completed command result')
  t.equal(node.term, 1n, 'keeps the newer local term')
  t.notOk(node._pongs.has('2'), 'the earlier-term ACK does not restore cleared liveness')
  t.equal(node._acks.size, 0, 'the completed response waiter is removed')
})

test('higher-term correlated ACK reaches its caller after stepdown', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'leader'
  node.leader = node.id
  node.followers = ['2']

  const cid = 'higher-term-ack'
  const pending = node._sendAndAwaitAck('3', {
    type: 'append', term: node.term, termP: -1n, seqP: -1n,
    commitSeq: -1n, cid,
  }, ACK_OPERATION.PING)
  const response = { type: 'ack', term: node.term + 1n, cid, seq: 0n }

  await node.onReceive('3', response)

  t.equal(await pending, response, 'higher-term ACK is returned to its caller')
  t.equal(node.term, response.term, 'higher-term ACK advances the local term')
  t.equal(node.state, 'follower', 'higher-term ACK steps the leader down')
  t.equal(log.elec.term, response.term, 'higher-term ACK persists the new term')
  t.equal(node._acks.has(cid), false, 'higher-term ACK removes its waiter')
  t.notOk(node._pongs.has('3'), 'higher-term ACK does not refresh liveness')
  t.deepEqual(node.followers, [], 'higher-term ACK does not restore a follower')

  node.close()
})

test('Raft ERR responses normalize metadata and coded backtracking', async (t) => {
  const log = lifecycleLog()
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log, {
    electionTimeout: 60_000,
    appendTimeout: 20,
  })
  node.open()

  const pending = node._sendAndAwaitAck(
    '2', { type: 'append', term: node.term, cid: 'backtrack-error' },
    ACK_OPERATION.APPEND
  ).then(() => null, (err) => err)
  await node.onReceive('2', {
    type: 'err', term: null, cid: 'backtrack-error', msg: 'invalid term',
    code: REPL_BACKTRACK, sqlCode: null,
  })
  t.equal(node._acks.has('backtrack-error'), true,
    'ERR with an invalid term leaves its waiter active')
  const stack = 'Error: termP mismatch at seqP 4\n    at remoteAppend (/remote/node.js:10:3)'
  await node.onReceive('2', {
    type: 'err',
    term: node.term,
    cid: 'backtrack-error',
    msg: 'termP mismatch at seqP 4',
    code: REPL_BACKTRACK,
    sqlCode: null,
    stack,
  })
  const err = await pending

  t.equal(sent.length, 1, 'sends the request once')
  t.ok(err instanceof ErrorWithCode, 'reconstructs an ErrorWithCode')
  t.equal(err.message, 'append ERR termP mismatch at seqP 4',
    'the remote error identifies the Raft operation')
  t.equal(err.code, REPL_BACKTRACK, 'preserves the structured backtrack code')
  t.equal(err.sqlCode, null, 'backtracking has no SQLite code')
  t.equal(err.stack, stack, 'the operation prefix preserves the remote error trace')
  t.equal(node._acks.size, 0, 'removes the rejected CID waiter')

  for (const [cid, fields, label] of [
    ['missing-error-metadata', { backtrack: true }, 'missing'],
    ['null-error-metadata', { code: null, sqlCode: null }, 'null'],
  ]) {
    const missing = node._sendAndAwaitAck(
      '2', { type: 'append', term: node.term, cid },
      ACK_OPERATION.APPEND
    ).then(() => null, (error) => error)
    await node.onReceive('2', {
      type: 'err', term: node.term, cid, msg: `${label} metadata`, ...fields,
    })
    const normalized = await missing
    t.ok(normalized instanceof ErrorWithCode,
      `${label} metadata reconstructs ErrorWithCode`)
    t.equal(normalized.code, null,
      `${label} code normalizes to null`)
    t.equal(normalized.sqlCode, null,
      `${label} sqlCode normalizes to null`)
    t.notOk(Object.hasOwn(normalized, 'backtrack'),
      `${label} metadata ignores legacy backtrack fields`)
  }

  const rpcErrors = [
    ['native-sqlite-code', 'native SQLite error', 'ERR_SQLITE_ERROR', 19,
      SQLITE_ERROR, 19],
    ['invalid-sql-code', 'invalid sqlCode', SQLITE_ERROR, '19',
      SQLITE_ERROR, null],
    ['sqlite-error', 'database failed', SQLITE_ERROR, 19,
      SQLITE_ERROR, 19],
  ]
  for (const [cid, msg, code, sqlCode, expectedCode, expectedSqlCode]
    of rpcErrors) {
    const rpcPending = node._sendAndAwaitAck(
      '2', { type: 'append', term: node.term, cid },
      ACK_OPERATION.APPEND
    ).then(() => null, (error) => error)
    await node.onReceive('2', {
      type: 'err', term: node.term, cid, msg, code, sqlCode,
    })
    const rpcErr = await rpcPending
    t.ok(rpcErr instanceof ErrorWithCode,
      `${msg} reconstructs ErrorWithCode`)
    t.equal(rpcErr.message, `append ERR ${msg}`,
      `${msg} retains operation context`)
    t.equal(rpcErr.code, expectedCode, `${msg} normalizes code`)
    t.equal(rpcErr.sqlCode, expectedSqlCode, `${msg} normalizes sqlCode`)
  }

  const timedout = await node._sendAndAwaitAck(
    '2', { type: 'append', term: node.term, cid: 'append-timeout' },
    ACK_OPERATION.APPEND
  ).then(() => null, (error) => error)
  t.ok(timedout instanceof ErrorWithCode,
    'an AppendEntries timeout uses ErrorWithCode')
  t.equal(timedout.code, APPEND_TIMEOUT,
    'an AppendEntries timeout has APPEND_TIMEOUT')
  t.equal(timedout.sqlCode, null,
    'an AppendEntries timeout has no SQLite code')
  t.equal(timedout.message, 'append timeout',
    'an AppendEntries timeout identifies its RPC scope')
  node.close()
})

test('forwarded Raft errors normalize missing metadata by operation', async (t) => {
  const log = lifecycleLog()
  let node = null
  node = new RaftNode('1', ids, (to, msg) => {
    setImmediate(() => node.onReceive(to, {
      type: 'err', term: node.term, cid: msg.cid, msg: 'rejected',
    }))
  }, log, {
    electionTimeout: 60_000,
  })
  node.open()
  node.leader = '2'

  const appendErr = await rejection(node.append(Buffer.from('value')))
  t.ok(appendErr instanceof ErrorWithCode,
    'forwarded append reconstructs ErrorWithCode')
  t.equal(appendErr.message, 'append ERR rejected',
    'forwarded append ERR identifies its operation')
  t.equal(appendErr.code, null,
    'forwarded append defaults a missing code to null')
  t.equal(appendErr.sqlCode, null,
    'forwarded append defaults a missing sqlCode to null')
  node.close()
})

test('forwarding requires a known leader', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog())
  const appendErr = await rejection(node._fwdToLeader(Buffer.from('data')))

  t.ok(appendErr instanceof ErrorWithCode, 'append uses ErrorWithCode')
  t.equal(appendErr?.message, 'forward no leader',
    'append identifies its operation')
  t.equal(appendErr?.code, NO_LEADER, 'append uses NO_LEADER')
  t.equal(appendErr?.sqlCode, null, 'append has no SQLite code')
})

test('forwarded batch validates result cardinality at its boundary', async (t) => {
  const log = lifecycleLog()
  let node = null
  node = new RaftNode('1', ids, (to, msg) => {
    setImmediate(() => node.onReceive(to, {
      type: 'ack', term: node.term, cid: msg.cid, seq: 0n, results: [null],
    }))
  }, log, {
    electionTimeout: 60_000,
  })
  node.open()
  node.leader = '2'

  const err = await rejection(
    node.appendBatch([Buffer.from('one'), Buffer.from('two')]),
  )
  t.match(err.message, /forward results must be array with len 2/,
    'forwarded batch rejects the wrong number of results')
  t.equal(err.code, RAFT_ILLEGAL,
    'forwarded result cardinality is a Raft invariant failure')
  t.equal(err.sqlCode, null, 'Raft invariant failure has no SQLite code')

  node.close()
})

test('duplicate votes do not restore other pruned followers', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()
  node.state = 'leader'
  node.leader = node.id
  node._votes = ['2', '3']
  node.followers = ['3']

  node._rxVote({ term: node.term, voteGranted: true }, '3')

  t.deepEqual(node._votes, ['2', '3'], 'keeps votes unique without sorting')
  t.deepEqual(node.followers, ['3'], 'does not rebuild followers from historical votes')

  node.close()
})

test('append and appendBatch validate their input', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
  })
  node.open()

  const invalid = async (promise, pattern, label) => {
    const err = await rejection(promise)
    t.ok(err instanceof ErrorWithCode, `${label} uses ErrorWithCode`)
    t.equal(err?.code, ARGUMENT_ILLEGAL,
      `${label} has ARGUMENT_ILLEGAL code`)
    t.equal(err?.sqlCode, null, `${label} has no SQLite code`)
    t.match(err?.message, pattern, label)
  }

  await invalid(node.append(null), /^data must be non-empty buffer$/,
    'append requires a buffer')
  await invalid(node.append(Buffer.alloc(0)), /^data must be non-empty buffer$/,
    'append requires data')
  await invalid(node.appendBatch(null), /^data must be array with length > 0$/,
    'batch requires an array')
  await invalid(node.appendBatch([]), /^data must be array with length > 0$/,
    'batch requires entries')
  await invalid(node.appendBatch([Buffer.from('ok'), 'nope']),
    /^data must be array of non-empty buffers$/, 'batch requires buffers')
  await invalid(node.appendBatch([Buffer.from('ok'), Buffer.alloc(0)]),
    /^data must be array of non-empty buffers$/,
    'batch requires non-empty buffers')

  node.close()
})

test('leader local append failure closes the node', async (t) => {
  const failure = new Error('local append failed')
  const log = lifecycleLog()
  log.append = () => { throw failure }
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
  })
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  node.open()
  node.state = 'leader'
  node.leader = '1'
  const errored = new Promise((resolve) => node.once('error', resolve))

  await rejects(t, node.append(Buffer.from('data')), /local append failed/,
    'append rejects with the log failure')
  await errored

  t.deepEqual(fatals, [failure], 'emits the append failure as fatal')
  t.deepEqual(errors, [failure], 're-emits the append failure after close')
  t.notOk(node.isOpen, 'closes the node')
})

test('close rejects pending ACK waiters', async (t) => {
  const log = lifecycleLog()
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
    pingTimeout: 60_000,
    appendTimeout: 60_000,
  })
  node.open()
  node.leader = '2'

  const append = rejects(t, node.append(Buffer.from('data')), /node not open/,
    'pending append rejects')
  t.equal(node._acks.size, 1, 'one ACK waiter is pending')

  node.close()
  await append
  t.equal(node._acks.size, 0, 'ACK waiters are removed')
})

test('leader only commits current-term entries by replica count', async (t) => {
  const entries = new Map()
  const reads = []
  const log = {
    *iter(seq) {
      reads.push(seq)
      const entry = entries.get(seq)
      if (entry) { yield entry }
    },
  }
  const node = new RaftNode('1', ids, () => {}, log)
  const commits = []
  node.state = 'candidate'
  node.term = 2n
  log.seq = 4n
  node._startPingTimer = () => {}
  node._leaderAppendNoOp = () => {}
  node._apply = async () => {}
  node._rxVote({ term: 2n, voteGranted: true }, '2')
  node._rxVote({ term: 2n, voteGranted: true }, '3')

  const oldEntry = Buffer.alloc(8)
  oldEntry.writeBigUInt64LE(1n)
  entries.set(4n, oldEntry)
  node._replicationState('2', node.term, node.seq + 1n).matchIndex = 4n
  node._replicationState('3', node.term, node.seq + 1n).matchIndex = 4n
  node.on('commit', (seq) => commits.push(seq))

  node._checkCommit()
  t.equal(node._commitSeq, -1n, 'does not commit an old-term entry from replica count')
  t.deepEqual(commits, [], 'does not emit a commit for the old-term entry')

  const currentEntry = Buffer.alloc(8)
  currentEntry.writeBigUInt64LE(2n)
  entries.set(5n, currentEntry)
  log.seq = 5n
  node._replicationState('2', node.term, node.seq + 1n).matchIndex = 5n
  node._checkCommit()
  t.equal(node._commitSeq, 5n, 'commits once a current-term entry reaches quorum')
  t.deepEqual(commits, [5n], 'emits the current-term commit')
  t.deepEqual(reads, [4n, 5n], 'checks each quorum candidate stored term')
})

test('commit term is read before commit publication', async (t) => {
  const order = []
  const entry = Buffer.alloc(8)
  entry.writeBigUInt64LE(2n)
  const log = {
    *iter() {
      order.push('read')
      yield entry
    },
  }
  const node = new RaftNode('1', ids, () => {}, log)
  node.state = 'leader'
  node.term = 2n
  log.seq = 4n
  node.followers = ['2', '3']
  node._replicationState('2', node.term, node.seq + 1n).matchIndex = 4n
  node._apply = async () => {}
  node.on('commit', () => order.push('commit'))

  node._checkCommit()

  t.equal(node._commitSeq, 4n, 'commits the current-term entry')
  t.deepEqual(order, ['read', 'commit'],
    'reads durable term before publishing the commit')
})

test('commit log read failure closes the node', async (t) => {
  const failure = new Error('commit term read failed')
  let closed = false
  const log = {
    isOpen: true,
    *iter() {
      throw failure
    },
    close() {
      closed = true
      this.isOpen = false
    },
  }
  const node = new RaftNode('1', ids, () => {}, log)
  const fatals = []
  const errors = []
  node._open = true
  node.state = 'leader'
  node.term = 2n
  log.seq = 4n
  node.followers = ['2', '3']
  node._replicationState('2', node.term, node.seq + 1n).matchIndex = 4n
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))

  node._checkCommit()
  await errored

  t.deepEqual(fatals, [failure], 'emits the log read failure as fatal')
  t.deepEqual(errors, [failure], 'emits the original read error exactly once')
  t.ok(closed, 'closes the log')
  t.notOk(node.isOpen, 'closes the node')
  t.equal(node._commitSeq, -1n, 'does not advance commit after a read error')
})

test('replica count does not report success for an uncommitted old-term entry', async (t) => {
  const entry = Buffer.alloc(8)
  entry.writeBigUInt64LE(1n)
  const log = {
    *iter() {
      yield entry
    },
  }
  const node = new RaftNode('1', ids, () => {}, log)
  node._open = true
  node.state = 'leader'
  node.term = 2n
  log.seq = 4n
  node.followers = ['2', '3']
  node._appendToFollower = async (to, begin, end) => {
    node._replicationState(to, node.term, begin).matchIndex = end
  }
  let pings = 0
  node._pingFollowers = () => { pings++ }

  const err = await rejection(node._appendToFollowers(4n, 4n))
  t.equal(err?.message, 'append not commit',
    'append rejects when the replicated entry cannot advance commit')
  t.equal(err?.code, NOT_COMMIT, 'uncommitted append uses NOT_COMMIT')
  t.equal(err?.sqlCode, null, 'uncommitted append has no SQLite code')
  t.equal(node._commitSeq, -1n, 'old-term entry remains uncommitted')
  t.equal(pings, 0, 'does not announce an entry that failed the commit bound')
})

test('append reports follower quorum failure with NOT_COMMIT', async (t) => {
  const node = new RaftNode('1', ids, () => {}, lifecycleLog())
  node._open = true
  node.state = 'leader'
  node.term = 0n
  node.followers = ['2', '3']
  node._appendToFollower = async () => { throw new Error('follower unavailable') }

  const err = await rejection(node._appendToFollowers(0n, 0n))
  t.equal(err?.message, 'append not commit',
    'uses the normalized not-commit message')
  t.equal(err?.code, NOT_COMMIT, 'follower quorum failure uses NOT_COMMIT')
  t.equal(err?.sqlCode, null, 'follower quorum failure has no SQLite code')
})

test('self vote persists before RequestVote is sent', async (t) => {
  const order = []
  const writes = []
  const sent = []
  const log = {
    seq: -1n,
    term: -1n,
    elec: { term: 0n, votedFor: null },
    election(term, votedFor) {
      order.push('persist')
      writes.push([term, votedFor])
      this.elec = { term, votedFor }
    },
  }
  const node = new RaftNode('1', ids, (to, msg) => {
    order.push(`send:${to}`)
    sent.push([to, { ...msg }])
  }, log)
  node._open = true
  node.state = 'follower'
  node.term = 0n
  node._votedFor = null
  node.on('change', () => order.push('change'))

  node._voteForSelf()
  t.deepEqual(writes, [[1n, '1']],
    'writes the incremented term and self vote together')
  t.deepEqual(sent.map(([to]) => to), ['2', '3'], 'sends RequestVote after persistence')
  t.ok(sent.every(([, msg]) => msg.type === 'vote_request' && msg.term === 1n),
    'sends requests for the persisted term')
  t.deepEqual(order, ['persist', 'change', 'send:2', 'send:3'],
    'persists before publishing or sending RequestVote')
})

test('self vote persistence failure emits error', async (t) => {
  const failure = new Error('self vote write failed')
  const errors = []
  const sent = []
  const log = {
    isOpen: true,
    seq: -1n,
    term: -1n,
    elec: { term: 0n, votedFor: null },
    election() { throw failure },
    close() { this.isOpen = false },
  }
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, msg]), log)
  node._open = true
  node.state = 'follower'
  node.term = 0n
  node._votedFor = null
  node.on('error', (err) => errors.push(err))
  const errored = new Promise((resolve) => node.once('error', resolve))

  node._voteForSelf()
  await errored

  t.deepEqual(errors, [failure], 'emits the persistence error exactly once')
  t.deepEqual(sent, [], 'does not send RequestVote after persistence fails')
})

test('granted vote persists before response is sent', async (t) => {
  const order = []
  const writes = []
  const sent = []
  const changes = []
  const log = {
    seq: -1n,
    term: -1n,
    elec: { term: 0n, votedFor: null },
    election(term, votedFor) {
      order.push('persist')
      writes.push([term, votedFor])
      this.elec = { term, votedFor }
    },
  }
  const node = new RaftNode('1', ids, (to, msg) => {
    order.push('send')
    sent.push([to, { ...msg }])
  }, log, {
    electionTimeout: 60_000,
  })
  node._open = true
  node.state = 'follower'
  node.term = 0n
  node._votedFor = null
  node.on('change', (state) => {
    order.push('change')
    changes.push(state)
  })

  node._rxVoteRequest({ term: 2n, termP: -1n, seqP: -1n }, '2')
  t.deepEqual(writes, [[2n, '2']],
    'writes the new term and granted vote together')
  t.equal(sent.length, 1, 'sends one response after persistence')
  t.equal(sent[0][1].voteGranted, true, 'grants the persisted vote')
  t.deepEqual(changes.map(({ term, leader }) => [term, leader]), [[2n, null]],
    'publishes the persisted term without inventing a leader')
  t.deepEqual(order, ['persist', 'send', 'change'],
    'persists and sends the vote before publishing the change')
  node._stopTimers()
})

test('rejected vote does not persist unchanged election state', async (t) => {
  const writes = []
  const sent = []
  const log = {
    seq: -1n,
    term: -1n,
    elec: { term: 1n, votedFor: '3' },
    election(term, votedFor) {
      writes.push([term, votedFor])
    },
  }
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log)
  node._open = true
  node.state = 'follower'
  node.term = 2n
  node._votedFor = null

  node._rxVoteRequest({ term: 1n, termP: -1n, seqP: -1n }, '2')
  t.deepEqual(writes, [], 'does not persist stale election state')
  t.equal(sent.length, 1, 'sends the rejection immediately')
  t.equal(sent[0][1].voteGranted, false, 'rejects the stale-term request')
})

test('duplicate durable granted vote responds without another write', async (t) => {
  const writes = []
  const sent = []
  const log = {
    seq: -1n,
    term: -1n,
    elec: { term: 2n, votedFor: '2' },
    election(term, votedFor) {
      writes.push([term, votedFor])
    },
  }
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log)
  node._open = true
  node.state = 'follower'
  node.leader = '2'
  node.term = 2n
  node._votedFor = '2'

  node._rxVoteRequest({ term: 2n, termP: -1n, seqP: -1n }, '2')
  t.deepEqual(writes, [], 'does not rewrite an already durable grant')
  t.equal(sent.length, 1, 'responds after persistence')
  t.equal(sent[0][1].voteGranted, true, 'repeats the durable grant')
  node._stopTimers()
})

test('persisted vote identity survives restart', async (t) => {
  const log = lifecycleLog()
  log.elec.term = 4n
  log.elec.votedFor = '2'
  const sent = []
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log, {
    electionTimeout: 60_000,
  })
  node.open()

  await node.onReceive('2', { type: 'vote_request', term: 4n, termP: -1n, seqP: -1n })
  await node.onReceive('3', { type: 'vote_request', term: 4n, termP: -1n, seqP: -1n })

  t.equal(sent.length, 2, 'sends both vote responses')
  t.equal(sent[0][1].voteGranted, true, 'repeats the durable grant')
  t.equal(sent[1][1].voteGranted, false, 'rejects a different candidate')
  t.equal(node.term, 4n, 'waits for a later election term')
  node.close()
})

test('election persistence failure closes the node before emitting error', async (t) => {
  const failure = new Error('election write failed')
  const sent = []
  const fatals = []
  const errors = []
  const log = lifecycleLog()
  let persistenceResult = null
  log.election = () => { throw failure }
  const node = new RaftNode('1', ids, (to, msg) => sent.push([to, { ...msg }]), log)
  const persistElection = node._persistElection.bind(node)
  node._persistElection = () => {
    persistenceResult = persistElection()
    return persistenceResult
  }
  node.on('fatal', (err) => fatals.push(err))
  node.on('error', (err) => errors.push(err))
  node.open()

  const errored = new Promise((resolve) => node.once('error', resolve))

  const receiving = node.onReceive('2', {
    type: 'vote_request', term: 2n, termP: -1n, seqP: -1n,
  })

  t.equal(persistenceResult, false, 'reports election persistence failure')
  t.deepEqual(fatals, [failure], 'emits the original persistence error as fatal')
  t.notOk(node.isOpen, 'marks the node unavailable immediately')
  t.deepEqual(sent, [], 'does not send a vote response')
  t.notOk(log.isOpen, 'closes the log synchronously')
  t.deepEqual(errors, [], 'defers public error reporting until after close')

  await receiving
  t.equal(await errored, failure, 're-emits the same error after closing')
  t.deepEqual(errors, [failure], 'emits the original persistence error once')

  const reopened = thrown(() => node.open())
  t.match(reopened?.message ?? '', /node not open/,
    'the failed instance cannot reopen')
  if (node.isOpen) { node.close() }

  const replacementLog = lifecycleLog()
  const replacement = new RaftNode('1', ids, () => {}, replacementLog, {
    electionTimeout: 60_000,
  })
  replacement.open()
  t.ok(replacement.isOpen, 'a fresh node and log wrapper can recover normally')
  replacement.close()
})

test('fatal close failure emits the close and original errors', async (t) => {
  const failure = new Error('election write failed')
  const closeFailure = new Error('close failed')
  const log = lifecycleLog()
  log.election = () => { throw failure }
  const closeLog = log.close.bind(log)
  let closeCalls = 0
  log.close = () => {
    closeCalls++
    throw closeFailure
  }
  const node = new RaftNode('1', ids, () => {}, log, {
    electionTimeout: 60_000,
  })
  const fatals = []
  const errors = []
  node.on('fatal', (err) => fatals.push(err))
  const errored = new Promise((resolve) => {
    node.on('error', (err) => {
      errors.push(err)
      if (errors.length === 2) { resolve() }
    })
  })
  node.open()

  node._voteForSelf()
  t.ok(node.eventNames().includes('error'),
    'fatal close failure retains listeners while errors are pending')
  await errored

  t.deepEqual(fatals, [failure], 'only the originating failure is emitted as fatal')
  t.deepEqual(errors, [failure, closeFailure], 'emits fatal error before the close failure')
  t.deepEqual(node.eventNames(), [],
    'fatal close failure removes listeners after both errors')
  t.equal(closeCalls, 1, 'does not recursively retry the failed fatal close')
  t.notOk(node.isOpen, 'node remains unavailable')

  const beforeRetry = thrown(() => node.open())
  t.match(beforeRetry?.message ?? '', /node not open/,
    'fatal close failure cannot be bypassed with open')

  log.close = closeLog
  node.close()
  t.notOk(log.isOpen, 'an explicit close retries and completes storage cleanup')

  const afterRetry = thrown(() => node.open())
  t.match(afterRetry?.message ?? '', /node not open/,
    'storage cleanup does not make the failed instance reusable')
  if (node.isOpen) { node.close() }
})
