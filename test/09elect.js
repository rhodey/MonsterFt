import test from 'tape'
import { NOT_LEADER } from '../src/error.js'
import { sleep, comms, connect } from './util.js'
import { open, close, ready } from './util.js'
import { leaders, followers } from './util.js'

const reset = (nodes) => nodes.forEach((node) => node.log.del())

test('test elect n=3', async (t) => {
  t.plan(5)
  t.teardown(() => close(nodes))

  const coms = comms()
  const nodes = connect(coms, 3)
  await reset(nodes)
  open(nodes)
  await ready(nodes)

  let ok = nodes.every((node) => ['1', '2', '3'].includes(node.id))
  t.ok(ok, 'ids correct')
  ok = nodes.every((node) => node.isOpen)
  t.ok(ok, 'nodes are open')

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  const count = arr[0].followers.length
  arr = followers(nodes)
  t.equal(arr.length, 2, '2 followers')
  t.equal(count, 2, '2 followers again')
})

test('test elect n=3 and 1 offline', async (t) => {
  t.plan(4)
  t.teardown(() => close(nodes))

  const allowSend = (to, from, msg) => from !== '3'
  const coms = comms(allowSend)
  const nodes = connect(coms, 3)
  await reset(nodes)
  open(nodes)
  await ready(nodes, 2)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.notEqual(arr[0].id, '3', 'leader not node 3')

  arr = arr[0].followers
  t.equal(arr.length, 1, '1 follower')
  t.notEqual(arr[0], '3', 'follower not node 3')
})

test('test elect n=5', async (t) => {
  t.plan(3)
  t.teardown(() => close(nodes))

  const coms = comms()
  const nodes = connect(coms, 5)
  await reset(nodes)
  open(nodes)
  await ready(nodes)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')

  const count = arr[0].followers.length
  arr = followers(nodes)
  t.equal(arr.length, 4, '4 followers')
  t.equal(count, 4, '4 followers again')
})

test('test elect n=5 and 1 offline', async (t) => {
  t.plan(4)
  t.teardown(() => close(nodes))

  const allowSend = (to, from, msg) => from !== '5'
  const coms = comms(allowSend)
  const nodes = connect(coms, 5)
  await reset(nodes)
  open(nodes)
  await ready(nodes, 4)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.notEqual(arr[0].id, '5', 'leader not node 5')

  arr = arr[0].followers
  t.equal(arr.length, 3, '3 followers')

  const ok = arr.every((id) => id !== '5')
  t.ok(ok, 'followers not node 5')
})

test('test elect n=5 and 1 delayed', async (t) => {
  t.teardown(() => close(nodes))

  const delaySend = (to, from, msg) => from === '5' ? 1_000 : 0
  const coms = comms(undefined, delaySend)
  const nodes = connect(coms, 5)
  await reset(nodes)
  open(nodes)

  await ready(nodes, 4)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.notEqual(arr[0].id, '5', 'leader not node 5')

  arr = arr[0].followers
  t.equal(arr.length, 3, '3 followers')

  const ok = arr.every((id) => id !== '5')
  t.ok(ok, 'followers not node 5')

  let total = 0
  const timeout = 20_000
  while (total < timeout) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length !== 1) {
      t.equal(arr.length, 1, '1 leader')
      break
    }

    const count = arr[0].followers.length
    if (count >= 4) { break }
  }

  const count = arr[0].followers.length
  t.equal(count, 4, '4 followers')
  total >= timeout && t.fail('timeout')
  t.end()
})

test('test elect n=5 and 2 offline', async (t) => {
  t.plan(6)
  t.teardown(() => close(nodes))

  const allowSend = (to, from, msg) => from !== '4' && from !== '5'
  const coms = comms(allowSend)
  const nodes = connect(coms, 5)
  await reset(nodes)
  open(nodes)
  await ready(nodes, 3)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.notEqual(arr[0].id, '4', 'leader not node 4')
  t.notEqual(arr[0].id, '5', 'leader not node 5')

  arr = arr[0].followers
  t.equal(arr.length, 2, '2 followers')

  let ok = arr.every((id) => id !== '4')
  t.ok(ok, 'followers not node 4')

  ok = arr.every((id) => id !== '5')
  t.ok(ok, 'followers not node 5')
})

test('test elect n=5 and 2 delayed', async (t) => {
  t.teardown(() => close(nodes))

  const delaySend = (to, from, msg) => {
    return (from === '4' || from === '5') ? 1_000 : 0
  }
  const coms = comms(undefined, delaySend)
  const nodes = connect(coms, 5)
  await reset(nodes)
  open(nodes)
  await ready(nodes, 3)

  let arr = leaders(nodes)
  t.equal(arr.length, 1, '1 leader')
  t.notEqual(arr[0].id, '4', 'leader not node 4')
  t.notEqual(arr[0].id, '5', 'leader not node 5')

  arr = arr[0].followers
  t.equal(arr.length, 2, '2 followers')

  let ok = arr.every((id) => id !== '4')
  t.ok(ok, 'followers not node 4')

  ok = arr.every((id) => id !== '5')
  t.ok(ok, 'followers not node 5')

  let total = 0
  const timeout = 20_000
  while (total < timeout) {
    await sleep(100)
    total += 100

    arr = leaders(nodes)
    if (arr.length !== 1) {
      t.equal(arr.length, 1, '1 leader')
      break
    }

    const count = arr[0].followers.length
    if (count >= 4) { break }
  }

  const count = arr[0].followers.length
  t.equal(count, 4, '4 followers')
  total >= timeout && t.fail('timeout')
  t.end()
})

const readyWithin = async (nodes, ms) => {
  let timer = null
  const timedOut = new Promise((res) => { timer = setTimeout(res, ms, 'timeout') })
  try {
    return await Promise.race([ready(nodes).then(() => 'ready'), timedOut])
  } finally {
    clearTimeout(timer)
  }
}

test('a throwing change listener does not hide leadership from awaitLeader', async (t) => {
  t.teardown(() => close(nodes))

  const coms = comms()
  const nodes = connect(coms, 3)
  await reset(nodes)
  const failure = new Error('change listener failure')
  const errors = []
  nodes.forEach((node) => {
    node.on('change', (change) => {
      if (change.leader !== null) { throw failure }
    })
    node.on('error', (err) => errors.push(err))
  })
  open(nodes)

  t.equal(await readyWithin(nodes, 10_000), 'ready',
    'awaitLeader resolves on every node')
  t.equal(leaders(nodes).length, 1, '1 leader')
  t.ok(nodes.every((node) => node.leader !== null), 'every node knows the leader')
  t.ok(errors.length > 0 && errors.every((err) => err === failure),
    'listener failures are reported as errors')
})

test('change payloads copy membership and followers', async (t) => {
  t.teardown(() => close(nodes))

  const coms = comms()
  const nodes = connect(coms, 3)
  await reset(nodes)
  let shared = 0
  nodes.forEach((node) => {
    node.on('change', (change) => {
      if (change.nodes === node.nodes || change.followers === node.followers) {
        shared++
      }
      change.nodes.push('mutated')
      change.followers.push('mutated')
    })
  })
  open(nodes)

  t.equal(await readyWithin(nodes, 10_000), 'ready', 'cluster elects a leader')
  t.equal(shared, 0, 'payload arrays are never the node arrays')
  for (const node of nodes) {
    t.deepEqual(node.nodes, ['1', '2', '3'],
      `listener mutation leaves node ${node.id} membership unchanged`)
  }
  t.notOk(leaders(nodes)[0].followers.includes('mutated'),
    'listener mutation leaves leader followers unchanged')
})

const waitFor = async (fn, ms) => {
  const end = Date.now() + ms
  while (!fn() && Date.now() < end) { await sleep(5) }
  return fn()
}

test('leader publishes follower removal and a single step-down change', async (t) => {
  t.teardown(() => close(nodes))

  const down = new Set()
  const allowSend = (to, from) => !down.has(to) && !down.has(from)
  const coms = comms(allowSend)
  const nodes = connect(coms, 3, null, { pingTimeout: 300 })
  await reset(nodes)
  open(nodes)
  t.equal(await readyWithin(nodes, 10_000), 'ready', 'cluster elects a leader')
  const leader = leaders(nodes)[0]
  t.ok(await waitFor(() => leader.followers.length === 2, 2_000),
    'leader has both followers')
  const changes = []
  leader.on('change', (change) => changes.push(change))
  const [lost, kept] = leader.followers

  down.add(lost)
  t.ok(await waitFor(() => changes.length > 0, 2_000),
    'removing one follower publishes a change')
  t.equal(changes[0]?.state, 'leader', 'the node is still leader')
  t.deepEqual(changes[0]?.followers, [kept], 'the change omits the lost follower')
  t.deepEqual(leader.followers, [kept], 'leader keeps the remaining follower')

  changes.length = 0
  down.add(kept)
  t.ok(await waitFor(() => changes.length > 0, 2_000),
    'losing quorum publishes a change')
  t.equal(changes[0]?.state, 'follower',
    'the first change after losing quorum is the step-down')
})

test('a leader that stepped down rejects a forwarded append with NOT_LEADER', async (t) => {
  t.teardown(() => close(nodes))

  let dropAcksTo = null
  const allowSend = (to, from, msg) => !(to === dropAcksTo && msg.type === 'ack')
  const coms = comms(allowSend)
  const nodes = connect(coms, 3, null, { pingTimeout: 300 })
  await reset(nodes)
  open(nodes)
  t.equal(await readyWithin(nodes, 10_000), 'ready', 'cluster elects a leader')
  const leader = leaders(nodes)[0]
  const follower = nodes.find((node) => node !== leader)
  const term = leader.term
  let outcome = null
  leader.on('change', (change) => {
    if (change.state !== 'follower' || outcome) { return }
    outcome = {
      term: change.term,
      followerLeader: follower.leader,
      result: follower.append(Buffer.from('forwarded')).then(() => null, (err) => err),
    }
  })

  dropAcksTo = leader.id
  t.ok(await waitFor(() => outcome !== null, 5_000),
    'the leader steps down after losing acknowledgements')
  t.equal(outcome?.term, term, 'the leader steps down within its term')
  t.equal(outcome?.followerLeader, leader.id,
    'the follower still forwards to the former leader')
  const err = await outcome?.result
  t.equal(err?.code, NOT_LEADER, 'the forwarded append is rejected with NOT_LEADER')
})
