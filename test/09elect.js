import test from 'tape'
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
