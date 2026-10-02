import test from 'tape'
import { sleep, comms, connect } from './util.js'
import { open, close, ready } from './util.js'
import { leaders, followers } from './util.js'

const reset = (nodes) => nodes.forEach((node) => node.log.del())
const applyNull = (node, bufs) => bufs.map(() => null)
const awaitApplied = (node) => {
  if (node._applySeq >= node._commitSeq) { return Promise.resolve() }
  return node.awaitEvent('apply', (seq) => seq >= node._commitSeq)
}

const replicaStates = () => {
  let states = new WeakMap()
  return {
    get(node) {
      let state = states.get(node)
      if (!state) {
        state = { count: 0n, nextSeq: 0n }
        states.set(node, state)
      }
      return state
    },
    reset() {
      states = new WeakMap()
    },
  }
}

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

const testSeq = (t, a, b, c, node) => {
  const name = `node ${node.id} ${node.state}`
  t.equal(a, b, `${name} ret = ${b}`)
  t.equal(node.seq, c, `${name} seq = ${c}`)
  t.equal(node.log.seq, c, `${name} log seq = ${c}`)
}

const testSeqMulti = (t, a, b, c, nodes) => nodes.forEach((node) => testSeq(t, a, b, c, node))

const testHead = (t, data, node) => {
  const name = `node ${node.id} ${node.state}`
  t.deepEqual(toObj(node.head), data, `${name} head = data`)
  t.deepEqual(toObj(node.log.head), data, `${name} log head = data`)
}

const testHeadMulti = (t, data, nodes) => nodes.forEach((node) => testHead(t, data, node))

test('test elect n=3 then append 6', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  const replicas = replicaStates()
  const apply = (node, bufs, seqs, terms) => {
    const state = replicas.get(node)
    const results = []
    bufs.forEach((buf, idx) => {
      t.equal(seqs[idx], state.nextSeq++, `node ${node.id} fn seq ok`)
      results.push(++state.count)
    })
    return results
  }
  const opts = () => ({ apply, quorum: 3 })

  let nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq
  await awaitApplied(leader)
  let state = replicas.get(leader)
  state.count = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  data = { bb: 2 }
  ok = await leader.append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

  // follower 1
  data = { ccc: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 3n, s + 3n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 3n, `3 = result`)

  data = { dd: 4 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 4n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 4n, `4 = result`)

  // Cold restart: fresh nodes must replay their durable logs into fresh state.
  close(nodes)
  replicas.reset()
  nodes = connect(coms, 3, null, opts)
  open(nodes)
  await ready(nodes, null, true)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  s = leader.seq
  await awaitApplied(leader)
  state = replicas.get(leader)
  t.equal(state.nextSeq, s + 1n, 'fresh leader replayed every durable sequence from zero')
  t.equal(state.count, s + 1n, 'fresh leader rebuilt state from the durable log')
  state.count = 0n

  // follower 2
  data = { e: 5 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  data = { a: 6 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

})

test('test elect n=3 then append 6 - async', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  const replicas = replicaStates()
  const apply = async (node, bufs, seqs, terms) => {
    await sleep(50)
    const state = replicas.get(node)
    const results = []
    bufs.forEach((buf, idx) => {
      t.equal(seqs[idx], state.nextSeq++, `node ${node.id} fn seq ok`)
      results.push(++state.count)
    })
    return results
  }
  const opts = () => ({ apply, quorum: 3 })

  let nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq
  await awaitApplied(leader)
  let state = replicas.get(leader)
  state.count = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  data = { bb: 2 }
  ok = await leader.append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

  // follower 1
  data = { ccc: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 3n, s + 3n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 3n, `3 = result`)

  data = { dd: 4 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 4n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 4n, `4 = result`)

  // Cold restart: fresh nodes must replay their durable logs into fresh state.
  close(nodes)
  replicas.reset()
  nodes = connect(coms, 3, null, opts)
  open(nodes)
  await ready(nodes, null, true)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  s = leader.seq
  await awaitApplied(leader)
  state = replicas.get(leader)
  t.equal(state.nextSeq, s + 1n, 'fresh leader replayed every durable sequence from zero')
  t.equal(state.count, s + 1n, 'fresh leader rebuilt state from the durable log')
  state.count = 0n

  // follower 2
  data = { e: 5 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  data = { a: 6 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 2n, `2 = result`)

})

test('test elect n=3 then append batch', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  const replicas = replicaStates()
  const apply = (node, bufs, seqs, terms) => {
    const state = replicas.get(node)
    const results = []
    bufs.forEach((buf, idx) => {
      t.equal(seqs[idx], state.nextSeq++, `node ${node.id} fn seq ok`)
      results.push(++state.count)
    })
    return results
  }
  const opts = () => ({ apply, quorum: 3 })

  let nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq
  await awaitApplied(leader)
  let state = replicas.get(leader)
  state.count = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  ok = await leader.appendBatch(data.map(toBuf))
  seq = ok[0]
  let results = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 3n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 2n, `2 = result`)
  t.equal(results[1], 3n, `3 = result`)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  ok = await flw[0].appendBatch(data.map(toBuf))
  seq = ok[0]; results = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 5n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 4n, `4 = result`)
  t.equal(results[1], 5n, `5 = result`)

  data = { c: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 6n, s + 6n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 6n, `6 = result`)

  // Cold restart: fresh nodes must replay their durable logs into fresh state.
  close(nodes)
  replicas.reset()
  nodes = connect(coms, 3, null, opts)
  open(nodes)
  await ready(nodes, null, true)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  s = leader.seq
  await awaitApplied(leader)
  state = replicas.get(leader)
  t.equal(state.nextSeq, s + 1n, 'fresh leader replayed every durable sequence from zero')
  t.equal(state.count, s + 1n, 'fresh leader rebuilt state from the durable log')
  state.count = 0n

  data = { c: 3 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

})

test('test elect n=3 then append batch - async', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()

  const replicas = replicaStates()
  const apply = async (node, bufs, seqs, terms) => {
    await sleep(50)
    const state = replicas.get(node)
    const results = []
    bufs.forEach((buf, idx) => {
      t.equal(seqs[idx], state.nextSeq++, `node ${node.id} fn seq ok`)
      results.push(++state.count)
    })
    return results
  }
  const opts = () => ({ apply, quorum: 3 })

  let nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq
  await awaitApplied(leader)
  let state = replicas.get(leader)
  state.count = 0n

  // leader
  let data = { a: 1 }
  let ok = await leader.append(toBuf(data))
  let [seq, result] = ok
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  ok = await leader.appendBatch(data.map(toBuf))
  seq = ok[0]
  let results = ok[1]
  testSeqMulti(t, seq, s + 2n, s + 3n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 2n, `2 = result`)
  t.equal(results[1], 3n, `3 = result`)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  ok = await flw[0].appendBatch(data.map(toBuf))
  seq = ok[0]; results = ok[1]
  testSeqMulti(t, seq, s + 4n, s + 5n, nodes)
  testHeadMulti(t, data[1], nodes)
  t.equal(results.length, 2, `2 result`)
  t.equal(results[0], 4n, `4 = result`)
  t.equal(results[1], 5n, `5 = result`)

  data = { c: 3 }
  ok = await flw[0].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 6n, s + 6n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 6n, `6 = result`)

  // Cold restart: fresh nodes must replay their durable logs into fresh state.
  close(nodes)
  replicas.reset()
  nodes = connect(coms, 3, null, opts)
  open(nodes)
  await ready(nodes, null, true)

  leader = leaders(nodes)[0]
  leader.opts.apply = apply
  flw = followers(nodes)
  flw.forEach((node) => node.opts.apply = applyNull)
  s = leader.seq
  await awaitApplied(leader)
  state = replicas.get(leader)
  t.equal(state.nextSeq, s + 1n, 'fresh leader replayed every durable sequence from zero')
  t.equal(state.count, s + 1n, 'fresh leader rebuilt state from the durable log')
  state.count = 0n

  data = { c: 3 }
  ok = await flw[1].append(toBuf(data))
  seq = ok[0]; result = ok[1]
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
  t.equal(result, 1n, `1 = result`)

})
