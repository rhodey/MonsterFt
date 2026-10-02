import test from 'tape'
import { sleep, comms, connect } from './util.js'
import { open, close, ready } from './util.js'
import { leaders, followers } from './util.js'

const reset = (nodes) => nodes.forEach((node) => node.log.del())

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

const toObj = (buf) => {
  if (buf === null) { return null }
  return JSON.parse(buf.toString('utf8'))
}

const testSeq = (t, ok, b, c, node) => {
  const [a] = ok
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
  const opts = { quorum: 3 } // force full repl
  let nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  const ok = nodes.every((node) => ['1', '2', '3'].includes(node.id))
  t.ok(ok, 'ids correct')

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq

  // leader
  let data = { a: 1 }
  let seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  t.equal(seq[1], null, 'append returns the default null result')
  testHeadMulti(t, data, nodes)

  data = { bb: 2 }
  seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)

  // follower 1
  data = { ccc: 3 }
  seq = await flw[0].append(toBuf(data))
  testSeqMulti(t, seq, s + 3n, s + 3n, nodes)
  testHeadMulti(t, data, nodes)

  data = { dd: 4 }
  seq = await flw[0].append(toBuf(data))
  testSeqMulti(t, seq, s + 4n, s + 4n, nodes)
  testHeadMulti(t, data, nodes)

  // restart with fresh node and log wrappers over the same durable files
  close(nodes)
  nodes = connect(coms, 3, null, opts)
  open(nodes)
  await ready(nodes, null, true)

  leader = leaders(nodes)[0]
  flw = followers(nodes)
  s = leader.seq

  // follower 2
  data = { e: 5 }
  seq = await flw[1].append(toBuf(data))
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)

  data = { a: 6 }
  seq = await flw[1].append(toBuf(data))
  testSeqMulti(t, seq, s + 2n, s + 2n, nodes)
  testHeadMulti(t, data, nodes)
})

test('test elect n=3 then append batch', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const opts = { quorum: 3 } // force full repl
  let nodes = connect(coms, 3, null, opts)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  const ok = nodes.every((node) => ['1', '2', '3'].includes(node.id))
  t.ok(ok, 'ids correct')

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq

  let data = { a: 1 }
  let seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)

  // leader batch
  data = [{ b: 2 }, { c: 3 }]
  seq = await leader.appendBatch(data.map(toBuf))
  testSeqMulti(t, seq, s + 2n, s + 3n, nodes)
  t.deepEqual(seq[1], [null, null], 'appendBatch returns default null results')
  testHeadMulti(t, data[1], nodes)

  data = { d: 4 }
  seq = await leader.append(toBuf(data))
  testSeqMulti(t, seq, s + 4n, s + 4n, nodes)
  testHeadMulti(t, data, nodes)

  // follower 1 batch
  data = [{ a: 1 }, { b: 2 }]
  seq = await flw[0].appendBatch(data.map(toBuf))
  testSeqMulti(t, seq, s + 5n, s + 6n, nodes)
  testHeadMulti(t, data[1], nodes)

  data = { c: 3 }
  seq = await flw[0].append(toBuf(data))
  testSeqMulti(t, seq, s + 7n, s + 7n, nodes)
  testHeadMulti(t, data, nodes)

  // follower 2 batch
  data = [{ a: 1 }, { b: 2 }]
  seq = await flw[1].appendBatch(data.map(toBuf))
  testSeqMulti(t, seq, s + 8n, s + 9n, nodes)
  testHeadMulti(t, data[1], nodes)

  // restart with fresh node and log wrappers over the same durable files
  close(nodes)
  nodes = connect(coms, 3, null, opts)
  open(nodes)
  await ready(nodes, null, true)

  leader = leaders(nodes)[0]
  flw = followers(nodes)
  s = leader.seq

  data = { c: 3 }
  seq = await flw[1].append(toBuf(data))
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)
})
