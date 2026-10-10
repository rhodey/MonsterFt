import test from 'tape'
import { RaftNode } from '../src/index.js'
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

const replaceNode = (nodes, oldNode, coms) => {
  const replacement = new RaftNode(
    oldNode.id,
    oldNode.nodes,
    (to, msg) => coms.send(to, oldNode.id, msg),
    oldNode.log.path,
  )
  const index = nodes.indexOf(oldNode)
  if (index < 0) { throw new Error(`node ${oldNode.id} not found`) }
  nodes[index] = replacement
  coms.register(replacement)
  return replacement
}

test('test elect n=3 then del 1 follower', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3, null)
  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq

  let data = { a: 1 }
  let seq = (await leader.append(toBuf(data)))[0]
  t.equal(seq, s + 1n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 1n, `leader log seq = ${s + 1n}`)

  data = { bb: 2 }
  seq = (await leader.append(toBuf(data)))[0]
  t.equal(seq, s + 2n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 2n, `leader log seq = ${s + 1n}`)

  // del
  const oldFollower = flw[0]
  close([oldFollower])
  oldFollower.log.del()
  const replacement = replaceNode(nodes, oldFollower, coms)
  open([replacement])
  await ready([replacement])

  leader = leaders(nodes)[0]
  flw = followers(nodes)
  s = seq

  data = { ccc: 3 }
  seq = (await leader.append(toBuf(data)))[0]

  const next = s + 1n
  const name = `node ${leader.id} ${leader.state}`
  t.equal(seq, next, `${name} ret = ${next}`)
  t.equal(leader.seq, next, `${name} seq = ${next}`)
  t.equal(leader.log.seq, next, `${name} log seq = ${next}`)

  await ready(nodes, null, true)
  await sleep(500)
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)

  // full
  data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const node of nodes) {
    let count = 0n
    for (let next of node.log.iter(count)) {
      next = next.entry
      if (next.length <= 0) { continue }
      next = toObj(next)
      t.deepEqual(next, data[count], `node ${node.id} data ${count} ok`)
      count++
    }
  }
})

test('test elect n=3 then del 1 leader', async (t) => {
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3, null)
  await reset(nodes)
  open(nodes)
  await ready(nodes)

  let leader = leaders(nodes)[0]
  let flw = followers(nodes)
  t.equal(flw.length, 2, '2 followers')

  let s = leader.seq

  let data = { a: 1 }
  let seq = (await leader.append(toBuf(data)))[0]
  t.equal(seq, s + 1n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 1n, `leader log seq = ${s + 1n}`)

  data = { bb: 2 }
  seq = (await leader.append(toBuf(data)))[0]
  t.equal(seq, s + 2n, `leader ret = ${s + 1n}`)
  t.equal(leader.seq, s + 2n, `leader log seq = ${s + 1n}`)

  // del
  console.log('leader =>', leader.id, seq)
  const copy = leader
  close([copy])
  copy.log.del()

  // make elect
  await sleep(5000)
  await ready(flw)
  leader = leaders(flw)[0]
  s = leader.seq
  console.log('leader =>', leader.id, s)

  // restart
  const replacement = replaceNode(nodes, copy, coms)
  open([replacement])
  await ready(nodes)
  leader = leaders(nodes)[0]
  flw = followers(nodes)
  s = leader.seq
  console.log('leader =>', leader.id, s)

  data = { ccc: 3 }
  seq = (await leader.append(toBuf(data)))[0]

  const next = s + 1n
  const name = `node ${leader.id} ${leader.state}`
  t.equal(seq, next, `${name} ret = ${next}`)
  t.equal(leader.seq, next, `${name} seq = ${next}`)
  t.equal(leader.log.seq, next, `${name} log seq = ${next}`)

  await sleep(1000)
  testSeqMulti(t, seq, s + 1n, s + 1n, nodes)
  testHeadMulti(t, data, nodes)

  // full
  data = [{ a: 1 }, { bb: 2 }, { ccc: 3 }]
  for (const node of nodes) {
    let count = 0n
    for (let next of node.log.iter(count)) {
      next = next.entry
      if (next.length <= 0) { continue }
      next = toObj(next)
      t.deepEqual(next, data[count], `node ${node.id} data ${count} ok`)
      count++
    }
  }
})
