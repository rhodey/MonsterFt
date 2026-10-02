import test from 'tape'
import { SQLiteLog } from '../src/index.js'

import { comms, connect } from './util.js'
import { open, close, ready } from './util.js'
import { leaders, followers } from './util.js'
import { databasePath, toEntry } from './util.js'

const reset = (nodes) => nodes.forEach((node) => node.log.del())

const toBuf = (obj) => {
  if (obj === null) { return null }
  obj = JSON.stringify(obj)
  return Buffer.from(obj, 'utf8')
}

function testAppendSmall(t) {
  t.plan(1)
  const log = new SQLiteLog(databasePath('test'))
  log.del()
  log.open()

  const data = []
  const count = 100
  for (let i = 0; i < count; i++) {
    data.push(toEntry(toBuf({ i })))
  }

  const begin = Date.now()
  for (const buf of data) {
    log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 100 small', (t) => testAppendSmall(t))

function testAppendLarge(t) {
  t.plan(1)
  const log = new SQLiteLog(databasePath('test'))
  log.del()
  log.open()

  const data = []
  const count = 100
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toEntry(toBuf({ i, large })))
  }

  const begin = Date.now()
  for (const buf of data) {
    log.append(buf)
  }

  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append 100 large', (t) => testAppendLarge(t))

function testAppendLargeBatch(t) {
  t.plan(1)
  const log = new SQLiteLog(databasePath('test'))
  log.del()
  log.open()

  const data = []
  const count = 1_000
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toEntry(toBuf({ i, large })))
  }

  const begin = Date.now()
  log.appendBatch(data)
  const ms = Date.now() - begin
  console.log(`done ${count} in ${ms}ms`)

  const seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)

  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  t.teardown(() => log.close())
  console.log(`\n`)
}

test('test append batch 1000 large', (t) => testAppendLargeBatch(t))

async function testAppendLargeNodes(t) {
  t.plan(2)
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3)

  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  const leader = leaders(nodes)[0]
  const follow = followers(nodes)
  t.equal(follow.length, 2, '2 followers')

  const data = []
  const count = 100
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  let begin = Date.now()
  for (const buf of data) {
    await leader.append(buf)
  }

  let ms = Date.now() - begin
  console.log(`done leader ${count} in ${ms}ms`)
  let seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms\n`)

  begin = Date.now()
  for (const buf of data) {
    await follow[0].append(buf)
  }

  ms = Date.now() - begin
  console.log(`done follower ${count} in ${ms}ms`)
  seconds = ms / 1000
  avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  console.log(`\n`)
}

test('test append 100 large - nodes', (t) => testAppendLargeNodes(t))

async function testAppendLargeBatchNodes(t) {
  t.plan(2)
  t.teardown(() => close(nodes))
  const coms = comms()
  const nodes = connect(coms, 3)

  await reset(nodes)
  open(nodes)
  await ready(nodes, null, true)

  const leader = leaders(nodes)[0]
  const follow = followers(nodes)
  t.equal(follow.length, 2, '2 followers')

  const data = []
  const count = 1_000
  const large = new Array(1024).fill('a').join('')
  for (let i = 0; i < count; i++) {
    data.push(toBuf({ i, large }))
  }

  let begin = Date.now()
  await leader.appendBatch(data)

  let ms = Date.now() - begin
  console.log(`done leader ${count} in ${ms}ms`)
  let seconds = ms / 1000
  let avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms\n`)

  begin = Date.now()
  await follow[0].appendBatch(data)

  ms = Date.now() - begin
  console.log(`done follower ${count} in ${ms}ms`)
  seconds = ms / 1000
  avg = (count / seconds).toFixed(1)
  console.log(`${avg} append per second`)
  avg = (avg / 1000).toFixed(2)
  console.log(`${avg} append per ms`)

  t.pass('ok')
  console.log(`\n`)
}

test('test append batch 1000 large - nodes', (t) => testAppendLargeBatchNodes(t))
