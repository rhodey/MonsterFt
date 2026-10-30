import test from 'tape'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import {
  ProtocolStore, databasePaths, openApplyDatabase, readDatabase, readState, stateDigest,
} from '../src/storage.js'
import * as Err from '../src/error.js'

const roles = ['procA', 'procB', 'procC']
const orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]]

function fixture(outlier = 1) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-adoption-'))
  const base = path.join(directory, 'node')
  const paths = databasePaths(base)
  let generation = 0
  let stores = []
  let db2 = null
  const close = () => {
    for (const store of stores) { store.close() }
    db2?.close()
    db2 = null
  }
  const open = () => {
    const seed = Buffer.alloc(16, ++generation)
    stores = roles.map((role) => new ProtocolStore(role, base))
    for (const store of stores) { store.open(seed) }
    db2 = openApplyDatabase(base, seed)
  }
  const snapshot = () => roles.map((role) => readDatabase(paths[role], (db) => ({
    state: readState(db),
    entries: db.prepare('SELECT seq, term, entry FROM raft_log ORDER BY seq').all(),
  })))
  open()
  for (let i = 0; i < stores.length; i++) {
    if (i === outlier) { continue }
    stores[i].log.election(5n, 'leader')
    stores[i].log.append({ term: 5n, entry: Buffer.alloc(0) })
  }
  const capture = () => stores.map((store) => store.capture())
  const reports = capture()
  const selected = reports[0].state
  return {
    get stores() { return stores }, paths, snapshot, selected,
    capture,
    restart(t) {
      close()
      open()
      const reports = capture()
      t.ok(reports.every(({ state }) => state.equals(selected)),
        'fresh owners still independently select the original majority')
      for (const store of stores) { store.adopt() }
      for (const store of stores) { store.reconcile() }
      t.ok(snapshot().every(({ state }) => stateDigest(state).equals(selected)),
        'reopening converges to the original majority')
    },
    cleanup() { close(); fs.rmSync(directory, { recursive: true, force: true }) },
  }
}

const adoptionError = (error) => error.code === Err.MONSTER_CORRUPT ||
  error.code === Err.MONSTER_ILLEGAL

test('adoption preserves the selected majority for every outlier and execution order', (t) => {
  for (let outlier = 0; outlier < roles.length; outlier++) {
    for (const order of orders) {
      const f = fixture(outlier)
      try {
        for (const index of order) { f.stores[index].adopt() }
        t.ok(f.snapshot().every(({ state }) => stateDigest(state).equals(f.selected)),
          `${roles[outlier]} outlier with order ${order.join('/')}: all copies match`)
        t.ok(f.stores.every((store) => store._adopted && !store.db.isTransaction),
          'successful adoption commits and marks every owner ready')
      } finally { f.cleanup() }
    }
  }
  t.end()
})

test('isolated adoption selection and donor faults cannot commit unsupported replacement', (t) => {
  const cases = ['selected reference', 'selected bit', 'own reference', 'self donor', 'unknown donor']
  for (const fault of cases) {
    const f = fixture()
    try {
      const before = f.snapshot()
      const outlier = f.stores[1]
      const target = fault === 'selected reference' || fault === 'own reference'
        ? f.stores[0] : outlier
      const captured = target._captured
      if (fault === 'selected reference') { captured.selectedDigest = outlier._captured.ownDigest }
      if (fault === 'selected bit') { captured.selectedDigest[0] ^= 1 }
      if (fault === 'own reference') { captured.ownDigest = outlier._captured.ownDigest }
      if (fault === 'self donor') { captured.donor = target.role }
      if (fault === 'unknown donor') { captured.donor = 'procD' }
      t.throws(() => target.adopt(), adoptionError, `${fault}: rejects the fault`)
      t.notOk(target._adopted, 'failed adoption never marks the owner ready')
      t.deepEqual(f.snapshot(), before, 'independent readers see no committed replacement')
      target.close()
      t.deepEqual(f.snapshot(), before, 'owner cleanup preserves the original durable copies')
      f.restart(t)
    } finally { f.cleanup() }
  }
  t.end()
})

test('a donor change to the other supporting local Proc remains a valid adoption', (t) => {
  const f = fixture()
  try {
    const target = f.stores[1]
    t.equal(target._captured.donor, 'procA')
    target._captured.donor = 'procC'
    target.adopt()
    t.ok(f.snapshot().every(({ state }) => stateDigest(state).equals(f.selected)),
      'the expected state is unchanged when the donor is another majority member')
  } finally { f.cleanup() }
  t.end()
})

test('replacing a complete captured decision cannot turn the minority into the next majority', (t) => {
  for (let outlier = 0; outlier < roles.length; outlier++) {
    const f = fixture(outlier)
    try {
      const before = f.snapshot()
      const target = f.stores.find((store, index) => index !== outlier)
      const captured = target._captured
      // One aggregate-reference substitution changes source and expected state
      // together. The independent read of the other local DB1 must contradict this choice.
      target._captured = {
        ownDigest: captured.ownDigest,
        selectedDigest: f.stores[outlier]._captured.ownDigest,
        donor: roles[outlier],
        receipt: captured.receipt,
      }
      t.throws(() => target.adopt(), (error) => error.code === Err.MONSTER_CORRUPT &&
        /DB1 proc[ABC] does not match selected state/.test(error.message),
      `${roles[outlier]} minority: the unaffected local Proc refuses its promotion`)
      t.deepEqual(f.snapshot(), before, 'no incorrect copy commits before failure')
      target.close()
      t.deepEqual(f.snapshot(), before, 'rollback leaves the original majority intact')
      f.restart(t)
    } finally { f.cleanup() }
  }
  t.end()
})

test('one flipped copy decision cannot install or approve an unsupported state', (t) => {
  for (const mode of ['skip outlier copy', 'copy matching owner']) {
    const f = fixture()
    const equals = Buffer.prototype.equals
    let injected = false
    try {
      const target = f.stores[mode === 'skip outlier copy' ? 1 : 2]
      const { ownDigest, selectedDigest } = target._captured
      const before = f.snapshot()
      Buffer.prototype.equals = function (other) {
        const same = equals.call(this, other)
        if (!injected && this === ownDigest && other === selectedDigest) {
          injected = true
          return !same
        }
        return same
      }
      t.throws(() => target.adopt(), adoptionError, `${mode}: independent evidence rejects the decision`)
      Buffer.prototype.equals = equals
      t.ok(injected, 'exactly the intended decision was changed once')
      t.notOk(target._adopted, 'the faulty decision does not mark adoption complete')
      t.deepEqual(f.snapshot(), before, 'the decision does not change durable state')
      target.close()
      f.restart(t)
    } finally {
      Buffer.prototype.equals = equals
      f.cleanup()
    }
  }
  t.end()
})

test('destination verification rejects corrupted copy values before commit', (t) => {
  for (const field of ['election term', 'log payload', 'meta repair state']) {
    const f = fixture()
    const target = f.stores[1]
    const db = target.db
    const prepare = db.prepare
    let injected = false
    try {
      const before = f.snapshot()
      db.prepare = function (sql) {
        const statement = prepare.call(this, sql)
        const matches = field === 'election term' ? /INSERT INTO raft_election/.test(sql)
          : field === 'log payload' ? /INSERT INTO raft_log/.test(sql)
            : /UPDATE monsterft_meta SET applied_seq/.test(sql)
        if (!matches) { return statement }
        const run = statement.run
        statement.run = function (...args) {
          if (!injected) {
            injected = true
            if (field === 'election term') { args[0] = 4n }
            if (field === 'log payload') { args[2] = Buffer.from('changed during copy') }
            if (field === 'meta repair state') { args[2] = BigInt(Err.REPAIR_NO_QUORUM) }
          }
          return run.apply(this, args)
        }
        return statement
      }
      t.throws(() => target.adopt(), (error) => error.code === Err.MONSTER_CORRUPT,
        `${field}: destination no longer matches the independently selected state`)
      db.prepare = prepare
      t.ok(injected, 'one copied value was changed')
      t.notOk(target._adopted)
      t.ok(db.isTransaction, 'failed copy remains uncommitted for owner cleanup')
      t.deepEqual(f.snapshot(), before, 'another connection still sees the untouched original state')
      target.close()
      t.deepEqual(f.snapshot(), before, 'closing rolls back every part of the failed copy')
      f.restart(t)
    } finally {
      db.prepare = prepare
      f.cleanup()
    }
  }
  t.end()
})
