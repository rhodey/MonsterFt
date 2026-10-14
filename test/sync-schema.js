import test from 'tape'
import { pack } from 'msgpackr'
import { HASH_BYTES, validate } from '../src/util.js'
import { monsterSchemas, validateSyncEntry } from '../src/utilm.js'
import * as Err from '../src/error.js'

const node = { nodes: ['a', 'b', 'c'], quorum: 2 }
const sync = (fields = {}) => ({
  type: 'sync', cmdSeq: 1n, quorum: true,
  agree: ['a', 'b'], disagree: ['c'], digest: Buffer.alloc(HASH_BYTES),
  ...fields,
})
const corrupt = (err) => err instanceof Err.ErrorWithCode && err.code === Err.MONSTER_CORRUPT

test('SYNC schemas enforce each decision without rewriting entry fields', (t) => {
  for (const digest of [Buffer.alloc(HASH_BYTES), new Uint8Array(HASH_BYTES)]) {
    const extra = { bytes: Buffer.from('extension') }
    const entry = sync({ digest, extra })
    const encoded = pack(entry)
    t.equal(validate(monsterSchemas.sync, entry, 'SYNC entry', Err.MONSTER_CORRUPT), entry,
      'validation preserves the original entry')
    t.deepEqual(validateSyncEntry(node, entry), { agree: ['a', 'b'], disagree: ['c'] })
    t.equal(entry.digest, digest, 'digest view is not copied')
    t.equal(entry.extra, extra, 'extension object is unchanged')
    t.deepEqual(pack(entry), encoded, 'validation preserves encoded bytes')
  }
  for (const digest of [undefined, null, 'unused', 42, Buffer.alloc(1), { arbitrary: true }]) {
    const entry = sync({ quorum: false, agree: [], disagree: node.nodes, digest })
    t.doesNotThrow(() => validateSyncEntry(node, entry), 'false quorum ignores unused digest')
    t.equal(entry.digest, digest, 'unused digest is not changed')
  }
  const absentDigest = sync({ quorum: false, agree: [] })
  delete absentDigest.digest
  t.doesNotThrow(() => validateSyncEntry(node, absentDigest), 'false quorum needs no digest')
  t.notOk(Object.hasOwn(absentDigest, 'digest'), 'validation does not add a missing digest')
  for (const digest of [undefined, null, 'invalid', Buffer.alloc(0), Buffer.alloc(HASH_BYTES - 1),
    Buffer.alloc(HASH_BYTES + 1)]) {
    t.throws(() => validateSyncEntry(node, sync({ digest })),
      (err) => corrupt(err) && err.message === 'SYNC entry digest is corrupt',
      'true quorum requires a digest of the existing length')
  }
  t.throws(() => validateSyncEntry(node, sync({ quorum: false })),
    (err) => corrupt(err) && err.message === 'SYNC entry agree is corrupt',
    'false quorum requires an empty agree array')
  for (const quorum of [undefined, null, 0, 1, 'true']) {
    t.throws(() => validateSyncEntry(node, sync({ quorum })), corrupt, 'quorum remains boolean')
  }
  t.end()
})

test('SYNC membership and configured quorum remain contextual checks', (t) => {
  for (const fields of [
    { agree: ['a'] }, { agree: ['b', 'a'] }, { agree: ['a', 'a'] }, { agree: ['a', 'z'] },
    { disagree: ['c', 'c'] }, { disagree: ['c', 'a'] }, { disagree: ['z'] },
  ]) {
    const entry = sync(fields)
    t.ok(monsterSchemas.sync.validate(entry), 'structure does not impose cluster policy')
    t.throws(() => validateSyncEntry(node, entry), corrupt, 'context rejects invalid membership')
  }
  t.throws(() => validateSyncEntry({ ...node, quorum: 3 }, sync()), corrupt,
    'receiving node still applies its configured quorum')
  t.doesNotThrow(() => validateSyncEntry(node, sync({ disagree: ['a', 'c'] })),
    'existing overlap policy is preserved')
  t.end()
})
