import * as Err from './error.js'
import * as util from './util.js'
import { digestEntry, isDigest } from './utilm.js'

const asBuffer = (value) =>
  Buffer.from(value.buffer, value.byteOffset, value.byteLength)

const corrupt = (message) => {
  throw new Err.ErrorWithCode(message, Err.MONSTER_CORRUPT)
}

function readMeta(db1) {
  const table = db1.prepare(`
    SELECT type FROM sqlite_schema WHERE name = 'monsterft_meta'
  `).get()
  if (table?.type !== 'table') { corrupt('meta table is missing') }
  const rows = db1.prepare(`
    SELECT id, applied_seq, applied_entry_hash, repair_state,
           pending_cmd_seq, applied_local_digest
    FROM monsterft_meta
  `).all()
  if (rows.length !== 1 || rows[0].id !== 1n) {
    corrupt('meta row is missing or not singleton')
  }
  const meta = rows[0]
  if (!util.isSeq(meta.applied_seq)) { corrupt('applied sequence is corrupt') }
  if (meta.repair_state !== 0n && meta.repair_state !== 1n && meta.repair_state !== 2n) {
    corrupt('stored repair state is corrupt')
  }
  if (meta.applied_seq === -1n) {
    if (meta.repair_state !== 0n) {
      corrupt('repair_state must be 0 when applied_seq is -1')
    }
    if (meta.applied_entry_hash !== null || meta.applied_local_digest !== null) {
      corrupt('initial applied hashes must be null')
    }
  } else {
    if (!isDigest(meta.applied_entry_hash)) { corrupt('applied entry hash is corrupt') }
    if (!isDigest(meta.applied_local_digest)) { corrupt('applied local digest is corrupt') }
  }
  const pending = meta.pending_cmd_seq
  if (pending !== null &&
      (!util.isSeq(pending) || pending < 0n || pending > meta.applied_seq)) {
    corrupt('pending command sequence is corrupt')
  }
  return meta
}

function readApply(db2) {
  const table = db2.prepare(`
    SELECT type FROM sqlite_schema WHERE name = 'monsterft_meta_apply'
  `).get()
  if (table?.type !== 'table') { corrupt('apply meta table is missing') }
  const rows = db2.prepare(`
    SELECT id, seq, entry_hash, local_digest FROM monsterft_meta_apply
  `).all()
  if (rows.length !== 1 || rows[0].id !== 1n) {
    corrupt('apply meta row is missing or not singleton')
  }
  const receipt = rows[0]
  if (!util.isSeq(receipt.seq)) { corrupt('apply sequence is corrupt') }
  if (receipt.seq === -1n) {
    if (receipt.entry_hash !== null || receipt.local_digest !== null) {
      corrupt('initial apply hashes must be null')
    }
  } else {
    if (!isDigest(receipt.entry_hash)) { corrupt('apply entry hash is corrupt') }
    if (!isDigest(receipt.local_digest)) { corrupt('apply local digest is corrupt') }
  }
  return receipt
}

function verifyEntry(db1, seq, hash, name='applied') {
  const row = db1.prepare('SELECT term, entry FROM raft_log WHERE seq = ?').get(seq)
  if (row === undefined) { corrupt(`${name} log entry not found`) }
  if (!util.isTerm(row.term)) {
    throw new Err.ErrorWithCode('stored log term is corrupt', Err.LOG_CORRUPT)
  }
  if (!(row.entry instanceof Uint8Array)) {
    throw new Err.ErrorWithCode('entry must be blob', Err.LOG_CORRUPT)
  }
  if (!asBuffer(hash).equals(digestEntry(row.term, row.entry))) {
    corrupt(`${name} entry hash does not match`)
  }
}

function copyApply(db1, receipt) {
  try {
    const result = db1.prepare(`
      UPDATE monsterft_meta
      SET applied_seq = ?, applied_entry_hash = ?,
          pending_cmd_seq = ?, applied_local_digest = ?
      WHERE id = 1 AND applied_seq = ? AND pending_cmd_seq IS NULL
        AND repair_state = 0
    `).run(
      receipt.seq,
      receipt.entry_hash,
      receipt.seq === 0n ? null : receipt.seq,
      receipt.local_digest,
      receipt.seq - 1n,
    )
    if (result.changes !== 1n) {
      throw new Err.ErrorWithCode('update count not one', Err.SQLITE_ERROR)
    }
  } catch (err) {
    throw Err.wrapError(err, Err.SQLITE_ERROR, 'DB1 copy apply ')
  }
}

function reconcile(db1, db2) {
  const receipt = readApply(db2)
  const meta = readMeta(db1)
  if (meta.applied_seq >= 0n) {
    verifyEntry(db1, meta.applied_seq, meta.applied_entry_hash)
  }
  if (receipt.seq > meta.applied_seq) {
    if (receipt.seq !== meta.applied_seq + 1n) {
      corrupt('apply receipt is more than one entry ahead of DB1 checkpoint')
    }
    if (meta.pending_cmd_seq !== null) {
      corrupt('apply receipt is ahead of DB1 checkpoint while a CMD is pending')
    }
    if (meta.repair_state !== 0n) {
      corrupt('apply receipt is ahead of DB1 checkpoint while a repair fence is set')
    }
    verifyEntry(db1, receipt.seq, receipt.entry_hash, 'apply')
    copyApply(db1, receipt)
    return readMeta(db1)
  }
  if (receipt.seq === -1n) {
    if (meta.applied_seq !== -1n) {
      corrupt('apply receipt seq is -1 but DB1 applied_seq is not')
    }
    return meta
  }
  if (!asBuffer(meta.applied_local_digest).equals(asBuffer(receipt.local_digest))) {
    corrupt('apply local digest does not match')
  }
  if (meta.pending_cmd_seq !== null && meta.pending_cmd_seq !== receipt.seq) {
    corrupt('pending command seq does not match apply receipt')
  }
  const first = db1.prepare('SELECT seq FROM raft_log ORDER BY seq LIMIT 1').get()
  if (first === undefined || !util.isSeq(first.seq) || first.seq < 0n) {
    throw new Err.ErrorWithCode('stored log begin is corrupt', Err.LOG_CORRUPT)
  }
  if (receipt.seq >= first.seq) {
    verifyEntry(db1, receipt.seq, receipt.entry_hash, 'apply')
  }
  return meta
}

function createMeta(db1) {
  db1.exec(`
    CREATE TABLE monsterft_meta (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      applied_seq INTEGER NOT NULL,
      applied_entry_hash BLOB,
      repair_state INTEGER NOT NULL CHECK (repair_state IN (0, 1, 2)),
      pending_cmd_seq INTEGER,
      applied_local_digest BLOB,
      CHECK (
        (applied_seq = -1 AND applied_entry_hash IS NULL AND
         applied_local_digest IS NULL) OR
        (applied_seq >= 0 AND applied_entry_hash IS NOT NULL AND
         length(applied_entry_hash) = 32 AND applied_local_digest IS NOT NULL AND
         length(applied_local_digest) = 32)
      ),
      CHECK (pending_cmd_seq IS NULL OR
             (pending_cmd_seq >= 0 AND pending_cmd_seq <= applied_seq))
    ) STRICT;
    INSERT INTO monsterft_meta
      (id, applied_seq, applied_entry_hash, repair_state,
       pending_cmd_seq, applied_local_digest)
    VALUES (1, -1, NULL, 0, NULL, NULL);
  `)
}

function createApply(db2) {
  db2.exec(`
    CREATE TABLE monsterft_meta_apply (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      seq INTEGER NOT NULL,
      entry_hash BLOB,
      local_digest BLOB,
      CHECK (
        (seq = -1 AND entry_hash IS NULL AND local_digest IS NULL) OR
        (seq >= 0 AND entry_hash IS NOT NULL AND length(entry_hash) = 32 AND
         local_digest IS NOT NULL AND length(local_digest) = 32)
      )
    ) STRICT;
    INSERT INTO monsterft_meta_apply (id, seq, entry_hash, local_digest)
    VALUES (1, -1, NULL, NULL);
  `)
}

function createInTransaction(database, create) {
  database.exec('BEGIN IMMEDIATE')
  try {
    create(database)
    database.exec('COMMIT')
  } catch (err) {
    if (database.isTransaction) {
      try { database.exec('ROLLBACK') } catch { }
    }
    throw err
  }
}

function initialize(db1, db2) {
  const schema1 = db1.prepare('SELECT name, type FROM sqlite_schema').all()
  const schema2 = db2.prepare('SELECT name, type FROM sqlite_schema').all()
  const hasMeta = schema1.some((row) => row.name === 'monsterft_meta' && row.type === 'table')
  const hasApply = schema2.some((row) => row.name === 'monsterft_meta_apply' && row.type === 'table')
  if (!hasMeta || !hasApply) {
    const meta = hasMeta ? readMeta(db1) : null
    const receipt = hasApply ? readApply(db2) : null
    const emptyLog = db1.prepare('SELECT seq FROM raft_log LIMIT 1').get() === undefined
    const pristine1 = schema1.every((row) =>
      row.type === 'table' && ['raft_log', 'raft_election', 'monsterft_meta'].includes(row.name))
    const pristine2 = schema2.every((row) =>
      row.type === 'table' && row.name === 'monsterft_meta_apply')
    if (!emptyLog) {
      corrupt('meta init requires empty raft_log')
    }
    if (!pristine1) {
      corrupt('meta init found unexpected DB1 schema')
    }
    if (!pristine2) {
      corrupt('meta init found unexpected DB2 schema')
    }
    if (meta !== null && meta.applied_seq !== -1n) {
      corrupt('meta init requires DB1 applied_seq = -1')
    }
    if (receipt !== null && receipt.seq !== -1n) {
      corrupt('meta init requires DB2 apply seq = -1')
    }
    if (!hasMeta) { createInTransaction(db1, createMeta) }
    if (!hasApply) { createInTransaction(db2, createApply) }
  }
  return reconcile(db1, db2)
}

export { initialize, reconcile, copyApply }
