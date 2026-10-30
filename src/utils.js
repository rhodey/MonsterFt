import { z } from 'zod'
import * as Err from './error.js'
import * as util from './util.js'
import { digestEntry } from './utilm.js'

const generationSchema = z.custom((value) => value instanceof Uint8Array &&
  value.byteLength === 16 && value.some((byte) => byte !== 0))
const metaFields = z.looseObject({
  generation: generationSchema,
  pending_cmd_seq: util.schemas.nonnegativeSeq.nullable(),
})
const initialMetaSchema = metaFields.extend({
  applied_seq: z.literal(-1n), repair_state: z.literal(BigInt(Err.REPAIR_NONE)),
  applied_entry_hash: z.null(), applied_local_digest: z.null(),
})
const appliedMetaSchema = metaFields.extend({
  applied_seq: util.schemas.nonnegativeSeq,
  repair_state: z.literal([
    BigInt(Err.REPAIR_NONE), BigInt(Err.REPAIR_NO_QUORUM), BigInt(Err.REPAIR_OUTSIDE),
  ]),
  applied_entry_hash: util.schemas.digest, applied_local_digest: util.schemas.digest,
})
const initialReceiptSchema = z.looseObject({
  seq: z.literal(-1n), entry_hash: z.null(), local_digest: z.null(),
})
const appliedReceiptSchema = z.looseObject({
  seq: util.schemas.nonnegativeSeq,
  entry_hash: util.schemas.digest, local_digest: util.schemas.digest,
})

const asBuffer = (value) =>
  Buffer.from(value.buffer, value.byteOffset, value.byteLength)

const corrupt = (message) => {
  throw new Err.ErrorWithCode(message, Err.MONSTER_CORRUPT)
}

function validateReceipt(receipt) {
  return util.validate(receipt?.seq === -1n ? initialReceiptSchema : appliedReceiptSchema,
    receipt, 'apply receipt', Err.MONSTER_CORRUPT)
}

function checkGeneration(db, table, generation, statement = null) {
  const row = (statement ?? db.prepare(`SELECT generation FROM ${table} WHERE id = 1`)).get()
  const name = table === 'monsterft_meta' ? 'DB1' : 'DB2'
  util.validate(generationSchema, row?.generation, `${name} generation`, Err.MONSTER_CORRUPT)
  if (!asBuffer(row.generation).equals(generation)) {
    throw new Err.ErrorWithCode(`${name} generation different`, Err.GENERATION_DIFF)
  }
}

function claimGeneration(db, table, generation) {
  if (!util.isSeed(generation)) {
    throw new Err.ErrorWithCode('generation must be a nonzero 16-byte Buffer', Err.ARGUMENT_ILLEGAL)
  }
  if (!db.isTransaction) {
    throw new Err.ErrorWithCode('generation claim requires a transaction', Err.MONSTER_ILLEGAL)
  }
  if (db.prepare(`UPDATE ${table} SET generation = ? WHERE id = 1`).run(generation).changes !== 1n) {
    corrupt(`${table} row is missing`)
  }
}

function verifyEntry(db1, seq, hash, name='applied') {
  const row = db1.prepare('SELECT term, entry FROM raft_log WHERE seq = ?').get(seq)
  if (row === undefined) { corrupt(`${name} log entry not found`) }
  util.validate(util.logSchemas.record, row, 'stored log', Err.LOG_CORRUPT)
  if (!asBuffer(hash).equals(digestEntry(row.term, row.entry))) {
    corrupt(`${name} entry hash does not match`)
  }
}

function createApply(db2, generation) {
  db2.exec(`
    CREATE TABLE monsterft_meta_apply (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      generation BLOB NOT NULL CHECK (length(generation) = 16),
      seq INTEGER NOT NULL,
      entry_hash BLOB,
      local_digest BLOB,
      CHECK (
        (seq = -1 AND entry_hash IS NULL AND local_digest IS NULL) OR
        (seq >= 0 AND entry_hash IS NOT NULL AND length(entry_hash) = ${util.HASH_BYTES} AND
         local_digest IS NOT NULL AND length(local_digest) = ${util.HASH_BYTES})
      )
    ) STRICT;
  `)
  db2.prepare(`
    INSERT INTO monsterft_meta_apply (id, generation, seq, entry_hash, local_digest)
    VALUES (1, ?, -1, NULL, NULL)
  `).run(generation)
}

function createMeta(db1, generation) {
  db1.exec(`
    CREATE TABLE monsterft_meta (
      id INTEGER PRIMARY KEY CHECK (id = 1),
      generation BLOB NOT NULL CHECK (length(generation) = 16),
      applied_seq INTEGER NOT NULL,
      applied_entry_hash BLOB,
      repair_state INTEGER NOT NULL CHECK (repair_state IN (${Err.REPAIR_NONE}, ${Err.REPAIR_NO_QUORUM}, ${Err.REPAIR_OUTSIDE})),
      pending_cmd_seq INTEGER,
      applied_local_digest BLOB,
      CHECK (
        (applied_seq = -1 AND applied_entry_hash IS NULL AND
         applied_local_digest IS NULL) OR
        (applied_seq >= 0 AND applied_entry_hash IS NOT NULL AND
         length(applied_entry_hash) = ${util.HASH_BYTES} AND applied_local_digest IS NOT NULL AND
         length(applied_local_digest) = ${util.HASH_BYTES})
      ),
      CHECK (pending_cmd_seq IS NULL OR
             (pending_cmd_seq >= 0 AND pending_cmd_seq <= applied_seq))
    ) STRICT;
  `)
  db1.prepare(`
    INSERT INTO monsterft_meta
      (id, generation, applied_seq, applied_entry_hash, repair_state,
       pending_cmd_seq, applied_local_digest)
    VALUES (1, ?, -1, NULL, ${Err.REPAIR_NONE}, NULL, NULL)
  `).run(generation)
}

function readApply(db2) {
  const table = db2.prepare(`
    SELECT type FROM sqlite_schema WHERE name = 'monsterft_meta_apply'
  `).get()
  if (table?.type !== 'table') { corrupt('apply meta table is missing') }
  const rows = db2.prepare(`
    SELECT id, generation, seq, entry_hash, local_digest FROM monsterft_meta_apply
  `).all()
  if (rows.length !== 1 || rows[0].id !== 1n) {
    corrupt('apply meta row is missing or not singleton')
  }
  const { id, generation, ...receipt } = rows[0]
  util.validate(generationSchema, generation, 'DB2 generation', Err.MONSTER_CORRUPT)
  return validateReceipt(receipt)
}

function readMeta(db1) {
  const table = db1.prepare(`
    SELECT type FROM sqlite_schema WHERE name = 'monsterft_meta'
  `).get()
  if (table?.type !== 'table') { corrupt('meta table is missing') }
  const rows = db1.prepare(`
    SELECT id, generation, applied_seq, applied_entry_hash, repair_state,
           pending_cmd_seq, applied_local_digest
    FROM monsterft_meta
  `).all()
  if (rows.length !== 1 || rows[0].id !== 1n) {
    corrupt('meta row is missing or not singleton')
  }
  util.validate(rows[0].applied_seq === -1n ? initialMetaSchema : appliedMetaSchema,
    rows[0], 'DB1 meta', Err.MONSTER_CORRUPT)
  const { generation, ...meta } = rows[0]
  const pending = meta.pending_cmd_seq
  if (pending !== null && pending > meta.applied_seq) {
    corrupt('pending command sequence is corrupt')
  }
  return meta
}

function copyApplyInto(db1, receipt) {
  try {
    const result = db1.prepare(`
      UPDATE monsterft_meta
      SET applied_seq = ?, applied_entry_hash = ?,
          pending_cmd_seq = ?, applied_local_digest = ?
      WHERE id = 1 AND applied_seq = ? AND pending_cmd_seq IS NULL
        AND repair_state = ${Err.REPAIR_NONE}
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

function reconcileReceipt(db1, receipt) {
  validateReceipt(receipt)
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
    if (meta.repair_state !== BigInt(Err.REPAIR_NONE)) {
      corrupt('apply receipt is ahead of DB1 checkpoint while a repair fence is set')
    }
    verifyEntry(db1, receipt.seq, receipt.entry_hash, 'apply')
    copyApplyInto(db1, receipt)
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
  util.validate(util.schemas.nonnegativeSeq, first?.seq, 'stored log begin', Err.LOG_CORRUPT)
  if (receipt.seq >= first.seq) {
    verifyEntry(db1, receipt.seq, receipt.entry_hash, 'apply')
  }
  return meta
}

function reconcile(db1, db2) {
  return reconcileReceipt(db1, readApply(db2))
}

export {
  claimGeneration, checkGeneration,
  readMeta, readApply, verifyEntry, copyApplyInto, createMeta, createApply,
  validateReceipt, reconcileReceipt, reconcile,
}
