import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { pack } from 'msgpackr'
import { MonsterProc } from '../../src/proc.js'
import { openApplyDatabase, readDatabase, readState, databasePaths } from '../../src/storage.js'
import { encodeFrame, decodeFrame, decodePayload } from '../../src/wire.js'

const roles = ['procA', 'procB', 'procC']
const record = (term=1n, entry='') => ({ term, entry: Buffer.from(entry) })
const cmd = (items=[Buffer.from('item')], matchIndex=[0n, 0n, 0n]) =>
  pack({ type: 'cmd', items, matchIndex })
const append = (fields={}) => ({
  type: 'append', cid: 'page', term: 1n, termP: -1n, seqP: -1n, commitSeq: -1n, ...fields,
})
const outputs = (reply) => (reply?.outputs ?? []).map(decodePayload)
const messages = (reply, type, to) => outputs(reply).filter((item) =>
  item.message.type === type && (to === undefined || item.to === to)).map((item) => item.message)

// Each ordered input is delivered to all three real Procs, preserving a
// restartable majority. Tests supply remote cluster and Apply responses.
function protocol(opts={}, nodes=['a', 'b', 'c'], id='a') {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-protocol-'))
  const base = path.join(directory, id)
  const paths = databasePaths(base)
  const diagnostics = []
  let procs
  let db2
  let cid = 0
  let sent = []
  const f = {
    base, paths, nodes, id, diagnostics,
    get proc() { return procs[0] },
    get procs() { return procs },
    get db2() { return db2 },
    outputs, messages,
    state() { return readDatabase(paths.procA, readState) },
    rows() { return readDatabase(paths.procA, (db) => db.prepare('SELECT * FROM raft_log ORDER BY seq').all()) },
    request(proc, msg) {
      const request = { cid: `local-${++cid}`, ...msg }
      proc.onReceive('gate', encodeFrame(request))
      return sent.find((item) => item.role === proc.role && item.reply.cid === request.cid)?.reply
    },
    open() {
      sent = []
      procs = roles.map((role) => {
        const proc = new MonsterProc(role, id, nodes, (to, frame) => {
          const reply = decodeFrame(frame)
          sent.push({ role, reply })
          f.onSend?.(proc, reply)
        }, base, { electionTimeout: 150, appendTimeout: 500, localTimeout: 60_000, ...opts })
        for (const type of ['warn', 'error']) {
          proc.on(type, (error) => diagnostics.push({ role, type, error }))
        }
        return proc
      })
      for (const proc of procs) f.request(proc, { type: 'hello', seed: Buffer.alloc(16, 1) })
      for (const proc of procs) f.request(proc, { type: 'open' })
      db2 = openApplyDatabase(base)
      for (const type of ['capture', 'adopt', 'reconcile']) {
        for (const proc of procs) {
          if (f.request(proc, { type })?.type !== 'ack') throw proc._fatalError ?? new Error(type)
        }
      }
      f.input('start')
      return f
    },
    input(kind, fields={}) {
      const request = {
        type: 'input', kind, tickCount: f.proc.tickCount + 1n,
        timems: f.proc.executor.timems, ...fields,
      }
      return procs.map((proc) => f.request(proc, request))[0]
    },
    rpc(message, from=nodes.find((peer) => peer !== id), timems=f.proc.executor.timems) {
      return f.input('rpc', { from, timems,
        frame: encodeFrame({ to: id, message: { from, ...message } }),
      })
    },
    tick(timems) { return f.input('tick', { timems }) },
    elect() {
      f.tick(f.proc.executor.timers.get(f.proc.engine.electionTimer).due)
      let reply
      for (const from of nodes.filter((peer) => peer !== id).slice(0, f.proc.engine.quorum - 1)) {
        reply = f.rpc({ type: 'vote', term: f.proc.engine.term, voteGranted: true }, from)
      }
      return reply
    },
    complete(execute, outcomes=execute.items?.map(() => [0, 'ok']) ?? [], digest=Buffer.alloc(32)) {
      const receipt = { seq: execute.seq, entry_hash: execute.entryHash, local_digest: digest }
      db2.prepare('UPDATE monsterft_meta_apply SET seq=?, entry_hash=?, local_digest=?')
        .run(receipt.seq, receipt.entry_hash, digest)
      return f.input('apply', { reply: { type: 'ack', cid: execute.cid, receipt, outcomes } })
    },
    setup() {
      const reply = f.rpc(append({ data: [record()], commitSeq: 0n }))
      f.complete(messages(reply, 'execute', 'apply')[0])
      return f
    },
    leader() {
      const election = f.elect()
      let reply
      for (const to of nodes.filter((peer) => peer !== id).slice(0, f.proc.engine.quorum - 1)) {
        const page = messages(election, 'append', to).find((msg) => msg.data)
        reply = f.rpc({ type: 'ack', term: page.term, cid: page.cid, seq: 0n }, to)
      }
      f.complete(messages(reply, 'execute', 'apply')[0])
      return f
    },
    stop() {
      for (const proc of procs ?? []) proc.close()
      if (db2) { db2.close(); db2 = null }
    },
    reopen() { f.stop(); return f.open() },
    close() { f.stop(); fs.rmSync(directory, { recursive: true, force: true }) },
  }
  try { return f.open() } catch (err) { f.close(); throw err }
}

export { protocol, append, record, cmd, outputs, messages }
