import fs from 'node:fs'
import { MonsterProc } from '../../src/proc.js'
import { MonsterApply } from '../../src/apply.js'
import { udxClient } from '../../src/net/udx.js'
import { REPAIR_QUORUM_IMPOSSIBLE } from '../../src/error.js'

const [role, id, base, socketPath] = process.argv.slice(2)
const connection = await udxClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error('unexpected owner destination') }
  connection.write(frame)
}
const diagnostics = (kind) => (err) => fs.appendFileSync(`${base}.diagnostics`,
  `${JSON.stringify({
    role, kind, message: err.message, code: err.code, open: component.isOpen, pid: process.pid,
  })}\n`)
const opts = {
  electionTimeout: 300, appendTimeout: 2_000,
  localTimeout: 3_000,
}
const component = role === 'apply'
  ? new MonsterApply(id, localSend, base, {
    async apply(db, data, term, seq) {
      fs.appendFileSync(`${base}.calls`, `${seq}\n`)
      if (seq === 0n) {
        db.exec('CREATE TABLE items (id INTEGER PRIMARY KEY, value BLOB NOT NULL) STRICT')
        return null
      }
      db.prepare('INSERT INTO items (value) VALUES (?)').run(data)
      if (data.toString() === 'reject') { throw new Error('agreed rejection') }
      if (data.toString() === 'repair' && fs.existsSync(`${base}.reject`)) {
        throw new Error('dissenting Apply failure')
      }
      if (data.toString() === 'hang' && fs.existsSync(`${base}.hang`)) {
        fs.unlinkSync(`${base}.hang`)
        fs.writeFileSync(`${base}.blocked`, '')
        await new Promise(() => {})
      }
      return data.toString() === 'diverge' ? id : data.toString()
    },
  })
  : new MonsterProc(role, id, ['a', 'b', 'c'], localSend, base, opts)
component.on('error', (err) => {
  try { diagnostics('error')(err) }
  finally {
    if (role === 'apply' || err.code !== REPAIR_QUORUM_IMPOSSIBLE || !component.isOpen) {
      process.exit(1)
    }
  }
})
component.on('warn', diagnostics('warn'))
connection.on('data', (frame) => component.onReceive('gate', frame))
connection.on('error', () => process.exit(1))
connection.on('close', () => process.exit(0))
