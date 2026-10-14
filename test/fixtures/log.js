import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { SQLiteLog } from '../../src/log.js'

const toBuf = (value) => value === null ? null : Buffer.from(JSON.stringify(value))
const toEntry = (value, term=0n) => ({
  term, entry: Buffer.isBuffer(value) ? value : Buffer.from(value),
})

function logFixture(t, name, opts={}) {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'monsterft-log-'))
  const file = path.join(directory, name)
  const logs = []
  const create = () => {
    const log = new SQLiteLog(file, opts)
    logs.push(log)
    return log
  }
  t.teardown(() => {
    logs.forEach((log) => log.close())
    fs.rmSync(directory, { recursive: true, force: true })
  })
  return { file, create }
}

export { logFixture, toBuf, toEntry }
