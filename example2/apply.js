import { MonsterApply, udsClient } from '../src/index.js'

const [id, socketPath, databasePath] = process.argv.slice(2)

const apply = (db, data, term, seq) => {
  if (seq === 0n) {
    db.exec(`
      CREATE TABLE counters (
        name TEXT PRIMARY KEY NOT NULL,
        value INTEGER NOT NULL
      ) STRICT
    `)
    return
  }

  const command = JSON.parse(data.toString())
  if (command.type !== 'increment') {
    throw new Error(`unknown command: ${command.type}`)
  }

  db.prepare(`
    INSERT INTO counters (name, value) VALUES (?, 1)
      ON CONFLICT (name) DO UPDATE SET value = value + 1
  `).run(command.name)

  return db.prepare(
    'SELECT value FROM counters WHERE name = ?',
  ).get(command.name).value
}

const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}

const opts = { apply }
const proc = new MonsterApply(id, localSend, databasePath, opts)

proc.on('error', (err) => {
  console.error(`${id}/apply`, err.message, err)
  process.exit(1)
})
socket.on('data', (frame) => proc.onReceive('gate', frame))
socket.on('error', (err) => {
  console.error(`${id}/apply/socket`, err.message, err)
  process.exit(1)
})
socket.on('close', () => process.exit(1))
