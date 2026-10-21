import { MonsterApply, udxClient } from '../src/index.js'

const [id, socketPath, databasePath, serverUrl] = process.argv.slice(2)

const requestJson = async (url, options) => {
  const response = await fetch(url, options)
  const value = await response.json()
  if (!response.ok) { throw new Error(value.error) }
  return value
}

const apply = async (db, data, term, seq) => {
  if (seq === 0n) { return }

  const command = JSON.parse(data.toString())
  if (command.method === 'POST') {
    return requestJson(serverUrl, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'MonsterFt-Node-Id': id,
      },
      body: JSON.stringify({ requestId: command.requestId, fact: command.fact }),
    })
  }
  if (command.method === 'GET') {
    const url = new URL(serverUrl)
    url.searchParams.set('requestId', command.requestId)
    url.searchParams.set('observationId', command.observationId)
    return requestJson(url)
  }
  throw new Error(`unknown method: ${command.method}`)
}

const socket = await udxClient(socketPath)
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
