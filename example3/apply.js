import { MonsterApply, udsClient } from '../src/index.js'

const [id, socketPath, databasePath, serverUrl] = process.argv.slice(2)
const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}
const requestJson = async (url, options) => {
  const response = await fetch(url, options)
  const value = await response.json()
  if (!response.ok) { throw new Error(value.error) }
  return value
}
async function apply(db, data, term, seq) {
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

const component = new MonsterApply(id, localSend, databasePath, { apply })
component.on('warn', (err) => console.warn(`${id}/apply`, err))
component.on('error', (err) => console.error(`${id}/apply`, err))
socket.on('data', (frame) => component.onReceive('gate', frame))
socket.on('error', (err) => console.error(`${id}/apply/socket`, err))
socket.on('close', () => process.exit(1))
