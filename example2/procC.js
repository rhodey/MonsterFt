import { MonsterProc, udsClient } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const [id, socketPath, databasePath] = process.argv.slice(2)
const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}
const proc = new MonsterProc('procC', id, nodeIds, localSend, databasePath)
proc.on('warn', (err) => console.warn(`${id}/procC`, err))
proc.on('error', (err) => console.error(`${id}/procC`, err))
socket.on('data', (frame) => proc.onReceive('gate', frame))
socket.on('error', (err) => console.error(`${id}/procC/socket`, err))
socket.on('close', () => process.exit(1))
