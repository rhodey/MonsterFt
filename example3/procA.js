import { MonsterProc, udsClient } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const [id, socketPath, databasePath] = process.argv.slice(2)
const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}
const proc = new MonsterProc('procA', id, nodeIds, localSend, databasePath)
proc.on('warn', (err) => console.warn(`${id}/procA`, err))
proc.on('error', (err) => console.error(`${id}/procA`, err))
socket.on('data', (frame) => proc.onReceive('gate', frame))
socket.on('error', (err) => console.error(`${id}/procA/socket`, err))
// Component diagnostics do not end this process: a repair completion may follow.
// Losing the parent transport does; the next attempt must use a fresh process.
socket.on('close', () => process.exit(1))
