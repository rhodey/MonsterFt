import { ErrorCodes, MonsterProc, udsClient } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const [id, socketPath, databasePath] = process.argv.slice(2)
const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}
const proc = new MonsterProc('procB', id, nodeIds, localSend, databasePath)
proc.on('warn', (err) => console.warn(`${id}/procB`, err.message, err))
proc.on('error', (err) => {
  console.error(`${id}/procB`, err.message, err)
  if (err.code !== ErrorCodes.REPAIR_QUORUM_IMPOSSIBLE || !proc.isOpen) {
    process.exit(1)
  }
})
socket.on('data', (frame) => proc.onReceive('gate', frame))
socket.on('error', (err) => {
  console.error(`${id}/procB/socket`, err.message, err)
  process.exit(1)
})
socket.on('close', () => process.exit(1))
