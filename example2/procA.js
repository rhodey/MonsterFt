import { ErrorCodes, MonsterProc, udsClient } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const [id, socketPath, databasePath] = process.argv.slice(2)
const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}

const opts = { electionTimeout: 100 }
const proc = new MonsterProc(
  'procA', id, nodeIds, localSend, databasePath, opts
)

proc.on('error', (err) => {
  console.error(`${id}/procA`, err.message, err)
  if (err.code !== ErrorCodes.REPAIR_QUORUM_IMPOSSIBLE || !proc.isOpen) {
    process.exit(1)
  }
})
socket.on('data', (frame) => proc.onReceive('gate', frame))
socket.on('error', (err) => {
  console.error(`${id}/procA/socket`, err.message, err)
  process.exit(1)
})
socket.on('close', () => process.exit(1))
