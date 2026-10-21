import { writeSync } from 'node:fs'
import { format } from 'node:util'
import { ErrorCodes, MonsterProc, udsClient } from '../src/index.js'

const nodeIds = ['1', '2', '3']
const [id, socketPath, databasePath] = process.argv.slice(2)
const socket = await udsClient(socketPath)
const localSend = (to, frame) => {
  if (to !== 'gate') { throw new Error(`unexpected local destination: ${to}`) }
  socket.write(frame)
}
const proc = new MonsterProc('procA', id, nodeIds, localSend, databasePath)
proc.on('warn', (err) => writeSync(2, format(`${id}/procA`, err.message, err) + '\n'))
proc.on('error', (err) => {
  try { writeSync(2, format(`${id}/procA`, err.message, err) + '\n') }
  finally {
    if (err.code !== ErrorCodes.REPAIR_QUORUM_IMPOSSIBLE || !proc.isOpen) {
      process.exit(1)
    }
  }
})
socket.on('data', (frame) => proc.onReceive('gate', frame))
socket.on('error', (err) => {
  try { writeSync(2, format(`${id}/procA/socket`, err.message, err) + '\n') }
  finally { process.exit(1) }
})
socket.on('close', () => process.exit(1))
