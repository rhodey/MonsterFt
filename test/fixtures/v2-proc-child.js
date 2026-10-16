import { MonsterProc } from '../../src/proc.v2.js'
import { errorToWire } from '../../src/wire.v2.js'

let component = null
let starting = null

process.on('message', (message) => {
  if (message.type === 'boot') {
    // Independent processes need not have identical JavaScript stack settings.
    Error.stackTraceLimit = { procA: 4, procB: 12, procC: 30 }[message.role]
    starting = setTimeout(() => {
      starting = null
      component = new MonsterProc(message.role, message.id, message.nodes,
        (to, frame) => {
          if (to !== 'gate') { throw new Error('unexpected Proc destination') }
          process.send({ type: 'frame', frame })
        }, message.databasePath, message.opts)
      component.on('error', (err) => process.send({ type: 'error', error: errorToWire(err) }))
      process.send({ type: 'ready' })
    }, message.delay)
  } else if (message.type === 'frame') {
    // Before this component comes online, the startup hello may be lost.
    component?.onReceive('gate', message.frame)
  } else if (message.type === 'exit') {
    clearTimeout(starting)
    component?.close()
    process.disconnect()
  }
})

process.on('disconnect', () => {
  clearTimeout(starting)
  component?.close()
})
