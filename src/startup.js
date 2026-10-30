import * as Err from './error.js'
import { isInputType } from './utilm.js'
import { Random } from './random.js'
import { MonsterEngine } from './monster.js'

const illegal = (message) => new Err.ErrorWithCode(message, Err.MONSTER_ILLEGAL)

class ProcStartup {
  constructor(proc, opts) {
    this.proc = proc
    this.opts = opts
    this.seed = null
    this.step = 'hello'
  }

  receive(parsed) {
    const { message } = parsed
    const proc = this.proc
    if (message.type === 'hello') {
      if (this.step !== 'hello') { throw illegal('Proc received hello after startup') }
      // Hello can repeat until open, without advancing the shared generator.
      this.seed = message.seed
      proc._ack(message)
      return
    }
    if (this.seed === null) { throw illegal('Proc has not received hello') }
    if (message.type === 'open' && this.step === 'hello') {
      proc.random = new Random(this.seed)
      proc.store.open(this.seed)
      this.step = 'opened'
      proc._ack(message)
    } else if (message.type === 'capture' && this.step === 'opened') {
      const selection = proc.store.capture()
      this.step = 'captured'
      proc._ack(message, { selection })
    } else if (message.type === 'adopt' && this.step === 'captured') {
      proc.store.adopt()
      this.step = 'adopted'
      proc._ack(message)
    } else if (message.type === 'reconcile' && this.step === 'adopted') {
      proc.store.reconcile()
      proc.engine = new MonsterEngine(proc, this.opts)
      this.step = 'reconciled'
      proc._ack(message)
    } else if (isInputType(message.type) && this.step === 'reconciled') {
      if (message.type !== 'start') { throw illegal('Proc runtime has not started') }
      proc._input(message, parsed.clusterMessage, parsed.warning)
    } else {
      throw illegal('Proc startup step is illegal')
    }
  }
}

export { ProcStartup }
