import { Gate } from './gate.v2.js'
import { MonsterProc } from './proc.v2.js'
import { MonsterApply } from './apply.v2.js'
import { databasePaths } from './storage.v2.js'
import { deleteSQLiteFiles } from './util.v2.js'
import * as Err from './error.v2.js'
import { certify } from './certify.v2.js'

class MonsterFt extends Gate {
  static certify(databasePath, allowUnresolved=false) {
    return certify(databasePath, allowUnresolved)
  }

  constructor(id, nodes, send, databasePath, opts = {}) {
    opts = typeof opts === 'function' ? opts() : opts
    if (!opts || typeof opts !== 'object') {
      throw new Err.ErrorWithCode('options must be an object', Err.ARGUMENT_ILLEGAL)
    }
    const components = new Map()
    let gate = null
    const localSend = (from) => (to, frame) => {
      if (to === 'gate') { return gate.onReceive(from, frame) }
      return components.get(to).onReceive(from, frame)
    }
    super(id, nodes, send, databasePath, {
      ...opts, localSend: opts.localSend === undefined ? localSend('gate') : opts.localSend,
    })
    gate = this
    this.components = components
    if (opts.localSend === undefined) {
      for (const role of ['procA', 'procB', 'procC']) {
        components.set(role, new MonsterProc(role, id, nodes, localSend(role), databasePath, opts))
      }
      components.set('apply', new MonsterApply(id, localSend('apply'), databasePath, opts))
    }
  }

  del() {
    if (this.isOpen || this._opening || (this._closing && !this._closeComplete) ||
        [...this.components.values()].some((component) => component.db !== null)) {
      throw new Err.ErrorWithCode('node databases are open', Err.LOG_OPEN)
    }
    for (const path of Object.values(databasePaths(this.databasePath))) { deleteSQLiteFiles(path) }
  }
}

export { MonsterFt }
