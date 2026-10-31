import { EventEmitter } from 'node:events'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { Gate } from './gate.js'
import { oneShot } from './util.js'
import * as Err from './error.js'

// Each Gate startup supplies a random generation, claimed in DB1 and DB2.
// Proc and Apply check it inside their write transactions, so a replacement's
// claim makes an old child process fail with GENERATION_DIFF on its next check.
// This guards writes even if supervision leaves an old child process running;

const ROLES = ['procA', 'procB', 'procC', 'apply']
const noop = () => {}
const closed = () => new Err.ErrorWithCode('supervisor is stopped', Err.NODE_NOT_OPEN)
const failure = (message) => new Err.ErrorWithCode(message, Err.SUPERVISOR_ERROR)

class Supervisor extends EventEmitter {
  constructor(setup, opts = {}) {
    super()
    if (typeof setup !== 'function' || !opts || typeof opts !== 'object') {
      throw new Err.ErrorWithCode('Supervisor arguments are illegal', Err.ARGUMENT_ILLEGAL)
    }
    this.restartDelay = opts.restartDelay ?? 1_000
    this.startupTimeout = opts.startupTimeout ?? 30_000
    if (!Number.isSafeInteger(this.restartDelay) || this.restartDelay < 0 ||
        !Number.isSafeInteger(this.startupTimeout) || this.startupTimeout < 1) {
      throw new Err.ErrorWithCode('Supervisor timeouts are illegal', Err.ARGUMENT_ILLEGAL)
    }
    this._setup = setup
    this._gate = null
    this._run = null
    this._openWait = null
    this._loopWork = null
    this._closeWork = null
    this._drainWork = null
    this._stopped = false
    this._closed = false
    this._wake = null
  }

  get gate() { return this._gate }
  get isStopped() { return this._stopped }

  _emitSafe(type, value) {
    for (const listener of this.rawListeners(type)) {
      try { listener.call(this, value) }
      catch (err) {
        if (type !== 'error') { this._emitSafe('error', Err.wrapError(err)) }
      }
    }
  }

  open() {
    if (this._stopped) { return Promise.reject(closed()) }
    if (this._openWait !== null) { return this._openWait.promise }
    this._openWait = oneShot()
    this._loopWork = Promise.resolve().then(() => this._loop())
    return this._openWait.promise
  }

  _fail(run, error, report = true) {
    if (this._run !== run || run.stopping || run.error !== null) { return }
    const err = Err.wrapError(error, report ? Err.SUPERVISOR_ERROR : null)
    run.error = err
    run.failed.resolve(err)
    this._gate?.stop()
    if (report) { this._emitSafe('error', err) }
  }

  _delay() {
    if (this._stopped) { return Promise.resolve() }
    return new Promise((resolve) => {
      const finish = () => {
        clearTimeout(timer)
        this._wake = null
        resolve()
      }
      const timer = setTimeout(finish, this.restartDelay)
      this._wake = finish
    })
  }

  _validate(setup) {
    if (!(setup?.gate instanceof Gate) || setup.gate.isOpen ||
        (setup.ready !== undefined && typeof setup.ready !== 'function') ||
        (setup.failed !== undefined && typeof setup.failed?.then !== 'function') ||
        (setup.stop !== undefined && typeof setup.stop !== 'function') ||
        (setup.cleanup !== undefined && typeof setup.cleanup !== 'function')) {
      throw failure('setup requires a fresh Gate, optional ready/stop/cleanup functions and failed promise')
    }
    const entries = new Map()
    for (const role of ROLES) {
      const child = setup.children?.[role]
      const file = child?.file instanceof URL ? fileURLToPath(child.file) : child?.file
      const args = child?.args ?? []
      if (typeof file !== 'string' || file.length === 0 ||
          !Array.isArray(args) || !args.every((arg) => typeof arg === 'string')) {
        throw failure(`${role} must provide a file and string arguments`)
      }
      entries.set(role, { file, args })
    }
    return entries
  }

  _spawn(run, role, entry) {
    const child = spawn(process.execPath, [entry.file, ...entry.args], {
      stdio: ['ignore', 'inherit', 'inherit'],
    })
    const record = { child, ended: false, exited: oneShot(), killFailed: oneShot() }
    run.records.set(role, record)
    run.children.set(role, child)
    child.on('error', (err) => {
      if (child.pid === undefined) {
        record.ended = true
        record.exited.resolve(null)
      } else if (run.stopping) {
        record.killFailed.resolve(Err.wrapError(err, Err.SUPERVISOR_ERROR, `${role} kill `))
        return
      }
      this._fail(run, Err.wrapError(err, Err.SUPERVISOR_ERROR, `${role} process `))
    })
    child.once('exit', (code, signal) => {
      record.ended = true
      record.exited.resolve(null)
      this._fail(run, failure(`${role} exited (${signal ?? code})`))
    })
  }

  async _start(run, entries) {
    for (const [role, entry] of entries) {
      if (run.stopping || this._stopped) { throw closed() }
      this._spawn(run, role, entry)
    }
    await run.setup.ready?.(run.children)
    if (run.stopping || this._stopped || run.error !== null) { throw run.error ?? closed() }
    try {
      await run.setup.gate.open()
    } catch (err) {
      // Gate already reports its own startup failure.
      this._fail(run, err, false)
      throw err
    }
  }

  _stopRun(run, graceful = false) {
    if (run.stopWork !== null) { return run.stopWork }
    run.stopping = true
    clearTimeout(run.timer)
    this._gate = null
    run.failed.resolve(closed())
    let closeError = null
    // Publish the promise before Gate cancellation or cleanup can reenter.
    run.stopWork = Promise.resolve().then(async () => {
      if (run.setup?.gate instanceof Gate) {
        if (run.onError) { run.setup.gate.removeListener('error', run.onError) }
        run.setup.gate.stop()
        if (graceful) {
          try { await run.setup.gate.close() }
          catch (err) { closeError = Err.wrapError(err) }
        }
      }
      let stopError = null
      try {
        if (typeof run.setup?.stop === 'function') { run.setup.stop() }
      }
      catch (err) { stopError = Err.wrapError(err, Err.SUPERVISOR_ERROR) }
      for (const record of run.records.values()) {
        if (record.ended) { continue }
        try { record.child.kill('SIGKILL') }
        catch (err) { record.killFailed.resolve(Err.wrapError(err, Err.SUPERVISOR_ERROR, 'child kill ')) }
      }
      const results = await Promise.all([...run.records.values()].map((record) =>
        record.ended ? null : Promise.race([record.exited.promise, record.killFailed.promise])))
      const err = results.find((result) => result !== null)
      if (err) { throw err }
      if (typeof run.setup?.cleanup === 'function') { await run.setup.cleanup() }
      if (stopError !== null) { throw stopError }
      if (this._run === run) { this._run = null }
    }).catch((err) => {
      throw Err.wrapError(err, Err.SUPERVISOR_ERROR)
    }).then(() => {
      if (closeError !== null) { throw closeError }
    })
    return run.stopWork
  }

  close(graceful = false) {
    this._stopped = true
    this._openWait?.reject(closed())
    this._wake?.()
    if (this._closeWork !== null) { return this._closeWork }
    if (this._closed) { return Promise.resolve() }
    const closing = oneShot()
    this._closeWork = closing.promise
    const gate = this._gate
    // Reserve this shutdown before stopping Gate can wake the supervision loop.
    const stopping = this._run === null ? null : this._stopRun(this._run, graceful)
    gate?.stop()
    const finish = async () => {
      await stopping
      await this._loopWork
      if (this._run !== null) { await this._stopRun(this._run, graceful) }
      this._closed = true
      this._emitSafe('close')
    }
    finish().then(closing.resolve, closing.reject)
    return this._closeWork
  }

  drainCmd() {
    if (this._drainWork !== null) { return this._drainWork }
    const gate = this._gate
    this._stopped = true
    this._openWait?.reject(closed())
    this._wake?.()
    const draining = oneShot()
    this._drainWork = draining.promise
    this._drainWork.catch(noop)
    let work
    try {
      if (!gate?.isOpen) { throw closed() }
      work = gate.drainCmd()
    } catch (err) {
      work = Promise.reject(err)
    }
    const finish = async () => {
      let error = null
      try { await work } catch (err) { error = err }
      try { await this.close() }
      catch (err) {
        if (error === null) { error = err }
        else { this._emitSafe('error', Err.wrapError(err)) }
      }
      if (error !== null) { throw error }
    }
    finish().then(draining.resolve, draining.reject)
    return this._drainWork
  }

  async _loop() {
    while (!this._stopped) {
      let setup
      try {
        setup = await this._setup()
      } catch (err) {
        if (!this._stopped) {
          this._emitSafe('error', Err.wrapError(err, Err.SUPERVISOR_ERROR, 'Supervisor setup '))
        }
        await this._delay()
        continue
      }
      const run = {
        setup, children: new Map(), records: new Map(), failed: oneShot(),
        error: null, stopping: false, stopWork: null, timer: null, onError: null,
      }
      this._run = run
      if (setup?.failed !== undefined) {
        Promise.resolve(setup.failed).catch((err) => this._fail(run, err, false))
      }
      try {
        const entries = this._validate(setup)
        if (this._stopped) { continue }
        this._gate = setup.gate
        run.onError = (err) => {
          if (run.opened && !setup.gate.isOpen) { this._fail(run, err, false) }
        }
        setup.gate.on('error', run.onError)
        run.timer = setTimeout(() => {
          this._fail(run, failure('supervisor startup timeout'))
        }, this.startupTimeout)
        const err = await Promise.race([
          this._start(run, entries).then(() => null, (error) => error),
          run.failed.promise,
        ])
        clearTimeout(run.timer)
        if (err !== null) { throw err }
        if (this._stopped) { continue }
        if (!setup.gate.isOpen) {
          const err = closed()
          this._fail(run, err, false)
          throw err
        }
        run.opened = true
        this._openWait.resolve()
        this._emitSafe('open', setup.gate)
        await run.failed.promise
      } catch (err) {
        if (!this._stopped) { this._fail(run, err) }
      } finally {
        try {
          await this._stopRun(run)
        } catch (err) {
          this._stopped = true
          this._openWait.reject(err)
          this._emitSafe('error', err)
          return
        }
      }
      await this._delay()
    }
  }
}

export { Supervisor }
