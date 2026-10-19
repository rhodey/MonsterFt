import * as Err from './error.js'

// Only runnable tasks belong in the queue. External waits register a future
// continuation; they never keep this input's SQLite transaction open.
class Executor {
  constructor() {
    this.timems = 0
    this.queue = []
    this.timers = new Map()
    this.nextTimer = 0
    this.running = false
  }

  enqueue(task) { this.queue.push(task) }

  after(ms, task, interval = 0) {
    const id = ++this.nextTimer
    this.timers.set(id, { id, due: this.timems + ms, task, interval })
    return id
  }

  cancel(id) { this.timers.delete(id) }

  run(timems, handler) {
    if (this.running || !Number.isSafeInteger(timems) || timems < this.timems) {
      throw new Err.ErrorWithCode('executor input order is illegal', Err.RAFT_ILLEGAL)
    }
    this.running = true
    this.timems = timems
    try {
      this.enqueue(handler)
      this.drain()
      const fired = new Set()
      while (true) {
        const due = [...this.timers.values()]
          .filter((timer) => timer.due <= timems && !fired.has(timer.id))
          .sort((a, b) => a.due - b.due || a.id - b.id)[0]
        if (!due) { break }
        fired.add(due.id)
        if (due.interval > 0) { due.due = timems + due.interval }
        else { this.timers.delete(due.id) }
        this.enqueue(due.task)
        this.drain()
      }
    } finally {
      this.running = false
      this.queue.length = 0
    }
  }

  drain() {
    while (this.queue.length > 0) { this.queue.shift()() }
  }

  clear() {
    this.queue.length = 0
    this.timers.clear()
  }
}

export { Executor }
