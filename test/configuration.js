import test from 'tape'
import { Gate, MonsterProc } from '../src/index.js'
import { protocol } from './fixtures/protocol.js'
import * as Err from '../src/error.js'

test('component membership and protocol configuration retain supported boundaries', (t) => {
  const noop = () => {}
  const invalid = (fn) => t.throws(fn, (err) => err.code === Err.ARGUMENT_ILLEGAL)
  for (const [id, nodes] of [
    ['', ['a', 'b', 'c']], [1, ['a', 'b', 'c']], ['d', ['a', 'b', 'c']],
    ['a', ['a', 'b']], ['a', ['a', 'a', 'b']], ['a', null], ['a', 'abc'],
    ['a', ['a', '', 'c']], ['a', ['a', 2, 'c']],
  ]) {
    invalid(() => new Gate(id, nodes, noop, noop))
    invalid(() => new MonsterProc('procA', id, nodes, noop, '/tmp/not-opened'))
  }
  for (const name of ['electionTimeout', 'appendTimeout', 'rpcMax', 'rpcMaxBytes']) {
    const minimum = name === 'electionTimeout' ? 10 : 1
    for (const value of [null, false, 0, -1, 1.5, NaN, Infinity, '100', 100n, minimum - 1]) {
      invalid(() => protocol({ [name]: value }))
    }
    const f = protocol({ [name]: minimum })
    t.equal(f.proc.engine.opts[name], minimum)
    f.close()
  }
  for (const quorum of [1, 4, 2.5, NaN, '3']) invalid(() => protocol({ quorum }))
  const f = protocol({ quorum: 3 })
  t.equal(f.proc.engine.quorum, 3, 'all-node quorum is allowed')
  f.close()
  t.end()
})
