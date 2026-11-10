import { Buffer } from 'node:buffer'
import type { ChildProcess } from 'node:child_process'
import { EventEmitter } from 'node:events'
import type { Server } from 'node:net'
import { PassThrough } from 'node:stream'
import type { DatabaseSync } from 'node:sqlite'
import {
  Gate, MonsterProc, MonsterApply, Supervisor, TcpCluster, UdxRouter,
  tcpClient, tcpServer, udxClient, udxServer, certify, ErrorCodes, ErrorWithCode,
  type ApplyCallback, type ChangeEvent, type CmdEvent, type SyncEvent,
  type CommandOutcome, type FrameSocket, type FrameErrorHandler,
  type GateOptions, type ProcOptions, type ProcRole, type LocalComponent,
  type SupervisorSetup, type SupervisorOptions, type TcpPeer, type TcpClusterOptions,
  type UdxRouterOptions, type Send, type ErrorCode,
} from 'monsterft'

declare function expect<T>(value: T): void
type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends
  (<T>() => T extends B ? 1 : 2) ? true : false

const nodes = ['a', 'b', 'c'] as const
const frame = Buffer.from('increment')
const send: Send = (_to, _frame) => {}
const queued: Send = (_to, _frame) => true
const asynchronous: Send = async (_to, _frame) => false
const gateOptions: GateOptions = { tickInterval: 10, localTimeout: 1000, applyTimeout: 5000 }
const gate = new Gate<bigint>('a', nodes, queued, asynchronous, () => gateOptions)

expect<Promise<void>>(gate.open())
expect<Promise<void>>(gate.awaitLeader())
expect<Promise<void>>(gate.awaitLeader(true))
expect<void>(gate.onReceive('b', frame))
expect<string>(gate.id)
expect<string[]>(gate.nodes)
expect<boolean>(gate.isOpen)
expect<bigint>(gate.tickCount)
expect<number>(gate.timems)
expect<bigint>(gate.term)
expect<bigint>(gate.seq)
expect<bigint>(gate.commitSeq)
expect<bigint>(gate.commitTerm)
expect<string | null>(gate.leader)
expect<'leader' | 'follower' | 'candidate'>(gate.state)

const changes = (status: ChangeEvent) => {
  expect<string | null>(status.leader)
  expect<bigint>(status.term)
  expect<string[]>(status.followers)
  // @ts-expect-error Public progress does not include appliedSeq.
  status.appliedSeq
  // @ts-expect-error The public name is leader, not leaderId.
  status.leaderId
}
expect<typeof gate>(gate.on('change', status => {
  expect<Equal<typeof status, ChangeEvent>>(true)
  changes(status)
}))
gate.once('change', changes)
gate.addListener('change', changes)
gate.prependListener('change', changes)
gate.prependOnceListener('change', changes)
gate.removeListener('change', changes)
gate.off('change', changes)
gate.on('commit', seq => expect<Equal<typeof seq, bigint>>(true))
gate.on('cmd', event => expect<Equal<typeof event, CmdEvent>>(true))
gate.on('sync', event => {
  expect<Equal<typeof event, SyncEvent>>(true)
  expect<bigint>(event.syncSeq)
  expect<string[]>(event.agree)
  expect<boolean>(event.quorum)
})
gate.on('error', error => {
  expect<Equal<typeof error, ErrorWithCode>>(true)
  expect<number | null>(error.code)
  expect<number | null>(error.sqlCode)
})
gate.on('warn', error => expect<ErrorWithCode>(error))
gate.on('close', value => expect<undefined>(value))
gate.on('newListener', name => expect<string | symbol>(name))
gate.emit('commit', 1n)
expect<EventEmitter>(gate)
expect<Promise<ChangeEvent>>(gate.awaitEvent('change', status => status.leader !== null))
expect<Promise<bigint>>(gate.awaitEvent('commit', seq => seq > 0n))
expect<Promise<CmdEvent>>(gate.awaitEvent('cmd', event => event.cmdSeq > 0n))
expect<Promise<SyncEvent>>(gate.awaitEvent('sync'))
expect<Promise<ErrorWithCode>>(gate.awaitEvent('error'))
expect<Promise<undefined>>(gate.awaitEvent('close'))

const [seq, result] = await gate.append(frame)
expect<Equal<typeof seq, bigint>>(true)
expect<Equal<typeof result, bigint>>(true)
const [batchSeq, outcomes] = await gate.appendBatch([frame, frame] as const)
expect<bigint>(batchSeq)
expect<CommandOutcome<bigint>[]>(outcomes)
for (const outcome of outcomes) {
  if (outcome.status === 'fulfilled') {
    expect<Equal<typeof outcome.value, bigint>>(true)
    // @ts-expect-error Fulfilled outcomes have no rejection reason.
    outcome.reason
  } else {
    expect<ErrorWithCode>(outcome.reason)
    // @ts-expect-error Rejected outcomes have no successful value.
    outcome.value
  }
}
const untyped = new Gate('a', nodes, send, send, {})
const [, unknownResult] = await untyped.append(frame)
expect<Equal<typeof unknownResult, unknown>>(true)
// @ts-expect-error Unspecified results need narrowing, not implicit any.
expect<bigint>(unknownResult)
// @ts-expect-error append requires a Buffer, not an arbitrary Uint8Array.
gate.append(new Uint8Array(2))
// @ts-expect-error Batch items must be Buffers.
gate.appendBatch(['increment'])
// @ts-expect-error Timeouts are numbers of milliseconds.
new Gate('a', nodes, send, send, { localTimeout: '1000' })
// @ts-expect-error No Gate fatal event is advertised.
gate.on('fatal', () => {})
// @ts-expect-error commit carries a bigint, not an object or number.
gate.on('commit', (value: number) => {})
// @ts-expect-error Typed emit must agree with the event's payload.
gate.emit('commit', 1)
// @ts-expect-error Event names are checked by awaitEvent as well.
gate.awaitEvent('changed')
expect<void>(gate.stop())
expect<Promise<void>>(gate.close())
expect<Promise<void>>(gate.drainCmd())

const procOptions: ProcOptions = {
  localTimeout: 1000, electionTimeout: 1500, appendTimeout: 1500, quorum: 2,
  rpcMax: 1024, rpcMaxBytes: 1024 * 1024, iterStepSize: 64,
  keepTarget: 100, keepTrigger: 200,
}
const role: ProcRole = 'procA'
const proc = new MonsterProc(role, 'a', nodes, send, '/tmp/a', () => procOptions)
expect<ProcRole>(proc.role)
expect<DatabaseSync | null>(proc.db)
expect<boolean>(proc.isOpen)
expect<void>(proc.onReceive('gate', frame))
expect<void>(proc.close())
proc.on('warn', error => expect<ErrorWithCode>(error))
proc.once('close', () => {})
// @ts-expect-error Only the three protocol roles are valid.
new MonsterProc('apply', 'a', nodes, send, '/tmp/a')
// @ts-expect-error Proc.close is synchronous.
expect<Promise<void>>(proc.close())

const applyCallback: ApplyCallback<bigint> = (db, data, term, cmdSeq, index, matchIndex) => {
  expect<DatabaseSync>(db)
  expect<bigint>(term)
  expect<bigint>(cmdSeq)
  expect<number>(index)
  expect<bigint[] | null>(matchIndex)
  // @ts-expect-error Setup passes null data.
  data.toString()
  if (data === null) {
    db.exec('CREATE TABLE counter(id INTEGER PRIMARY KEY, n INTEGER)')
    return
  }
  expect<Buffer>(data)
  const session = db.createSession({ db: 'main' })
  expect<Uint8Array>(session.patchset())
  session.close()
  return 1n
}
const apply = new MonsterApply('a', send, '/tmp/a', applyCallback)
new MonsterApply<bigint>('a', send, '/tmp/a', async (db, data) => {
  if (data === null) return
  return 2n
})
// @ts-expect-error A specified result type also checks the Apply callback's return value.
new MonsterApply<bigint>('a', send, '/tmp/a', () => 'not a bigint')
expect<DatabaseSync | null>(apply.db)
expect<Promise<void>>(apply.onReceive('gate', frame))
expect<Promise<void>>(apply.close())
apply.on('error', error => expect<ErrorWithCode>(error))
// @ts-expect-error Callback command data is nullable.
new MonsterApply('a', send, '/tmp/a', (db: DatabaseSync, data: Buffer) => 1)

const peers: Record<string, TcpPeer> = {
  a: { host: '127.0.0.1', basePort: 41000 },
  b: { host: '127.0.0.1', basePort: 41010 },
  c: { host: '127.0.0.1', basePort: 41020 },
}
const tcpOptions: TcpClusterOptions = {
  bindHost: '0.0.0.0', reconnectDelay: 250, connectTimeout: 5000,
  maxFrameBytes: 1024, maxQueuedBytes: 4096,
}
const tcp = new TcpCluster('a', peers, tcpOptions)
const udxOptions: UdxRouterOptions = { directory: '/tmp', maxFrameBytes: 1024 }
const udx = new UdxRouter(['procA', 'procB', 'procC', 'apply'], udxOptions)
for (const transport of [tcp, udx]) {
  expect<Promise<void>>(transport.open())
  expect<boolean>(transport.isOpen)
  expect<boolean>(transport.isStopped)
  expect<Promise<never>>(transport.failed)
  transport.on('message', (from, buffer) => {
    expect<Equal<typeof from, string>>(true)
    expect<Equal<typeof buffer, Buffer>>(true)
  })
  transport.on('warn', error => expect<ErrorWithCode>(error))
  expect<void>(transport.stop())
  expect<Promise<void>>(transport.close())
}
expect<boolean>(tcp.send('b', frame))
expect<true>(udx.send('apply', frame))
expect<Promise<void>>(udx.ready())
expect<string | undefined>(udx.paths['apply'])
const routedSend: Send = (to, buffer) => tcp.send(to, buffer)
const routedLocalSend: Send = (to, buffer) => udx.send(to, buffer)
// @ts-expect-error Router messages deliver Buffers.
tcp.on('message', (from: string, data: string) => {})
// @ts-expect-error UDX does not accept caller-selected socket paths.
new UdxRouter(['apply'], { paths: { apply: '/tmp/apply' } })

const setup = (): SupervisorSetup<bigint> => ({
  gate,
  children: {
    procA: { file: new URL('./procA.js', import.meta.url), args: ['a'] as const },
    procB: { file: './procB.js' },
    procC: { file: './procC.js' },
    apply: { file: './apply.js' },
  },
  ready(children) {
    expect<Map<LocalComponent, ChildProcess>>(children)
    children.get('apply')?.on('exit', code => expect<number | null>(code))
    return Promise.all([udx.ready()])
  },
  failed: Promise.race([tcp.failed, udx.failed]),
  stop: () => { gate.stop(); tcp.stop(); udx.stop() },
  cleanup: () => Promise.all([tcp.close(), udx.close()]),
})
const supervisorOptions: SupervisorOptions = { restartDelay: 1000, startupTimeout: 30000 }
const supervisor = new Supervisor(async () => setup(), supervisorOptions)
expect<Gate<bigint> | null>(supervisor.gate)
expect<boolean>(supervisor.isStopped)
supervisor.on('open', current => {
  expect<Promise<[bigint, bigint]>>(current.append(frame))
})
supervisor.on('error', error => expect<ErrorWithCode>(error))
supervisor.on('close', () => {})
expect<Promise<void>>(supervisor.open())
expect<Promise<void>>(supervisor.close())
expect<Promise<void>>(supervisor.close(true))
expect<Promise<void>>(supervisor.drainCmd())
// @ts-expect-error Graceful close takes a boolean.
supervisor.close({ graceful: true })
// @ts-expect-error Every setup must include an Apply child.
const incomplete: SupervisorSetup = { gate, children: { procA: { file: 'a.js' }, procB: { file: 'b.js' }, procC: { file: 'c.js' } } }

const onConnectionError: FrameErrorHandler = (error, connection) => {
  expect<Error>(error)
  // Native stream errors are not guaranteed to have MonsterFt's error fields.
  if (error instanceof ErrorWithCode) expect<number | null>(error.code)
  expect<FrameSocket | null>(connection)
}
const controller = new AbortController()
expect<Promise<Server>>(tcpServer(41000, (connection, buffer) => {
  expect<FrameSocket>(connection)
  expect<Buffer>(buffer)
  connection.write(buffer)
}, onConnectionError, { host: '127.0.0.1', signal: controller.signal, onConnection: socket => socket.canSend(frame) }))
const socket = await tcpClient(41000, '127.0.0.1', buffer => expect<Buffer>(buffer), {
  connectTimeout: 5000, maxFrameBytes: 1024, signal: controller.signal,
})
expect<FrameSocket>(socket)
socket.on('data', buffer => expect<Equal<typeof buffer, Buffer>>(true))
socket.once('data', buffer => expect<Equal<typeof buffer, Buffer>>(true))
socket.on('error', error => expect<Error>(error))
socket.on('close', () => {})
expect<Buffer | null>(socket.read())
expect<boolean>(socket.canSend(frame))
expect<boolean>(socket.write(frame))
socket.write(frame, error => expect<Error | null | undefined>(error))
socket.write(frame, 'utf8', () => {})
socket.pipe(new PassThrough({ objectMode: true }))
socket.end(frame)
socket.end(() => {})
socket.end(undefined, () => {})
// @ts-expect-error The framed stream only accepts Buffer messages.
socket.write('not a frame')
// @ts-expect-error data listeners receive Buffer, not string.
socket.on('data', (data: string) => {})
expect<Promise<FrameSocket>>(udxClient('/tmp/worker', { signal: controller.signal }))
expect<Promise<Server>>(udxServer('/tmp/worker', connection => {
  connection.on('data', buffer => expect<Buffer>(buffer))
}, onConnectionError, { maxQueuedBytes: 8192, signal: controller.signal }))

const error = new ErrorWithCode('application failed', 42, null)
new ErrorWithCode('message only')
expect<number | null>(error.code)
expect<ErrorCode>(ErrorCodes.REPAIR_NO_QUORUM)
// @ts-expect-error ErrorCodes is frozen.
ErrorCodes.NOT_LEADER = 0
// @ts-expect-error Runtime classes stay values, but FrameSocket is only a root-level type export.
new FrameSocket(socket.socket)
expect<bigint>(certify('/tmp/a'))
expect<bigint>(certify('/tmp/a', true))
// @ts-expect-error Certification is synchronous.
expect<Promise<bigint>>(certify('/tmp/a'))
