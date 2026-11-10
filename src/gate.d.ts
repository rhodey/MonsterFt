import type { Buffer } from 'node:buffer'
import { EventEmitter } from 'node:events'
import type { CommandOutcome, DiagnosticEvents, Send } from './types.js'

export interface GateOptions {
  /** Milliseconds; defaults to 10. */
  tickInterval?: number
  /** Milliseconds; defaults to 1,000. */
  localTimeout?: number
  /** Milliseconds; defaults to 5,000. */
  applyTimeout?: number
}

export interface ChangeEvent {
  state: 'follower' | 'candidate' | 'leader'
  leader: string | null
  term: bigint
  seq: bigint
  commitSeq: bigint
  commitTerm: bigint
  followers: string[]
}

export interface CmdEvent {
  type: 'cmd'
  cmdSeq: bigint
  cmdCount: number
}

export interface SyncEvent {
  type: 'sync'
  cmdSeq: bigint
  syncSeq: bigint
  quorum: boolean
  agree: string[]
  disagree: string[]
}

export interface GateEvents extends DiagnosticEvents {
  change: [status: ChangeEvent]
  commit: [seq: bigint]
  cmd: [event: CmdEvent]
  sync: [event: SyncEvent]
}

/** Result describes decoded application results; it does not validate them at runtime. */
export class Gate<Result = unknown> extends EventEmitter<GateEvents> {
  constructor(
    id: string,
    nodes: readonly string[],
    send: Send,
    localSend: Send,
    opts?: GateOptions | (() => GateOptions),
  )
  id: string
  nodes: string[]
  readonly isOpen: boolean
  tickInterval: number
  localTimeout: number
  applyTimeout: number
  tickCount: bigint
  timems: number
  state: ChangeEvent['state']
  leader: string | null
  term: bigint
  seq: bigint
  commitSeq: bigint
  commitTerm: bigint
  followers: string[]

  open(): Promise<void>
  close(): Promise<void>
  stop(): void
  drainCmd(): Promise<void>
  onReceive(from: string, frame: Buffer): void
  /** Keep the command Buffer unchanged until the operation settles. */
  append(data: Buffer): Promise<[cmdSeq: bigint, value: Result]>
  /** Keep the array and its Buffers unchanged until the operation settles. */
  appendBatch(items: readonly Buffer[]): Promise<[cmdSeq: bigint, outcomes: CommandOutcome<Result>[]]>
  awaitLeader(commit?: boolean): Promise<void>
  awaitEvent<K extends keyof GateEvents>(
    type: K,
    matches?: (value: GateEvents[K][0]) => unknown,
  ): Promise<GateEvents[K][0]>
}
