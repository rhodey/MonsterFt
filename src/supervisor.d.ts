import type { ChildProcess } from 'node:child_process'
import { EventEmitter } from 'node:events'
import type { URL } from 'node:url'
import type { ErrorWithCode } from './error.js'
import type { Gate } from './gate.js'
import type { EmitterEvents, LocalComponent, MaybePromise } from './types.js'

export interface ChildSpec {
  file: string | URL
  args?: readonly string[]
}

export interface SupervisorSetup<Result = unknown> {
  gate: Gate<Result>
  children: Record<LocalComponent, ChildSpec>
  ready?: (children: Map<LocalComponent, ChildProcess>) => unknown
  failed?: PromiseLike<unknown>
  /** Synchronous cancellation before children are terminated. */
  stop?: () => void
  cleanup?: () => unknown
}

export interface SupervisorOptions {
  /** Milliseconds; defaults to 1,000. */
  restartDelay?: number
  /** Milliseconds; defaults to 30,000. */
  startupTimeout?: number
}

export interface SupervisorEvents<Result = unknown> extends EmitterEvents {
  open: [gate: Gate<Result>]
  error: [error: ErrorWithCode]
  close: [value?: undefined]
}

export class Supervisor<Result = unknown> extends EventEmitter<SupervisorEvents<Result>> {
  constructor(setup: () => MaybePromise<SupervisorSetup<Result>>, opts?: SupervisorOptions)
  readonly gate: Gate<Result> | null
  readonly isStopped: boolean
  restartDelay: number
  startupTimeout: number

  open(): Promise<void>
  /** true requests coordinated Gate closure before terminating children. */
  close(graceful?: boolean): Promise<void>
  drainCmd(): Promise<void>
}
