import type { Buffer } from 'node:buffer'
import type { ErrorWithCode } from './error.js'

export type MaybePromise<T> = T | PromiseLike<T>
export type ProcRole = 'procA' | 'procB' | 'procC'
export type LocalComponent = ProcRole | 'apply'

/** Fulfilled return values are ignored; throws and promise rejections report failure. */
export type Send = (to: string, frame: Buffer) => unknown

export interface EmitterEvents {
  newListener: [event: string | symbol, listener: (...args: any[]) => void]
  removeListener: [event: string | symbol, listener: (...args: any[]) => void]
}

export interface DiagnosticEvents extends EmitterEvents {
  warn: [error: ErrorWithCode]
  error: [error: ErrorWithCode]
  close: [value?: undefined]
}

export type CommandOutcome<Result = unknown> =
  | { status: 'fulfilled'; value: Result }
  | { status: 'rejected'; reason: ErrorWithCode }
