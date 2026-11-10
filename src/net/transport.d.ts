import type { Buffer } from 'node:buffer'
import { EventEmitter } from 'node:events'
import type { ErrorWithCode } from '../error.js'
import type { EmitterEvents } from '../types.js'
import type { FrameOptions } from './stream.js'

export interface TransportEvents extends EmitterEvents {
  message: [from: string, frame: Buffer]
  warn: [error: ErrorWithCode]
  error: [error: ErrorWithCode]
}

export class Transport extends EventEmitter<TransportEvents> {
  constructor(opts?: FrameOptions)
  readonly isOpen: boolean
  readonly isStopped: boolean
  /** Rejects on terminal runtime failure; remains pending on normal shutdown. */
  failed: Promise<never>
  open(): Promise<void>
  stop(): void
  close(): Promise<void>
}

export function illegal(message: string): ErrorWithCode
export function stopped(): ErrorWithCode
