import type { Buffer } from 'node:buffer'
import { EventEmitter } from 'node:events'
import type { DatabaseSync } from 'node:sqlite'
import type { DiagnosticEvents, MaybePromise, Send } from './types.js'

/** Setup receives null data/matchIndex, seq 0n and index 0; its return value is ignored. */
export type ApplyCallback<Result = unknown> = (
  db: DatabaseSync,
  data: Buffer | null,
  term: bigint,
  seq: bigint,
  index: number,
  matchIndex: bigint[] | null,
) => MaybePromise<Result | void>

export class MonsterApply<Result = unknown> extends EventEmitter<DiagnosticEvents> {
  constructor(id: string, localSend: Send, databasePath: string, apply: ApplyCallback<Result>)
  id: string
  tickCount: bigint
  db: DatabaseSync | null
  readonly isOpen: boolean

  onReceive(from: string, frame: Buffer): Promise<void>
  close(): Promise<void>
}

export { MonsterApply as Apply }
