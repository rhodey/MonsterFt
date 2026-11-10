import type { Buffer } from 'node:buffer'
import { EventEmitter } from 'node:events'
import type { DatabaseSync } from 'node:sqlite'
import type { DiagnosticEvents, ProcRole, Send } from './types.js'

export interface ProcOptions {
  /** Milliseconds; defaults to 1,000. */
  localTimeout?: number
  /** Milliseconds; defaults to 1,500. */
  electionTimeout?: number
  /** Milliseconds; defaults to 1,500. */
  appendTimeout?: number
  quorum?: number
  /** Maximum entries per replication request; defaults to 1,024. */
  rpcMax?: number
  /** Replication byte budget; defaults to 64 MiB. */
  rpcMaxBytes?: number
  /** Entries per SQLite iteration step; defaults to 1,024. */
  iterStepSize?: number
  /** Retention entry counts: provide both, with 2 <= keepTarget < keepTrigger. */
  keepTarget?: number
  keepTrigger?: number
}

export class MonsterProc extends EventEmitter<DiagnosticEvents> {
  constructor(
    role: ProcRole,
    id: string,
    nodes: readonly string[],
    localSend: Send,
    databasePath: string,
    opts?: ProcOptions | (() => ProcOptions),
  )
  role: ProcRole
  id: string
  nodes: string[]
  localTimeout: number
  tickCount: bigint
  readonly isOpen: boolean
  readonly db: DatabaseSync | null

  onReceive(from: string, frame: Buffer): void
  close(): void
}
