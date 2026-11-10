import type { Buffer } from 'node:buffer'
import type { ListenOptions, NetConnectOpts, Server, Socket } from 'node:net'
import { Duplex, type Readable } from 'node:stream'

export interface FrameOptions {
  /** Defaults to 128 MiB. */
  maxFrameBytes?: number
  /** Per connection; defaults to 256 MiB. */
  maxQueuedBytes?: number
}

export interface FrameConnectOptions extends FrameOptions {
  connectTimeout?: number
  signal?: AbortSignal
}

export interface FrameListenOptions extends FrameOptions {
  signal?: AbortSignal
}

interface FrameEvents {
  data: [frame: Buffer]
  error: [error: Error]
  close: []
  end: []
  drain: []
  finish: []
  pause: []
  resume: []
  readable: []
  pipe: [source: Readable]
  unpipe: [source: Readable]
}
type FrameListener<K> = (...args: K extends keyof FrameEvents ? FrameEvents[K] : any[]) => void

/** An object-mode Duplex whose messages are Buffers. */
export class FrameSocket extends Duplex {
  constructor(socket: Socket, opts?: FrameOptions)
  socket: Socket
  maxFrameBytes: number
  maxQueuedBytes: number
  queuedBytes: number
  canSend(frame: Buffer): boolean
  read(size?: number): Buffer | null
  write(frame: Buffer, callback?: (error?: Error | null) => void): boolean
  write(frame: Buffer, encoding?: BufferEncoding, callback?: (error?: Error | null) => void): boolean
  end(callback?: () => void): this
  end(frame: Buffer | null | undefined, callback?: () => void): this
  end(frame: Buffer | null | undefined, encoding?: BufferEncoding, callback?: () => void): this
  on<K extends string | symbol>(event: K, listener: FrameListener<K>): this
  once<K extends string | symbol>(event: K, listener: FrameListener<K>): this
  addListener<K extends string | symbol>(event: K, listener: FrameListener<K>): this
  prependListener<K extends string | symbol>(event: K, listener: FrameListener<K>): this
  prependOnceListener<K extends string | symbol>(event: K, listener: FrameListener<K>): this
  removeListener<K extends string | symbol>(event: K, listener: FrameListener<K>): this
  off<K extends string | symbol>(event: K, listener: FrameListener<K>): this
}

/** connection is null for a listener error and present for a connection error. */
export type FrameErrorHandler = (error: Error, connection: FrameSocket | null) => void

export function frameOptions(opts?: FrameOptions): Required<FrameOptions>
export function connectFrames(address: NetConnectOpts, opts?: FrameConnectOptions): Promise<FrameSocket>
export function listenFrames(
  address: ListenOptions,
  onConnection: (connection: FrameSocket) => void,
  errCb: FrameErrorHandler,
  opts?: FrameListenOptions,
): Promise<Server>
