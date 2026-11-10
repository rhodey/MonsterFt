import type { Buffer } from 'node:buffer'
import type { FrameOptions } from './stream.js'
import { Transport } from './transport.js'

export interface UdxRouterOptions extends FrameOptions {
  /** Parent of the private socket directory; defaults to /tmp. */
  directory?: string
}

export class UdxRouter extends Transport {
  constructor(names: readonly string[], opts?: UdxRouterOptions)
  names: string[]
  /** Populated as open() creates the endpoints. */
  paths: Record<string, string>
  ready(): Promise<void>
  /** Queues a frame or throws and fails the local transport. */
  send(to: string, frame: Buffer): true
}
