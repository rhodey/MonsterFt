import type { Buffer } from 'node:buffer'
import type { FrameOptions } from './stream.js'
import { Transport } from './transport.js'

export interface TcpPeer {
  host: string
  basePort: number
}

export interface TcpClusterOptions extends FrameOptions {
  bindHost?: string
  /** Milliseconds; defaults to 250. */
  reconnectDelay?: number
  /** Milliseconds; defaults to 5,000. */
  connectTimeout?: number
}

export class TcpCluster extends Transport {
  constructor(id: string, peers: Record<string, TcpPeer>, opts?: TcpClusterOptions)
  id: string
  nodes: string[]
  peers: Record<string, TcpPeer>
  bindHost: string
  reconnectDelay: number
  connectTimeout: number
  /** true means queued, false means dropped; neither is a delivery acknowledgment. */
  send(to: string, frame: Buffer): boolean
}
