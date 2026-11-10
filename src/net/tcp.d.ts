import type { Buffer } from 'node:buffer'
import type { Server } from 'node:net'
import type { FrameConnectOptions, FrameErrorHandler, FrameListenOptions, FrameSocket } from './stream.js'

export interface TcpServerOptions extends FrameListenOptions {
  host?: string
  onConnection?: (connection: FrameSocket) => void
}

export function tcpClient(
  port: number,
  host: string,
  msgCb?: (frame: Buffer) => void,
  opts?: FrameConnectOptions,
): Promise<FrameSocket>

export function tcpServer(
  port: number,
  msgCb: (connection: FrameSocket, frame: Buffer) => void,
  errCb: FrameErrorHandler,
  opts?: TcpServerOptions,
): Promise<Server>
