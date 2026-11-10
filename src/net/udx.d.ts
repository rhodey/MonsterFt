import type { Server } from 'node:net'
import type { FrameConnectOptions, FrameErrorHandler, FrameListenOptions, FrameSocket } from './stream.js'

export function udxClient(path: string, opts?: FrameConnectOptions): Promise<FrameSocket>
export function udxServer(
  path: string,
  onConnection: (connection: FrameSocket) => void,
  errCb: FrameErrorHandler,
  opts?: FrameListenOptions,
): Promise<Server>
