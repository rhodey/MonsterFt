import { connectFrames, listenFrames } from './stream.js'

function udsServer(path, onConnection, errCb, opts = {}) {
  return listenFrames({ path }, onConnection, errCb, opts)
}

function udsClient(path, opts = {}) {
  return connectFrames({ path }, opts)
}

export { udsServer, udsClient }
