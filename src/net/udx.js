import { connectFrames, listenFrames } from './stream.js'

function udxServer(path, onConnection, errCb, opts = {}) {
  return listenFrames({ path }, onConnection, errCb, opts)
}

function udxClient(path, opts = {}) {
  return connectFrames({ path }, opts)
}

export { udxServer, udxClient }
