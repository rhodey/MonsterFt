import { connectFrames, listenFrames } from './stream.js'

function udxClient(path, opts = {}) {
  return connectFrames({ path }, opts)
}

function udxServer(path, onConnection, errCb, opts = {}) {
  return listenFrames({ path }, onConnection, errCb, opts)
}

export { udxServer, udxClient }
