import { connectFrames, listenFrames } from './stream.js'

const noop = () => {}

function tcpServer(port, msgCb, errCb, opts = {}) {
  const address = { port, host: opts.host ?? '0.0.0.0' }
  return listenFrames(address, (connection) => {
    opts.onConnection?.(connection)
    if (!connection.destroyed) {
      connection.on('data', (frame) => msgCb(connection, frame))
    }
  }, errCb, opts)
}

async function tcpClient(port, host, msgCb = noop, opts = {}) {
  const connection = await connectFrames({ port, host }, opts)
  connection.on('data', msgCb)
  return connection
}

export { tcpServer, tcpClient }
