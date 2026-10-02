import net from 'node:net'
import { PackrStream, UnpackrStream } from 'msgpackr'
import { EncryptingStream, DecryptingStream } from './stream.js'

const noop = () => {}

function tcpServer(key, port, msgCb, errCb) {
  const server = net.createServer((sock) => {
    let decrypt
    let unpack
    let pack
    let encrypt
    const close = () => {
      const streams = [decrypt, unpack, pack, encrypt, sock]
      streams.forEach((stream) => stream?.unpipe())
      streams.forEach((stream) => stream?.removeAllListeners())
      streams.forEach((stream) => stream?.destroy())
    }
    sock.on('error', close)
    try {
      decrypt = new DecryptingStream(key)
      unpack = new UnpackrStream()
      pack = new PackrStream()
      encrypt = new EncryptingStream(key)
      decrypt.on('error', close)
      unpack.on('error', close)
      pack.on('error', close)
      encrypt.on('error', close)
      sock.once('close', close)
      pack.once('close', close)
      const onData = (data) => msgCb(pack, data)
      sock.pipe(decrypt).pipe(unpack).on('data', onData)
      pack.pipe(encrypt).pipe(sock)
    } catch (err) {
      close()
      errCb(err)
    }
  })
  return new Promise((res, rej) => {
    const netError = (err) => new Error(`${port} net error ${err.message}`, { cause: err })
    const onListenError = (err) => rej(netError(err))
    server.once('error', onListenError)
    try {
      server.listen(port, '0.0.0.0', () => {
        server.removeListener('error', onListenError)
        server.on('error', (err) => errCb(netError(err)))
        res(server)
      })
    } catch (err) {
      server.removeListener('error', onListenError)
      rej(err)
    }
  })
}

function tcpClient(key, port, host, msgCb=noop) {
  const sock = new net.Socket()
  return new Promise((res, rej) => {
    let pack
    let encrypt
    let decrypt
    let unpack
    const close = () => {
      const streams = [encrypt, decrypt, unpack, sock]
      pack?.unpipe()
      streams.forEach((stream) => stream?.unpipe())
      streams.forEach((stream) => stream?.removeAllListeners())
      streams.forEach((stream) => stream?.destroy())
      pack?.destroy()
    }
    const onConnectError = (err) => {
      close()
      rej(new Error(`net error ${err.message}`, { cause: err }))
    }
    const onConnectClose = () => {
      close()
      rej(new Error(`close`))
    }
    sock.once('error', onConnectError)
    sock.once('close', onConnectClose)
    sock.once('connect', () => {
      sock.removeListener('error', onConnectError)
      sock.removeListener('close', onConnectClose)
      try {
        pack = new PackrStream()
        encrypt = new EncryptingStream(key)
        decrypt = new DecryptingStream(key)
        unpack = new UnpackrStream()
        const abort = (err) => {
          try {
            pack.emit('error', err)
          } finally {
            close()
          }
        }
        sock.on('error', (err) => abort(new Error(`net error ${err.message}`, { cause: err })))
        encrypt.on('error', (err) => abort(new Error(`encrypt error ${err.message}`, { cause: err })))
        decrypt.on('error', (err) => abort(new Error(`decrypt error ${err.message}`, { cause: err })))
        unpack.on('error', (err) => abort(new Error(`unpack error ${err.message}`, { cause: err })))
        pack.once('close', () => {
          close()
          pack.removeAllListeners()
        })
        sock.once('close', close)
        pack.pipe(encrypt).pipe(sock)
        sock.pipe(decrypt).pipe(unpack).on('data', msgCb)
      } catch (err) {
        close()
        pack?.removeAllListeners()
        rej(err)
        return
      }
      res(pack)
    })
    try {
      sock.connect(port, host)
    } catch (err) {
      close()
      rej(err)
    }
  })
}

export { tcpServer, tcpClient }
