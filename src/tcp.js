import net from 'node:net'
import { PackrStream, UnpackrStream } from 'msgpackr'
import { EncryptingStream, DecryptingStream } from './stream.js'
import * as Err from './error.js'

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
    const onError = (err) => {
      try {
        errCb(err)
      } finally {
        close()
      }
    }
    sock.on('error', onError)
    try {
      decrypt = new DecryptingStream(key)
      unpack = new UnpackrStream()
      pack = new PackrStream()
      encrypt = new EncryptingStream(key)
      decrypt.on('error', onError)
      unpack.on('error', onError)
      pack.on('error', onError)
      encrypt.on('error', onError)
      sock.once('close', close)
      pack.once('close', close)
      const onData = (data) => msgCb(pack, data)
      sock.pipe(decrypt).pipe(unpack).on('data', onData)
      pack.pipe(encrypt).pipe(sock)
    } catch (err) {
      onError(err)
    }
  })
  return new Promise((res, rej) => {
    const netError = (err) => Err.wrapError(err, null, `${port} net error `)
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
      rej(Err.wrapError(err, null, 'net error '))
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
        sock.on('error', (err) => abort(Err.wrapError(err, null, 'net error ')))
        encrypt.on('error', (err) => abort(Err.wrapError(err, null, 'encrypt error ')))
        decrypt.on('error', (err) => abort(Err.wrapError(err, null, 'decrypt error ')))
        unpack.on('error', (err) => abort(Err.wrapError(err, null, 'unpack error ')))
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
