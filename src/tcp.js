import net from 'node:net'
import { Transform } from 'node:stream'
import { EncryptingStream, DecryptingStream } from './stream.js'
import * as Err from './error.js'

const noop = () => {}

// Object mode preserves one Buffer per encrypted message, even while buffered.
class FrameStream extends Transform {
  // Socket closure destroys this stream after downstream writes have drained.
  constructor() { super({ objectMode: true, autoDestroy: false }) }

  _transform(frame, encoding, callback) {
    if (!Buffer.isBuffer(frame)) {
      callback(new Err.ErrorWithCode('TCP message must be a Buffer', Err.ARGUMENT_ILLEGAL))
      return
    }
    callback(null, frame)
  }
}

function tcpServer(key, port, msgCb, errCb) {
  const server = net.createServer((sock) => {
    let decrypt
    let frames
    let encrypt
    const close = () => {
      const streams = [decrypt, frames, encrypt, sock]
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
      decrypt = new DecryptingStream(key, { readableObjectMode: true })
      frames = new FrameStream()
      encrypt = new EncryptingStream(key, { writableObjectMode: true })
      decrypt.on('error', onError)
      frames.on('error', onError)
      encrypt.on('error', onError)
      sock.once('close', close)
      frames.once('close', close)
      sock.pipe(decrypt).on('data', (frame) => msgCb(frames, frame))
      frames.pipe(encrypt).pipe(sock)
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
    let frames
    let encrypt
    let decrypt
    const close = () => {
      const streams = [encrypt, decrypt, sock]
      frames?.unpipe()
      streams.forEach((stream) => stream?.unpipe())
      streams.forEach((stream) => stream?.removeAllListeners())
      streams.forEach((stream) => stream?.destroy())
      frames?.destroy()
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
        frames = new FrameStream()
        frames.on('error', close)
        encrypt = new EncryptingStream(key, { writableObjectMode: true })
        decrypt = new DecryptingStream(key, { readableObjectMode: true })
        const abort = (err) => {
          try {
            frames.emit('error', err)
          } finally {
            close()
          }
        }
        sock.on('error', (err) => abort(Err.wrapError(err, null, 'net error ')))
        encrypt.on('error', (err) => abort(Err.wrapError(err, null, 'encrypt error ')))
        decrypt.on('error', (err) => abort(Err.wrapError(err, null, 'decrypt error ')))
        frames.once('close', () => {
          close()
          frames.removeAllListeners()
        })
        sock.once('close', close)
        frames.pipe(encrypt).pipe(sock)
        sock.pipe(decrypt).on('data', (frame) => msgCb(frame))
      } catch (err) {
        close()
        frames?.removeAllListeners()
        rej(err)
        return
      }
      res(frames)
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
