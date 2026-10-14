import test from 'tape'
import { spawnSync } from 'node:child_process'
import { oneShot } from '../src/util.js'

test('oneShot preserves its first resolution', async (t) => {
  const shot = oneShot()
  const { resolve, reject } = shot
  const value = { result: 'first' }
  t.notOk(shot.settled)
  t.equal(resolve(value), true, 'detached resolve chooses the outcome')
  t.ok(shot.settled, 'settlement is visible immediately')
  t.equal(resolve('later'), false)
  t.equal(reject(new Error('later rejection')), false)
  t.equal(await shot.promise, value, 'the original result is preserved')
  t.end()
})

test('oneShot preserves its first rejection', async (t) => {
  const shot = oneShot()
  const { resolve, reject } = shot
  const error = new Error('first rejection')
  const caught = shot.promise.catch((err) => err)
  t.notOk(shot.settled)
  t.equal(reject(error), true, 'detached reject chooses the outcome')
  t.ok(shot.settled, 'settlement is visible immediately')
  t.equal(reject(new Error('later rejection')), false)
  t.equal(resolve('later'), false)
  t.equal(await caught, error, 'the original error is preserved')
  t.end()
})

test('oneShot leaves unhandled rejection policy to its caller', (t) => {
  const moduleUrl = new URL('../src/util.js', import.meta.url).href
  for (const handled of [false, true]) {
    const child = spawnSync(process.execPath, [
      '--unhandled-rejections=strict', '--input-type=module', '--eval', `
        import { oneShot } from ${JSON.stringify(moduleUrl)}
        const shot = oneShot()
        ${handled ? 'shot.promise.catch(() => {})' : ''}
        shot.reject(new Error('unhandled oneShot rejection'))
      `,
    ], { encoding: 'utf8' })
    t.error(child.error)
    t.equal(child.status, handled ? 0 : 1,
      handled ? 'an explicit handler suppresses runtime failure' : 'an ignored rejection remains unhandled')
    if (!handled) { t.match(child.stderr, /Error: unhandled oneShot rejection/) }
  }
  t.end()
})
