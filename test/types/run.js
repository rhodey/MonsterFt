import assert from 'node:assert/strict'
import { execFileSync } from 'node:child_process'
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import * as runtime from '../../src/index.js'

const root = fileURLToPath(new URL('../../', import.meta.url))
const require = createRequire(import.meta.url)
const compilerPackage = require.resolve('typescript/package.json')
const compiler = join(dirname(compilerPackage), require('typescript/package.json').bin.tsc)
const directory = mkdtempSync(join(tmpdir(), 'monsterft-types-'))

try {
  // Compile against the installed tarball, never a paths alias or repository symlink.
  const packed = JSON.parse(execFileSync('npm', [
    'pack', '--json', '--ignore-scripts', '--pack-destination', directory,
  ], { cwd: root, encoding: 'utf8' }))[0]
  assert(packed.files.some(({ path }) => path === 'src/index.d.ts'))
  assert(!packed.files.some(({ path }) => path.startsWith('test/')))

  writeFileSync(join(directory, 'package.json'), JSON.stringify({
    name: 'monsterft-types-consumer', private: true, type: 'module',
    dependencies: { monsterft: `file:./${packed.filename}` },
  }))
  execFileSync('npm', [
    'install', '--ignore-scripts', '--no-package-lock', '--no-audit', '--no-fund', '--prefer-offline',
  ], { cwd: directory, stdio: 'inherit' })

  const installed = JSON.parse(readFileSync(join(directory, 'node_modules/monsterft/package.json')))
  assert.equal(installed.types, 'src/index.d.ts')
  assert(installed.dependencies['@types/node'], 'Node declarations must be a consumer dependency')
  for (const name of ['consumer.ts', 'tsconfig.json']) {
    cpSync(new URL(name, import.meta.url), join(directory, name))
  }

  // A declaration-only class or stale error-code literal must not invent a runtime export.
  const exports = Object.keys(runtime).sort().map((name) => JSON.stringify(name)).join(' | ')
  const codes = Object.entries(runtime.ErrorCodes).map(([name, code]) =>
    `const ${name}: ${code} = api.ErrorCodes.${name}`).join('\n')
  writeFileSync(join(directory, 'runtime-contract.ts'), `
import * as api from 'monsterft'
type Equal<A, B> = [A] extends [B] ? ([B] extends [A] ? true : false) : false
const exportsMatch: Equal<keyof typeof api, ${exports}> = true
${codes}
`)

  execFileSync(process.execPath, [compiler, '-p', directory], {
    cwd: directory, stdio: 'inherit',
  })
  console.log('TypeScript package consumer passed')
} finally {
  rmSync(directory, { recursive: true, force: true })
}
