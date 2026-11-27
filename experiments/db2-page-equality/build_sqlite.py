#!/usr/bin/env python3
"""Build a disposable DBPAGE/session-enabled library from a local amalgamation."""
import argparse
import hashlib
import json
import pathlib
import platform
import subprocess
import tempfile

parser = argparse.ArgumentParser()
parser.add_argument('--source', type=pathlib.Path, default=pathlib.Path(__file__).resolve().parents[3] /
                    'other/better-sqlite3/deps/sqlite3/sqlite3.c')
parser.add_argument('--output', type=pathlib.Path)
args = parser.parse_args()
if not args.source.is_file():
    parser.error('supply --source PATH/TO/sqlite3.c (a SQLite amalgamation)')
suffix = '.dylib' if platform.system() == 'Darwin' else '.so'
output = args.output or pathlib.Path(tempfile.mkdtemp(prefix='monsterft-db2-sqlite-')) / ('libsqlite3' + suffix)
output.parent.mkdir(parents=True, exist_ok=True)
command = ['cc', '-O2', '-fPIC', '-dynamiclib' if platform.system() == 'Darwin' else '-shared',
           '-DSQLITE_THREADSAFE=2', '-DSQLITE_ENABLE_DBPAGE_VTAB',
           '-DSQLITE_ENABLE_SESSION', '-DSQLITE_ENABLE_PREUPDATE_HOOK',
           str(args.source), '-o', str(output), '-lm', '-lpthread']
subprocess.run(command, check=True)
info = {'library': str(output.resolve()), 'source': str(args.source.resolve()),
        'sourceSha256': hashlib.sha256(args.source.read_bytes()).hexdigest(), 'command': command}
output.with_suffix(output.suffix + '.json').write_text(json.dumps(info, indent=2) + '\n')
print(json.dumps(info))
