#!/usr/bin/env python3
"""Run isolated release-built three-node performance trials; verify after timing."""
import argparse
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import resource
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time


def command(args, cwd=None):
    try:
        return subprocess.check_output(args, cwd=cwd, text=True, stderr=subprocess.STDOUT).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        return str(error)


def machine(root, artifacts):
    data = dict(platform=platform.platform(), architecture=platform.machine(),
                logical_cpus=os.cpu_count(), rust=command(['rustc', '--version']),
                revision=command(['git', 'rev-parse', 'HEAD'], root),
                git_status=command(['git', 'status', '--short'], root),
                filesystem=command(['df', '-h', str(artifacts)]))
    if platform.system() == 'Darwin':
        data['cpu'] = command(['sysctl', '-n', 'machdep.cpu.brand_string'])
        data['physical_cpus'] = command(['sysctl', '-n', 'hw.physicalcpu'])
        data['memory_bytes'] = command(['sysctl', '-n', 'hw.memsize'])
    else:
        data['cpu'] = command(['lscpu'])
        data['memory'] = Path('/proc/meminfo').read_text()
        data['mount'] = command(['findmnt', '-T', str(artifacts)])
        data['block_devices'] = command(['lsblk', '-o', 'NAME,SIZE,TYPE,MODEL'])
    digest = hashlib.sha256()
    for file in sorted(list((root/'src').rglob('*.rs')) + list((root/'bench').rglob('*.rs')) + [root/'Cargo.toml', root/'Cargo.lock']):
        digest.update(str(file.relative_to(root)).encode()); digest.update(file.read_bytes())
    data['source_sha256'] = digest.hexdigest()
    return data


def free_ports():
    for _ in range(200):
        base = random.SystemRandom().randrange(20000, 55000)
        sockets = []
        try:
            for offset in (0, 1, 2, 10, 11, 12, 20, 21, 22):
                sock = socket.socket(); sockets.append(sock)
                sock.bind(('127.0.0.1', base + offset))
            return base
        except OSError:
            pass
        finally:
            for sock in sockets: sock.close()
    raise RuntimeError('cannot reserve a free cluster port range')


def read_json(path):
    return json.loads(path.read_text())


def wait_for(description, predicate, processes, directory, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for failure in directory.glob('failed-*.json'):
            raise RuntimeError(f'{description}: {failure.read_text()}')
        for node, proc in processes.items():
            if proc.poll() is not None:
                raise RuntimeError(f'node {node} exited {proc.returncode}: {(directory/f"node{node}.log").read_text()[-4000:]}')
        result = predicate()
        if result: return result
        if time.monotonic() < deadline: time.sleep(.05)
    raise TimeoutError(description)


def verify_node(directory, node, expected, last):
    # Owners have exited. All SQL inspection remains outside workload timing.
    main = directory/'data'/f'{node}2'
    assert main.is_file(), main
    with closing(sqlite3.connect(str(main))) as db:
        value = db.execute('SELECT n FROM counter WHERE id=1').fetchone()[0]
        assert value == expected, (node, value, expected)
        receipt = db.execute('SELECT seq,entry_hash,local_digest FROM monsterft_meta_apply WHERE id=1').fetchone()
        assert receipt[0] == last, (node, receipt[0], last)
    copies = []
    for role in ('a', 'b', 'c'):
        path = directory/'data'/f'{node}1{role}'
        assert path.is_file(), path
        with closing(sqlite3.connect(str(path))) as db:
            meta = db.execute('SELECT * FROM monsterft_meta').fetchall()
            election = db.execute('SELECT * FROM raft_election').fetchall()
            log = db.execute('SELECT seq,term,entry FROM raft_log ORDER BY seq').fetchall()
            fence, pending = db.execute('SELECT repair_state,pending_cmd_seq FROM monsterft_meta').fetchone()
            assert fence == 0 and pending is None, (node, role, fence, pending)
            copies.append((meta, election, log))
    assert copies[0] == copies[1] == copies[2], f'node {node} DB1 copies differ'
    return dict(counter=expected, receipt=[receipt[0], receipt[1].hex(), receipt[2].hex()], db1_copies_agree=True)


def verify_databases(directory, done):
    expected, last = done['total_commands'], done['last_cmd_seq']
    results = [verify_node(directory, node, expected, last) for node in ('1', '2', '3')]
    assert results[0]['receipt'] == results[1]['receipt'] == results[2]['receipt'], 'cluster Apply receipts differ'
    return dict(counter=expected, receipt_seq=last, db1_copies_agree=True, db2_receipts_agree=True)


def probe(binary, delay_us, samples):
    result = subprocess.run([str(binary), str(free_ports()), str(delay_us), str(samples)],
                            capture_output=True, text=True, timeout=45)
    if result.returncode: raise RuntimeError(result.stdout + result.stderr)
    return json.loads(result.stdout)


def calibrate(binary, target, baseline, samples):
    if target == 0:
        return dict(target_rtt_us=0, one_way_delay_us=0, actual=baseline,
                    target_below_loopback=False, attempts=[])
    attempts = [(0.0, baseline)]
    delay = max(0, (target-baseline['rtt_us']['p50'])/2)
    for _ in range(7):
        if delay <= 0: break
        observed = probe(binary, delay, samples)
        attempts.append((delay, observed))
        error = observed['rtt_us']['p50'] - target
        if abs(error) <= max(5, target*.03): break
        next_delay = max(.001, delay-error/2)
        if abs(next_delay-delay) < .001: break
        delay = next_delay
    chosen, observed = min(attempts, key=lambda pair: abs(pair[1]['rtt_us']['p50']-target))
    # Confirm the chosen setting independently of the tuning samples.
    confirmed = probe(binary, chosen, samples)
    return dict(target_rtt_us=target, one_way_delay_us=chosen, actual=confirmed,
                target_below_loopback=target<baseline['rtt_us']['p50'],
                error_us=confirmed['rtt_us']['p50']-target,
                attempts=[dict(one_way_delay_us=d, actual=o) for d,o in attempts])


def run_trial(binary, directory, config):
    directory.mkdir()
    config = dict(config, run_dir=str(directory), base_port=free_ports())
    (directory/'config.json').write_text(json.dumps(config, indent=2))
    procs, logs = {}, []
    started = time.monotonic()
    cpu_before = resource.getrusage(resource.RUSAGE_CHILDREN)
    cpu_before = cpu_before.ru_utime + cpu_before.ru_stime
    try:
        for node in ('1', '2', '3'):
            log = open(directory/f'node{node}.log', 'wb'); logs.append(log)
            procs[node] = subprocess.Popen([str(binary), 'node', node, str(directory/'config.json')],
                                          stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        def ready():
            paths = [directory/f'ready-{node}.json' for node in procs]
            if not all(p.exists() for p in paths): return None
            states = [read_json(p) for p in paths]
            if len({(s['leader'], s['term']) for s in states}) != 1: return None
            return states[0]
        state = wait_for('three nodes agree on elected leader', ready, procs, directory, 60)
        (directory/'go.tmp').write_text(json.dumps(dict(leader=state['leader'], term=state['term'])))
        (directory/'go.tmp').replace(directory/'go.json')
        startup_secs = time.monotonic() - started
        wait_for('leader benchmark completion', lambda: (directory/'done.json').exists(), procs,
                 directory, (config['warmup_ms']+config['measure_ms'])/1000 + 120)
        done = read_json(directory/'done.json')
        wait_for('final SYNC on every node', lambda: all((directory/f'settled-{node}.json').exists() for node in procs), procs, directory, 60)
        settled = {node: read_json(directory/f'settled-{node}.json') for node in procs}
        (directory/'stop').touch()
        for node, proc in procs.items():
            assert proc.wait(timeout=30) == 0, (node, (directory/f'node{node}.log').read_text())
        cpu_after = resource.getrusage(resource.RUSAGE_CHILDREN)
        cpu_seconds = cpu_after.ru_utime + cpu_after.ru_stime - cpu_before
        verification_started = time.monotonic()
        verified = verify_databases(directory, done)
        result = dict(config=config, result=done, nodes=settled, verification=verified,
                      startup_secs=startup_secs, cpu_seconds_total_trial=cpu_seconds, verification_secs=time.monotonic()-verification_started,
                      total_wall_secs=time.monotonic()-started)
        (directory/'result.json').write_text(json.dumps(result, indent=2))
        return result
    finally:
        (directory/'stop').touch()
        for proc in procs.values():
            if proc.poll() is None: proc.terminate()
        for proc in procs.values():
            try: proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL); proc.wait(timeout=5)
        for log in logs: log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--artifacts', type=Path)
    parser.add_argument('--binary-dir', type=Path)
    parser.add_argument('--no-build', action='store_true')
    parser.add_argument('--profiles', default='baseline,lan,az')
    parser.add_argument('--windows', default='1,32')
    parser.add_argument('--batch-size', type=int, default=1)
    parser.add_argument('--warmup', type=float, default=10)
    parser.add_argument('--seconds', type=float, default=30)
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--tick-ms', type=int, default=10)
    parser.add_argument('--audit-batches', type=int, default=64)
    parser.add_argument('--probe-samples', type=int, default=1000)
    parser.add_argument('--lan-rtt-us', type=float, default=100)
    parser.add_argument('--az-rtt-us', type=float, default=440)
    args = parser.parse_args()
    root = args.root.resolve()
    assert args.warmup >= 0 and args.seconds > 0 and args.repeats > 0
    assert 1 <= args.batch_size <= 1024
    windows = [int(v) for v in args.windows.split(',')]; assert all(v > 0 for v in windows)
    profiles = {name: {'baseline': 0, 'lan': args.lan_rtt_us, 'az': args.az_rtt_us}[name] for name in args.profiles.split(',')}
    if not args.no_build:
        subprocess.run(['cargo','build','--locked','--release','--example','monsterft-bench','--example','monsterft-netprobe'], cwd=root, check=True)
    binaries = (args.binary_dir or root/'target/release/examples').resolve()
    directory = (args.artifacts or Path(tempfile.mkdtemp(prefix='monsterft-bench-'))).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    assert not any(directory.iterdir()), 'artifacts directory must be empty'
    print(f'Artifacts: {directory}', flush=True)
    summary = dict(date=datetime.now(timezone.utc).isoformat(), machine=machine(root,directory),
                   settings=vars(args)|{'root':str(root),'artifacts':str(directory),'binary_dir':str(binaries)}, calibration={}, trials=[])
    def save(): (directory/'summary.json').write_text(json.dumps(summary, indent=2, default=str))
    baseline = probe(binaries/'monsterft-netprobe', 0, args.probe_samples)
    summary['calibration']['baseline'] = baseline
    save()
    for profile, rtt in profiles.items():
        setting = calibrate(binaries/'monsterft-netprobe', rtt, baseline, args.probe_samples)
        delay, calibration = setting['one_way_delay_us'], setting['actual']
        summary['calibration'][profile] = setting
        save()
        print(f'{profile}: target RTT {rtt}us; added {delay:.2f}us each way; observed median RTT {calibration["rtt_us"]["p50"]:.2f}us', flush=True)
        for repeat in range(1,args.repeats+1):
            # Alternate order so the queued case is not always measured second.
            for window in windows if repeat % 2 else reversed(windows):
                name=f'{profile}-q{window}-r{repeat}'
                config=dict(rtt_us=round(rtt),one_way_delay_us=delay,window=window,batch_size=args.batch_size,
                            warmup_ms=round(args.warmup*1000),measure_ms=round(args.seconds*1000),
                            tick_ms=args.tick_ms,audit_batches=args.audit_batches)
                print(f'Start {name}', flush=True)
                result=run_trial(binaries/'monsterft-bench',directory/name,config)
                result.update(profile=profile,repeat=repeat)
                summary['trials'].append(result);save()
                measured=result['result']
                print(f'{name}: {measured["commands_per_sec"]:.2f} CMD/s; p50 {measured["latency_us"]["p50"]:.2f}us; verified',flush=True)
    print('Completed all trials; summary.json contains raw per-trial results.',flush=True)


if __name__ == '__main__':
    main()
