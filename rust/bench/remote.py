#!/usr/bin/env python3
"""Run the existing workload across three SSH-accessible hosts using private TCP."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import math
import os
from pathlib import Path
import resource
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import time

sys.dont_write_bytecode = True
import run as local


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, indent=2))
    temporary.replace(path)


def read_optional(path):
    return json.loads(path.read_text()) if path.exists() else None


def terminate_owned(pid, config_path):
    # A PID file is not sufficient: confirm this is still our exact trial process.
    try:
        arguments = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
        if str(config_path).encode() not in arguments:
            return
        os.killpg(pid, signal.SIGTERM)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline and Path(f'/proc/{pid}').exists():
            time.sleep(.1)
        if Path(f'/proc/{pid}').exists():
            arguments = Path(f'/proc/{pid}/cmdline').read_bytes().split(b'\0')
            if str(config_path).encode() in arguments:
                os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except FileNotFoundError:
        pass


def host_run(node, directory, binary):
    config_path = directory/'config.json'
    config = local.read_json(config_path)
    started = time.monotonic()
    with open(directory/f'node{node}.log', 'wb') as log, open(directory/'vmstat.log', 'wb') as activity:
        monitor = subprocess.Popen(['vmstat', '-w', '-t', '1'], stdout=activity)
        try:
            process = subprocess.Popen([binary, 'node', node, str(config_path)], stdout=log,
                                       stderr=subprocess.STDOUT, start_new_session=True)
        except BaseException:
            monitor.terminate()
            monitor.wait(timeout=5)
            raise
        (directory/'node.pid').write_text(str(process.pid))
        def interrupted(signum, frame):
            raise RuntimeError(f'host runner interrupted by signal {signum}')
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(sig, interrupted)
        try:
            code = process.wait(timeout=(config['warmup_ms']+config['measure_ms'])/1000+180)
        finally:
            if process.poll() is None:
                terminate_owned(process.pid, config_path)
                process.wait(timeout=15)
            monitor.terminate()
            monitor.wait(timeout=5)
            usage = resource.getrusage(resource.RUSAGE_CHILDREN)
            write_json(directory/'exit.json', dict(code=process.returncode,
                       wall_seconds=time.monotonic()-started,
                       cpu_seconds=usage.ru_utime+usage.ru_stime,
                       voluntary_context_switches=usage.ru_nvcsw,
                       involuntary_context_switches=usage.ru_nivcsw))
    return code


def read_exact(sock, length):
    result = bytearray()
    while len(result) < length:
        part = sock.recv(length-len(result))
        if not part:
            raise RuntimeError('TCP echo closed before a full message')
        result.extend(part)
    return bytes(result)


def echo_server(port):
    with socket.socket() as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', port)); server.listen(1); server.settimeout(20)
        print('ready', flush=True)
        connection, _ = server.accept()
        with connection:
            connection.settimeout(15)
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            while True:
                first = connection.recv(256)
                if not first:
                    return
                frame = first + read_exact(connection, 256-len(first))
                connection.sendall(frame)


def echo_client(host, port):
    with socket.create_connection((host,port), timeout=10) as connection:
        connection.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        samples=[]; sequence=0; warm_until=time.monotonic()+1
        while len(samples)<1000:
            payload=sequence.to_bytes(8,'big')+bytes(248)
            start=time.perf_counter_ns()
            connection.sendall(payload)
            returned=read_exact(connection,256)
            elapsed=(time.perf_counter_ns()-start)/1000
            if returned != payload: raise RuntimeError('TCP echo bytes changed')
            if time.monotonic()>=warm_until:samples.append(elapsed)
            sequence+=1
    samples.sort()
    return dict(samples=len(samples),payload_bytes=256,implementation='Python TCP_NODELAY echo; includes harness overhead',
                rtt_us={name:samples[math.ceil(len(samples)*fraction)-1] for name,fraction in [('p50',.5),('p95',.95),('p99',.99)]})


def host_command(args):
    action=args[0]
    if action=='_echo':echo_server(int(args[1]));return 0
    if action=='_rtt':print(json.dumps(echo_client(args[1],int(args[2]))));return 0
    if action=='_meta':
        os.environ['PATH']=str(Path.home()/'.cargo/bin')+os.pathsep+os.environ['PATH']
        print(json.dumps(local.machine(Path(__file__).resolve().parents[1],Path(args[1]))));return 0
    node,directory=args[1],Path(args[2])
    if action=='_init':
        directory.mkdir(parents=True,exist_ok=False)
        write_json(directory/'config.json',json.load(sys.stdin));return 0
    if action=='_run':return host_run(node,directory,args[3])
    if action=='_poll':
        print(json.dumps({name:read_optional(directory/file) for name,file in {
            'ready':f'ready-{node}.json','done':'done.json','settled':f'settled-{node}.json',
            'failed':f'failed-{node}.json','exit':'exit.json'}.items()}));return 0
    if action=='_put':
        name=args[3]
        assert name in ('go.json','done.json','stop')
        write_json(directory/name,json.load(sys.stdin));return 0
    if action=='_abort':
        if directory.exists():
            write_json(directory/'stop',{})
            if (directory/'node.pid').exists():
                terminate_owned(int((directory/'node.pid').read_text()),directory/'config.json')
        return 0
    if action=='_verify':
        done=local.read_json(directory/'done.json')
        print(json.dumps(local.verify_node(directory,node,done['total_commands'],done['last_cmd_seq'])));return 0
    if action=='_artifacts':
        print(json.dumps({p.name:p.read_text() for p in directory.iterdir()
                          if p.is_file() and p.suffix in ('.json','.log')}));return 0
    raise ValueError(action)


class Remote:
    def __init__(self,args,host,control):
        self.host=host; self.script=str(Path(args.remote_source)/'bench/remote.py')
        self.ssh=['ssh','-i',str(args.identity),'-o','BatchMode=yes','-o','ConnectTimeout=10',
                  '-o','ServerAliveInterval=10','-o','ServerAliveCountMax=3',
                  '-o','StrictHostKeyChecking=yes','-o',f'UserKnownHostsFile={args.known_hosts}',
                  '-o','ControlMaster=auto','-o','ControlPersist=60','-o',f'ControlPath={control}/%h']
        if args.user:self.ssh.extend(['-l',args.user])
        self.ssh.append(host)
    def command(self,*args):return self.ssh+[shlex.join(['python3',self.script,*map(str,args)])]
    def call(self,*args,data=None,timeout=30):
        result=subprocess.run(self.command(*args),input=None if data is None else json.dumps(data),
                              capture_output=True,text=True,timeout=timeout)
        if result.returncode:raise RuntimeError(f'{self.host}: {result.stderr or result.stdout}')
        return json.loads(result.stdout) if result.stdout.strip() else None
    def close(self):
        subprocess.run(self.ssh[:-1]+['-O','exit',self.ssh[-1]],capture_output=True,timeout=10)


def parallel(function,values):
    with ThreadPoolExecutor(max_workers=3) as pool:return list(pool.map(function,values))


def trial(args,remotes,name,window,*,before_start=None):
    local_dir=args.artifacts/name;local_dir.mkdir()
    remote_dir=Path(args.remote_root)/name
    config=dict(run_dir=str(remote_dir),base_port=args.base_port,hosts=args.private_hosts,rtt_us=0,
                one_way_delay_us=0,window=window,warmup_ms=round(args.warmup*1000),
                measure_ms=round(args.seconds*1000),tick_ms=10,audit_batches=64,
                batch_size=args.batch_size)
    write_json(local_dir/'config.json',config)
    nodes=list(zip(['1','2','3'],remotes)); created=[]; processes=[];logs=[]; started=time.monotonic()
    def snapshots():
        values=parallel(lambda pair:pair[1].call('_poll',pair[0],remote_dir),nodes)
        for value in values:
            if value['failed']:raise RuntimeError(f'component failure: {value["failed"]}')
            if value['exit']:raise RuntimeError(f'node exited early: {value["exit"]}')
        if any(p.poll() is not None for p in processes):raise RuntimeError('SSH host runner exited early')
        return values
    def wait_for(label,predicate,timeout):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            answer=predicate(snapshots())
            if answer:return answer
            time.sleep(1)
        raise TimeoutError(label)
    def stop_and_collect():
        parallel(lambda pair:pair[1].call('_put',pair[0],remote_dir,'stop',data={}),nodes)
        for process in processes:
            if process.wait(timeout=40)!=0:raise RuntimeError('host runner failed during cleanup')
        states=parallel(lambda pair:pair[1].call('_poll',pair[0],remote_dir),nodes)
        if any(s['failed'] or s['exit']['code']!=0 for s in states):raise RuntimeError(f'cleanup failed: {states}')
        return states
    try:
        def initialize(pair):
            pair[1].call('_init',pair[0],remote_dir,data=config)
            created.append(pair)
        parallel(initialize,nodes)
        for node,remote in nodes:
            log=open(local_dir/f'ssh-{node}.log','wb');logs.append(log)
            processes.append(subprocess.Popen(remote.command('_run',node,remote_dir,args.binary),stdout=log,stderr=subprocess.STDOUT))
        def ready(states):
            if not all(s['ready'] for s in states):return None
            votes={(s['ready']['leader'],s['ready']['term']) for s in states}
            if len(votes)!=1:return None
            leader,term=next(iter(votes));return dict(leader=leader,term=term)
        elected=wait_for('leader agreement',ready,90)
        if before_start is not None and not before_start(nodes,remote_dir,elected):
            states=stop_and_collect()
            result=dict(config=config,elected=elected,skipped=True,
                        exits={n:s['exit'] for (n,_),s in zip(nodes,states)})
            write_json(local_dir/'result.json',result)
            return result
        parallel(lambda pair:pair[1].call('_put',pair[0],remote_dir,'go.json',data=elected),nodes)
        done=wait_for('workload completion',lambda states:next((s['done'] for s in states if s['done']),None),args.warmup+args.seconds+90)
        if done['leader']!=elected['leader'] or done['term']!=elected['term']:raise RuntimeError('leader changed')
        parallel(lambda pair:pair[1].call('_put',pair[0],remote_dir,'done.json',data=done),nodes)
        settled=wait_for('final SYNC',lambda states:[s['settled'] for s in states] if all(s['settled'] for s in states) else None,60)
        states=stop_and_collect()
        verification=parallel(lambda pair:pair[1].call('_verify',pair[0],remote_dir),nodes)
        if len({tuple(v['receipt']) for v in verification})!=1:raise RuntimeError('Apply receipts disagree across hosts')
        if any(s['errors'] or s['restarts'] or s['dropped_frames'] for s in settled):raise RuntimeError('invalid node statistics')
        result=dict(config=config,result=done,nodes={n:s for (n,_),s in zip(nodes,settled)},
                    verification=verification,exits={n:s['exit'] for (n,_),s in zip(nodes,states)},
                    total_wall_secs=time.monotonic()-started)
        write_json(local_dir/'result.json',result)
        return result
    finally:
        for node,remote in created:
            try:remote.call('_abort',node,remote_dir,timeout=20)
            except Exception as e:print(f'cleanup {node}: {e}',file=sys.stderr)
        for process in processes:
            try:process.wait(timeout=15)
            except subprocess.TimeoutExpired:process.terminate();process.wait(timeout=10)
        for log in logs:log.close()
        for node,remote in created:
            try:
                files=remote.call('_artifacts',node,remote_dir)
                destination=local_dir/f'node{node}';destination.mkdir(exist_ok=True)
                for file,contents in files.items():(destination/file).write_text(contents)
            except Exception as e:print(f'artifact collection {node}: {e}',file=sys.stderr)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--hosts',required=True);p.add_argument('--private-hosts',required=True)
    p.add_argument('--user',help='SSH username (defaults to SSH configuration or current username)')
    p.add_argument('--identity',required=True,type=Path);p.add_argument('--known-hosts',required=True,type=Path)
    p.add_argument('--remote-source',required=True);p.add_argument('--binary',required=True)
    p.add_argument('--remote-root',required=True);p.add_argument('--artifacts',required=True,type=Path)
    p.add_argument('--base-port',type=int,default=43000)
    p.add_argument('--warmup',type=float,default=10);p.add_argument('--seconds',type=float,default=30)
    p.add_argument('--windows',default='1,32');p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--batch-size',type=int,default=1)
    p.add_argument('--profile',choices=('same-az','multi-az'),default='same-az')
    p.add_argument('--skip-rtt',action='store_true')
    args=p.parse_args();args.hosts=args.hosts.split(',');args.private_hosts=args.private_hosts.split(',')
    assert len(args.hosts)==len(args.private_hosts)==3
    assert all(Path(p).is_absolute() for p in (args.remote_root,args.remote_source,args.binary))
    for host in args.hosts+args.private_hosts:ipaddress.ip_address(host)
    assert 1<=args.base_port<=65513 and args.warmup>=0 and args.seconds>0 and args.repeats>0
    assert 1<=args.batch_size<=1024
    windows=[int(w) for w in args.windows.split(',')];assert all(1<=w<=256 for w in windows)
    args.artifacts=args.artifacts.resolve();args.artifacts.mkdir(parents=True,exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='mftssh-') as control:
        remotes=[Remote(args,h,control) for h in args.hosts]
        summary=dict(date=datetime.now(timezone.utc).isoformat(),topology='three physical hosts; private TCP; no injected delay',
                     settings={k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},machines=[],rtt=[],trials=[])
        def save():write_json(args.artifacts/'summary.json',summary)
        try:
            summary['machines']=parallel(lambda r:r.call('_meta',str(Path(args.remote_root).parent)),remotes)
            hashes={m['source_sha256'] for m in summary['machines']}
            if len(hashes)!=1:raise RuntimeError('host source fingerprints differ')
            summary['controller_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();save()
            if not args.skip_rtt:
                for a,b in [(0,1),(0,2),(1,2)]:
                    echo=subprocess.Popen(remotes[b].command('_echo',args.base_port),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                    try:
                        if echo.stdout.readline().strip()!='ready':raise RuntimeError(echo.stderr.read())
                        measured=remotes[a].call('_rtt',args.private_hosts[b],args.base_port,timeout=30)
                        if echo.wait(timeout=10)!=0:raise RuntimeError(echo.stderr.read())
                        measured.update(nodes=[a+1,b+1]);summary['rtt'].append(measured);save()
                        print('RTT',a+1,b+1,measured['rtt_us'],flush=True)
                    finally:
                        if echo.poll() is None:echo.terminate();echo.wait(timeout=10)
            for repeat in range(1,args.repeats+1):
                for window in windows if repeat%2 else reversed(windows):
                    name=f'q{window}-r{repeat}';print('Start',name,flush=True)
                    result=trial(args,remotes,name,window);result.update(repeat=repeat,profile=args.profile)
                    summary['trials'].append(result);save()
                    print(name,f"{result['result']['commands_per_sec']:.2f} CMD/s; verified",flush=True)
            print('Completed all remote trials.',flush=True)
        except Exception as e:
            summary['failure']=str(e);save();raise
        finally:
            for remote in remotes:
                try:remote.close()
                except Exception:pass


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1].startswith('_'):
        raise SystemExit(host_command(sys.argv[1:]))
    main()
