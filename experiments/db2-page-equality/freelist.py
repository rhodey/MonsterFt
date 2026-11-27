#!/usr/bin/env python3
"""Explore free-page canonicalization; this does not change the repair design.

Extends lifecycle.py's identical-command workload beyond the first raw-page
difference. Reads comparisons on independent connections so observation does
not warm the writers' caches. Never applies patchsets or changesets.
"""
import argparse
import json
import pathlib
import tempfile

import lifecycle as life
from sqlite_probe import DB, metadata


def classify(pages):
    """Validate the complete freelist before interpreting leaf bytes as unused."""
    total = len(pages)
    count = int.from_bytes(pages[1][36:40], 'big')
    trunk = int.from_bytes(pages[1][32:36], 'big')
    trunks, leaves, seen = [], [], set()
    while trunk:
        assert 1 < trunk <= total and trunk not in seen
        seen.add(trunk)
        trunks.append(trunk)
        page = pages[trunk]
        leaf_count = int.from_bytes(page[4:8], 'big')
        assert leaf_count <= (len(page) - 8) // 4
        for i in range(leaf_count):
            leaf = int.from_bytes(page[8+i*4:12+i*4], 'big')
            assert 1 < leaf <= total and leaf not in seen
            leaves.append(leaf)
            seen.add(leaf)
        trunk = int.from_bytes(page[:4], 'big')
    assert len(seen) == count
    return {'trunks': trunks, 'leaves': leaves}


def mask_leaves(view):
    freelist = classify(view['raw'])
    result = dict(view['canonical'])
    for page in freelist['leaves']:
        result[page] = bytes(len(result[page]))
    return result, freelist


class Observer:
    def __init__(self, masked):
        self.masked = masked
        self.checks = 0
        self.raw_mismatch_checks = 0
        self.masked_mismatch_checks = 0
        self.first_raw_mismatch = None
        self.first_masked_mismatch = None
        self.difference_kinds = {}
        self.found_free_differences = set()
        self.reuse_observations = []
        self.reused = set()
        self.metadata_equal = True

    def __call__(self, seq, dbs, template, views):
        normalized = [mask_leaves(view) for view in views]
        raw_diffs = [life.page_diff(views[0]['canonical'], view['canonical'])
                     for view in views[1:]]
        masked_diffs = [life.page_diff(normalized[0][0], item[0])
                        for item in normalized[1:]]
        free = [set(item[1]['leaves']) for item in normalized]
        trunks = [set(item[1]['trunks']) for item in normalized]
        self.metadata_equal &= all(item[1] == normalized[0][1] for item in normalized[1:])
        self.checks += 1
        if any(raw_diffs):
            self.raw_mismatch_checks += 1
            if self.first_raw_mismatch is None:
                self.first_raw_mismatch = {'seq': seq, 'diffs': raw_diffs}
        if any(masked_diffs):
            self.masked_mismatch_checks += 1
            if self.first_masked_mismatch is None:
                self.first_masked_mismatch = {'seq': seq, 'diffs': masked_diffs}
        for member, diffs in enumerate(raw_diffs, 1):
            for diff in diffs:
                page = diff['page']
                kind = ('leaf' if page in free[0] and page in free[member] else
                        'trunk' if page in trunks[0] or page in trunks[member] else 'active_or_size')
                self.difference_kinds[kind] = self.difference_kinds.get(kind, 0) + 1
                if kind == 'leaf':
                    self.found_free_differences.add(page)
        for page in self.found_free_differences - self.reused:
            if all(page in view['raw'] and page not in free[i] and page not in trunks[i]
                   for i, view in enumerate(views)):
                equal = all(view['canonical'][page] == views[0]['canonical'][page]
                            for view in views[1:])
                self.reused.add(page)
                self.reuse_observations.append({'seq': seq, 'page': page, 'active_bytes_equal': equal})
        if self.masked:
            return [{**view, 'canonical': normalized[i][0]} for i, view in enumerate(views)]
        return views

    def result(self):
        return {key: value for key, value in vars(self).items() if key not in (
            'found_free_differences', 'reused')} | {
            'differing_free_pages': sorted(self.found_free_differences),
            'reused_differing_pages': sorted(self.reused),
        }


def run(args):
    root = pathlib.Path(args.work_dir) if args.work_dir else pathlib.Path(tempfile.mkdtemp(prefix='monsterft-freelist-'))
    root.mkdir(parents=True, exist_ok=True)
    life.setup_sessions()
    suite = life.Suite(root)
    info = DB(root / 'metadata.db')
    runtime = metadata(info)
    info.close()
    runtime['work_dir'] = str(root)
    results = []
    for profile in ('on-equal', 'split'):
        for secure_delete in (False, True):
            observer = Observer(masked=True)
            result = life.lifecycle_workload(
                suite, 0, profile=profile, secure_delete=secure_delete,
                max_commands=args.commands, stop_on_mismatch=False,
                observer=observer, name_suffix='-leaf-mask')
            observation = observer.result()
            observation.update({
                'profile': profile, 'secure_delete': secure_delete,
                'restart_counts': result['restart_counts'],
                'patchset_failures': [failure for failure in result['failures']
                                      if failure['reason'] == 'patchset mismatch'],
                'integrity': result['integrity'],
                'final_page_counts': result['page_counts'],
            })
            results.append(observation)
            print(json.dumps({'profile':profile,'secure_delete':secure_delete,
                              'checks':observation['checks'],
                              'raw_mismatch_checks':observation['raw_mismatch_checks'],
                              'masked_mismatch_checks':observation['masked_mismatch_checks'],
                              'reuse':len(observation['reuse_observations'])}), flush=True)
    for maintenance in ('scan-only', 'zero-leaves'):
        observer = Observer(masked=False)
        writes = {'scanned_pages': 0, 'written_pages': 0, 'written_bytes': 0}

        def before_commit(db):
            pages = db.pages()
            free = classify(pages)
            writes['scanned_pages'] += len(pages)
            if maintenance == 'zero-leaves':
                for page in free['leaves']:
                    if any(pages[page]):
                        db.sql('INSERT INTO sqlite_dbpage(pgno,data) VALUES(%d,zeroblob(%d))' % (page, len(pages[page])))
                        writes['written_pages'] += 1
                        writes['written_bytes'] += len(pages[page])

        result = life.lifecycle_workload(
            suite, 0, profile='split', secure_delete=False,
            max_commands=args.commands, stop_on_mismatch=False,
            observer=observer, name_suffix='-' + maintenance,
            before_commit=before_commit)
        observation = observer.result()
        observation.update({
            'profile': 'split', 'secure_delete': False, 'maintenance': maintenance,
            'restart_counts': result['restart_counts'],
            'patchset_failures': [failure for failure in result['failures']
                                  if failure['reason'] == 'patchset mismatch'],
            'integrity': result['integrity'], 'writes': writes,
            'final_page_counts': result['page_counts'],
        })
        results.append(observation)
        print(json.dumps({'maintenance':maintenance,'checks':observation['checks'],
                          'raw_mismatch_checks':observation['raw_mismatch_checks'],
                          'masked_mismatch_checks':observation['masked_mismatch_checks'],
                          'writes':writes}), flush=True)
    pathlib.Path(args.output).write_text(json.dumps({'runtime': runtime, 'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--commands', type=int, default=320)
    parser.add_argument('--work-dir')
    parser.add_argument('--output', default=str(pathlib.Path(__file__).with_name('freelist-results.json')))
    run(parser.parse_args())
