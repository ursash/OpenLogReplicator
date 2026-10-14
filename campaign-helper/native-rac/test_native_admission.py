#!/usr/bin/env python3
"""Run bounded native CLI/state admission checks against a built OLR, preferably on P9.

Usage: python3 test_native_admission.py --binary /path/OpenLogReplicator --out /private/evidence
No database connection, client START, shared process stop or Kafka operation is performed.
"""
import argparse
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--out', required=True)
    args = parser.parse_args()
    root = Path(args.out).resolve()
    root.mkdir(parents=True, exist_ok=False)
    base = {
        'version': '2.0.0',
        'memory': {'min-mb': 64, 'max-mb': 256},
        'source': [{'alias': 'TEST', 'name': 'ORCLRAC', 'flags': 69728,
                    'reader': {'type': 'rac-online', 'user': 'native_test', 'password': 'dummy-test-only',
                               'server': '//127.0.0.1:1521/ORCLDG'},
                    'format': {'type': 'json', 'message': 2, 'schema': 7},
                    'filter': {'table': [{'owner': 'PRODDTA', 'table': 'F4311'}]}}],
        'target': [{'alias': 'TEST-OUT', 'source': 'TEST',
                    'writer': {'type': 'network', 'uri': '127.0.0.1:39401'}}]
    }
    results = []

    def case(name, seed=None, edit=None, init=False, expected=''):
        directory = root / name
        directory.mkdir()
        state = directory / 'state'
        state.mkdir()
        cfg = copy.deepcopy(base)
        cfg['state'] = {'type': 'disk', 'path': str(state)}
        if edit:
            edit(cfg)
        if seed:
            for filename, value in seed.items():
                (state / filename).write_text(json.dumps(value))
        config = directory / 'OpenLogReplicator.json'
        config.write_text(json.dumps(cfg))
        os.chmod(str(config), 0o600)
        config_hash = hashlib.sha256(config.read_bytes()).hexdigest()
        before = {p.name: p.read_bytes() for p in state.glob('*.json')}
        cmd = [args.binary, '-f', str(config)] + (['--rac-init'] if init else [])
        lock = None
        if name == 'busy-state':
            lock = (state / 'ORCLRAC-rac.lock').open('w')
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            run = subprocess.run(cmd, cwd=str(directory), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
        finally:
            if lock:
                lock.close()
        (directory / 'olr.log').write_bytes(run.stdout)
        assert hashlib.sha256(config.read_bytes()).hexdigest() == config_hash, name
        after = {p.name: p.read_bytes() for p in state.glob('*.json')}
        log = run.stdout.decode('utf-8', 'replace')
        if name == 'fresh-intent':
            assert run.returncode == 0, log
            assert set(after) == {'ORCLRAC-rac-intent.json'}, after
            assert json.loads(after['ORCLRAC-rac-intent.json']) == {'intent-format': 1, 'database': 'ORCLRAC'}
        else:
            assert run.returncode != 0 and expected in log, (name, run.returncode, log)
            assert before == after, (name, 'state mutated on refusal')
        results.append({'case': name, 'exit': run.returncode, 'pass': True, 'config_sha256': config_hash})

    case('fresh-intent', init=True)
    intent = {'intent-format': 1, 'database': 'ORCLRAC'}
    case('duplicate-intent', {'ORCLRAC-rac-intent.json': intent}, init=True, expected='empty source state')
    case('empty-without-intent', expected='one-shot --rac-init')
    case('legacy-cursor', {'ORCLRAC-chkpt.json': {'cursor-format': 3}}, expected='orphan or legacy RAC state')
    case('orphan-schema', {'ORCLRAC-chkpt-123.json': {'schema-scn': 123}, 'ORCLRAC-rac-intent.json': intent},
         expected='orphan or legacy RAC state')
    case('wrong-intent', {'ORCLRAC-rac-intent.json': {'intent-format': 1, 'database': 'OTHER'}}, expected='invalid native RAC initialization intent')
    case('bad-descriptor', {'ORCLRAC-rac-bootstrap.json': {'descriptor-format': 2}}, expected='ERROR')
    case('manual-scn', edit=lambda cfg: cfg['source'][0]['reader'].update({'start-scn': 123}), init=True, expected='remove reader field start-scn')
    case('stock-reader-init', edit=lambda cfg: cfg['source'][0]['reader'].update({'type': 'online'}), init=True,
         expected='only valid for reader type rac-online')
    case('busy-state', init=True, expected='already in use')
    report = {'scope': 'native CLI and negative state admission only; no DATA-PASS',
              'binary_sha256': hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(), 'results': results}
    (root / 'RESULTS.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': len(results), 'scope': report['scope'], 'evidence': str(root)}))


if __name__ == '__main__':
    main()
