#!/usr/bin/env python3
"""Run real Parser guard fixtures on P9, including baseline defect detection.

Usage: --decode DIR --binary FIXTURE_BINARY --candidate baseline|fixed --out NEW_DIR
Cases use immutable raw vectors. Negative controls omit BEGIN or create a partial
transaction buffer entry; they are isolated parser tests, never DATA acceptance.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--decode', required=True)
    ap.add_argument('--binary', required=True)
    ap.add_argument('--candidate', choices=['baseline', 'fixed'], required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    root = Path(args.out)
    root.mkdir(parents=True, exist_ok=False)
    decoded = Path(args.decode)
    events = json.loads((decoded / 'events.json').read_text())
    summary = json.loads((decoded / 'SUMMARY.json').read_text())
    pairs = {x['commit_event']: x['begin_event'] for x in json.loads((decoded / 'correlations.json').read_text())}
    by_id = {x['event_id']: x for x in events}
    post = [by_id[i] for i in summary['post_cut_commit_event_ids']]
    cut, con, pdb = summary['cut'], summary['expected_con_id'], summary['qualified_pdb_id']
    cases = []

    def add(record, mode, scenario, expected, **extra):
        cfg = {'native': mode != 'manual', 'metadata_db_id': pdb, 'metadata_con_id': con}
        if mode == 'noncdb-guard':
            cfg.update(native=False, metadata_db_id=0, metadata_con_id=0)
        case = dict(cfg, name='%s-%s-event%d-%d' % (mode, scenario, record['event_id'], len(cases)),
                    mode=mode, scenario=scenario, cut=cut, record=record, expected_fixed_code=expected, **extra)
        cases.append(case)

    for record in post:
        for mode in ['native', 'manual', 'noncdb-guard']:
            expected = 10076 if mode == 'noncdb-guard' or record['con_id'] == con else 0
            for scenario in ['missing-null', 'missing-partial']:
                add(record, mode, scenario, expected)
        if record['con_id'] != con and pairs[record['event_id']] is not None:
            begin = by_id[pairs[record['event_id']]]
            assert begin['pdb_id'] != pdb
            for mode in ['native', 'manual']:
                add(record, mode, 'filtered-begin-commit', 0, begin=begin)
    # Real rollback and pre-cut controls retain the existing exceptions and cut.
    controls = []
    for container in sorted(set(x['con_id'] for x in events)):
        for rollback in [False, True]:
            rows = [x for x in events if x['op'] == '5.4' and x['con_id'] == container and x['sqn'] != 0 and
                    x['rollback'] == rollback and (rollback or x['record_scn'] <= cut)]
            if rows:
                controls.append(max(rows, key=lambda x: x['record_scn']))
    for record in controls:
        for mode in ['native', 'manual', 'noncdb-guard']:
            for scenario in ['missing-null', 'missing-partial']:
                add(record, mode, scenario, 0)
    target_begin = next(x for x in events if x['op'] == '5.2' and x['pdb_id'] == pdb and x['sqn'] != 0)
    root_begin = next(x for x in events if x['op'] == '5.2' and x['con_id'] == 1 and x['sqn'] != 0)
    add(target_begin, 'native', 'begin-only', 0)
    add(root_begin, 'native', 'begin-only', 0)
    add(target_begin, 'native', 'begin-only', 10077)
    cases[-1]['metadata_db_id'] = 0
    add(target_begin, 'native', 'begin-only', 10077)
    cases[-1]['metadata_db_id'] = pdb + 1
    # No other ordinary PDB exists in these inputs. Model a different qualified
    # container only in isolated metadata; every input byte and the cut stay real.
    target_commit = next(x for x in post if x['con_id'] == con)
    for mode in ['native', 'manual']:
        for scenario in ['missing-null', 'missing-partial']:
            add(target_commit, mode, scenario, 0, scope_model='other-PDB metadata fixture, not observed catalog/runtime')
            cases[-1].update(metadata_con_id=con + 1, metadata_db_id=pdb + 1)
    env = dict(os.environ, LD_LIBRARY_PATH='/u01/app/oracle/product/19.3.0/dbhome_1/lib:/opt/openlogreplicator/lib:/opt/openlogreplicator/deps/protobuf/lib64')
    results = []
    with (root / 'cases.jsonl').open('w') as inputs, (root / 'raw-results.jsonl').open('w') as raw:
        for case in cases:
            text = json.dumps(case)
            inputs.write(text + '\n')
            proc = subprocess.run([args.binary], input=text.encode(), env=env, stdout=subprocess.PIPE,
                                  stderr=subprocess.STDOUT, timeout=15)
            log = proc.stdout.decode('utf-8', 'replace')
            raw.write(json.dumps({'name': case['name'], 'exit': proc.returncode, 'log': log}) + '\n')
            raw.flush()
            lines = [s[len('SCOPE_RESULT '):] for s in log.splitlines() if s.startswith('SCOPE_RESULT ')]
            assert proc.returncode == 0 and len(lines) == 1, (case['name'], proc.returncode, log)
            observed = json.loads(lines[0])
            assert observed['production_opcode_decode_matches'] is True
            if case['scenario'] != 'begin-only':
                assert observed['phase'] == 'commit' and observed['transaction_begin'] is False
                assert observed['transaction_present'] == (case['scenario'] == 'missing-partial'), (case['name'], observed)
            expected = case['expected_fixed_code']
            baseline_defect = (expected == 0 and case['record']['op'] == '5.4' and not case['record']['rollback'] and
                               case['record']['record_scn'] > cut and case['metadata_db_id'] > 0 and
                               case['record']['con_id'] != case['metadata_con_id'])
            actual_expected = 10076 if args.candidate == 'baseline' and baseline_defect else expected
            assert observed['observed_code'] == actual_expected, (case['name'], actual_expected, observed)
            results.append({'name': case['name'], 'mode': case['mode'], 'scenario': case['scenario'],
                            'event_id': case['record']['event_id'], 'expected_fixed_code': expected,
                            'observed': observed, 'baseline_p1_exposed': baseline_defect})
    exposed = sum(x['baseline_p1_exposed'] for x in results)
    assert exposed > 0
    report = {'scope': 'Actual linked production Parser guard execution on raw archived vectors; no DATA/ACK/recovery acceptance',
              'candidate': args.candidate, 'verdict': 'BASELINE_P1_REPRODUCED' if args.candidate == 'baseline' else 'FIXED_GUARD_CASES_PASS',
              'binary_sha256': hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(),
              'decode_events_sha256': hashlib.sha256((decoded / 'events.json').read_bytes()).hexdigest(),
              'cases': len(results), 'cases_exposing_original_P1': exposed,
              'noncdb_scope': 'dbId0 metadata guard controls on real P9 vectors, not a non-CDB database runtime test',
              'other_pdb_scope': 'Different-conId metadata unit fixture; no other ordinary PDB observed in sealed archives',
              'results': results}
    (root / 'RESULT.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'results'}))


if __name__ == '__main__':
    main()
