#!/usr/bin/env python3
"""Decode immutable 5.2/5.4 inputs for the real parser container-scope regression.

Run on P9: --scanner REDO_SCAN.py --input-pin INPUT.json --out NEW_DIRECTORY
No database access. Retains raw record/vector bytes and correlates BEGIN/COMMIT.
"""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scanner', required=True)
    ap.add_argument('--input-pin', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--cut', type=int, required=True)
    ap.add_argument('--con-id', type=int, required=True)
    ap.add_argument('--pdb-id', type=int, required=True)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    spec = importlib.util.spec_from_file_location('scan', args.scanner)
    scan = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scan)
    original = scan.parse_record
    events = []
    current = {}

    def parse(rec, compat, where):
        result = original(rec, compat, where)
        if result is None:
            return result
        thread, seq, block, offset = map(int, re.fullmatch(r't(\d+) s(\d+) b(\d+) o(\d+)', where).groups())
        off = 68 if rec[4] & 4 else 24
        field_offset = 32 if compat >= scan.REDO_VERSION_12_1 else 24
        while off < len(rec):
            fl = off + field_offset
            count = (scan.u16(rec, fl) - 2) // 2
            pos = off + field_offset + ((scan.u16(rec, fl) + 2) & 0xfffc)
            fields = []
            for i in range(1, count + 1):
                size = scan.u16(rec, fl + i * 2)
                fields.append((pos, size))
                pos += (size + 3) & 0xfffc
            op = (rec[off] << 8) | rec[off + 1]
            if op in (0x0502, 0x0504):
                assert fields and fields[0][1] >= (32 if op == 0x0502 else 20)
                first = fields[0][0]
                cls = scan.u16(rec, off + 2)
                assert cls >= 15
                usn, slt, sqn = (cls - 15) // 2, scan.u16(rec, first), scan.u32(rec, first + 4)
                flags = scan.u16(rec, first + 16) if op == 0x0502 else rec[first + 16]
                pdbid = 0
                if op == 0x0502 and len(fields) > 1:
                    fp, size = fields[1] if fields[1][1] == 4 else (fields[2] if len(fields) > 2 else (None, 0))
                    if fp is not None:
                        assert size >= 4
                        pdbid = scan.u32(rec, fp)
                events.append(dict(current, event_id=len(events), op='5.2' if op == 0x0502 else '5.4',
                                   thread=thread, sequence=seq, block=block, offset=offset,
                                   vector_record_offset=off, field1_record_offset=first,
                                   record_scn=scan.rec_scn(rec, 0), subscn=scan.u16(rec, 12),
                                   con_id=scan.u16(rec, off + 24) if field_offset == 32 else 0,
                                   xid='%d.%d.%d' % (usn, slt, sqn), usn=usn, slt=slt, sqn=sqn,
                                   flags=flags, rollback=op == 0x0504 and bool(flags & 4), pdb_id=pdbid,
                                   record_header_hex=rec[:68 if rec[4] & 4 else 24].hex(),
                                   vector_hex=rec[off:pos].hex(), field1_hex=rec[first:first + fields[0][1]].hex(),
                                   record_sha256=hashlib.sha256(rec).hexdigest(), record_hex=rec.hex()))
            off = pos
        return result

    scan.parse_record = parse
    checked = []
    inputs = json.loads(Path(args.input_pin).read_text())['files']
    for entry in inputs:
        p = Path(entry['frozen_copy'])
        digest = hashlib.sha256(p.read_bytes()).hexdigest()
        assert digest == entry['sha256'], str(p)
        current.clear()
        current.update(input_path=str(p), input_sha256=digest)
        header, groups = scan.scan_file(str(p), True)
        loc = {(r['block'], r['offset']): group for group in groups for r in group['records']}
        for ev in events:
            if ev['input_path'] == str(p):
                group = loc[(ev['block'], ev['offset'])]
                ev.update(block_size=header['block_size'], record_file_offset=ev['block'] * header['block_size'] + ev['offset'],
                          lwn_scn=group['lwn_scn_prod'], lwn_timestamp=group['time'])
        checked.append({'path': str(p), 'sha256': digest, 'header': header})
    ordered = sorted(events, key=lambda e: (e['record_scn'], e['subscn'], e['thread'], e['sequence'], e['block'], e['offset'], e['vector_record_offset']))
    begins = {}
    correlations = []
    contradictions = []
    for ev in ordered:
        key = ev['thread'], ev['xid']
        if ev['op'] == '5.2':
            begins[key] = ev
            continue
        begin = begins.get(key)
        pair = {'commit_event': ev['event_id'], 'begin_event': begin['event_id'] if begin else None,
                'thread': ev['thread'], 'xid': ev['xid'], 'commit_con_id': ev['con_id'],
                'begin_con_id': begin['con_id'] if begin else None, 'begin_pdb_id': begin['pdb_id'] if begin else None,
                'record_scn': ev['record_scn'], 'rollback': ev['rollback'], 'after_cut': ev['record_scn'] > args.cut}
        correlations.append(pair)
        if begin and begin['sqn'] != 0 and begin['pdb_id'] == args.pdb_id and ev['con_id'] != args.con_id:
            contradictions.append(pair)
    commits = [ev for ev in events if ev['op'] == '5.4']
    post = [ev for ev in commits if ev['record_scn'] > args.cut and ev['sqn'] != 0 and not ev['rollback']]
    summary = {'scope': 'Raw archived BEGIN/COMMIT decode and correlation only; product guard execution is separate',
               'cut': args.cut, 'expected_con_id': args.con_id, 'qualified_pdb_id': args.pdb_id,
               'scanner_sha256': hashlib.sha256(Path(args.scanner).read_bytes()).hexdigest(),
               'input_pin_sha256': hashlib.sha256(Path(args.input_pin).read_bytes()).hexdigest(),
               'input_files': checked, 'begin_count': len(events) - len(commits), 'commit_count': len(commits),
               'rollback_count': sum(ev['rollback'] for ev in commits),
               'commits_by_con_id': {str(c): sum(ev['con_id'] == c for ev in commits) for c in sorted(set(ev['con_id'] for ev in commits))},
               'post_cut_nonrollback_by_con_id': {str(c): sum(ev['con_id'] == c for ev in post) for c in sorted(set(ev['con_id'] for ev in post))},
               'post_cut_commit_event_ids': [ev['event_id'] for ev in post],
               'qualified_pdb_commit_container_contradictions': contradictions}
    (out / 'events.json').write_text(json.dumps(events, indent=2) + '\n')
    (out / 'correlations.json').write_text(json.dumps(correlations, indent=2) + '\n')
    (out / 'SUMMARY.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({k: v for k, v in summary.items() if k != 'input_files'}))
    assert not contradictions, 'STOP: qualified PDB COMMIT container contradicts BEGIN identity'


if __name__ == '__main__':
    main()
