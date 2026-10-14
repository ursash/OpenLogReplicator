#!/usr/bin/env python3
"""Link the isolated COMMIT-scope fixture to an existing P9 production build.

Usage: --source-root BUILD_SOURCE --fixture ParserCommitScopeTest.cpp --out NEW_DIR
The product build is read-only. Exact linked objects, commands and fixture are pinned.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--source-root', required=True)
    ap.add_argument('--fixture', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    source, fixture, out = Path(args.source_root), Path(args.fixture), Path(args.out)
    out.mkdir(parents=True, exist_ok=False)
    build = source / 'cmake-build-release'
    flags = {}
    for line in (build / 'CMakeFiles/OpenLogReplicator.dir/flags.make').read_text().splitlines():
        if line.startswith(('CXX_DEFINES =', 'CXX_INCLUDES =', 'CXX_FLAGS =')):
            key, value = line.split(' = ', 1)
            flags[key] = shlex.split(value)
    original = shlex.split((build / 'CMakeFiles/OpenLogReplicator.dir/link.txt').read_text())
    compile_cmd = [original[0]] + flags['CXX_DEFINES'] + flags['CXX_INCLUDES'] + flags['CXX_FLAGS']
    compile_cmd += ['-I' + str(source), '-c', str(fixture), '-o', str(out / 'fixture.o')]
    omitted = {'CMakeFiles/OpenLogReplicator.dir/src/OpenLogReplicator.cpp.o', 'CMakeFiles/OpenLogReplicator.dir/src/main.cpp.o'}
    linked = []
    link_cmd = [original[0], str(out / 'fixture.o')]
    target_next = False
    for token in original[1:]:
        if token in omitted:
            continue
        if target_next:
            link_cmd.append(str(out / 'ParserCommitScopeTest'))
            target_next = False
        elif token == '-o':
            link_cmd.append(token)
            target_next = True
        elif token.endswith(('.o', '.a')):
            path = Path(token) if Path(token).is_absolute() else build / token
            linked.append(path)
            link_cmd.append(str(path))
        else:
            link_cmd.append(token)
    receipt = {'source_root': str(source), 'fixture': str(fixture), 'fixture_sha256': digest(fixture),
               'production_binary_sha256': digest(build / 'OpenLogReplicator'),
               'linked_production_objects': {str(path): digest(path) for path in linked},
               'commands': {'compile': compile_cmd, 'link': link_cmd}, 'results': []}
    for phase, cmd in [('compile', compile_cmd), ('link', link_cmd)]:
        run = subprocess.run(cmd, cwd=str(out), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=120)
        (out / (phase + '.log')).write_bytes(run.stdout)
        receipt['results'].append({'phase': phase, 'exit': run.returncode})
        (out / 'BUILD.json').write_text(json.dumps(receipt, indent=2) + '\n')
        if run.returncode:
            raise RuntimeError(phase + ' failed; inspect retained log')
    receipt['fixture_binary_sha256'] = digest(out / 'ParserCommitScopeTest')
    (out / 'BUILD.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps({'binary': str(out / 'ParserCommitScopeTest'), 'sha256': receipt['fixture_binary_sha256'], 'linked_object_count': len(linked)}))


if __name__ == '__main__':
    main()
