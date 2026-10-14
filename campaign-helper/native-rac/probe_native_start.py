#!/usr/bin/env python3
"""Bounded P9 wire probe of native START or descriptor resume (not a stock-client acceptance substitute).

Usage: python3 probe_native_start.py --binary PATH --config PRIVATE_JSON --out NEW_DIR --scn N
Use --continue-idx N with a retained descriptor for an explicit durable-cursor resume.
Only the spawned OLR is stopped. Database state, schema, source data and Kafka are not changed.
"""
import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import re
import signal
import socket
import struct
import subprocess
import time


def varint(value):
    out = bytearray()
    while value > 127:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def field(tag, value):
    return varint(tag << 3) + varint(value)


def request(code, database, scn=None, idx=None):
    name = database.encode()
    out = field(1, code) + varint(18) + varint(len(name)) + name
    if scn is not None:
        out += field(8 if code == 2 and idx is not None else 3, scn)
    if idx is not None:
        out += field(9, idx)
    return out


def exact(sock, size):
    out = b''
    while len(out) < size:
        data = sock.recv(size - len(out))
        if not data:
            raise EOFError()
        out += data
    return out


def frame(sock):
    return exact(sock, struct.unpack('<I', exact(sock, 4))[0])


def send(sock, data):
    sock.sendall(struct.pack('<I', len(data)) + data)


def error_lines(text):
    out = []
    for ln in text.splitlines():
        if ' ERROR ' in ln:
            out.append(ln)
    return out


def owned_shutdown_errors(log_path, owned_stop_utc):
    """Split ERROR lines at owned SIGINT using log prefix timestamps vs owned_stop_utc."""
    if not log_path.is_file():
        return [], []
    text = log_path.read_text(errors='replace')
    before, after = [], []
    stop = owned_stop_utc.replace(tzinfo=timezone.utc)
    ts_re = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
    for ln in error_lines(text):
        m = ts_re.match(ln)
        if not m:
            before.append(ln)
            continue
        try:
            ts = datetime.strptime(m.group(1), '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
        except ValueError:
            before.append(ln)
            continue
        (after if ts >= stop else before).append(ln)
    return before, after


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--config', required=True)
    parser.add_argument('--out', required=True)
    parser.add_argument('--scn', required=True, type=int)
    parser.add_argument('--continue-idx', type=int)
    parser.add_argument('--ack-last', action='store_true')
    parser.add_argument('--rows', type=int, default=100000)
    parser.add_argument('--idle-s', type=float, default=5)
    args = parser.parse_args()
    root = Path(args.out).resolve()
    root.mkdir(parents=True, exist_ok=False)
    config = Path(args.config).resolve()
    cfg = json.loads(config.read_text())
    database = cfg['source'][0]['name']
    host, port = cfg['target'][0]['writer']['uri'].rsplit(':', 1)
    with socket.socket() as free:
        free.bind((host, int(port)))
    log = (root / 'olr.log').open('wb')
    process = subprocess.Popen([args.binary, '-f', str(config)], cwd=str(config.parent), stdout=log, stderr=subprocess.STDOUT)
    record = {'binary_sha256': hashlib.sha256(Path(args.binary).read_bytes()).hexdigest(), 'requested_scn': args.scn,
              'requested_idx': args.continue_idx, 'scope': 'bounded native wire/schema probe; not stock Debezium or DATA-PASS'}
    sock = None
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                sock = socket.create_connection((host, int(port)), timeout=2)
                break
            except OSError:
                if time.monotonic() > deadline or process.poll() is not None:
                    raise
                time.sleep(.1)
        sock.settimeout(cfg['source'][0]['reader'].get('startup-timeout-s', 300) + 15)
        start = time.monotonic()
        send(sock, request(0, database))
        try:
            info = frame(sock)
            record['info_hex'] = info.hex()
            code = 2 if info.startswith(b'\x08\x04') else 1
            send(sock, request(code, database, args.scn, args.continue_idx))
            response = frame(sock)
            record['response_hex'] = response.hex()
            record['replicate'] = response.startswith(b'\x08\x04')
        except EOFError:
            record['replicate'] = False
            record['response'] = 'CLOSED'
        record['startup_wait_s'] = time.monotonic() - start
        rows = []
        if record['replicate']:
            sock.settimeout(args.idle_s)
            while len(rows) < args.rows:
                try:
                    raw = frame(sock)
                    rows.append(json.loads(raw))
                except (socket.timeout, EOFError):
                    break
            if rows and args.ack_last:
                last = rows[-1]
                ack = request(3, database) + field(8, int(last['c_scn'])) + field(9, int(last['c_idx']))
                send(sock, ack)
                time.sleep(.3)
                record['ack_sent'] = {'c_scn': last['c_scn'], 'c_idx': last['c_idx']}
        (root / 'events.jsonl').write_text(''.join(json.dumps(row, separators=(',', ':')) + '\n' for row in rows))
        record['raw_events'] = len(rows)
    finally:
        if sock:
            sock.close()
        owned_stop_utc = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        record['owned_stop_utc'] = owned_stop_utc
        record['owned_stop_event'] = 'SIGINT_to_owned_OLR_after_probe_idle'
        log.flush()
        log_path = root / 'olr.log'
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
        try:
            record['owned_process_exit'] = process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            record['owned_process_exit'] = process.wait()
            record['forced_stop'] = True
        log.close()
        before, after = owned_shutdown_errors(log_path, owned_stop_utc)
        record['errors_before_owned_stop'] = before
        record['errors_after_owned_stop'] = after
        state = Path(cfg['source'][0]['state']['path'])
        record['state_files'] = sorted(p.name for p in state.glob('*.json'))
        (root / 'RESULTS.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps(record))


if __name__ == '__main__':
    main()
