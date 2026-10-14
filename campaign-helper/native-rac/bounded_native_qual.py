#!/usr/bin/env python3
"""Bounded P9 native qualification: preflight AS OF, rac-init, START/schema, row checks, no-ACK resume.

Run on DG1. Secrets JSON is read in-process; passwords never appear in argv, logs or MANIFEST.
"""
import argparse
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

CONFIG_TEMPLATE = {
    "version": "1.9.0",
    "source": [{
        "alias": "NATIVE",
        "name": "ORCLRAC",
        "flags": 69728,
        "memory": {"min-mb": 64, "max-mb": 512},
        "reader": {
            "type": "rac-online",
            "user": "C##DBZUSER",
            "server": "//10.129.81.234:1521/orclpdb1",
            "startup-timeout-s": 300,
        },
        "format": {
            "type": "json",
            "message": 2,
            "xid": 0,
            "db": 3,
            "timestamp": 0,
            "timestamp-tz": 0,
            "timestamp-all": 1,
            "interval-dts": 9,
            "interval-ytm": 4,
            "char": 0,
            "scn": 0,
            "scn-type": 1,
            "rid": 1,
            "unknown": 0,
            "schema": 7,
            "column": 2,
            "unknown-type": 0,
            "flush-buffer": 1048576,
        },
        "filter": {"table": [{"owner": "OLR_RAC_TEST", "table": "EV|PP"}]},
    }],
    "target": [{"alias": "NATIVE-OUT", "source": "NATIVE", "writer": {"type": "network", "uri": "127.0.0.1:0"}}],
}

M4_BINARY_SHA = "3d37b0038e3980981dbc8c26637987ff682962ab2c58288f3be26c88f58f653b"
ORACLE_HOME = os.environ.get("ORACLE_HOME", "/u01/app/oracle/product/19.3.0/dbhome_1")
SQLPLUS = os.environ.get("P9_SQLPLUS", os.path.join(ORACLE_HOME, "bin", "sqlplus"))
CONNECT = "//10.129.81.234:1521/orclpdb1"
SYS15 = (
    "CCOL$", "CDEF$", "COL$", "DEFERRED_STG$", "ECOL$", "LOB$", "LOBCOMPPART$", "LOBFRAG$",
    "OBJ$", "TAB$", "TABCOMPART$", "TABPART$", "TABSUBPART$", "TS$", "USER$",
)
BOOTSTRAP7 = ("TAB$", "TS$", "OBJ$", "COL$", "USER$", "CDEF$", "CCOL$")


def oracle_env():
    env = os.environ.copy()
    env["ORACLE_HOME"] = ORACLE_HOME
    env["LD_LIBRARY_PATH"] = ":".join([
        os.path.join(ORACLE_HOME, "lib"),
        "/opt/openlogreplicator/lib",
        "/opt/openlogreplicator/deps/protobuf/lib64",
    ])
    env["NLS_LANG"] = ".AL32UTF8"
    return env


def sha256_file(path):
    h = hashlib.sha256()
    with open(str(path), "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def load_password(secrets):
    data = json.loads(Path(secrets).read_text())
    if "db_cdc_password" in data:
        return data["db_cdc_password"]
    raise SystemExit("secrets JSON missing db_cdc_password")


def sqlplus_script(password, statements):
    """Run SQL as C##DBZUSER via sqlplus /nolog; CONNECT only on private stdin (not argv)."""
    header = (
        "WHENEVER SQLERROR EXIT FAILURE\n"
        "SET HEADING OFF FEEDBACK OFF PAGESIZE 0 VERIFY OFF ECHO OFF\n"
    )
    connect = "CONNECT C##DBZUSER/{pw}@{conn}\n".format(pw=password, conn=CONNECT)
    body = header + statements + "\nEXIT\n"
    proc = subprocess.Popen(
        [SQLPLUS, "-S", "-L", "/nolog"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=oracle_env(),
    )
    out, err = proc.communicate(input=(connect + body).encode("utf-8"))
    text = out.decode("utf-8", "replace").strip()
    err_text = err.decode("utf-8", "replace").strip()
    if proc.returncode != 0 or "ORA-" in text or "ORA-" in err_text:
        raise RuntimeError((err_text or text or "sqlplus failed")[:500])
    return text


def cleanup_owned(secrets_path, attempt_root):
    sp = Path(secrets_path)
    if sp.is_file():
        try:
            sp.unlink()
        except OSError:
            pass
    for pattern in ("config.private.json", "online-smoke/config.private.json"):
        for p in Path(attempt_root).glob("**/" + pattern.split("/")[-1]):
            if p.is_file():
                try:
                    p.unlink()
                except OSError:
                    pass


def dictionary_fifteen(password, scn):
    tables = {}
    for name in SYS15:
        key = "sys_" + name.replace("$", "_")
        try:
            val = int(sqlplus_script(password, "SELECT COUNT(*) FROM SYS.{n};\n".format(n=name)).split()[-1])
            tables[key] = {"ok": True, "value": val, "bootstrap7": name in BOOTSTRAP7}
        except Exception as exc:
            tables[key] = {"ok": False, "error": str(exc)[:300], "bootstrap7": name in BOOTSTRAP7}
    asof = {}
    for label, tbl in (("ts_asof", "TS$"), ("obj_asof", "OBJ$"), ("tab_asof", "TAB$")):
        try:
            sql = "SELECT COUNT(*) FROM SYS.{t} AS OF SCN {s};\n".format(t=tbl, s=scn)
            val = int(sqlplus_script(password, sql).split()[-1])
            asof[label] = {"ok": True, "value": val}
        except Exception as exc:
            asof[label] = {"ok": False, "error": str(exc)[:300]}
    try:
        ident = sqlplus_script(
            password,
            "SELECT SYS_CONTEXT('USERENV','CON_ID')||'|'||SYS_CONTEXT('USERENV','CON_NAME')||'|'||"
            "SYS_CONTEXT('USERENV','SESSION_USER') FROM DUAL;\n",
        ).splitlines()[0].strip()
    except Exception as exc:
        ident = {"error": str(exc)[:200]}
    plain_ok = sum(1 for v in tables.values() if v.get("ok"))
    return {
        "session_identity": ident,
        "sys15_plain_ok_count": plain_ok,
        "sys15_plain_required": 15,
        "bootstrap7": {k: v for k, v in tables.items() if v.get("bootstrap7")},
        "non_bootstrap8": {k: v for k, v in tables.items() if not v.get("bootstrap7")},
        "tables": tables,
        "asof": asof,
        "mandatory_pass": plain_ok == 15 and all(v.get("ok") for v in asof.values()),
    }


def mandatory_preflight_pass(pre, dict_audit):
    if not dict_audit.get("mandatory_pass"):
        return False
    if not pre.get("v_thread", {}).get("ok") or not pre.get("v_containers", {}).get("ok"):
        return False
    if not pre.get("applied_scn", {}).get("start_scn_lte_applied"):
        return False
    th = pre.get("threads_closed_non_srl_at_scn", {})
    return isinstance(th, dict) and th.get(1, 0) > 0 and th.get(2, 0) > 0


def preflight(password, scn, out):
    checks = {}
    for label, sql in (
        ("ts_plain", "SELECT COUNT(*) FROM SYS.TS$;\n"),
        ("ts_asof", "SELECT COUNT(*) FROM SYS.TS$ AS OF SCN {scn};\n".format(scn=scn)),
        ("obj_asof", "SELECT COUNT(*) FROM SYS.OBJ$ AS OF SCN {scn} WHERE ROWNUM <= 1;\n".format(scn=scn)),
        ("tab_asof", "SELECT COUNT(*) FROM SYS.TAB$ AS OF SCN {scn} WHERE ROWNUM <= 1;\n".format(scn=scn)),
        ("v_thread", "SELECT COUNT(*) FROM SYS.V_$THREAD WHERE ROWNUM <= 1;\n"),
        ("v_containers", "SELECT COUNT(*) FROM SYS.V_$CONTAINERS WHERE ROWNUM <= 1;\n"),
    ):
        try:
            val = int(sqlplus_script(password, sql).split()[-1])
            checks[label] = {"ok": True, "value": val}
        except Exception as exc:
            checks[label] = {"ok": False, "error": str(exc)[:300]}
    try:
        applied = int(sqlplus_script(password, "SELECT CURRENT_SCN FROM SYS.V_$DATABASE;\n").split()[-1])
        checks["applied_scn"] = {"ok": True, "value": applied, "start_scn_lte_applied": scn <= applied}
    except Exception as exc:
        checks["applied_scn"] = {"ok": False, "error": str(exc)[:300]}
    arch_sql = (
        "SELECT MIN(FIRST_CHANGE#)||':'||MAX(NEXT_CHANGE#) FROM SYS.V_$ARCHIVED_LOG "
        "WHERE STATUS='A' AND DELETED='NO' AND THREAD# IN (1,2) "
        "AND FIRST_CHANGE# <= {scn} AND NEXT_CHANGE# > {scn};\n".format(scn=scn)
    )
    try:
        pair = sqlplus_script(password, arch_sql).split(":")[-2:]
        checks["arch_window"] = {"first_min": int(pair[0]), "next_max": int(pair[1])}
    except Exception as exc:
        checks["arch_window"] = {"error": str(exc)[:300]}
    th_sql = (
        "SELECT THREAD#||':'||COUNT(*) FROM SYS.V_$ARCHIVED_LOG "
        "WHERE STATUS='A' AND DELETED='NO' AND STANDBY_DEST='NO' AND FIRST_CHANGE# <= {scn} AND NEXT_CHANGE# > {scn} "
        "GROUP BY THREAD# ORDER BY 1;\n".format(scn=scn)
    )
    threads = {}
    try:
        for line in sqlplus_script(password, th_sql).splitlines():
            if ":" in line:
                t, c = line.strip().split(":", 1)
                threads[int(t)] = int(c)
    except Exception as exc:
        threads = {"error": str(exc)[:300]}
    checks["threads_closed_non_srl_at_scn"] = threads
    cut_plan = out / "cut-plan.json"
    if cut_plan.is_file():
        checks["cut_plan"] = json.loads(cut_plan.read_text())
    (out / "preflight-oracle.json").write_text(json.dumps(checks, indent=2) + "\n")
    return checks


def write_config(root, password, port):
    cfg = copy.deepcopy(CONFIG_TEMPLATE)
    cfg["source"][0]["reader"]["password"] = password
    cfg["source"][0]["state"] = {"type": "disk", "path": str(root / "state")}
    cfg["target"][0]["writer"]["uri"] = "127.0.0.1:{0}".format(port)
    path = root / "config.private.json"
    path.write_text(json.dumps(cfg))
    os.chmod(str(path), 0o600)
    return path


def run_cmd(cmd, cwd, log_name, root, timeout=600):
    log_path = root / log_name
    with open(str(log_path), "wb") as log:
        try:
            return subprocess.run(cmd, cwd=str(cwd), stdout=log, stderr=subprocess.STDOUT, timeout=timeout,
                                  check=False, env=oracle_env())
        except subprocess.TimeoutExpired:
            log.write(b"\n[bounded_native_qual: timeout, sending SIGINT]\n")
            return subprocess.CompletedProcess(cmd, 124)


def state_manifest(state):
    if not state.is_dir():
        return {"missing": True}
    out = {}
    for p in sorted(state.glob("*")):
        if p.is_file():
            size = p.stat().st_size
            out[p.name] = {"size": size, "sha256": sha256_file(p) if size <= 32000000 else "large"}
    return out


def parse_olr_log(path):
    text = path.read_text(errors="replace") if path.is_file() else ""
    startup_s = None
    m = re.search(r"client requested to start from scn: (\d+)", text)
    if m:
        t0 = None
        for ln in text.splitlines():
            if "client requested to start from scn" in ln:
                t0 = ln[:19]
            if t0 and "Replicator for" in ln and "shutting down" in ln:
                startup_s = {"start_scn": int(m.group(1)), "fatal_shutdown": True}
                break
        if "native RAC schema and bootstrap lineage published" in text:
            startup_s = startup_s or {}
            startup_s["published"] = True
    return {
        "published": "native RAC schema and bootstrap lineage published" in text,
        "errors": re.findall(r"ERROR \d+ .*", text),
        "last_info": [ln for ln in text.splitlines() if " INFO " in ln][-8:],
        "startup": startup_s,
    }


def fetch_row(password, table, row_id):
    sql = "SELECT ID, INST, TAG, VAL, PAD FROM OLR_RAC_TEST.{tbl} WHERE ID = {id};\n".format(tbl=table, id=row_id)
    try:
        line = sqlplus_script(password, sql).splitlines()[0].strip()
        if not line:
            return None
        parts = [p.strip() for p in line.split()]
        if len(parts) < 5:
            return None
        return {"ID": int(parts[0]), "INST": int(parts[1]) if parts[1] != "" else None,
                "TAG": parts[2], "VAL": int(parts[3]), "PAD": parts[4]}
    except Exception:
        return "error"


def business_rows(password, events_path):
    if not events_path.is_file():
        return {"checked": 0, "mismatch": [], "note": "no events file"}
    checked = 0
    mismatch = []
    threads_seen = set()
    for line in events_path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        for part in row.get("payload") or []:
            op = part.get("op")
            if op not in ("c", "u", "d"):
                continue
            schema = part.get("schema") or {}
            owner = schema.get("owner") or part.get("owner")
            name = schema.get("table") or part.get("table")
            if owner != "OLR_RAC_TEST" or name not in ("EV", "PP"):
                continue
            after = part.get("after") or {}
            before = part.get("before") or {}
            key_id = after.get("ID") or before.get("ID")
            if key_id is None:
                continue
            inst = after.get("INST") or before.get("INST")
            if inst is not None:
                try:
                    threads_seen.add(int(inst))
                except (TypeError, ValueError):
                    pass
            db_row = fetch_row(password, name, key_id)
            checked += 1
            if op == "d":
                if db_row not in (None, "error") and db_row is not None:
                    mismatch.append({"id": key_id, "table": name, "op": "d", "detail": "row still present", "db": db_row})
                continue
            if db_row in (None, "error"):
                mismatch.append({"id": key_id, "table": name, "op": op, "detail": "oracle lookup failed or missing", "db": db_row})
                continue
            for col in ("INST", "TAG", "VAL", "PAD"):
                if col in after and after[col] != db_row.get(col):
                    mismatch.append({"id": key_id, "table": name, "col": col, "event": after[col], "db": db_row.get(col)})
    return {"checked": checked, "mismatch": mismatch, "threads_seen": sorted(threads_seen)}


def run_online_smoke(binary, password, root, port):
    """Bounded stock online reader attach only; does not replace native qualification."""
    smoke_root = root / "online-smoke"
    smoke_root.mkdir(exist_ok=True)
    cfg = copy.deepcopy(CONFIG_TEMPLATE)
    cfg["source"][0]["reader"] = {
        "type": "online",
        "user": "C##DBZUSER",
        "password": password,
        "server": "//10.129.81.234:1521/orclpdb1",
    }
    cfg["source"][0]["state"] = {"type": "disk", "path": str(smoke_root / "state")}
    cfg["target"][0]["writer"]["uri"] = "127.0.0.1:{0}".format(port)
    (smoke_root / "state").mkdir(exist_ok=True)
    path = smoke_root / "config.private.json"
    path.write_text(json.dumps(cfg))
    os.chmod(str(path), 0o600)
    t0 = time.monotonic()
    proc = run_cmd([str(binary), "-f", str(path)], smoke_root, "olr.log", smoke_root, timeout=12)
    olr = parse_olr_log(smoke_root / "olr.log")
    return {
        "scope": "stock online reader smoke only",
        "elapsed_s": time.monotonic() - t0,
        "process_exit": proc.returncode,
        "olr": olr,
        "acceptance": "INVALID_FOR_POSITIVE_ONLINE_ACCEPTANCE",
        "note": "no client/metadata/rows; not a regression PASS",
    }


def run_probe(probe, binary, config, out, scn, extra):
    cmd = [sys.executable, str(probe), "--binary", str(binary), "--config", str(config), "--out", str(out),
           "--scn", str(scn), "--rows", "50000", "--idle-s", "10"] + extra
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, timeout=900,
                          env=oracle_env())
    (out / "probe-runner.log").write_text(proc.stdout)
    result_path = out / "RESULTS.json"
    if result_path.is_file():
        return json.loads(result_path.read_text())
    return {"probe_error": proc.stdout[-2000:]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--attempt-dir", required=True)
    parser.add_argument("--binary", required=True)
    parser.add_argument("--secrets", required=True)
    parser.add_argument("--probe-script", required=True)
    parser.add_argument("--start-scn", type=int, required=True)
    parser.add_argument("--port", type=int, default=39520)
    parser.add_argument("--dictionary-audit-only", action="store_true")
    parser.add_argument("--wire-start-only", action="store_true", default=True)
    parser.add_argument("--skip-online-smoke", action="store_true", default=True)
    args = parser.parse_args()

    root = Path(args.attempt_dir).resolve()
    secrets_path = Path(args.secrets).resolve()
    exit_code = 1
    try:
        if (root / "MANIFEST.json").exists():
            raise SystemExit("refusing to reuse completed attempt: {0}".format(root))
        root.mkdir(parents=True, exist_ok=True)
        binary = Path(args.binary).resolve()
        probe = Path(args.probe_script).resolve()
        binary_sha = sha256_file(binary)
        if binary_sha != M4_BINARY_SHA:
            manifest = {
                "verdict": "FAIL_BINARY_PIN",
                "binary_sha256": binary_sha,
                "expected_binary_sha256": M4_BINARY_SHA,
            }
            (root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(json.dumps(manifest))
            return 5

        password = load_password(secrets_path)
        manifest = {
            "scope": "PDB-reopen causal + bounded native wire START; not 60-minute or stock Debezium DATA-PASS",
            "flags": 69728,
            "attempt_dir": str(root),
            "binary_sha256": binary_sha,
            "expected_binary_sha256": M4_BINARY_SHA,
            "start_scn": args.start_scn,
            "port": args.port,
            "phases": {},
        }
        manifest["phases"]["dictionary_audit"] = dictionary_fifteen(password, args.start_scn)
        (root / "dictionary-audit.json").write_text(json.dumps(manifest["phases"]["dictionary_audit"], indent=2) + "\n")

        if args.dictionary_audit_only:
            manifest["verdict"] = "DICTIONARY_AUDIT_ONLY"
            if manifest["phases"]["dictionary_audit"].get("mandatory_pass"):
                manifest["verdict"] = "DICTIONARY_AUDIT_PASS"
            else:
                manifest["verdict"] = "FAIL_DICTIONARY_AUDIT"
            (root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(json.dumps({"verdict": manifest["verdict"]}))
            exit_code = 0 if manifest["verdict"] == "DICTIONARY_AUDIT_PASS" else 2
            return exit_code

        manifest["phases"]["preflight"] = preflight(password, args.start_scn, root)
        if not mandatory_preflight_pass(manifest["phases"]["preflight"], manifest["phases"]["dictionary_audit"]):
            manifest["verdict"] = "FAIL_MANDATORY_PREFLIGHT"
            manifest["primary_blocker"] = "DICTIONARY_OR_ARCH_GATE"
            (root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(json.dumps(manifest))
            exit_code = 2
            return exit_code

        (root / "state").mkdir(parents=True, exist_ok=True)
        config = write_config(root, password, args.port)
        manifest["config_sha256"] = sha256_file(config)

        init = run_cmd([str(binary), "-f", str(config), "--rac-init"], root, "rac-init.log", root, timeout=60)
        manifest["phases"]["rac_init"] = {
            "exit": init.returncode,
            "state": state_manifest(root / "state"),
            "olr": parse_olr_log(root / "rac-init.log"),
        }
        if init.returncode != 0 or not (root / "state" / "ORCLRAC-rac-intent.json").is_file():
            manifest["verdict"] = "FAIL_RAC_INIT"
            (root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(json.dumps(manifest))
            exit_code = 1
            return exit_code

        start_out = root / "phase-start"
        start_res = run_probe(probe, binary, config, start_out, args.start_scn, [])
        manifest["phases"]["start"] = start_res
        manifest["phases"]["start"]["olr"] = parse_olr_log(start_out / "olr.log")
        manifest["phases"]["start"]["state"] = state_manifest(root / "state")

        if not start_res.get("replicate"):
            manifest["verdict"] = "FAIL_START_NO_REPLICATE"
            manifest["false_pass_guards"] = {
                "replicate": False,
                "schema_published": manifest["phases"]["start"]["olr"].get("published"),
                "raw_events": start_res.get("raw_events", 0),
                "process_exit": start_res.get("owned_process_exit"),
                "log_errors": manifest["phases"]["start"]["olr"].get("errors"),
            }
            (root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
            print(json.dumps(manifest))
            exit_code = 3
            return exit_code

        published = manifest["phases"]["start"]["olr"].get("published")
        err_before = start_res.get("errors_before_owned_stop") or []
        err_after = start_res.get("errors_after_owned_stop") or []
        manifest["phases"]["start"]["owned_stop"] = {
            "utc": start_res.get("owned_stop_utc"),
            "event": start_res.get("owned_stop_event"),
            "exit": start_res.get("owned_process_exit"),
        }
        after_ok = not err_after or all(
            "ORA-01013" in e or "signal: 2" in e for e in err_after
        )
        intent_gone = "ORCLRAC-rac-intent.json" not in (start_res.get("state_files") or [])
        has_descriptor = "ORCLRAC-rac-bootstrap.json" in (start_res.get("state_files") or [])
        if err_before:
            manifest["verdict"] = "FAIL_START_LOG_ERRORS_BEFORE_OWNED_STOP"
            manifest["primary_blocker"] = "OLR_ERROR_BEFORE_PROBE_STOP"
            exit_code = 4
        elif not after_ok:
            manifest["verdict"] = "FAIL_START_LOG_ERRORS_AFTER_OWNED_STOP"
            exit_code = 4
        elif published and start_res.get("replicate") and intent_gone and has_descriptor:
            manifest["verdict"] = "BOUNDED_WIRE_START_PASS"
            exit_code = 0
        else:
            manifest["verdict"] = "FAIL_BOUNDED_WIRE_START"
            exit_code = 4
        (root / "MANIFEST.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(json.dumps({"verdict": manifest["verdict"]}))
        return exit_code
    finally:
        cleanup_owned(secrets_path, root)


if __name__ == "__main__":
    sys.exit(main())
