#!/usr/bin/env python3
"""Flink 1.20 process identity: Java exe + first JVM main class + exact --configDir."""
from __future__ import annotations

import os
import sys
from pathlib import Path

JM_CLASS = "org.apache.flink.runtime.entrypoint.StandaloneSessionClusterEntrypoint"
TM_CLASS = "org.apache.flink.runtime.taskexecutor.TaskManagerRunner"

ROLE_CLASS = {
    "standalonesession": JM_CLASS,
    "taskexecutor": TM_CLASS,
}

JVM_SKIP_NEXT = frozenset({"-cp", "-classpath", "--classpath"})


def read_proc_exe(pid: int) -> str:
    try:
        return os.readlink(f"/proc/{pid}/exe")
    except OSError as e:
        raise ValueError(f"cannot read /proc/{pid}/exe: {e}") from e


def read_proc_cmdline(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError as e:
        raise ValueError(f"cannot read /proc/{pid}/cmdline: {e}") from e
    if not raw:
        return []
    return [p.decode(errors="replace") for p in raw.split(b"\0") if p]


def normalize_dir(path: str) -> str:
    return str(Path(path).resolve())


def parse_config_dir(argv: list[str]) -> str | None:
    for i, tok in enumerate(argv):
        if tok.startswith("--configDir="):
            return tok.split("=", 1)[1]
        if tok == "--configDir" and i + 1 < len(argv):
            return argv[i + 1]
    return None


def first_jvm_main_class(argv: list[str]) -> str | None:
    """First application main-class token after java binary and JVM-only options."""
    if not argv:
        return None
    i = 1
    while i < len(argv):
        tok = argv[i]
        if tok.startswith("-") and tok != "--":
            if tok in JVM_SKIP_NEXT and i + 1 < len(argv):
                i += 2
                continue
            if (
                tok.startswith("-D")
                or tok.startswith("-X")
                or tok.startswith("-XX:")
                or tok.startswith("-javaagent:")
            ):
                i += 1
                continue
            return None
        return tok
    return None


def verify_java_process(
    *,
    exe: str,
    argv: list[str],
    role: str,
    expected_conf_dir: str,
) -> None:
    if role not in ROLE_CLASS:
        raise ValueError(f"unknown role {role}")
    exe_base = Path(exe).name
    if "java" not in exe_base.lower() and not exe.rstrip("/").endswith("/java"):
        raise ValueError(f"exe is not Java: {exe}")

    expected_class = ROLE_CLASS[role]
    main = first_jvm_main_class(argv)
    if main is None:
        raise ValueError("could not determine JVM main class")
    if main != expected_class:
        raise ValueError(f"main class {main!r} != required {expected_class!r}")

    for i, tok in enumerate(argv):
        if tok in ("-cp", "-classpath", "--classpath") and i + 1 < len(argv):
            if expected_class in argv[i + 1]:
                raise ValueError("Flink class appears inside classpath")

    conf = parse_config_dir(argv)
    if not conf:
        raise ValueError("missing --configDir in argv")
    if normalize_dir(conf) != normalize_dir(expected_conf_dir):
        raise ValueError(f"--configDir {conf!r} != expected {expected_conf_dir!r}")


def verify_live_pid(pid: int, role: str, expected_conf_dir: str) -> None:
    if not Path(f"/proc/{pid}").is_dir():
        raise ValueError(f"stale pid {pid}")
    exe = read_proc_exe(pid)
    argv = read_proc_cmdline(pid)
    verify_java_process(exe=exe, argv=argv, role=role, expected_conf_dir=expected_conf_dir)


def run_selftest() -> int:
    import json

    conf = "/tmp/flink-aux-smoke-demo/conf"
    p9_jm_argv = [
        "/usr/lib/jvm/java-11/bin/java",
        "-Xmx1600m",
        "-cp",
        "/opt/flink/lib/flink-dist-1.20.5.jar:/opt/flink/lib/*",
        JM_CLASS,
        "-D",
        "log.file=/tmp/jm.log",
        "--configDir",
        conf,
    ]
    p9_tm_argv = [
        "/usr/lib/jvm/java-11/bin/java",
        "-cp",
        "/opt/flink/lib/flink-dist-1.20.5.jar",
        TM_CLASS,
        "--configDir",
        conf,
    ]
    cases: list[dict] = []

    def ok(name: str, fn):
        try:
            fn()
            cases.append({"case": name, "expected": "pass", "got": "pass"})
        except ValueError as e:
            cases.append({"case": name, "expected": "pass", "got": "fail", "error": str(e)})

    def fail(name: str, fn):
        try:
            fn()
            cases.append({"case": name, "expected": "fail", "got": "pass"})
        except ValueError:
            cases.append({"case": name, "expected": "fail", "got": "fail"})

    ok(
        "p9_fixture_jm",
        lambda: verify_java_process(
            exe="/usr/lib/jvm/java-11/bin/java", argv=p9_jm_argv, role="standalonesession", expected_conf_dir=conf
        ),
    )
    ok(
        "p9_fixture_tm",
        lambda: verify_java_process(
            exe="/usr/lib/jvm/java-11/bin/java", argv=p9_tm_argv, role="taskexecutor", expected_conf_dir=conf
        ),
    )
    fail(
        "other_main_before_flink_class",
        lambda: verify_java_process(
            exe="/usr/lib/jvm/java-11/bin/java",
            argv=["java", "-cp", "x.jar", "other.Main", JM_CLASS, "--configDir", conf],
            role="standalonesession",
            expected_conf_dir=conf,
        ),
    )
    fail(
        "bash_sleep_spoof",
        lambda: verify_java_process(
            exe="/usr/bin/bash",
            argv=["bash", "-c", "sleep", "600", "--configDir", conf],
            role="standalonesession",
            expected_conf_dir=conf,
        ),
    )
    fail(
        "class_in_cp_only",
        lambda: verify_java_process(
            exe="/usr/lib/jvm/java-11/bin/java",
            argv=["java", "-cp", f"foo.jar:{JM_CLASS}", "other.Main", "--configDir", conf],
            role="standalonesession",
            expected_conf_dir=conf,
        ),
    )

    main_fx = Path(__file__).resolve().parent.parent / "tests/fixtures/main20261012T054241Z_java_argv.json"
    if main_fx.is_file():
        import json as _json

        blob = _json.loads(main_fx.read_text())
        main_conf = blob["jm"]["conf"]
        ok(
            "main_ps_jm_802602_classpath",
            lambda: verify_java_process(
                exe="/usr/bin/java",
                argv=blob["jm"]["argv"],
                role="standalonesession",
                expected_conf_dir=main_conf,
            ),
        )
        ok(
            "main_ps_tm_803121_classpath",
            lambda: verify_java_process(
                exe="/usr/bin/java",
                argv=blob["tm"]["argv"],
                role="taskexecutor",
                expected_conf_dir=main_conf,
            ),
        )

    all_ok = all(c["expected"] == c["got"] for c in cases)
    print(json.dumps({"flink_identity_parser_selftest": all_ok, "cases": cases}, indent=2))
    return 0 if all_ok else 1


def _emit_ok_pid(pid_s: str, role: str) -> None:
    print(f"PID_OK pid={pid_s} role={role}", file=sys.stderr)
    sys.stdout.write(pid_s)


def main() -> None:
    if len(sys.argv) >= 2 and sys.argv[1] == "--selftest":
        raise SystemExit(run_selftest())
    if sys.argv[1] == "verify-live":
        _, _cmd, pid_s, role, conf = sys.argv
        verify_live_pid(int(pid_s), role, conf)
        _emit_ok_pid(pid_s, role)
        return
    if sys.argv[1] == "verify-fixture":
        import json as _json

        _, _cmd, pid_s, role, conf, exe, argv_json = sys.argv
        argv = _json.loads(argv_json)
        verify_java_process(exe=exe, argv=argv, role=role, expected_conf_dir=conf)
        _emit_ok_pid(pid_s, role)
        return
    raise SystemExit(
        "usage: flink_identity_parser.py verify-live|verify-fixture ... | --selftest"
    )


if __name__ == "__main__":
    main()
