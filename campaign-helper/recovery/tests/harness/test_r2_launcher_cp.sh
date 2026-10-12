#!/usr/bin/env bash
# R2 ConnectDistributed relaunch must pass worker-lib/* as one -cp argument (Java expands wildcard).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PHASE="$ROOT/bin/recovery_phase.sh"
PREP=$(mktemp -d)
mkdir -p "$PREP/worker-lib"
touch "$PREP/worker-lib/connect-api.jar" "$PREP/worker-lib/connect-runtime.jar" "$PREP/worker-lib/kafka-clients.jar"

if rg -q 'java -cp "\$PREP/worker-lib/\*"' "$PHASE"; then
  echo "ok r2_cp_source_pattern"
else
  echo "FAIL r2_cp_source_pattern: expected quoted worker-lib/* in recovery_phase.sh"
  exit 1
fi

if rg -q 'java -cp "\$PREP/worker-lib"/\*' "$PHASE"; then
  echo "FAIL r2_cp_no_shell_glob: found shell-expanded worker-lib\"/* form"
  exit 1
fi

# Shell glob outside quotes yields one argv per jar (broken for java -cp).
set -- java -cp "$PREP/worker-lib"/*
wrong=$#
set -- java -cp "$PREP/worker-lib/*"
right=$#

if [ "$wrong" -lt 5 ]; then
  echo "FAIL r2_cp_wrong_argv_count: glob-outside gave $wrong args (need multiple jars in fixture)"
  exit 1
fi
if [ "$right" -ne 3 ]; then
  echo "FAIL r2_cp_right_argv_count: want 3 got $right (java -cp 'path/*' MainClass)"
  exit 1
fi
if [ "${3:-}" != "$PREP/worker-lib/*" ]; then
  echo "FAIL r2_cp_wildcard_literal: classpath arg=${3:-}"
  exit 1
fi

echo "R2_LAUNCHER_CP_TEST=PASS"
