#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="${1:-$ROOT/FROZEN-SHA256.txt}"
(
  cd "$ROOT"
  find . -type f \
    ! -path './evidence/*' \
    ! -path './__pycache__/*' \
    ! -path './vendor/*' \
    ! -name 'FROZEN-SHA256.txt' \
    ! -name '*.pyc' \
    -print0 | sort -z | xargs -0 sha256sum
) >"$OUT"
echo "wrote $OUT"
