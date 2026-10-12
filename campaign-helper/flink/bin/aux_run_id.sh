#!/usr/bin/env bash
# Strict aux run id → fixed paths under /tmp (no path injection).
set -euo pipefail

AUX_RUN_ID_REGEX='^aux[0-9]{8}T[0-9]{6}Z$'

validate_aux_run_id() {
  local id=$1
  [[ "$id" =~ $AUX_RUN_ID_REGEX ]] || {
    echo "FATAL: AUX_RUN_ID must match $AUX_RUN_ID_REGEX (got: $id)" >&2
    return 1
  }
}

aux_runtime_root() {
  local id=$1
  validate_aux_run_id "$id"
  printf '/tmp/flink-aux-smoke-%s' "$id"
}

resolve_aux_runtime_root() {
  local id=$1
  local root
  root="$(aux_runtime_root "$id")"
  root="$(readlink -f "$root" 2>/dev/null || echo "$root")"
  case "$root" in
    /tmp/flink-aux-smoke-*) ;;
    *)
      echo "FATAL: resolved runtime root outside aux namespace: $root" >&2
      return 1
      ;;
  esac
  printf '%s' "$root"
}
