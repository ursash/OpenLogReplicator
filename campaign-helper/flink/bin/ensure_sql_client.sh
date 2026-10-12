#!/usr/bin/env bash
# Apache Flink 1.20.5 sql-client.sh (CSA parcel omits it); cached under helper vendor/.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=flink_env.sh
source "$SCRIPT_DIR/flink_env.sh"

VENDOR="${FLINK_VENDOR_DIR:-$FLINK_HELPER_ROOT/vendor}"
mkdir -p "$VENDOR"
APACHE_ROOT="$PRIVATE_LIB/flink-1.20.5"
SQL_CLIENT_BIN="$APACHE_ROOT/bin/sql-client.sh"
TARBALL="$VENDOR/flink-1.20.5-bin-scala_2.12.tgz"
URL="${FLINK_BIN_URL:-https://archive.apache.org/dist/flink/flink-1.20.5/flink-1.20.5-bin-scala_2.12.tgz}"

if [ ! -f "$TARBALL" ]; then
  echo "fetch $URL -> $TARBALL"
  curl -fsSL "$URL" -o "$TARBALL.part"
  mv "$TARBALL.part" "$TARBALL"
  sha256sum "$TARBALL" | tee "$TARBALL.sha256"
fi
if [ ! -x "$SQL_CLIENT_BIN" ]; then
  tar -xzf "$TARBALL" -C "$PRIVATE_LIB"
fi
export SQL_CLIENT="$SQL_CLIENT_BIN"
[ "${FLINK_HELPER_QUIET:-}" = 1 ] || echo "SQL_CLIENT=$SQL_CLIENT"
