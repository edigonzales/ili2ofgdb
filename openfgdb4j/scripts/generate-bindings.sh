#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ -n "${JEXTRACT_BIN:-}" ]]; then
  JEXTRACT_BIN="${JEXTRACT_BIN}"
elif [[ -x "/Users/stefan/Downloads/jextract-25/bin/jextract" ]]; then
  JEXTRACT_BIN="/Users/stefan/Downloads/jextract-25/bin/jextract"
else
  JEXTRACT_BIN="jextract"
fi

if ! command -v "$JEXTRACT_BIN" >/dev/null 2>&1; then
  echo "jextract not found: $JEXTRACT_BIN" >&2
  echo "Set JEXTRACT_BIN to a valid executable path." >&2
  exit 1
fi

mkdir -p "$ROOT_DIR/openfgdb4j/src/generated/java"

"$JEXTRACT_BIN" \
  --include-dir "$ROOT_DIR/openfgdb4j/native/include" \
  --target-package ch.ehi.openfgdb4j.ffm \
  --header-class-name OpenFgdbNative \
  --output "$ROOT_DIR/openfgdb4j/src/generated/java" \
  "$ROOT_DIR/openfgdb4j/native/include/openfgdb_c_api.h"

# jextract may emit SymbolLookup#findOrThrow, which is not available on all installed JDKs.
while IFS= read -r -d '' file; do
  perl -0777 -i -pe 's/SYMBOL_LOOKUP\.findOrThrow\("([^"]+)"\)/SYMBOL_LOOKUP.find("$1").orElseThrow(() -> new IllegalStateException("Native symbol not found: $1"))/g' "$file"
done < <(find "$ROOT_DIR/openfgdb4j/src/generated/java" -name '*.java' -print0)
