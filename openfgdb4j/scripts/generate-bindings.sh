#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
JEXTRACT_BIN="/Users/stefan/Downloads/jextract-25/bin/jextract"

mkdir -p "$ROOT_DIR/openfgdb4j/src/generated/java"

"$JEXTRACT_BIN" \
  --include-dir "$ROOT_DIR/openfgdb4j/native/include" \
  --target-package ch.ehi.openfgdb4j.ffm \
  --header-class-name OpenFgdbNative \
  --output "$ROOT_DIR/openfgdb4j/src/generated/java" \
  "$ROOT_DIR/openfgdb4j/native/include/openfgdb_c_api.h"

# jextract currently emits SymbolLookup#findOrThrow, which is not available on all installed JDKs.
find "$ROOT_DIR/openfgdb4j/src/generated/java" -name '*.java' -print0 | while IFS= read -r -d '' file; do
  perl -0777 -i -pe 's/SYMBOL_LOOKUP\.findOrThrow\("([^"]+)"\)/SYMBOL_LOOKUP.find("$1").orElseThrow(() -> new IllegalStateException("Native symbol not found: $1"))/g' "$file"
done
