#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

load_versions
ensure_dirs

if [[ "${OPENFGDB4J_GDAL_MINIMAL_REBUILD:-0}" == "1" ]]; then
  echo "Rebuild requested: clearing stage + work directories"
  rm -rf "$BUILD_WORK_DIR" "$STAGE_DIR"
  ensure_dirs
fi

"$SCRIPT_DIR/prepare-tree.sh"
"$SCRIPT_DIR/build-sqlite.sh"
"$SCRIPT_DIR/build-proj.sh"
"$SCRIPT_DIR/build-gdal.sh"

echo "gdal-minimal build complete"
echo "  stage: $STAGE_DIR"
