#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd tar find
load_versions
ensure_dirs

"$SCRIPT_DIR/fetch-sources.sh"

extract_archive_to "$GDAL_ARCHIVE" "$GDAL_SRC_DIR"
extract_archive_to "$PROJ_ARCHIVE" "$PROJ_SRC_DIR"
extract_archive_to "$SQLITE_ARCHIVE" "$SQLITE_SRC_DIR"

echo "Prepared source tree under: $SRC_DIR"
