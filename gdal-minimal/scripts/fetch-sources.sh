#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd curl shasum awk
load_versions
ensure_dirs

download_archive "$GDAL_URL" "$GDAL_ARCHIVE"
download_archive "$PROJ_URL" "$PROJ_ARCHIVE"
download_archive "$SQLITE_URL" "$SQLITE_ARCHIVE"

echo "All source archives downloaded and verified."
