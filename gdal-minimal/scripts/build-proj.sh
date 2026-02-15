#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/common.sh"

require_cmd "$CMAKE_BIN"
load_versions
ensure_dirs

if [[ "${OPENFGDB4J_GDAL_MINIMAL_REBUILD:-0}" != "1" && -f "$(stage_lib libproj.a)" ]]; then
  echo "proj already built: $(stage_lib libproj.a)"
  exit 0
fi

"$SCRIPT_DIR/build-sqlite.sh"

PROJ_SRC="$SRC_DIR/$PROJ_SRC_DIR"
if [[ ! -f "$PROJ_SRC/CMakeLists.txt" ]]; then
  echo "proj source not prepared: $PROJ_SRC" >&2
  exit 1
fi

PROJ_BUILD="$BUILD_WORK_DIR/proj"
rm -rf "$PROJ_BUILD"
mkdir -p "$PROJ_BUILD"

export PATH="$STAGE_DIR/bin:$PATH"

"$CMAKE_BIN" -S "$PROJ_SRC" -B "$PROJ_BUILD" -G "Unix Makefiles" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_OSX_ARCHITECTURES=arm64 \
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
  -DCMAKE_INSTALL_PREFIX="$STAGE_DIR" \
  -DBUILD_SHARED_LIBS=OFF \
  -DBUILD_TESTING=OFF \
  -DBUILD_CCT=OFF \
  -DBUILD_CS2CS=OFF \
  -DBUILD_GEOD=OFF \
  -DBUILD_GIE=OFF \
  -DBUILD_PROJ=OFF \
  -DBUILD_PROJINFO=OFF \
  -DBUILD_PROJSYNC=OFF \
  -DENABLE_TIFF=OFF \
  -DENABLE_CURL=OFF \
  -DEMBED_RESOURCE_FILES=ON \
  -DUSE_ONLY_EMBEDDED_RESOURCE_FILES=ON \
  -DSQLITE3_INCLUDE_DIR="$STAGE_DIR/include" \
  -DSQLITE3_LIBRARY="$STAGE_DIR/lib/libsqlite3.a"

"$CMAKE_BIN" --build "$PROJ_BUILD" --config Release -- -j"$JOBS"
"$CMAKE_BIN" --build "$PROJ_BUILD" --target install --config Release

if [[ ! -f "$(stage_lib libproj.a)" ]]; then
  echo "proj static library missing after build" >&2
  exit 1
fi

echo "Built proj static: $(stage_lib libproj.a)"
