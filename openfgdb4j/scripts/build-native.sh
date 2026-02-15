#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT_DIR="$ROOT_DIR/openfgdb4j"
BUILD_DIR="$PROJECT_DIR/build/native"
CMAKE_BIN="/Applications/CMake.app/Contents/bin/cmake"
GDAL_MINIMAL_STAGE="${OPENFGDB4J_GDAL_MINIMAL_ROOT:-$ROOT_DIR/gdal-minimal/build/stage}"

export OPENFGDB4J_GDAL_MINIMAL_ROOT="$GDAL_MINIMAL_STAGE"
export CMAKE_BIN

"$ROOT_DIR/gdal-minimal/scripts/build-all.sh"

mkdir -p "$BUILD_DIR"

"$CMAKE_BIN" -S "$PROJECT_DIR/native" -B "$BUILD_DIR" -G "Unix Makefiles" \
  -DOPENFGDB4J_GDAL_MINIMAL_ROOT="$GDAL_MINIMAL_STAGE"
"$CMAKE_BIN" --build "$BUILD_DIR" --config Release
