#!/bin/zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
PROJECT_DIR="$ROOT_DIR/openfgdb4j"
BUILD_CLASSES_DIR="$PROJECT_DIR/build/classes"
BUILD_JAR_DIR="$PROJECT_DIR/build/java"
JAVAC_BIN="${JAVAC_BIN:-/Users/stefan/.sdkman/candidates/java/25-tem/bin/javac}"
JAR_BIN="${JAR_BIN:-$(dirname "$JAVAC_BIN")/jar}"

rm -rf "$BUILD_CLASSES_DIR"
mkdir -p "$BUILD_CLASSES_DIR" "$BUILD_JAR_DIR"

typeset -a JAVA_SOURCES
JAVA_SOURCES=(${(f)"$(find "$PROJECT_DIR/src/main/java" "$PROJECT_DIR/src/generated/java" -name '*.java' -print)"})

if [[ ${#JAVA_SOURCES[@]} -eq 0 ]]; then
  echo "No Java sources found"
  exit 1
fi

"$JAVAC_BIN" --release 22 -d "$BUILD_CLASSES_DIR" "${JAVA_SOURCES[@]}"
"$JAR_BIN" --create --file "$BUILD_JAR_DIR/openfgdb4j.jar" -C "$BUILD_CLASSES_DIR" .
