# openfgdb4j

This module provides a native `openfgdb` C API plus Java FFM bindings.

The native build uses a repo-local static GDAL toolchain from `gdal-minimal` by default.
Supported runtime targets are:

- Linux: `amd64`, `arm64`
- macOS: `amd64`, `arm64`
- Windows: `amd64`

## Build native (Unix)

```bash
openfgdb4j/scripts/build-native.sh
```

This runs `gdal-minimal/scripts/build-all.sh` first and then builds the native library
against staged static libraries (`gdal`, `proj`, `sqlite3`).

## Build native (Windows)

```powershell
openfgdb4j/scripts/ci/build-and-test-windows.ps1
```

This uses `gdal-minimal/scripts/build-all.ps1` (MSVC + CMake).

## Generate bindings

```bash
openfgdb4j/scripts/generate-bindings.sh
```

Optional override:

- `JEXTRACT_BIN=/absolute/path/to/jextract`

## Build Java JAR

```bash
openfgdb4j/scripts/build-java.sh
```

The generated JAR is written to:

- `openfgdb4j/build/java/openfgdb4j.jar`

## Runtime backend mode

`openfgdb4j` supports an explicit backend mode via environment variable:

- `OPENFGDB4J_BACKEND=gdal` (default)
- `OPENFGDB4J_BACKEND=adapter`

No automatic fallback is performed between backends.

Optional debug output:

- `OPENFGDB4J_DEBUG=1`

Negative-path testing:

- `OPENFGDB4J_GDAL_FORCE_FAIL=1`

## Build controls

- `OPENFGDB4J_TARGET_OS=linux|macos|windows`
- `OPENFGDB4J_TARGET_ARCH=amd64|arm64`
- `OPENFGDB4J_GDAL_MINIMAL_ROOT=/absolute/path/to/stage`
- `OPENFGDB4J_GDAL_MINIMAL_REBUILD=1`
- `OPENFGDB4J_GDAL_MINIMAL_JOBS=<n>`
- `OPENFGDB4J_CMAKE_GENERATOR=<generator>`
- `CMAKE_BIN=<cmake-path>`

## CI helper scripts

- `openfgdb4j/scripts/ci/build-and-test-unix.sh`
- `openfgdb4j/scripts/ci/build-and-test-windows.ps1`
- `openfgdb4j/scripts/ci/check-linkage-unix.sh`
- `openfgdb4j/scripts/ci/check-linkage-windows.ps1`
- `openfgdb4j/scripts/ci/package-artifact.sh`
- `openfgdb4j/scripts/ci/package-artifact.ps1`

The package scripts create per-platform artifact directories containing:

- `native/<library>`
- `java/openfgdb4j.jar`
- `include/openfgdb_c_api.h`
- `metadata/manifest.json`
- `metadata/sha256sums.txt`

## Duplicated smoke assets (intentional)

For later repository split, smoke assets are duplicated inside `openfgdb4j`:

- `openfgdb4j/test/data/smoke/abbaustellen/models`
- `openfgdb4j/test/data/smoke/abbaustellen/data`

## Vendor refresh (GDAL v3.12.0)

```bash
openfgdb4j/scripts/import-gdal-openfilegdb.sh
openfgdb4j/scripts/import-gdal-core-minimal.sh
```

Optional offline source archive:

- `OPENFGDB4J_GDAL_ARCHIVE=/path/to/gdal-v3.12.0.tar.gz`

## Tracked vs ignored artifacts

Intentionally versioned in git:

- `openfgdb4j/src/generated/java/**` (jextract output used by this repo)
- `gdal-minimal/manifests/**`, `gdal-minimal/scripts/**`, `gdal-minimal/README.md`
- `openfgdb4j/test/data/smoke/**` (split-prep duplicate smoke assets)

Never committed (ignored via `.gitignore`):

- `openfgdb4j/build/**`
- `gdal-minimal/build/**`
- `gdal-minimal/third_party/downloads/**`
- `gdal-minimal/third_party/src/**`
