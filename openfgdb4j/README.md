# openfgdb4j

This module provides a native `openfgdb` C API plus Java FFM bindings.

The native build uses a repo-local static GDAL toolchain from `gdal-minimal` by default
(macOS arm64).

## Build native

```bash
openfgdb4j/scripts/build-native.sh
```

This runs `gdal-minimal/scripts/build-all.sh` first and then builds `libopenfgdb.dylib`
against staged static libraries (`libgdal.a`, `libproj.a`, `libsqlite3.a`).

## Generate bindings

```bash
openfgdb4j/scripts/generate-bindings.sh
```

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

Current status of `gdal` mode:

- `OPENFGDB4J_BACKEND=gdal` uses the strict dual-path entrypoint and error semantics.
- The native build compiles vendored low-level OpenFileGDB core sources as a compile gate (`OPENFGDB4J_ENABLE_GDAL_VENDOR_PROBE=ON`).

Static toolchain overrides:

- `OPENFGDB4J_GDAL_MINIMAL_ROOT=/absolute/path/to/stage`
- `OPENFGDB4J_GDAL_MINIMAL_REBUILD=1`

Optional debug output:

- `OPENFGDB4J_DEBUG=1`

For negative-path testing:

- `OPENFGDB4J_GDAL_FORCE_FAIL=1`

## Vendor refresh (GDAL v3.12.0)

```bash
openfgdb4j/scripts/import-gdal-openfilegdb.sh
openfgdb4j/scripts/import-gdal-core-minimal.sh
```

Optional offline/airgapped source archive:

- `OPENFGDB4J_GDAL_ARCHIVE=/path/to/gdal-v3.12.0.tar.gz`

## Tracked vs ignored artifacts

Intentionally versioned in git:

- `openfgdb4j/src/generated/java/**` (jextract output used by this repo)
- `gdal-minimal/manifests/**`, `gdal-minimal/scripts/**`, `gdal-minimal/README.md`

Never committed (ignored via `.gitignore`):

- `openfgdb4j/build/**`
- `gdal-minimal/build/**`
- `gdal-minimal/third_party/downloads/**`
- `gdal-minimal/third_party/src/**`

## ili2ofgdb bindist smoke test

There is a dedicated smoke task that validates a packaged `ili2ofgdb` zip end-to-end.

It unpacks the bindist, runs a real `--schemaimport` with:

- `SO_AFU_ABBAUSTELLEN_20210630.ili`
- local dependencies from `ili2ofgdb/test/data/smoke/abbaustellen/models`

Run:

```bash
./gradlew ili2ofgdbBindistSmoke
```

The task fails if the smoke log contains known bad signatures such as:

- `filegdbtable.cpp at line 838`
- `skip domain assignment; column not found`
- `skip relationship; key column missing`
