# gdal-minimal

Deterministic macOS arm64 build pipeline for a static minimal GDAL stack used by `openfgdb4j`.

## Scope

- Target platform: macOS arm64 only.
- Builds static libraries into a stage directory:
  - `libsqlite3.a`
  - `libproj.a` (with embedded `proj.db` resources)
  - `libgdal.a` (OpenFileGDB enabled, optional drivers disabled)

## Layout

- `manifests/versions.lock`: pinned source versions and URLs.
- `manifests/SHA256SUMS`: archive checksums (mandatory verification).
- `scripts/*.sh`: fetch/prepare/build orchestration.
- `build/stage`: install/staging output (ignored by git).
- `third_party/downloads`: downloaded tarballs (ignored by git).
- `third_party/src`: extracted source trees (ignored by git).

## Environment variables

- `OPENFGDB4J_GDAL_MINIMAL_ROOT`: override stage directory path.
  - Default: `gdal-minimal/build/stage`
- `OPENFGDB4J_GDAL_MINIMAL_REBUILD=1`: force clean rebuild.
- `OPENFGDB4J_GDAL_MINIMAL_JOBS`: parallel build jobs.
- `CMAKE_BIN`: CMake executable path.

## Build

```bash
gdal-minimal/scripts/build-all.sh
```

## Notes

- All archives are SHA256-verified before extraction.
- The pipeline is deterministic through `versions.lock` + `SHA256SUMS`.
- Runtime system libraries (`/usr/lib/...`) remain dynamic.
