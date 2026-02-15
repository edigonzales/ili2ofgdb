cmake_minimum_required(VERSION 3.20)
project(openfgdb4j_native VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT APPLE)
  message(FATAL_ERROR "openfgdb4j currently supports macOS only.")
endif()

if(NOT CMAKE_SYSTEM_PROCESSOR MATCHES "^(arm64|aarch64)$")
  message(FATAL_ERROR "openfgdb4j currently supports macOS arm64 only.")
endif()

set(GDAL_VENDOR_OPENFILEGDB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gdal_openfilegdb/src/ogr/ogrsf_frmts/openfilegdb")
set(GDAL_VENDOR_CORE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../third_party/gdal_core_minimal/src")
set(OPENFGDB4J_GDAL_MINIMAL_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../gdal-minimal/build/stage" CACHE PATH
    "Path to gdal-minimal stage directory (contains include/ and lib/)")

set(OPENFGDB4J_GDAL_STATIC_LIB "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/libgdal.a")
set(OPENFGDB4J_PROJ_STATIC_LIB "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/libproj.a")
set(OPENFGDB4J_SQLITE_STATIC_LIB "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/lib/libsqlite3.a")
set(OPENFGDB4J_GDAL_INCLUDE_DIR "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/include")
set(OPENFGDB4J_GDAL_INCLUDE_GDAL_DIR "${OPENFGDB4J_GDAL_MINIMAL_ROOT}/include/gdal")

option(OPENFGDB4J_ENABLE_GDAL_VENDOR_PROBE "Compile vendored OpenFileGDB core sources as build probe" ON)

foreach(_required
    "${OPENFGDB4J_GDAL_STATIC_LIB}"
    "${OPENFGDB4J_PROJ_STATIC_LIB}"
    "${OPENFGDB4J_SQLITE_STATIC_LIB}")
  if(NOT EXISTS "${_required}")
    message(FATAL_ERROR
      "Missing staged static library: ${_required}\n"
      "Run gdal-minimal/scripts/build-all.sh or set OPENFGDB4J_GDAL_MINIMAL_ROOT.")
  endif()
endforeach()

if(NOT EXISTS "${OPENFGDB4J_GDAL_INCLUDE_DIR}")
  message(FATAL_ERROR
    "Missing GDAL include dir: ${OPENFGDB4J_GDAL_INCLUDE_DIR}\n"
    "Run gdal-minimal/scripts/build-all.sh or set OPENFGDB4J_GDAL_MINIMAL_ROOT.")
endif()

message(STATUS "openfgdb4j static stage root: ${OPENFGDB4J_GDAL_MINIMAL_ROOT}")
message(STATUS "openfgdb4j static libs: ${OPENFGDB4J_GDAL_STATIC_LIB};${OPENFGDB4J_PROJ_STATIC_LIB};${OPENFGDB4J_SQLITE_STATIC_LIB}")

add_library(openfgdb SHARED
    src/openfgdb_c_api.cpp
    src/openfgdb_backend_dispatch.cpp
    src/openfgdb_backend_adapter.cpp
    src/openfgdb_backend_gdal.cpp
    src/openfgdb_native_adapter.cpp
    src/openfgdb_catalog.cpp
    src/openfgdb_tableio.cpp
    src/openfgdb_handles.cpp
)

target_include_directories(openfgdb
    PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
    ${OPENFGDB4J_GDAL_INCLUDE_DIR}
    ${OPENFGDB4J_GDAL_INCLUDE_GDAL_DIR}
)

target_compile_definitions(openfgdb PRIVATE OPENFGDB_MACOS_ARM64=1)
target_link_libraries(openfgdb PRIVATE
    "${OPENFGDB4J_GDAL_STATIC_LIB}"
    "${OPENFGDB4J_PROJ_STATIC_LIB}"
    "${OPENFGDB4J_SQLITE_STATIC_LIB}"
    "-liconv"
)

if(APPLE)
  set_target_properties(openfgdb PROPERTIES
      INSTALL_NAME_DIR "@rpath"
  )
endif()

if(OPENFGDB4J_ENABLE_GDAL_VENDOR_PROBE)
  add_library(openfgdb_gdal_vendor_probe OBJECT
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbindex.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbindex_write.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable_write.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable_write_fields.cpp
      ${GDAL_VENDOR_OPENFILEGDB_DIR}/filegdbtable_freelist.cpp
  )

  target_include_directories(openfgdb_gdal_vendor_probe
      PRIVATE
      ${GDAL_VENDOR_OPENFILEGDB_DIR}
      ${GDAL_VENDOR_CORE_DIR}
      ${GDAL_VENDOR_CORE_DIR}/port
      ${GDAL_VENDOR_CORE_DIR}/gcore
      ${GDAL_VENDOR_CORE_DIR}/ogr
      ${GDAL_VENDOR_CORE_DIR}/ogr/ogrsf_frmts
  )

  target_compile_definitions(openfgdb_gdal_vendor_probe PRIVATE GDAL_COMPILATION=1)
  target_compile_options(openfgdb_gdal_vendor_probe PRIVATE -ffunction-sections -fdata-sections)

  add_custom_target(openfgdb_gdal_vendor_probe_build DEPENDS openfgdb_gdal_vendor_probe)
  add_dependencies(openfgdb openfgdb_gdal_vendor_probe_build)
endif()
