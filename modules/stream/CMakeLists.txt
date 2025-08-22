# OpenCV Stream module — CMake configuration
#
# Exposed CMake cache variables:
#   WITH_STREAM (BOOL)                                   [ON]
#       Enable/disable building this module.
#
#   STREAM_BACKEND (STRING: AUTO|CIVETWEB|BOOST|MONGOOSE) [AUTO]
#       Backend selection. AUTO tries CIVETWEB → BOOST → MONGOOSE.
#
#   STREAM_CIVETWEB_DIR (PATH)                           [""]
#       Hint path to a custom CivetWeb installation root.
#
#   STREAM_BOOST_DIR (PATH)                              [""]
#       Hint path to a custom Boost installation root (headers/libs).
#
#   STREAM_COMPRESSION (STRING: AUTO|ON|OFF)             [AUTO]
#       Enables FFmpeg-based compression when available.
#         - AUTO : YES if FFmpeg is found, otherwise NO
#         - ON   : YES if FFmpeg is found, otherwise NO (and a post-config warning)
#         - OFF  : NO
#

set(the_description "OpenCV Stream Module: Real-time streaming API for live video/image feeds")

# Module toggle
ocv_option(WITH_STREAM "Build OpenCV stream module" ON)
if(NOT WITH_STREAM)
    ocv_module_disable(stream)
    return()
endif()

# ------------------------------------------------------------------------------
# Backends
# ------------------------------------------------------------------------------

# --- CivetWeb ---
# Try pkg-config first, then plain find_* as fallback. Supports optional C++ wrapper.
set(CIVETWEB_FOUND FALSE)
set(CIVETWEB_INCLUDE_DIRS "")
set(CIVETWEB_LIBRARIES "")

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_CIVETWEB     QUIET civetweb)
    pkg_check_modules(PC_CIVETWEB_CPP QUIET civetweb-cpp) # optional
endif()

# Includes
if(PC_CIVETWEB_FOUND)
    list(APPEND CIVETWEB_INCLUDE_DIRS ${PC_CIVETWEB_INCLUDE_DIRS})
elseif(STREAM_CIVETWEB_DIR)
    find_path(CIVETWEB_INCLUDE_DIR NAMES civetweb.h HINTS ${STREAM_CIVETWEB_DIR})
    if(CIVETWEB_INCLUDE_DIR)
        list(APPEND CIVETWEB_INCLUDE_DIRS ${CIVETWEB_INCLUDE_DIR})
    endif()
else()
    find_path(CIVETWEB_INCLUDE_DIR NAMES civetweb.h)
    if(CIVETWEB_INCLUDE_DIR)
        list(APPEND CIVETWEB_INCLUDE_DIRS ${CIVETWEB_INCLUDE_DIR})
    endif()
endif()

# Libraries (C core required; C++ wrapper optional)
if(PC_CIVETWEB_FOUND)
    list(APPEND CIVETWEB_LIBRARIES ${PC_CIVETWEB_LIBRARIES})
else()
    if(STREAM_CIVETWEB_DIR)
        find_library(CIVETWEB_C_LIB   NAMES civetweb     HINTS ${STREAM_CIVETWEB_DIR})
        find_library(CIVETWEB_CPP_LIB NAMES civetweb-cpp HINTS ${STREAM_CIVETWEB_DIR})
    else()
        find_library(CIVETWEB_C_LIB   NAMES civetweb)
        find_library(CIVETWEB_CPP_LIB NAMES civetweb-cpp)
    endif()
    if(CIVETWEB_C_LIB)
        list(APPEND CIVETWEB_LIBRARIES ${CIVETWEB_C_LIB})
    endif()
    if(CIVETWEB_CPP_LIB)
        list(APPEND CIVETWEB_LIBRARIES ${CIVETWEB_CPP_LIB})
    endif()
endif()

if(CIVETWEB_INCLUDE_DIRS AND CIVETWEB_LIBRARIES)
    set(CIVETWEB_FOUND TRUE)
endif()

# --- Boost (Beast) ---
# Do NOT call find_package(Boost); CMake >=3.30 removed FindBoost (CMP0167).
# Detect headers + (optionally) boost_system. If boost_system is missing,
# we’ll use header-only Boost.System later via a compile definition.
set(BOOST_FOUND FALSE)
set(BOOST_INCLUDE_DIRS "")
set(BOOST_LIBRARIES "")

if(STREAM_BOOST_DIR)
    find_path(BOOST_INCLUDE_DIR NAMES boost/beast/core.hpp HINTS ${STREAM_BOOST_DIR})
else()
    find_path(BOOST_INCLUDE_DIR NAMES boost/beast/core.hpp)
endif()
if(BOOST_INCLUDE_DIR)
    set(BOOST_FOUND TRUE)
    list(APPEND BOOST_INCLUDE_DIRS ${BOOST_INCLUDE_DIR})
endif()

if(STREAM_BOOST_DIR)
    find_library(BOOST_SYSTEM_LIB NAMES boost_system HINTS ${STREAM_BOOST_DIR})
else()
    find_library(BOOST_SYSTEM_LIB NAMES boost_system)
endif()
if(BOOST_SYSTEM_LIB)
    list(APPEND BOOST_LIBRARIES ${BOOST_SYSTEM_LIB})
endif()

set(CIVETWEB_FOUND       ${CIVETWEB_FOUND}       PARENT_SCOPE)
set(CIVETWEB_INCLUDE_DIRS "${CIVETWEB_INCLUDE_DIRS}" PARENT_SCOPE)
set(CIVETWEB_LIBRARIES    "${CIVETWEB_LIBRARIES}"    PARENT_SCOPE)

set(BOOST_FOUND          ${BOOST_FOUND}          PARENT_SCOPE)
set(BOOST_INCLUDE_DIRS   "${BOOST_INCLUDE_DIRS}" PARENT_SCOPE)
set(BOOST_LIBRARIES      "${BOOST_LIBRARIES}"    PARENT_SCOPE)


# Compression (FFmpeg) — optional
set(STREAM_COMPRESSION "AUTO" CACHE STRING "Enable compression via FFmpeg (AUTO, ON, OFF)")
set_property(CACHE STREAM_COMPRESSION PROPERTY STRINGS AUTO ON OFF)
string(TOUPPER "${STREAM_COMPRESSION}" STREAM_COMPRESSION)

# Discover FFmpeg quietly; never disables the module
ocv_check_modules(FFMPEG libavcodec libavformat libavutil libswscale QUIET)

# ------------------------------------------------------------------------------
# Backend selection
# ------------------------------------------------------------------------------
set(STREAM_BACKEND "AUTO" CACHE STRING "HTTP backend for stream module (AUTO, CIVETWEB, BOOST, MONGOOSE)")
set_property(CACHE STREAM_BACKEND PROPERTY STRINGS AUTO CIVETWEB BOOST MONGOOSE)
string(TOUPPER "${STREAM_BACKEND}" STREAM_BACKEND)

set(STREAM_SELECTED_BACKEND "")
set(STREAM_BACKEND_SOURCES "")
set(STREAM_BACKEND_INCLUDES "")
set(STREAM_BACKEND_LIBS "")
set(STREAM_POST_WARNINGS "")

# Honor explicit choice when available (defer warnings)
if(NOT STREAM_BACKEND STREQUAL "AUTO")
    if(STREAM_BACKEND STREQUAL "CIVETWEB" AND CIVETWEB_FOUND)
        set(STREAM_SELECTED_BACKEND "CIVETWEB")
    elseif(STREAM_BACKEND STREQUAL "BOOST" AND BOOST_FOUND)
        set(STREAM_SELECTED_BACKEND "BOOST")
    elseif(STREAM_BACKEND STREQUAL "MONGOOSE")
        set(STREAM_SELECTED_BACKEND "MONGOOSE")
    else()
        if(NOT STREAM_BACKEND STREQUAL "MONGOOSE")
            list(APPEND STREAM_POST_WARNINGS
                    "Stream: Requested backend '${STREAM_BACKEND}' not found; falling back to AUTO.")
        endif()
    endif()
endif()

# AUTO fallback chain
if(STREAM_SELECTED_BACKEND STREQUAL "")
    if(CIVETWEB_FOUND)
        set(STREAM_SELECTED_BACKEND "CIVETWEB")
    elseif(BOOST_FOUND)
        set(STREAM_SELECTED_BACKEND "BOOST")
    else()
        set(STREAM_SELECTED_BACKEND "MONGOOSE")
    endif()
endif()

# Backend-specific wiring
if(STREAM_SELECTED_BACKEND STREQUAL "CIVETWEB")
    set(STREAM_BACKEND_INCLUDES ${CIVETWEB_INCLUDE_DIRS})
    set(STREAM_BACKEND_LIBS     ${CIVETWEB_LIBRARIES})

elseif(STREAM_SELECTED_BACKEND STREQUAL "BOOST")
    set(STREAM_BACKEND_INCLUDES ${BOOST_INCLUDE_DIRS})
    set(STREAM_BACKEND_LIBS     ${BOOST_LIBRARIES})
    find_package(Threads QUIET)
    if(Threads_FOUND)
        list(APPEND STREAM_BACKEND_LIBS Threads::Threads)
    endif()

elseif(STREAM_SELECTED_BACKEND STREQUAL "MONGOOSE")
    set(MONGOOSE_VERSION "7.18")
    set(MONGOOSE_HASH "e50fe3c2d43cdd5ed96833aae88ad7b1")
    set(MONGOOSE_URL "https://github.com/cesanta/mongoose/archive/refs/tags/${MONGOOSE_VERSION}.zip")
    set(MONGOOSE_DOWNLOAD_DIR "${OpenCV_BINARY_DIR}/3rdparty/mongoose")
    set(MONGOOSE_UNPACK_DIR   "${MONGOOSE_DOWNLOAD_DIR}/mongoose-${MONGOOSE_VERSION}")
    set(STREAM_BACKEND_SOURCES "${MONGOOSE_UNPACK_DIR}/mongoose.c")
    set(STREAM_BACKEND_INCLUDES "${MONGOOSE_UNPACK_DIR}")
    ocv_download(
            FILENAME "${MONGOOSE_VERSION}.zip"
            URL "${MONGOOSE_URL}"
            HASH "${MONGOOSE_HASH}"
            DESTINATION_DIR "${MONGOOSE_DOWNLOAD_DIR}"
            UNPACK
    )
endif()

# ------------------------------------------------------------------------------
# Compression decision (FFmpeg optional)
# ------------------------------------------------------------------------------
set(STREAM_COMPRESSION_ENABLED FALSE)
if(STREAM_COMPRESSION STREQUAL "ON")
    if(FFMPEG_FOUND)
        set(STREAM_COMPRESSION_ENABLED TRUE)
    else()
        list(APPEND STREAM_POST_WARNINGS
                "Stream: STREAM_COMPRESSION=ON but FFmpeg not found; disabling compression.")
    endif()
elseif(STREAM_COMPRESSION STREQUAL "AUTO")
    if(FFMPEG_FOUND)
        set(STREAM_COMPRESSION_ENABLED TRUE)
    endif()
else() # OFF: keep FALSE
endif()

# ------------------------------------------------------------------------------
# Targets
# ------------------------------------------------------------------------------
set(STREAM_BACKEND_DEFINITIONS "HAVE_STREAM_BACKEND_${STREAM_SELECTED_BACKEND}")
if(STREAM_COMPRESSION_ENABLED)
    list(APPEND STREAM_BACKEND_DEFINITIONS "HAVE_STREAM_COMPRESSION")
endif()

ocv_define_module(stream opencv_core)

# Add vendored source if using Mongoose
if(STREAM_SELECTED_BACKEND STREQUAL "MONGOOSE")
    target_sources(${the_module} PRIVATE ${STREAM_BACKEND_SOURCES})
endif()

# Definitions & includes
target_compile_definitions(${the_module} PRIVATE ${STREAM_BACKEND_DEFINITIONS})
if(STREAM_BACKEND_INCLUDES)
    target_include_directories(${the_module} PRIVATE ${STREAM_BACKEND_INCLUDES})
endif()

# Link libraries (backend + optional FFmpeg)
set(_stream_module_libs ${STREAM_BACKEND_LIBS})
if(STREAM_COMPRESSION_ENABLED)
    list(APPEND _stream_module_libs ${FFMPEG_LIBRARIES})
endif()
ocv_target_link_libraries(${the_module} PRIVATE ${_stream_module_libs})

# 4
if(BUILD_TESTS)
    # Make the flag available in both library and test sources
    target_compile_definitions(${the_module} PRIVATE ocv_build_tests=1)
    add_compile_definitions(OCV_BUILD_TESTS=1)

    # Common link set for all tests
    set(_stream_test_libs ${the_module} ${STREAM_BACKEND_LIBS})
    if(STREAM_COMPRESSION_ENABLED)
        list(APPEND _stream_test_libs ${FFMPEG_LIBRARIES})
    endif()

    # Helper to DRY up test targets
    function(add_stream_test name src)
        add_executable(${name} ${src})

        if(STREAM_SELECTED_BACKEND STREQUAL "MONGOOSE")
            target_sources(${name} PRIVATE ${STREAM_BACKEND_SOURCES})
        endif()

        if(STREAM_BACKEND_INCLUDES)
            target_include_directories(${name} PRIVATE ${STREAM_BACKEND_INCLUDES})
        endif()

        target_link_libraries(${name} PRIVATE ${_stream_test_libs})
        target_compile_definitions(${name} PRIVATE ${STREAM_BACKEND_DEFINITIONS})
    endfunction()

    # Tests
    add_stream_test(opencv_test_stream_http ${CMAKE_CURRENT_SOURCE_DIR}/test/http_test.cpp)
    add_stream_test(opencv_test_stream_websocket   ${CMAKE_CURRENT_SOURCE_DIR}/test/websocket_test.cpp)
endif()



# ------------------------------------------------------------------------------
# Summary (printed once; warnings deferred until after the summary)
# ------------------------------------------------------------------------------
set(_yn_CIVETWEB "NO")
set(_yn_BOOST    "NO")
set(_yn_FFMPEG   "NO")
set(_yn_COMP     "NO")
if(CIVETWEB_FOUND)
    set(_yn_CIVETWEB "YES")
endif()
if(BOOST_FOUND)
    set(_yn_BOOST    "YES")
endif()
if(FFMPEG_FOUND)
    set(_yn_FFMPEG   "YES")
endif()
if(STREAM_COMPRESSION_ENABLED)
    set(_yn_COMP "YES")
endif()

message(STATUS "\n\
Stream configuration summary:\n\
  Backend preference       : ${STREAM_BACKEND}\n\
  Backend selected         : ${STREAM_SELECTED_BACKEND}\n\
  CivetWeb found           : ${_yn_CIVETWEB}\n\
  Boost (Beast) found      : ${_yn_BOOST}\n\
  FFmpeg found             : ${_yn_FFMPEG}\n\
  Compression (FFmpeg)     : ${_yn_COMP}\n")
message(STATUS "Stream backend include path : ${STREAM_BACKEND_INCLUDES}")
message(STATUS "Stream backend lib path: ${STREAM_BACKEND_LIBS}")

foreach(_w IN LISTS STREAM_POST_WARNINGS)
    message(WARNING "${_w}")
endforeach()
