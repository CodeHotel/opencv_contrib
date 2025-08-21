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
# Backend discovery helper
# ------------------------------------------------------------------------------
macro(stream_find_backend _name _cmake_name _pkgconfig_name _header_name _lib_name)
    set(${_name}_FOUND FALSE)
    set(${_name}_INCLUDE_DIRS "")
    set(${_name}_LIBRARIES "")
    set(${_name}_HINTS "")
    if(STREAM_${_name}_DIR)
        set(${_name}_HINTS HINTS "${STREAM_${_name}_DIR}")
    endif()
    set(_found_in_macro FALSE)
    if(NOT _found_in_macro AND NOT "${_cmake_name}" STREQUAL "")
        find_package(${_cmake_name} QUIET ${${_name}_HINTS})
        if(${_cmake_name}_FOUND)
            set(${_name}_FOUND TRUE)
            set(${_name}_INCLUDE_DIRS "${${_cmake_name}_INCLUDE_DIRS}")
            set(${_name}_LIBRARIES "${${_cmake_name}_LIBRARIES}")
            set(_found_in_macro TRUE)
        endif()
    endif()
    if(NOT _found_in_macro AND NOT "${_pkgconfig_name}" STREQUAL "")
        ocv_check_modules(OCV_TEMP_${_name} ${_pkgconfig_name} QUIET)
        if(OCV_TEMP_${_name}_FOUND)
            set(${_name}_FOUND TRUE)
            set(${_name}_INCLUDE_DIRS "${OCV_TEMP_${_name}_INCLUDE_DIRS}")
            set(${_name}_LIBRARIES "${OCV_TEMP_${_name}_LIBRARIES}")
            set(_found_in_macro TRUE)
        endif()
    endif()
    if(NOT _found_in_macro AND NOT "${_header_name}" STREQUAL "")
        find_path(${_name}_INCLUDE_DIR NAMES ${_header_name} ${${_name}_HINTS})
        if(NOT "${_lib_name}" STREQUAL "")
            find_library(${_name}_LIBRARY NAMES ${_lib_name} ${${_name}_HINTS})
        else()
            set(${_name}_LIBRARY TRUE)
        endif()
        if(${_name}_INCLUDE_DIR AND ${_name}_LIBRARY)
            set(${_name}_FOUND TRUE)
            set(${_name}_INCLUDE_DIRS "${${_name}_INCLUDE_DIR}")
            if("${${_name}_LIBRARY}" STREQUAL "TRUE")
                set(${_name}_LIBRARIES "")
            else()
                set(${_name}_LIBRARIES "${${_name}_LIBRARY}")
            endif()
            set(_found_in_macro TRUE)
        endif()
    endif()
endmacro()

# ------------------------------------------------------------------------------
# Options & discovery (quiet)
# ------------------------------------------------------------------------------
set(STREAM_CIVETWEB_DIR "" CACHE PATH "Path to a custom CivetWeb installation root")
set(STREAM_BOOST_DIR    "" CACHE PATH "Path to a custom Boost installation root (headers or libs)")

# Backends
stream_find_backend(CIVETWEB civetweb civetweb civetweb.h civetweb)
stream_find_backend(BOOST    Boost    ""        "boost/beast/core.hpp" "boost_system")

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

# Tests
if(BUILD_TESTS)
    add_executable(test_stream test/test_stream.cpp)
    if(STREAM_SELECTED_BACKEND STREQUAL "MONGOOSE")
        target_sources(test_stream PRIVATE ${STREAM_BACKEND_SOURCES})
    endif()
    if(STREAM_BACKEND_INCLUDES)
        target_include_directories(test_stream PRIVATE ${STREAM_BACKEND_INCLUDES})
    endif()
    set(_stream_test_libs ${the_module} ${STREAM_BACKEND_LIBS})
    if(STREAM_COMPRESSION_ENABLED)
        list(APPEND _stream_test_libs ${FFMPEG_LIBRARIES})
    endif()
    target_link_libraries(test_stream PRIVATE ${_stream_test_libs})
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

foreach(_w IN LISTS STREAM_POST_WARNINGS)
    message(WARNING "${_w}")
endforeach()
