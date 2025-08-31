# OpenCV Stream module — CMake configuration
#
# Cache variables:
#   WITH_STREAM (BOOL)                                   [ON]
#   STREAM_HTTP_BACKEND (AUTO|CIVETWEB|BOOST|MONGOOSE)   [AUTO]
#   STREAM_WEBRTC (AUTO|ON|OFF)                          [AUTO]
#   STREAM_COMPRESSION (AUTO|ON|OFF)                     [AUTO]
#   STREAM_CIVETWEB_DIR (PATH)                           [""]
#   STREAM_BOOST_DIR  (PATH)                             [""]
#

set(the_description "OpenCV Stream Module: real-time streaming for video/image feeds")

# --------------------------------------------------------------------------- #
# Module toggle
# --------------------------------------------------------------------------- #
ocv_option(WITH_STREAM "Build OpenCV stream module" ON)
if(NOT WITH_STREAM)
    ocv_module_disable(stream)
    return()
endif()

# --------------------------------------------------------------------------- #
# Options
# --------------------------------------------------------------------------- #
set(STREAM_HTTP_BACKEND "AUTO" CACHE STRING "HTTP backend (AUTO, CIVETWEB, BOOST, MONGOOSE)")
set_property(CACHE STREAM_HTTP_BACKEND PROPERTY STRINGS AUTO CIVETWEB BOOST MONGOOSE)
string(TOUPPER "${STREAM_HTTP_BACKEND}" STREAM_HTTP_BACKEND)

set(STREAM_WEBRTC "AUTO" CACHE STRING "Enable WebRTC feature via GStreamer (AUTO, ON, OFF)")
set_property(CACHE STREAM_WEBRTC PROPERTY STRINGS AUTO ON OFF)
string(TOUPPER "${STREAM_WEBRTC}" STREAM_WEBRTC)

set(STREAM_COMPRESSION "AUTO" CACHE STRING "Enable FFmpeg compression (AUTO, ON, OFF)")
set_property(CACHE STREAM_COMPRESSION PROPERTY STRINGS AUTO ON OFF)
string(TOUPPER "${STREAM_COMPRESSION}" STREAM_COMPRESSION)

# Optional hint paths
set(STREAM_CIVETWEB_DIR "" CACHE PATH "CivetWeb prefix")
set(STREAM_BOOST_DIR    "" CACHE PATH "Boost prefix")

# --------------------------------------------------------------------------- #
# Dependencies
# --------------------------------------------------------------------------- #
find_package(PkgConfig QUIET)

# GStreamer (WebRTC feature)
set(WEBRTC_FOUND FALSE)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_GSTREAMER QUIET
            IMPORTED_TARGET
            gstreamer-1.0
            gstreamer-app-1.0
            gstreamer-video-1.0
            gstreamer-sdp-1.0
            gstreamer-webrtc-1.0
    )
    if(PC_GSTREAMER_FOUND)
        set(WEBRTC_FOUND TRUE)
    endif()
endif()

# CivetWeb (HTTP backend)
set(CIVETWEB_FOUND FALSE)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(PC_CIVETWEB QUIET IMPORTED_TARGET civetweb)
endif()
if(PC_CIVETWEB_FOUND)
    set(CIVETWEB_FOUND TRUE)
    set(CIVETWEB_INCLUDE_DIRS ${PC_CIVETWEB_INCLUDE_DIRS})
    set(CIVETWEB_LIBRARIES    PkgConfig::PC_CIVETWEB)
else()
    find_path(CIVETWEB_INCLUDE_DIR NAMES civetweb.h HINTS ${STREAM_CIVETWEB_DIR} ENV CMAKE_PREFIX_PATH)
    find_library(CIVETWEB_C_LIB NAMES civetweb HINTS ${STREAM_CIVETWEB_DIR} ENV CMAKE_PREFIX_PATH)
    if(CIVETWEB_INCLUDE_DIR AND CIVETWEB_C_LIB)
        set(CIVETWEB_FOUND TRUE)
        set(CIVETWEB_INCLUDE_DIRS ${CIVETWEB_INCLUDE_DIR})
        set(CIVETWEB_LIBRARIES    ${CIVETWEB_C_LIB})
    endif()
endif()

# Boost (Beast) — headers + system lib
set(BOOST_FOUND FALSE)
find_path(BOOST_INCLUDE_DIR NAMES boost/beast/core.hpp HINTS ${STREAM_BOOST_DIR} ENV BOOST_ROOT)
if(BOOST_INCLUDE_DIR)
    set(BOOST_FOUND TRUE)
    set(BOOST_INCLUDE_DIRS ${BOOST_INCLUDE_DIR})
    find_library(BOOST_SYSTEM_LIB NAMES boost_system HINTS ${STREAM_BOOST_DIR} ENV BOOST_ROOT)
    set(BOOST_LIBRARIES ${BOOST_SYSTEM_LIB})
endif()
find_package(Threads QUIET)

# FFmpeg (compression feature)
ocv_check_modules(FFMPEG libavcodec libavformat libavutil libswscale QUIET)

# --------------------------------------------------------------------------- #
# Feature switches
# --------------------------------------------------------------------------- #
# WebRTC (independent of HTTP backend)
set(WEBRTC_ENABLED FALSE)
if(STREAM_WEBRTC STREQUAL "ON")
    if(WEBRTC_FOUND)
        set(WEBRTC_ENABLED TRUE)
    else()
        set(_post_warn_wrtc "Stream: STREAM_WEBRTC=ON but GStreamer WebRTC not found; disabling WebRTC.")
    endif()
elseif(STREAM_WEBRTC STREQUAL "AUTO" AND WEBRTC_FOUND)
    set(WEBRTC_ENABLED TRUE)
endif()

# Compression
set(STREAM_COMPRESSION_ENABLED FALSE)
if(STREAM_COMPRESSION STREQUAL "ON")
    if(FFMPEG_FOUND)
        set(STREAM_COMPRESSION_ENABLED TRUE)
    else()
        set(_post_warn_ff "Stream: STREAM_COMPRESSION=ON but FFmpeg not found; disabling compression.")
    endif()
elseif(STREAM_COMPRESSION STREQUAL "AUTO" AND FFMPEG_FOUND)
    set(STREAM_COMPRESSION_ENABLED TRUE)
endif()

# --------------------------------------------------------------------------- #
# HTTP backend selection
# AUTO preference: CIVETWEB -> BOOST -> MONGOOSE
# --------------------------------------------------------------------------- #
set(STREAM_SELECTED_HTTP_BACKEND "")
if(NOT STREAM_HTTP_BACKEND STREQUAL "AUTO")
    if(STREAM_HTTP_BACKEND STREQUAL "CIVETWEB" AND CIVETWEB_FOUND)
        set(STREAM_SELECTED_HTTP_BACKEND "CIVETWEB")
    elseif(STREAM_HTTP_BACKEND STREQUAL "BOOST" AND BOOST_FOUND)
        set(STREAM_SELECTED_HTTP_BACKEND "BOOST")
    elseif(STREAM_HTTP_BACKEND STREQUAL "MONGOOSE")
        set(STREAM_SELECTED_HTTP_BACKEND "MONGOOSE")
    else()
        set(_post_warn_backend "Stream: Requested HTTP backend '${STREAM_HTTP_BACKEND}' not available; falling back to AUTO.")
    endif()
endif()

if(STREAM_SELECTED_HTTP_BACKEND STREQUAL "")
    if(CIVETWEB_FOUND)
        set(STREAM_SELECTED_HTTP_BACKEND "CIVETWEB")
    elseif(BOOST_FOUND)
        set(STREAM_SELECTED_HTTP_BACKEND "BOOST")
    else()
        set(STREAM_SELECTED_HTTP_BACKEND "MONGOOSE")
    endif()
endif()

# Backend-specific wiring (HTTP only)
set(STREAM_HTTP_INCLUDES "")
set(STREAM_HTTP_LIBS "")

if(STREAM_SELECTED_HTTP_BACKEND STREQUAL "CIVETWEB")
    set(STREAM_HTTP_INCLUDES ${CIVETWEB_INCLUDE_DIRS})
    set(STREAM_HTTP_LIBS     ${CIVETWEB_LIBRARIES})

elseif(STREAM_SELECTED_HTTP_BACKEND STREQUAL "BOOST")
    set(STREAM_HTTP_INCLUDES ${BOOST_INCLUDE_DIRS})
    set(STREAM_HTTP_LIBS     ${BOOST_LIBRARIES})
    if(Threads_FOUND)
        list(APPEND STREAM_HTTP_LIBS Threads::Threads)
    endif()

elseif(STREAM_SELECTED_HTTP_BACKEND STREQUAL "MONGOOSE")
    set(MONGOOSE_VERSION "7.18")
    set(MONGOOSE_HASH "e50fe3c2d43cdd5ed96833aae88ad7b1")
    set(MONGOOSE_URL "https://github.com/cesanta/mongoose/archive/refs/tags/${MONGOOSE_VERSION}.zip")
    set(MONGOOSE_DOWNLOAD_DIR "${OpenCV_BINARY_DIR}/3rdparty/mongoose")
    set(MONGOOSE_UNPACK_DIR   "${MONGOOSE_DOWNLOAD_DIR}/mongoose-${MONGOOSE_VERSION}")
    set(STREAM_HTTP_SOURCES   "${MONGOOSE_UNPACK_DIR}/mongoose.c")
    set(STREAM_HTTP_INCLUDES  "${MONGOOSE_UNPACK_DIR}")
    ocv_download(
            FILENAME "${MONGOOSE_VERSION}.zip"
            URL "${MONGOOSE_URL}"
            HASH "${MONGOOSE_HASH}"
            DESTINATION_DIR "${MONGOOSE_DOWNLOAD_DIR}"
            UNPACK
    )
endif()

# --------------------------------------------------------------------------- #
# Targets
# --------------------------------------------------------------------------- #
set(STREAM_DEFINES "HAVE_STREAM_HTTP_${STREAM_SELECTED_HTTP_BACKEND}")
if(WEBRTC_ENABLED)
    list(APPEND STREAM_DEFINES "HAVE_STREAM_WEBRTC_GSTREAMER")
endif()
if(STREAM_COMPRESSION_ENABLED)
    list(APPEND STREAM_DEFINES "HAVE_STREAM_COMPRESSION")
endif()

ocv_define_module(stream opencv_core)

# make OCV_BUILD_TESTS visible to code if BUILD_TESTS=ON
if(BUILD_TESTS)
    list(APPEND STREAM_DEFINES "OCV_BUILD_TESTS")
endif()

if(STREAM_SELECTED_HTTP_BACKEND STREQUAL "MONGOOSE")
    target_sources(${the_module} PRIVATE ${STREAM_HTTP_SOURCES})
endif()

# HTTP backend includes/libs
if(STREAM_HTTP_INCLUDES)
    list(REMOVE_DUPLICATES STREAM_HTTP_INCLUDES)
    target_include_directories(${the_module} PRIVATE ${STREAM_HTTP_INCLUDES})
endif()
if(STREAM_HTTP_LIBS)
    target_link_libraries(${the_module} PRIVATE ${STREAM_HTTP_LIBS})
endif()

# WebRTC feature includes/libs
if(WEBRTC_ENABLED)
    if(PC_GSTREAMER_FOUND)
        target_link_libraries(${the_module} PRIVATE PkgConfig::PC_GSTREAMER)
    else()
        # Fallback: legacy variables if IMPORTED_TARGET wasn’t created
        target_include_directories(${the_module} PRIVATE ${PC_GSTREAMER_INCLUDE_DIRS})
        target_link_libraries(${the_module} PRIVATE ${PC_GSTREAMER_LIBRARIES})
        target_compile_options(${the_module} PRIVATE ${PC_GSTREAMER_CFLAGS_OTHER})
    endif()
endif()

# Compression libs
if(STREAM_COMPRESSION_ENABLED)
    target_link_libraries(${the_module} PRIVATE ${FFMPEG_LIBRARIES})
endif()

target_compile_definitions(${the_module} PRIVATE ${STREAM_DEFINES})

# Tests
if(BUILD_TESTS)
    function(add_stream_test name src)
        add_executable(${name} ${src})
        if(STREAM_SELECTED_HTTP_BACKEND STREQUAL "MONGOOSE")
            target_sources(${name} PRIVATE ${STREAM_HTTP_SOURCES})
        endif()
        if(STREAM_HTTP_INCLUDES)
            target_include_directories(${name} PRIVATE ${STREAM_HTTP_INCLUDES})
        endif()
        set(_test_libs ${the_module} ${STREAM_HTTP_LIBS} opencv_imgcodecs opencv_videoio)
        if(WEBRTC_ENABLED)
            if(PC_GSTREAMER_FOUND)
                list(APPEND _test_libs PkgConfig::PC_GSTREAMER)
            else()
                list(APPEND _test_libs ${PC_GSTREAMER_LIBRARIES})
            endif()
        endif()
        if(STREAM_COMPRESSION_ENABLED)
            list(APPEND _test_libs ${FFMPEG_LIBRARIES})
        endif()
        target_link_libraries(${name} PRIVATE ${_test_libs})
        message("TO ${name} appending private ${STREAM_DEFINES}")
        target_compile_definitions(${name} PRIVATE ${STREAM_DEFINES})
    endfunction()

    add_stream_test(opencv_test_stream_http      ${CMAKE_CURRENT_SOURCE_DIR}/test/http_test.cpp)
    add_stream_test(opencv_test_stream_websocket ${CMAKE_CURRENT_SOURCE_DIR}/test/websocket_test.cpp)
    add_stream_test(opencv_test_stream_raw    ${CMAKE_CURRENT_SOURCE_DIR}/test/raw_stream_test.cpp)
    add_stream_test(opencv_test_stream_fmp4    ${CMAKE_CURRENT_SOURCE_DIR}/test/fmp4_stream_test.cpp)
    add_stream_test(opencv_test_stream_webrtc    ${CMAKE_CURRENT_SOURCE_DIR}/test/webrtc_stream_test.cpp)

    # Add client impls only to the websocket test
    file(GLOB _ws_client_srcs CONFIGURE_DEPENDS
            "${CMAKE_CURRENT_SOURCE_DIR}/test/client/*.cpp")
    if(_ws_client_srcs)
        target_sources(opencv_test_stream_websocket PRIVATE ${_ws_client_srcs})
    endif()
endif()

# --------------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------------- #
set(_yn_CIVETWEB "NO")
set(_yn_BOOST "NO")
set(_yn_GST "NO")
set(_yn_FFMPEG "NO")
set(_yn_COMP "NO")
set(_yn_WEBRTC "NO")

if(CIVETWEB_FOUND)
    set(_yn_CIVETWEB "YES")
endif()
if(BOOST_FOUND)
    set(_yn_BOOST "YES")
endif()
if(WEBRTC_FOUND)
    set(_yn_GST "YES")
endif()
if(FFMPEG_FOUND)
    set(_yn_FFMPEG "YES")
endif()
if(STREAM_COMPRESSION_ENABLED)
    set(_yn_COMP "YES")
endif()
if(WEBRTC_ENABLED)
    set(_yn_WEBRTC "YES")
endif()

message(STATUS "\n\
Stream configuration summary:\n\
  HTTP backend preference  : ${STREAM_HTTP_BACKEND}\n\
  HTTP backend selected    : ${STREAM_SELECTED_HTTP_BACKEND}\n\
  CivetWeb found           : ${_yn_CIVETWEB}\n\
  Boost (Beast) found      : ${_yn_BOOST}\n\
  GStreamer (WebRTC) found : ${_yn_GST}\n\
  WebRTC enabled           : ${_yn_WEBRTC}\n\
  FFmpeg found             : ${_yn_FFMPEG}\n\
  Compression (FFmpeg)     : ${_yn_COMP}\n")

if(DEFINED _post_warn_backend)
    message(WARNING "${_post_warn_backend}")
endif()
if(DEFINED _post_warn_wrtc)
    message(WARNING "${_post_warn_wrtc}")
endif()
if(DEFINED _post_warn_ff)
    message(WARNING "${_post_warn_ff}")
endif()