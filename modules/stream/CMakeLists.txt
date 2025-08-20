#
# CMakeLists.txt for the OpenCV Stream module
#

set(the_description "OpenCV Stream Module: Real-time streaming API for live video/image feeds")

# Main switch to enable/disable the module
ocv_option(WITH_STREAM "Build OpenCV stream module" ON)
if(NOT WITH_STREAM)
    ocv_module_disable(stream)
    return()
endif()

# --- Check for hard dependency: FFmpeg ---
ocv_check_modules(FFMPEG libavcodec libavformat libavutil libswscale)
if(NOT FFMPEG_FOUND)
    message(STATUS "Disabling stream module: FFmpeg libraries are required.")
    ocv_module_disable(stream)
    return()
endif()

# --- Unified Backend Search Macro ---
macro(stream_find_backend _name _cmake_name _pkgconfig_name _header_name _lib_name)
    set(${_name}_FOUND FALSE)
    set(${_name}_INCLUDE_DIRS "")
    set(${_name}_LIBRARIES "")
    set(${_name}_HINTS "")
    if(STREAM_${_name}_DIR)
        set(${_name}_HINTS HINTS "${STREAM_${_name}_DIR}")
    endif()

    set(_found_in_macro FALSE)

    # Priority 1: Try native CMake find_package()
    if(NOT _found_in_macro AND NOT "${_cmake_name}" STREQUAL "")
        find_package(${_cmake_name} QUIET ${${_name}_HINTS})
        if(${_cmake_name}_FOUND)
            set(${_name}_FOUND TRUE)
            set(${_name}_INCLUDE_DIRS "${${_cmake_name}_INCLUDE_DIRS}")
            set(${_name}_LIBRARIES "${${_cmake_name}_LIBRARIES}")
            set(_found_in_macro TRUE)
        endif()
    endif()

    # Priority 2: Try pkg-config
    if(NOT _found_in_macro AND NOT "${_pkgconfig_name}" STREQUAL "")
        ocv_check_modules(OCV_TEMP_${_name} ${_pkgconfig_name} QUIET)
        if(OCV_TEMP_${_name}_FOUND)
            set(${_name}_FOUND TRUE)
            set(${_name}_INCLUDE_DIRS "${OCV_TEMP_${_name}_INCLUDE_DIRS}")
            set(${_name}_LIBRARIES "${OCV_TEMP_${_name}_LIBRARIES}")
            set(_found_in_macro TRUE)
        endif()
    endif()

    # Priority 3: Try manual path/library search
    if(NOT _found_in_macro AND NOT "${_header_name}" STREQUAL "")
        find_path(${_name}_INCLUDE_DIR NAMES ${_header_name} ${${_name}_HINTS})
        if(NOT "${_lib_name}" STREQUAL "")
            find_library(${_name}_LIBRARY NAMES ${_lib_name} ${${_name}_HINTS})
        else() # For header-only libs
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

# --- Expose manual path options and proactively check for all backends ---
set(STREAM_ASIO_DIR "" CACHE PATH "Path to a custom Asio installation root")
set(STREAM_CIVETWEB_DIR "" CACHE PATH "Path to a custom Civetweb installation root")
set(STREAM_MICROHTTPD_DIR "" CACHE PATH "Path to a custom microhttpd installation root")

message(STATUS "Stream: Checking for available backends...")
stream_find_backend(ASIO Asio "" asio.hpp "")
stream_find_backend(CIVETWEB civetweb civetweb civetweb.h civetweb)
stream_find_backend(MICROHTTPD "" libmicrohttpd microhttpd.h microhttpd)

if(ASIO_FOUND)
    message(STATUS "  - Asio:        YES")
else()
    message(STATUS "  - Asio:        NO")
endif()
if(CIVETWEB_FOUND)
    message(STATUS "  - Civetweb:    YES")
else()
    message(STATUS "  - Civetweb:    NO")
endif()
if(MICROHTTPD_FOUND)
    message(STATUS "  - microhttpd:  YES")
else()
    message(STATUS "  - microhttpd:  NO")
endif()


# --- Configure the HTTP backend ---
set(STREAM_BACKEND "AUTO" CACHE STRING "HTTP backend for stream module (AUTO, ASIO, CIVETWEB, MICROHTTPD, MONGOOSE)")
if(NOT STREAM_BACKEND)
    set(STREAM_BACKEND "AUTO")
endif()
string(TOUPPER "${STREAM_BACKEND}" STREAM_BACKEND)
message(STATUS "Stream: User backend preference: ${STREAM_BACKEND}")

set(STREAM_SELECTED_BACKEND "")
set(STREAM_BACKEND_SOURCES "")
set(STREAM_BACKEND_INCLUDES "")
set(STREAM_BACKEND_LIBS "")

# 1. Logic to select backend and set final variables
if(NOT STREAM_BACKEND STREQUAL "AUTO")
    if(STREAM_BACKEND STREQUAL "ASIO" AND ASIO_FOUND)
        set(STREAM_SELECTED_BACKEND "ASIO")
    elseif(STREAM_BACKEND STREQUAL "CIVETWEB" AND CIVETWEB_FOUND)
        set(STREAM_SELECTED_BACKEND "CIVETWEB")
    elseif(STREAM_BACKEND STREQUAL "MICROHTTPD" AND MICROHTTPD_FOUND)
        set(STREAM_SELECTED_BACKEND "MICROHTTPD")
    elseif(STREAM_BACKEND STREQUAL "MONGOOSE")
        set(STREAM_SELECTED_BACKEND "MONGOOSE")
    else()
        if(NOT STREAM_BACKEND STREQUAL "MONGOOSE")
            message(WARNING "Stream: Invalid or not-found backend '${STREAM_BACKEND}' specified. Using automatic detection.")
        endif()
    endif()
endif()

if(STREAM_SELECTED_BACKEND STREQUAL "")
    if(ASIO_FOUND)
        set(STREAM_SELECTED_BACKEND "ASIO")
    elseif(CIVETWEB_FOUND)
        set(STREAM_SELECTED_BACKEND "CIVETWEB")
    elseif(MICROHTTPD_FOUND)
        set(STREAM_SELECTED_BACKEND "MICROHTTPD")
    else()
        set(STREAM_SELECTED_BACKEND "MONGOOSE")
    endif()
endif()

# Set the final variables based on the selected backend
if(STREAM_SELECTED_BACKEND STREQUAL "ASIO")
    set(STREAM_BACKEND_INCLUDES ${ASIO_INCLUDE_DIRS})
    set(STREAM_BACKEND_LIBS ${ASIO_LIBRARIES})
elseif(STREAM_SELECTED_BACKEND STREQUAL "CIVETWEB")
    set(STREAM_BACKEND_INCLUDES ${CIVETWEB_INCLUDE_DIRS})
    set(STREAM_BACKEND_LIBS ${CIVETWEB_LIBRARIES})
elseif(STREAM_SELECTED_BACKEND STREQUAL "MICROHTTPD")
    set(STREAM_BACKEND_INCLUDES ${MICROHTTPD_INCLUDE_DIRS})
    set(STREAM_BACKEND_LIBS ${MICROHTTPD_LIBRARIES})
elseif(STREAM_SELECTED_BACKEND STREQUAL "MONGOOSE")
    set(MONGOOSE_VERSION "7.18")
    set(MONGOOSE_HASH "e50fe3c2d43cdd5ed96833aae88ad7b1")
    set(MONGOOSE_URL "https://github.com/cesanta/mongoose/archive/refs/tags/${MONGOOSE_VERSION}.zip")

    set(MONGOOSE_DOWNLOAD_DIR "${OpenCV_BINARY_DIR}/3rdparty/mongoose")
    set(MONGOOSE_UNPACK_DIR "${MONGOOSE_DOWNLOAD_DIR}/mongoose-${MONGOOSE_VERSION}")

    set(STREAM_BACKEND_SOURCES "${MONGOOSE_UNPACK_DIR}/mongoose.c")
    set(STREAM_BACKEND_INCLUDES "${MONGOOSE_UNPACK_DIR}")

    # CORRECTED: Use only the arguments supported by the ocv_download function.
    ocv_download(
            FILENAME "${MONGOOSE_VERSION}.zip"
            URL "${MONGOOSE_URL}"
            HASH "${MONGOOSE_HASH}"
            DESTINATION_DIR "${MONGOOSE_DOWNLOAD_DIR}"
            UNPACK # This is a boolean flag to trigger extraction
    )
endif()

set(STREAM_BACKEND_DEFINITIONS "HAVE_STREAM_BACKEND_${STREAM_SELECTED_BACKEND}")
message(STATUS "OpenCV Stream: Using '${STREAM_SELECTED_BACKEND}' backend.")

# --- Define the module and its dependencies ---
ocv_define_module(stream opencv_core
        SOURCES ${STREAM_BACKEND_SOURCES}
        PRIVATE_DEFINITIONS ${STREAM_BACKEND_DEFINITIONS}
)

if(STREAM_BACKEND_INCLUDES)
    target_include_directories(${the_module} PRIVATE ${STREAM_BACKEND_INCLUDES})
endif()

ocv_target_link_libraries(${the_module} PRIVATE
        ${FFMPEG_LIBRARIES}
        ${STREAM_BACKEND_LIBS}
)

# --- Define the test executable ---
if(BUILD_TESTS)
    add_executable(test_stream test/test_stream.cpp ${STREAM_BACKEND_SOURCES})
    if(STREAM_BACKEND_INCLUDES)
        target_include_directories(test_stream PRIVATE ${STREAM_BACKEND_INCLUDES})
    endif()
    target_link_libraries(test_stream PRIVATE
            ${the_module}
            ${FFMPEG_LIBRARIES}
            ${STREAM_BACKEND_LIBS}
    )
endif()