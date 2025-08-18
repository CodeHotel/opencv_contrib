set(the_description "OpenCV Stream Module: Real-time streaming API for live video/image feeds")

find_package(PkgConfig REQUIRED)

pkg_check_modules(MICROHTTPD IMPORTED_TARGET libmicrohttpd)
pkg_check_modules(AVCODEC IMPORTED_TARGET libavcodec)
pkg_check_modules(AVFORMAT IMPORTED_TARGET libavformat)
pkg_check_modules(AVUTIL IMPORTED_TARGET libavutil)
pkg_check_modules(SWSCALE IMPORTED_TARGET libswscale)

if(NOT (MICROHTTPD_FOUND AND AVCODEC_FOUND AND AVFORMAT_FOUND AND AVUTIL_FOUND AND SWSCALE_FOUND))
    ocv_module_disable(stream)
    return()
endif()

ocv_define_module(stream opencv_core)

ocv_target_link_libraries(${the_module} PRIVATE
    PkgConfig::MICROHTTPD
    PkgConfig::AVCODEC
    PkgConfig::AVFORMAT
    PkgConfig::AVUTIL
    PkgConfig::SWSCALE
)


add_executable(test_stream test/test_stream.cpp)

target_link_libraries(test_stream PRIVATE
        PkgConfig::MICROHTTPD
        PkgConfig::AVCODEC
        PkgConfig::AVFORMAT
        PkgConfig::AVUTIL
        PkgConfig::SWSCALE
)

