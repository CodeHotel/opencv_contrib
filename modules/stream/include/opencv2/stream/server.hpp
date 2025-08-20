#ifndef OPENCV_SERVER_HPP
#define OPENCV_SERVER_HPP

#if defined(HAVE_STREAM_BACKEND_ASIO)
    #include <asio.hpp>
#elif defined(HAVE_STREAM_BACKEND_CIVETWEB)
    #include <civetweb.h>
#elif defined(HAVE_STREAM_BACKEND_MICROHTTPD)
    #include <microhttpd.h>
#elif defined(HAVE_STREAM_BACKEND_MONGOOSE)
    #include "mongoose.h"
#else
    #error "No stream backend was selected during the CMake build!"
#endif

#endif //OPENCV_SERVER_HPP