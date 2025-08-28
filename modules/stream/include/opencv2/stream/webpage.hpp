#ifndef OPENCV_WEBPAGE_HPP
#define OPENCV_WEBPAGE_HPP

#include "opencv2/core/cvdef.h"
#include <cstddef>
#include <cstdint>

namespace cv {
namespace stream {
namespace webpage {

// MIME helpers
static const char* const kContentTypeHtml = "text/html; charset=utf-8";
static const char* const kContentTypeJs   = "application/javascript; charset=utf-8";

// HTML pages ---------------------------------------------------------------
// All functions write with snprintf-style assembly and return the total
// number of bytes that would have been written (excluding the trailing '\0').
// If the return value >= cap, the output was truncated.

CV_EXPORTS std::size_t root_page(
    char* dst, std::size_t cap,
    const char* mount_path,           // e.g. "/"
    const char* title                 // e.g. "OpenCV Stream"
);

CV_EXPORTS std::size_t embedded_video_page(
    char* dst, std::size_t cap,
    const char* stream_path,          // e.g. "/ws/raw" or "/ws/fmp4" or "/webrtc"
    const char* title                 // page title
);

CV_EXPORTS std::size_t webview_default_page(
    char* dst, std::size_t cap,
    const char* mount_path,           // base href for assets/links
    const char* ws_url,               // websocket signaling or data URL
    const char* title                 // page title
);

CV_EXPORTS std::size_t webview_jupyter_page(
    char* dst, std::size_t cap,
    const char* mount_path,           // base href for assets/links in notebooks
    const char* ws_url,               // websocket signaling or data URL
    const char* token                 // optional auth/token (can be empty string)
);

// JavaScript assets --------------------------------------------------------
// Same snprintf contract; callers set Content-Type to kContentTypeJs.

CV_EXPORTS std::size_t js_webrtc(
    char* dst, std::size_t cap,
    const char* signaling_path        // e.g. "/ws/webrtc"
);

CV_EXPORTS std::size_t js_fmp4(
    char* dst, std::size_t cap,
    const char* ws_media_path         // e.g. "/ws/fmp4"
);

CV_EXPORTS std::size_t js_raw(
    char* dst, std::size_t cap,
    const char* ws_media_path         // e.g. "/ws/raw"
);

} // namespace webpage
} // namespace stream
} // namespace cv

#endif // OPENCV_WEBPAGE_HPP
