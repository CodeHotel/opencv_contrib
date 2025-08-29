#ifndef OPENCV_WEBPAGE_HPP
#define OPENCV_WEBPAGE_HPP

#include "opencv2/core/cvdef.h"
#include <cstddef>
#include <cstdint>

namespace cv {
namespace stream {
namespace webpage {

// -----------------------------------------------------------------------------
// MIME helpers
// -----------------------------------------------------------------------------
CV_EXPORTS extern const char* const kContentTypeHtml;
CV_EXPORTS extern const char* const kContentTypeJs;
CV_EXPORTS extern const char* const kContentTypeCss;

// -----------------------------------------------------------------------------
// Dynamic controls schema (optional)
// -----------------------------------------------------------------------------

enum class ParamType {
    Number,
    Integer,
    Boolean,
    Enum,
    Text
};

struct ParamOption {
    const char* value;
    const char* label;
};

struct ParamSpec {
    const char* id;
    const char* label;
    ParamType   type;

    const char* defaultValue;

    double      minValue;    // ignored if minValue > maxValue
    double      maxValue;
    double      step;        // ignored if <= 0

    const ParamOption* options;
    std::size_t        numOptions;

    const char* help;        // nullable
    const char* group;       // nullable
    bool        readOnly;    // default false
};

// -----------------------------------------------------------------------------
// Media selection
// -----------------------------------------------------------------------------

// Which client-side runtime to use for the <video> element.
enum class VideoClient {
    Auto,   // pick based on stream_path scheme/heuristics
    WebRTC, // use js_webrtc()
    Fmp4,   // use js_fmp4()
    Raw     // use js_raw()
};

// Which media presentation style to render in a "webview" page.
enum class MediaView {
    VideoOnly,      // show only video
    ImageOnly,      // show only image
    TabsVideoImage  // tabs to switch between video and image
};

// -----------------------------------------------------------------------------
// HTML pages (snprintf-style; return would-be length, truncation if >= cap)
// -----------------------------------------------------------------------------

// Root index.
CV_EXPORTS std::size_t root_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* title
);

// Embedded VIDEO page (back-compat).
CV_EXPORTS std::size_t embedded_video_page(
    char* dst, std::size_t cap,
    const char* stream_path,
    const char* title
);

// Embedded VIDEO page with explicit JS client selection.
CV_EXPORTS std::size_t embedded_video_page(
    char* dst, std::size_t cap,
    const char* stream_path,
    VideoClient client,           // WebRTC / Fmp4 / Raw / Auto
    const char* title
);

// Embedded IMAGE page (simple <img>).
CV_EXPORTS std::size_t embedded_image_page(
    char* dst, std::size_t cap,
    const char* image_url,
    const char* title
);

// Embedded IMAGE page with auto-refresh.
CV_EXPORTS std::size_t embedded_image_page(
    char* dst, std::size_t cap,
    const char* image_url,
    int refresh_ms,               // <=0 disables auto-refresh
    const char* title
);

// --- Default webview ---------------------------------------------------------

// Back-compat (video-only, no controls).
CV_EXPORTS std::size_t webview_default_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_url,
    const char* title
);

// Video webview with controls.
CV_EXPORTS std::size_t webview_default_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_url,
    const char* ws_ctrl_url,      // nullable to omit controls
    const ParamSpec* controls,    // nullable
    std::size_t num_controls,
    const char* title
);

// Image webview (image-only; no controls).
CV_EXPORTS std::size_t webview_image_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* image_url,
    int refresh_ms,               // <=0 disables auto-refresh
    const char* title
);

// Combined media webview: tabs to switch between video and image.
// Uses VideoClient to select the video JS.
CV_EXPORTS std::size_t webview_media_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_video_url,     // media or signaling URL for video
    VideoClient video_client,     // WebRTC/Fmp4/Raw/Auto
    const char* image_url,        // still or MJPEG snapshot endpoint
    int refresh_ms,               // <=0 disables image auto-refresh
    const char* ws_ctrl_url,      // nullable controls WS
    const ParamSpec* controls,    // nullable
    std::size_t num_controls,
    const char* title
);

// --- Jupyter-friendly webview ------------------------------------------------

// Back-compat (video-only).
CV_EXPORTS std::size_t webview_jupyter_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_url
);

// Image-only jupyter webview.
CV_EXPORTS std::size_t webview_jupyter_image_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* image_url,
    int refresh_ms
);

// Combined media jupyter webview (tabs).
CV_EXPORTS std::size_t webview_jupyter_media_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_video_url,
    VideoClient video_client,
    const char* image_url,
    int refresh_ms,
    const char* ws_ctrl_url,      // nullable
    const ParamSpec* controls,    // nullable
    std::size_t num_controls
);

// -----------------------------------------------------------------------------
// JavaScript assets (snprintf-style; set Content-Type to kContentTypeJs)
// -----------------------------------------------------------------------------

CV_EXPORTS std::size_t js_webrtc(
    char* dst, std::size_t cap,
    const char* signaling_path
);

CV_EXPORTS std::size_t js_fmp4(
    char* dst, std::size_t cap,
    const char* ws_media_path
);

CV_EXPORTS std::size_t js_raw(
    char* dst, std::size_t cap,
    const char* ws_media_path
);

// Live image helper: updates <img> by cache-busting the URL every refresh_ms.
CV_EXPORTS std::size_t js_image_live(
    char* dst, std::size_t cap,
    const char* image_url,
    int refresh_ms                 // <=0 means one-shot
);

// UI runtime for dynamic controls.
CV_EXPORTS std::size_t js_controls_runtime(
    char* dst, std::size_t cap,
    const char* ws_ctrl_path
);

// -----------------------------------------------------------------------------
// Optional CSS
// -----------------------------------------------------------------------------
CV_EXPORTS std::size_t css_default_theme(
    char* dst, std::size_t cap
);

} // namespace webpage
} // namespace stream
} // namespace cv

#endif // OPENCV_WEBPAGE_HPP
