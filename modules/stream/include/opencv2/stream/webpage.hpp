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
static const char* const kContentTypeHtml = "text/html; charset=utf-8";
static const char* const kContentTypeJs   = "application/javascript; charset=utf-8";
static const char* const kContentTypeCss  = "text/css; charset=utf-8";

// -----------------------------------------------------------------------------
// Dynamic controls schema (optional)
// -----------------------------------------------------------------------------

// Parameter type rendered in the UI; encoded/transported as strings over WS.
enum class ParamType {
    Number,   // numeric text or slider; honor min/max/step if provided
    Integer,  // integer spinner; honor min/max/step if provided
    Boolean,  // checkbox/toggle (values "true"/"false")
    Enum,     // dropdown (use options[])
    Text      // freeform text input
};

// Option for Enum
struct ParamOption {
    const char* value;   // serialized value
    const char* label;   // human label
};

// One control in the panel. All string pointers must remain valid while
// generating the page; they are embedded into the output.
struct ParamSpec {
    const char* id;            // stable machine id (used in WS messages)
    const char* label;         // human label
    ParamType   type;

    // Default/current value as string. The UI echoes this on load.
    const char* defaultValue;

    // Optional numeric hints (used when type is Number/Integer)
    double      minValue;      // ignored if minValue > maxValue
    double      maxValue;
    double      step;          // ignored if <= 0

    // Optional enum options (only used when type == Enum)
    const ParamOption* options;
    std::size_t        numOptions;

    // Optional help text (tooltip), group/category name, and read-only flag
    const char* help;          // may be nullptr
    const char* group;         // may be nullptr
    bool        readOnly;      // default false
};

// -----------------------------------------------------------------------------
// HTML pages
// -----------------------------------------------------------------------------
// All functions write with snprintf-style assembly and return the total number
// of bytes that would have been written (excluding the trailing '\0').
// If the return value >= cap, the output was truncated.

// Root index with links to sub-pages.
CV_EXPORTS std::size_t root_page(
    char* dst, std::size_t cap,
    const char* mount_path,           // e.g. "/"
    const char* title                 // e.g. "OpenCV Stream"
);

// Simple embedded player (no controls), pointing to a media path:
//   "/ws/raw"  (raw frames), "/ws/fmp4" (MSE), or "/webrtc" (signaling)
CV_EXPORTS std::size_t embedded_video_page(
    char* dst, std::size_t cap,
    const char* stream_path,
    const char* title
);

// --- Default webview ---------------------------------------------------------
// Back-compat (no controls):
CV_EXPORTS std::size_t webview_default_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_url,               // media or signaling URL
    const char* title
);

// With optional live controls (sent/received over a dedicated control WS).
CV_EXPORTS std::size_t webview_default_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_url,               // media or signaling URL
    const char* ws_ctrl_url,          // control channel WS; set nullptr to omit panel
    const ParamSpec* controls,        // schema array; may be nullptr
    std::size_t num_controls,
    const char* title
);

// --- Jupyter-friendly webview ------------------------------------------------
// Back-compat (no controls):
CV_EXPORTS std::size_t webview_jupyter_page(
    char* dst, std::size_t cap,
    const char* mount_path,           // base href in notebooks
    const char* ws_url,               // media or signaling URL
    const char* token                 // optional auth/token (can be empty string)
);

// With optional live controls:
CV_EXPORTS std::size_t webview_jupyter_page(
    char* dst, std::size_t cap,
    const char* mount_path,
    const char* ws_url,
    const char* ws_ctrl_url,          // control channel WS; set nullptr to omit panel
    const ParamSpec* controls,
    std::size_t num_controls,
    const char* token                 // optional auth/token
);

// -----------------------------------------------------------------------------
// JavaScript assets (snprintf-style; set Content-Type to kContentTypeJs)
// -----------------------------------------------------------------------------

// Minimal WebRTC helper used by the default/jupyter pages.
// `signaling_path` is a relative or absolute WS/HTTP endpoint for SDP/ICE.
CV_EXPORTS std::size_t js_webrtc(
    char* dst, std::size_t cap,
    const char* signaling_path
);

// MSE (fMP4) player helper: connects to `ws_media_path` and appends segments.
CV_EXPORTS std::size_t js_fmp4(
    char* dst, std::size_t cap,
    const char* ws_media_path
);

// Raw frame viewer helper (WebSocket-driven).
CV_EXPORTS std::size_t js_raw(
    char* dst, std::size_t cap,
    const char* ws_media_path
);

// UI runtime for the dynamic controls panel. It renders from ParamSpec
// (embedded by the HTML generator) and syncs changes over `ws_ctrl_path`.
// The same script also listens for server-originated updates to reflect
// parameter changes occurring elsewhere in the pipeline.
CV_EXPORTS std::size_t js_controls_runtime(
    char* dst, std::size_t cap,
    const char* ws_ctrl_path       // e.g. "/ws/controls"; may be nullptr to no-op
);

// -----------------------------------------------------------------------------
// Optional CSS (tiny default styling for the controls panel & player shell).
// -----------------------------------------------------------------------------
CV_EXPORTS std::size_t css_default_theme(
    char* dst, std::size_t cap
);

} // namespace webpage
} // namespace stream
} // namespace cv

#endif // OPENCV_WEBPAGE_HPP
