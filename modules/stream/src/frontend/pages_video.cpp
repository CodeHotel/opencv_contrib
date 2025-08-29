#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"
#include <sstream>
#include <string>

namespace cv {
namespace stream {
namespace webpage {

std::size_t embedded_video_page(char* dst, std::size_t cap,
                                const char* stream_path,
                                const char* title) {
    // Heuristic selection
    std::string sp = stream_path ? stream_path : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;

    return embedded_video_page(dst, cap, stream_path, client, title);
}

std::size_t embedded_video_page(char* dst, std::size_t cap,
                                const char* stream_path,
                                VideoClient client,
                                const char* title) {
    using namespace internal;
    const std::string sp = stream_path ? stream_path : "";
    std::ostringstream os;
    os << shell_head(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'><div class='cv-col'>";
    if (client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' class='cv-video' autoplay playsinline controls></video>";
        os << "<script>" << build_js_webrtc(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' class='cv-video' autoplay playsinline controls muted></video>";
        os << "<script>" << build_js_fmp4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else { // Raw
        os << "<canvas id='cv-raw-canvas' class='cv-canvas'></canvas>";
        os << "<script>" << build_js_raw(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }
    os << "</div></div>" << shell_tail();
    return copy_to(dst, cap, os.str());
}

// Default webview (no controls)
std::size_t webview_default_page(char* dst, std::size_t cap,
                                 const char* mount_path,
                                 const char* ws_url,
                                 const char* title) {
    (void)mount_path;
    std::string sp = ws_url ? ws_url : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;
    return embedded_video_page(dst, cap, ws_url, client, title);
}

// Default webview (with controls)
std::size_t webview_default_page(char* dst, std::size_t cap,
                                 const char* mount_path,
                                 const char* ws_url,
                                 const char* ws_ctrl_url,
                                 const ParamSpec* controls,
                                 std::size_t num_controls,
                                 const char* title) {
    (void)mount_path;
    using namespace internal;

    std::string sp = ws_url ? ws_url : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;

    // Build control spec JSON
    std::ostringstream spec;
    spec << "[";
    for (std::size_t i=0;i<num_controls;i++) {
        const ParamSpec& p = controls[i];
        spec << (i? ",":"") << "{";
        spec << "\"id\":\"" << js_escape(p.id?p.id:"") << "\",";
        spec << "\"label\":\"" << js_escape(p.label?p.label:p.id?p.id:"") << "\",";
        const char* t = "Text";
        switch (p.type) {
            case ParamType::Number:  t="Number"; break;
            case ParamType::Integer: t="Integer"; break;
            case ParamType::Boolean: t="Boolean"; break;
            case ParamType::Enum:    t="Enum"; break;
            case ParamType::Text:    t="Text"; break;
        }
        spec << "\"type\":\"" << t << "\",";
        spec << "\"defaultValue\":\"" << js_escape(p.defaultValue?p.defaultValue:"") << "\",";
        spec << "\"minValue\":" << p.minValue << ",";
        spec << "\"maxValue\":" << p.maxValue << ",";
        spec << "\"step\":" << p.step << ",";
        spec << "\"readOnly\":" << (p.readOnly?"true":"false") << ",";
        spec << "\"help\":\"" << js_escape(p.help?p.help:"") << "\",";
        spec << "\"group\":\"" << js_escape(p.group?p.group:"") << "\",";
        spec << "\"options\":[";
        for (std::size_t k=0;k<p.numOptions;k++) {
            const ParamOption& o = p.options[k];
            if (k) spec << ",";
            spec << "{\"value\":\"" << js_escape(o.value?o.value:"") << "\",\"label\":\"" << js_escape(o.label?o.label:"") << "\"}";
        }
        spec << "]}";
    }
    spec << "]";

    std::ostringstream os;
    os << shell_head(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'>"
       << "  <div class='cv-col'>";

    if (client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' class='cv-video' autoplay playsinline controls></video>";
        os << "<script>" << build_js_webrtc(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' class='cv-video' autoplay playsinline controls muted></video>";
        os << "<script>" << build_js_fmp4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' class='cv-canvas'></canvas>";
        os << "<script>" << build_js_raw(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }

    os << "  </div>";
    os << "  <div class='cv-side'><div id='cv-controls'></div></div>";
    os << "</div>";

    if (num_controls > 0) {
        const std::string ctrl = ws_ctrl_url ? ws_ctrl_url : "";
        os << "<script>window.__CV_CTRL_SPEC=" << spec.str() << ";</script>";
        os << "<script>" << build_js_controls_rt(ctrl) << "</script>";
        os << "<script>__cv_controls_start('cv-controls','" << js_escape(ctrl) << "');</script>";
    }

    os << shell_tail();
    return copy_to(dst, cap, os.str());
}

} // namespace webpage
} // namespace stream
} // namespace cv
