#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"
#include <sstream>
#include <string>

namespace cv {
namespace stream {
namespace webpage {

std::size_t webview_media_page(char* dst, std::size_t cap,
                               const char* mount_path,
                               const char* ws_video_url,
                               VideoClient video_client,
                               const char* image_url,
                               int refresh_ms,
                               const char* ws_ctrl_url,
                               const ParamSpec* controls,
                               std::size_t num_controls,
                               const char* title) {
    (void)mount_path;
    using namespace internal;

    // controls spec
    std::ostringstream spec;
    spec << "[";
    for (std::size_t i=0;i<num_controls;i++) {
        const ParamSpec& p = controls[i];
        if (i) spec << ",";
        spec << "{";
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

    const std::string sp = ws_video_url ? ws_video_url : "";
    const std::string img = image_url ? image_url : "/image";
    const std::string ctrl = ws_ctrl_url ? ws_ctrl_url : "";

    std::ostringstream os;
    os << shell_head(title ? title : "OpenCV Stream")
       << "<div class='cv-wrap'>"
       << "  <div class='cv-col'>"
       << "    <div class='cv-tabs'>"
       << "      <div class='cv-tab active' id='tab-video'>Video</div>"
       << "      <div class='cv-tab' id='tab-image'>Image</div>"
       << "    </div>"
       << "    <div id='panel-video'>";

    if (video_client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' class='cv-video' autoplay playsinline controls></video>";
        os << "<script>" << build_js_webrtc(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (video_client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' class='cv-video' autoplay playsinline controls muted></video>";
        os << "<script>" << build_js_fmp4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' class='cv-canvas'></canvas>";
        os << "<script>" << build_js_raw(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }

    os << "    </div>"
       << "    <div id='panel-image' style='display:none'>"
       << "      <img id='cv-img' class='cv-img'>"
       << "    </div>"
       << "  </div>"
       << "  <div class='cv-side'><div id='cv-controls'></div></div>"
       << "</div>";

    // tabs script
    os << "<script>(() => {"
          "const tv=document.getElementById('tab-video');"
          "const ti=document.getElementById('tab-image');"
          "const pv=document.getElementById('panel-video');"
          "const pi=document.getElementById('panel-image');"
          "function act(a){ if(a==='v'){tv.classList.add('active');ti.classList.remove('active');pv.style.display='';pi.style.display='none';}"
          "else{ti.classList.add('active');tv.classList.remove('active');pi.style.display='';pv.style.display='none';}}"
          "tv.onclick=()=>act('v'); ti.onclick=()=>act('i');"
          "})();</script>";

    // image live JS
    os << "<script>" << build_js_img_live(img, refresh_ms) << "</script>";
    os << "<script>__cv_img_live('cv-img','" << js_escape(img) << "'," << refresh_ms << ");</script>";

    // controls
    if (num_controls > 0) {
        os << "<script>window.__CV_CTRL_SPEC=" << spec.str() << ";</script>";
        os << "<script>" << build_js_controls_rt(ctrl) << "</script>";
        os << "<script>__cv_controls_start('cv-controls','" << js_escape(ctrl) << "');</script>";
    }

    os << shell_tail();
    return internal::copy_to(dst, cap, os.str());
}

} // namespace webpage
} // namespace stream
} // namespace cv
