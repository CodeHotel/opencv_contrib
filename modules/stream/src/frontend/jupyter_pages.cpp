#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"
#include <sstream>
#include <string>

namespace cv {
namespace stream {
namespace webpage {

std::size_t webview_jupyter_page(char* dst, std::size_t cap,
                                 const char* mount_path,
                                 const char* ws_url) {
    (void)mount_path;
    using namespace internal;
    std::string sp = ws_url ? ws_url : "";
    VideoClient client = VideoClient::Raw;
    if (sp.find("webrtc") != std::string::npos) client = VideoClient::WebRTC;
    else if (sp.find("fmp4") != std::string::npos) client = VideoClient::Fmp4;

    std::ostringstream os;
    os << "<!doctype html><meta charset='utf-8'><style>body{margin:0;background:#000}</style>";
    if (client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' style='width:100%;height:auto' autoplay playsinline controls></video>";
        os << "<script>" << build_js_webrtc(sp) << "</script>";
        os << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' style='width:100%;height:auto' autoplay playsinline controls muted></video>";
        os << "<script>" << build_js_fmp4(sp) << "</script>";
        os << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' style='width:100%;height:auto;background:#000;display:block'></canvas>";
        os << "<script>" << build_js_raw(sp) << "</script>";
        os << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }
    return internal::copy_to(dst, cap, os.str());
}

std::size_t webview_jupyter_image_page(char* dst, std::size_t cap,
                                       const char* mount_path,
                                       const char* image_url,
                                       int refresh_ms) {
    (void)mount_path;
    using namespace internal;
    std::ostringstream os;
    os << "<!doctype html><meta charset='utf-8'><style>body{margin:0;background:#000}</style>"
       << "<img id='cv-img' style='max-width:100%;display:block;margin:0 auto'>"
       << "<script>" << build_js_img_live(image_url?image_url:"/image", refresh_ms) << "</script>"
       << "<script>__cv_img_live('cv-img','" << js_escape(image_url?image_url:"/image") << "'," << refresh_ms << ");</script>";
    return copy_to(dst, cap, os.str());
}

std::size_t webview_jupyter_media_page(char* dst, std::size_t cap,
                                       const char* mount_path,
                                       const char* ws_video_url,
                                       VideoClient video_client,
                                       const char* image_url,
                                       int refresh_ms,
                                       const char* ws_ctrl_url,
                                       const ParamSpec* controls,
                                       std::size_t num_controls) {
    (void)mount_path; (void)ws_ctrl_url; (void)controls; (void)num_controls;
    using namespace internal;
    const std::string sp = ws_video_url ? ws_video_url : "";
    const std::string img = image_url ? image_url : "/image";
    std::ostringstream os;
    os << "<!doctype html><meta charset='utf-8'><style>body{margin:0;background:#000;color:#fff;font-family:sans-serif} .t{display:flex;gap:8px;padding:8px} .b{padding:6px 10px;border:1px solid #333;border-radius:999px;cursor:pointer} .a{background:#222}</style>"
       << "<div class='t'><div class='b a' id='tv'>Video</div><div class='b' id='ti'>Image</div></div>"
       << "<div id='pv'>";
    if (video_client == VideoClient::WebRTC) {
        os << "<video id='cv-webrtc-video' style='width:100%;height:auto' autoplay playsinline controls></video>"
           << "<script>" << build_js_webrtc(sp) << "</script>"
           << "<script>__cv_webrtc_start('cv-webrtc','" << js_escape(sp) << "');</script>";
    } else if (video_client == VideoClient::Fmp4) {
        os << "<video id='cv-fmp4-video' style='width:100%;height:auto' autoplay playsinline controls muted></video>"
           << "<script>" << build_js_fmp4(sp) << "</script>"
           << "<script>__cv_fmp4_start('cv-fmp4','" << js_escape(sp) << "');</script>";
    } else {
        os << "<canvas id='cv-raw-canvas' style='width:100%;height:auto;background:#000;display:block'></canvas>"
           << "<script>" << build_js_raw(sp) << "</script>"
           << "<script>__cv_raw_start('cv-raw','" << js_escape(sp) << "');</script>";
    }
    os << "</div><div id='pi' style='display:none'><img id='cv-img' style='max-width:100%;display:block;margin:0 auto'></div>"
       << "<script>" << build_js_img_live(img, refresh_ms) << "</script>"
       << "<script>__cv_img_live('cv-img','" << js_escape(img) << "'," << refresh_ms << ");</script>"
       << "<script>(()=>{const tv=document.getElementById('tv'),ti=document.getElementById('ti'),pv=document.getElementById('pv'),pi=document.getElementById('pi');"
          "tv.onclick=()=>{tv.classList.add('a');ti.classList.remove('a');pv.style.display='';pi.style.display='none';};"
          "ti.onclick=()=>{ti.classList.add('a');tv.classList.remove('a');pi.style.display='';pv.style.display='none';};})();</script>";
    return internal::copy_to(dst, cap, os.str());
}

} // namespace webpage
} // namespace stream
} // namespace cv
