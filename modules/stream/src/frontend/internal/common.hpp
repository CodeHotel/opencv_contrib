#pragma once
#include <string>
#include <sstream>
#include <algorithm>
#include <cstring>

namespace cv {
    namespace stream {
        namespace webpage {
            namespace internal {

                // Small snprintf-style helper used across API fns.
                inline std::size_t copy_to(char* dst, std::size_t cap, const std::string& s) {
                    if (cap > 0) {
                        std::size_t n = std::min<std::size_t>(cap - 1, s.size());
                        if (n) std::memcpy(dst, s.data(), n);
                        dst[n] = '\0';
                    }
                    return s.size();
                }

                std::string html_escape(const std::string& s);
                std::string js_escape(const std::string& s);

                // CSS + HTML shell
                std::string default_css();
                std::string shell_head(const std::string& title);
                std::string shell_tail();

                // JS builders
                std::string build_js_webrtc(const std::string& signaling_path);
                std::string build_js_fmp4(const std::string& ws_media_path);
                std::string build_js_raw(const std::string& ws_media_path);
                std::string build_js_img_live(const std::string& img_url, int refresh_ms);
                std::string build_js_controls_rt(const std::string& ws_ctrl_path);

            } // namespace internal
        } // namespace webpage
    } // namespace stream
} // namespace cv
