#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"

namespace cv {
    namespace stream {
        namespace webpage {

            std::size_t js_webrtc(char* dst, std::size_t cap, const char* signaling_path) {
                return internal::copy_to(dst, cap,
                    internal::build_js_webrtc(signaling_path ? signaling_path : "/webrtc"));
            }
            std::size_t js_fmp4(char* dst, std::size_t cap, const char* ws_media_path) {
                return internal::copy_to(dst, cap,
                    internal::build_js_fmp4(ws_media_path ? ws_media_path : "/ws/fmp4"));
            }
            std::size_t js_raw(char* dst, std::size_t cap, const char* ws_media_path) {
                return internal::copy_to(dst, cap,
                    internal::build_js_raw(ws_media_path ? ws_media_path : "/ws/raw"));
            }
            std::size_t js_image_live(char* dst, std::size_t cap, const char* image_url, int refresh_ms) {
                return internal::copy_to(dst, cap,
                    internal::build_js_img_live(image_url ? image_url : "/image", refresh_ms));
            }
            std::size_t js_controls_runtime(char* dst, std::size_t cap, const char* ws_ctrl_path) {
                return internal::copy_to(dst, cap,
                    internal::build_js_controls_rt(ws_ctrl_path ? ws_ctrl_path : ""));
            }

        } // namespace webpage
    } // namespace stream
} // namespace cv
