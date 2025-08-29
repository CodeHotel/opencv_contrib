#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"
#include <sstream>

namespace cv {
    namespace stream {
        namespace webpage {

            std::size_t embedded_image_page(char* dst, std::size_t cap,
                                            const char* image_url,
                                            const char* title) {
                using namespace internal;
                std::ostringstream os;
                os << shell_head(title ? title : "OpenCV Stream")
                   << "<div class='cv-wrap'><div class='cv-col'>"
                   << "<img id='cv-img' class='cv-img' src='" << html_escape(image_url?image_url:"/image") << "'>"
                   << "</div></div>" << shell_tail();
                return copy_to(dst, cap, os.str());
            }

            std::size_t embedded_image_page(char* dst, std::size_t cap,
                                            const char* image_url,
                                            int refresh_ms,
                                            const char* title) {
                using namespace internal;
                std::ostringstream os;
                os << shell_head(title ? title : "OpenCV Stream")
                   << "<div class='cv-wrap'><div class='cv-col'>"
                   << "<img id='cv-img' class='cv-img'>"
                   << "</div></div>"
                   << "<script>" << build_js_img_live(image_url?image_url:"/image", refresh_ms) << "</script>"
                   << shell_tail();
                return copy_to(dst, cap, os.str());
            }

            std::size_t webview_image_page(char* dst, std::size_t cap,
                                           const char* mount_path,
                                           const char* image_url,
                                           int refresh_ms,
                                           const char* title) {
                (void)mount_path;
                return embedded_image_page(dst, cap, image_url, refresh_ms, title);
            }

        } // namespace webpage
    } // namespace stream
} // namespace cv
