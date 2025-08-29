#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"
#include <sstream>

namespace cv {
    namespace stream {
        namespace webpage {

            std::size_t root_page(char* dst, std::size_t cap,
                                  const char* mount_path,
                                  const char* title) {
                using namespace internal;
                std::ostringstream os;
                os << shell_head(title ? title : "OpenCV Stream")
                   << "<div class='cv-wrap'><div class='cv-col'>"
                   << "<h1 style='margin-top:0'>"
                   << html_escape(title ? title : "OpenCV Stream") << "</h1>"
                   << "<p class='cv-muted'>This is the stream root. Upstream code should register concrete pages and endpoints.</p>"
                   << "<p>Mount path: <code>" << html_escape(mount_path ? mount_path : "/") << "</code></p>"
                   << "</div></div>"
                   << shell_tail();
                return copy_to(dst, cap, os.str());
            }

        } // namespace webpage
    } // namespace stream
} // namespace cv
