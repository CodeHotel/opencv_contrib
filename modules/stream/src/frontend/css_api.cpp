#include "opencv2/stream/webpage.hpp"
#include "internal/common.hpp"

namespace cv {
    namespace stream {
        namespace webpage {

            std::size_t css_default_theme(char* dst, std::size_t cap) {
                return internal::copy_to(dst, cap, internal::default_css());
            }

        } // namespace webpage
    } // namespace stream
} // namespace cv
