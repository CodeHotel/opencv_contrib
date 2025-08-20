#ifndef OPENCV_ENCODER_HPP
#define OPENCV_ENCODER_HPP

#include <opencv2/core.hpp>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libswscale/swscale.h>
#include <libavutil/frame.h>
#include <libavutil/imgutils.h>
}

#endif //OPENCV_ENCODER_HPP