#ifndef OPENCV_ENCODER_HPP
#define OPENCV_ENCODER_HPP

#include <opencv2/core.hpp>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdint>
#include <optional>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libswscale/swscale.h>
#include <libavutil/frame.h>
#include <libavutil/imgutils.h>
}

namespace cv {
namespace stream {

// --- Core configuration ------------------------------------------------------

struct CV_EXPORTS_W EncoderParams {
    CV_WRAP EncoderParams();

    CV_PROP_RW int width = 0;
    CV_PROP_RW int height = 0;
    CV_PROP_RW int bitrate = 0;      // bits/sec
    CV_PROP_RW int framerate = 0;    // fps
    CV_PROP_RW int gopSize = 0;      // keyframe interval
    CV_PROP_RW std::string codecName; // "auto", "libx264", "h264_nvenc", ...
};

// --- Recording configuration -------------------------------------------------

enum class RecordingContainer {
    MP4, MKV, MOV
};

struct CV_EXPORTS_W RecordingParams {
    CV_WRAP RecordingParams();

    // Destination: file path or URI. Supports simple rolling pattern tokens:
    // {utc}, {local}, {seq}. Example: "/var/rec/cam-{utc}.mp4"
    CV_PROP_RW std::string destination;

    // Container/muxer to use.
    CV_PROP_RW RecordingContainer container = RecordingContainer::MP4;

    // Rolling segments (0 = single continuous file).
    CV_PROP_RW int segmentSeconds = 0;

    // Max number of segments to retain (<=0 = unlimited). Oldest segments are pruned.
    CV_PROP_RW int maxSegments = 0;

    // Force segment boundaries on keyframes when possible.
    CV_PROP_RW bool segmentOnKeyframe = true;

    // If true, start recording immediately once the encoder is opened.
    CV_PROP_RW bool autostart = false;

    // Optional faststart/moov-at-beginning for MP4.
    CV_PROP_RW bool optimizeForStreaming = true;

    // Optional metadata.
    CV_PROP_RW std::map<std::string, std::string> metadata;
};

// --- Encoder -----------------------------------------------------------------

class CV_EXPORTS_W Encoder {
public:
    CV_WRAP Encoder();
    ~Encoder();

    CV_WRAP bool open(const EncoderParams& params);
    CV_WRAP void release();
    CV_WRAP bool isOpened() const;

    // Input
    CV_WRAP bool push(const cv::Mat& frame);

    // Streaming egress
    bool pullAsRtp(std::vector<uint8_t>& packet);
    bool getFmp4InitializationSegment(std::vector<uint8_t>& initSegment);
    bool pullAsFmp4(std::vector<uint8_t>& fragment);

    // Recording control (can run concurrently with streaming)
    CV_WRAP bool configureRecording(const RecordingParams& params); // may be called before or after open()
    CV_WRAP bool startRecording();     // uses last configured RecordingParams
    CV_WRAP void stopRecording();
    CV_WRAP bool isRecording() const;

    // Manual segment split (e.g., on external event); keeps recording running.
    CV_WRAP bool splitSegment();

    // Introspection / utilities
    CV_WRAP static std::map<std::string, bool> getAvailableEncoders(); // encoder -> isHardware

    Encoder(const Encoder&) = delete;
    Encoder& operator=(const Encoder&) = delete;
    Encoder(Encoder&&) noexcept;
    Encoder& operator=(Encoder&&) noexcept;

private:
    class EncoderImpl;
    std::unique_ptr<EncoderImpl> pimpl;
};

} // namespace stream
} // namespace cv

#endif // OPENCV_ENCODER_HPP
