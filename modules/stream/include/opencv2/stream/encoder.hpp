#if defined(HAVE_STREAM_COMPRESSION)
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

// Supported output codecs (and their hardware/software aliases) for streaming/recording:
//   • H.264/AVC: "libx264", "h264_nvenc", "h264_qsv", "h264_videotoolbox", "h264_amf", "h264_vaapi", etc.
//   • VP8:       "libvpx" or "libvpx-vp8"
//   • AV1:       "libaom-av1", "libsvtav1", "av1_nvenc", "av1_qsv", "av1_vaapi", "av1_amf", etc.
// Other codecs are not guaranteed. "auto" selects the best available from {H.264 → VP8 → AV1} for WebRTC/MSE paths.

// --- Core configuration ------------------------------------------------------

struct CV_EXPORTS_W EncoderParams {
    CV_WRAP EncoderParams();

    // Frame geometry and rate
    CV_PROP_RW int width     = 0;
    CV_PROP_RW int height    = 0;
    CV_PROP_RW int bitrate   = 0;    // bits/sec
    CV_PROP_RW int framerate = 0;    // fps
    CV_PROP_RW int gopSize   = 0;    // keyframe interval (~seconds = gopSize/framerate)

    // Latency / GOP structure
    // Set maxBFrames=0 for strictly forward-decoded streams (recommended for real-time/WebRTC).
    CV_PROP_RW int  maxBFrames  = 0;     // default 0 (no B-frames)
    CV_PROP_RW bool lowLatency  = true;  // apply encoder-specific low-latency presets/tunes when available

    // Optional encoder “hints” (mapped to FFmpeg private options when applicable).
    // Examples:
    //   - H.264 (x264): profile="baseline", preset="ultrafast", tune="zerolatency"
    //   - H.264 (NVENC): preset="llhp", profile="high", and still set maxBFrames=0
    //   - VP8/AV1: may ignore profile/preset/tune
    CV_PROP_RW std::string profile;      // e.g. "baseline", "main", "high"
    CV_PROP_RW std::string preset;       // e.g. "ultrafast", "veryfast", "llhq", "llhp"
    CV_PROP_RW std::string tune;         // e.g. "zerolatency"

    // Extra encoder-specific private options (key/value), applied last and override above if overlapping.
    // Examples:
    //   codecOptions["bf"] = "0";            // NVENC max B-frames
    //   codecOptions["rc-lookahead"] = "0";  // NVENC/x264 reduce lookahead
    //   codecOptions["no-scenecut"] = "1";   // x264
    CV_PROP_RW std::map<std::string, std::string> codecOptions;

    // Name of the FFmpeg encoder to use. Must resolve to H.264, VP8, or AV1.
    // Examples:
    //   H.264: "auto", "libx264", "h264_nvenc", "h264_qsv", "h264_videotoolbox", "h264_amf", "h264_vaapi"
    //   VP8:   "libvpx", "libvpx-vp8"
    //   AV1:   "libaom-av1", "libsvtav1", "av1_nvenc", "av1_qsv", "av1_vaapi", "av1_amf"
    // If "auto", the implementation picks from {H.264 → VP8 → AV1} based on availability and runtime path.
    CV_PROP_RW std::string codecName; // default empty/auto allowed
};

// --- Recording configuration -------------------------------------------------

enum class RecordingContainer { MP4, MKV, MOV };

struct CV_EXPORTS_W RecordingParams {
    CV_WRAP RecordingParams();

    // Destination file/URI. Supports rolling tokens {utc}, {local}, {seq}.
    CV_PROP_RW std::string destination;

    CV_PROP_RW RecordingContainer container = RecordingContainer::MP4;

    // Segmented recording: 0 = continuous file.
    CV_PROP_RW int segmentSeconds = 0;

    // Retention: <=0 = unlimited; oldest segments pruned when exceeded.
    CV_PROP_RW int maxSegments = 0;

    // Prefer cutting segments on keyframes.
    CV_PROP_RW bool segmentOnKeyframe = true;

    // Start recording automatically on open().
    CV_PROP_RW bool autostart = false;

    // MP4 faststart/moov-first when applicable.
    CV_PROP_RW bool optimizeForStreaming = true;

    // Optional container metadata (key/value).
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

    // Input: BGR frames matching width/height.
    CV_WRAP bool push(const cv::Mat& frame);

    // Streaming egress:
    //  - pullAsRtp: returns pre-encoded access units/packets suitable for RTP/WebRTC.
    //      * H.264: Annex-B bytestream (AU-aligned, start codes present).
    //      * VP8/AV1: codec-native frames (no container).
    //  - getFmp4InitializationSegment / pullAsFmp4: init + fragments for MSE over WebSockets.
    bool pullAsRtp(std::vector<uint8_t>& packet);
    bool getFmp4InitializationSegment(std::vector<uint8_t>& initSegment);
    bool pullAsFmp4(std::vector<uint8_t>& fragment);

    // Recording (can run concurrently with streaming).
    CV_WRAP bool configureRecording(const RecordingParams& params);
    CV_WRAP bool startRecording();
    CV_WRAP void stopRecording();
    CV_WRAP bool isRecording() const;

    // Force a segment boundary while recording (keeps recording running).
    CV_WRAP bool splitSegment();

    // Returns available encoders filtered to those that produce H.264, VP8, or AV1.
    // key = FFmpeg encoder name, value = isHardwareAccelerated
    CV_WRAP static std::map<std::string, bool> getAvailableEncoders();

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
#endif
