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

/*
Supported output codecs for streaming/recording (no explicit hardware-frames
plumbing required; FFmpeg can upload/convert from system-memory frames):

  • H.264/AVC (HW or SW):
      "h264_nvenc", "h264_qsv", "h264_videotoolbox", "h264_amf", "h264_mf",
      "libx264", "libopenh264"

  • VP8 (SW):
      "libvpx", "libvpx-vp8"

  • AV1 (HW or SW):
      "av1_nvenc", "av1_qsv", "av1_amf",
      "libsvtav1", "librav1e", "libaom-av1"

Backends that typically require an explicit HW-frames path (e.g., VAAPI,
Vulkan Video, V4L2 M2M) are NOT attempted by "auto" in this implementation.

"auto" selection prefers fastest→slowest (typical) among backends that accept
system-memory frames:

  H.264 HW (NVENC/QSV/AMF/Videotoolbox/MF)
  → H.264 SW (libx264/libopenh264)
  → VP8 SW (libvpx)
  → AV1 HW (NVENC/QSV/AMF)
  → AV1 SW (SVT-AV1 → rav1e → libaom)

Other codecs are not guaranteed.
*/

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
    //   - H.264 (NVENC): preset="llhp"/"llhq", profile="high", and keep maxBFrames=0
    //   - VP8/AV1: may ignore profile/preset/tune depending on encoder
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
    //
    // Examples (kept compatible with system-memory input; no explicit HW-frames setup required):
    //   H.264: "auto", "libx264", "libopenh264",
    //          "h264_nvenc", "h264_qsv", "h264_videotoolbox", "h264_amf", "h264_mf"
    //   VP8:   "libvpx", "libvpx-vp8"
    //   AV1:   "libsvtav1", "librav1e", "libaom-av1",
    //          "av1_nvenc", "av1_qsv", "av1_amf"
    //
    // Notes:
    //   • VAAPI/Vulkan/V4L2M2M encoders are not selected by "auto" here because they
    //     typically require an explicit HW device/frames path; use them only if the
    //     implementation adds that plumbing.
    //
    // If "auto", the implementation picks from the priority set described above.
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
    // key = FFmpeg encoder name, value = isHardwareAccelerated (heuristic).
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
