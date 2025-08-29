#ifndef OPENCV_STREAM_HPP
#define OPENCV_STREAM_HPP

#include <opencv2/core/cvdef.h>
#include <opencv2/core/mat.hpp>
#include <opencv2/stream/server.hpp>
#include <opencv2/stream/webpage.hpp>
#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
#include <opencv2/stream/webrtc.hpp>
#endif
#if defined(HAVE_STREAM_COMPRESSION)
#include <opencv2/stream/encoder.hpp>
#endif
#include <opencv2/core/utils/logger.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace cv {
namespace stream {

// -----------------------------------------------------------------------------
// Core
// -----------------------------------------------------------------------------

enum class EndpointKind { Raw, Fmp4, WebRTC };

enum class AccessRole { ReadOnly, ReadWrite };

struct EndpointHandle {
    int id;
    EndpointHandle() : id(-1) {}
    explicit EndpointHandle(int i) : id(i) {}
    bool valid() const { return id >= 0; }
};

// Sources:
//  - FrameSource: uncompressed cv::Mat frames (used by non-WebRTC endpoints).
//  - EncodedSource: pre-encoded elementary AUs/OBUs (H.264/VP8/AV1) for WebRTC.
typedef std::function<bool(cv::Mat& outFrame, int64_t& ptsNs)> FrameSource;
typedef std::function<bool(std::vector<uint8_t>& au, bool& key, int64_t& ptsNs)> EncodedSource;

// -----------------------------------------------------------------------------
// Options
// -----------------------------------------------------------------------------

struct CV_EXPORTS_W RawOptions {
    CV_WRAP RawOptions() : width(0), height(0), framerate(0), editor(true),
                           ctrl(NULL), numControls(0), title("OpenCV Stream") {}
    CV_PROP_RW int width;
    CV_PROP_RW int height;
    CV_PROP_RW int framerate;
    CV_PROP_RW bool editor; // auto UI (ignored when secureMode==true)
    CV_PROP_RW const webpage::ParamSpec* ctrl;
    CV_PROP_RW std::size_t numControls;
    CV_PROP_RW std::string title;
};

struct CV_EXPORTS_W Fmp4Options {
    CV_WRAP Fmp4Options() : editor(true), ctrl(NULL), numControls(0), title("OpenCV Stream") {}
    CV_PROP_RW EncoderParams encoder;
    CV_PROP_RW bool editor; // auto UI (ignored when secureMode==true)
    CV_PROP_RW const webpage::ParamSpec* ctrl;
    CV_PROP_RW std::size_t numControls;
    CV_PROP_RW std::string title;
};

#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
struct CV_EXPORTS_W WebRtcOptions {
    CV_WRAP WebRtcOptions() : autostartOffer(true), title("OpenCV Stream") {}
    CV_PROP_RW WebRtcParams  webrtc;
    CV_PROP_RW IngestParams  ingest; // encoded-only (H.264/VP8/AV1 elementary stream)
    CV_PROP_RW bool autostartOffer;
    CV_PROP_RW std::string title;
};
#endif

struct CV_EXPORTS_W WebviewOptions {
    CV_WRAP WebviewOptions()
        : videoClient(webpage::VideoClient::Auto),
          ctrl(NULL), numControls(0), imageRefreshMs(0) {}
    CV_PROP_RW webpage::VideoClient videoClient;
    CV_PROP_RW const webpage::ParamSpec* ctrl;
    CV_PROP_RW std::size_t numControls;
    CV_PROP_RW std::string title;
    CV_PROP_RW int imageRefreshMs;
    CV_PROP_RW std::string wsCtrlPath;
};

// -----------------------------------------------------------------------------
// Stream orchestrator
// -----------------------------------------------------------------------------

class CV_EXPORTS_W Stream {
public:
    CV_WRAP Stream();
    ~Stream();

    // Process
    CV_WRAP bool start(const std::string& bindAddress, int port, bool secureMode = false, int numThreads = 1);
    CV_WRAP void stop();
    CV_WRAP bool isRunning() const;

    CV_WRAP void setSecureMode(bool on);
    CV_WRAP bool secureMode() const;

    // Root index (disabled by default when secureMode==true)
    CV_WRAP void enableRootIndex(bool on, const std::string& mountPath = "/", const std::string& title = "OpenCV Stream");

    // --- Endpoint registration ---

    // Uncompressed frame endpoints (HTTP-based viewers, etc.)
    CV_WRAP EndpointHandle addRaw(const std::string& path,
                                  const FrameSource& source,
                                  const RawOptions& opts = RawOptions());

    // fMP4 endpoint (server-side encoding/muxing)
    CV_WRAP EndpointHandle addFmp4(const std::string& path,
                                   const FrameSource& source,
                                   const Fmp4Options& opts = Fmp4Options());

#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
    // WebRTC endpoint — **encoded-only** (H.264/VP8/AV1 elementary stream units)
    CV_WRAP EndpointHandle addWebRtc(const std::string& path,
                                     const EncodedSource& encodedSource,
                                     const WebRtcOptions& opts);

    // NOTE: addWebRtcRaw(...) has been removed; WebRTC no longer accepts raw frames.
#endif

    CV_WRAP void remove(const EndpointHandle& h);
    CV_WRAP void removeByPath(const std::string& path);

    // --- Webviews (HTML pages) ---

    CV_WRAP void mountEmbedded(const std::string& pagePath,
                               const std::string& streamPath,
                               const WebviewOptions& view = WebviewOptions());

    CV_WRAP void mountJupyter(const std::string& pagePath,
                              const std::string& streamPath,
                              const WebviewOptions& view = WebviewOptions());

    // -------------------------------------------------------------------------
    // Token gating (per endpoint path)
    // -------------------------------------------------------------------------

    CV_WRAP bool allowAccess(const std::string& endpointPath,
                             const std::string& token,
                             AccessRole role = AccessRole::ReadOnly,
                             int ttlSeconds = 0);

    CV_WRAP void removeAccess(const std::string& endpointPath,
                              const std::string& token);

    CV_WRAP void clearAccess(const std::string& endpointPath);

    // -------------------------------------------------------------------------
    // PARAMS — dynamic runtime parameters (string-serialized values)
    // -------------------------------------------------------------------------
    // Notes:
    //  - When secureMode==false and the endpoint was registered with editor=true,
    //    these appear as live UI controls automatically.
    //  - When secureMode==true, no UI is exposed; use these functions to R/W.

    CV_WRAP bool setParam(const EndpointHandle& h,
                          const std::string& id,
                          const std::string& value);

    CV_WRAP bool getParam(const EndpointHandle& h,
                          const std::string& id,
                          std::string& outValue) const;

    CV_WRAP std::map<std::string, std::string> listParams(const EndpointHandle& h) const;

    // Convenience by path
    CV_WRAP bool setParamByPath(const std::string& path,
                                const std::string& id,
                                const std::string& value);

    CV_WRAP bool getParamByPath(const std::string& path,
                                const std::string& id,
                                std::string& outValue) const;

    CV_WRAP std::map<std::string, std::string> listParamsByPath(const std::string& path) const;

    // -------------------------------------------------------------------------
    // RECORDING — per-endpoint control (start/stop; optional destination override)
    // -------------------------------------------------------------------------
    // Notes:
    //  - For fMP4 endpoints, recording uses the same encoder/muxer chain.
    //  - For WebRTC endpoints, recording **muxes the pre-encoded input**; there is
    //    no internal encoder chain for WebRTC anymore.
    //  - Start returns the resolved destination (empty string on failure).
    //  - Destination "" uses the last configured RecordingParams/destination.

    CV_WRAP std::string startRecording(const EndpointHandle& h,
                                       const std::string& destination = std::string());

    CV_WRAP std::string stopRecording(const EndpointHandle& h);

    CV_WRAP bool configureRecording(const EndpointHandle& h,
                                    const RecordingParams& params);

    // Convenience by path
    CV_WRAP std::string startRecordingByPath(const std::string& path,
                                             const std::string& destination = std::string());

    CV_WRAP std::string stopRecordingByPath(const std::string& path);

    CV_WRAP bool configureRecordingByPath(const std::string& path,
                                          const RecordingParams& params);

    // Optional: manual split for rolling segments (no stop).
    CV_WRAP bool splitRecordingSegment(const EndpointHandle& h);
    CV_WRAP bool splitRecordingSegmentByPath(const std::string& path);

    // -------------------------------------------------------------------------
    // Introspection
    // -------------------------------------------------------------------------

    CV_WRAP std::vector<std::string> listEndpoints() const; // paths
    CV_WRAP EndpointKind kindOf(const std::string& path) const;

    Stream(const Stream&) = delete;
    Stream& operator=(const Stream&) = delete;
    Stream(Stream&&) noexcept;
    Stream& operator=(Stream&&) noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl;
};

CV_EXPORTS_W std::unique_ptr<Stream> createStream();

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

inline FrameSource makeFrameSource(const std::function<cv::Mat(void)>& fn, int /*fpsHint*/ = 0) {
    return [fn](cv::Mat& out, int64_t& ptsNs) -> bool {
        cv::Mat f = fn ? fn() : cv::Mat();
        if (f.empty()) return false;
        out = f;
        ptsNs = -1;
        return true;
    };
}

} // namespace stream
} // namespace cv

#endif // OPENCV_STREAM_HPP
