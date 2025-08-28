#ifndef OPENCV_WEBRTC_HPP
#define OPENCV_WEBRTC_HPP

#include "opencv2/core/cvdef.h"
#include <opencv2/core/mat.hpp>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace cv {
namespace stream {

// --- Signaling primitives ----------------------------------------------------

struct CV_EXPORTS_W Sdp {
    CV_PROP_RW std::string type; // "offer" | "answer"
    CV_PROP_RW std::string sdp;
};

struct CV_EXPORTS_W IceCandidate {
    CV_PROP_RW std::string candidate;
    CV_PROP_RW std::string sdpMid;
    CV_PROP_RW int         sdpMLineIndex;
    IceCandidate() : sdpMLineIndex(0) {}
};

// --- Configuration -----------------------------------------------------------

struct CV_EXPORTS_W IceServer {
    CV_PROP_RW std::string uri;        // stun:host:port | turn:host:port
    CV_PROP_RW std::string username;
    CV_PROP_RW std::string credential;
};

enum IngestMode {
    AutoEncodeRawBGR,
    PreEncodedH264AnnexB
};

enum VideoCodec {
    H264,
    VP8,
    VP9,
    AV1
};

enum ConnState {
    ConnNew,
    ConnConnecting,
    ConnConnected,
    ConnDisconnected,
    ConnFailed,
    ConnClosed
};

struct CV_EXPORTS_W WebRtcParams {
    CV_WRAP WebRtcParams();

    CV_PROP_RW std::vector<IceServer> iceServers;
    CV_PROP_RW bool trickleIce;
    CV_PROP_RW VideoCodec videoCodec;

    CV_PROP_RW int initialBitrate;
    CV_PROP_RW int minBitrate;
    CV_PROP_RW int maxBitrate;

    CV_PROP_RW int mtu;
    CV_PROP_RW bool lowLatency;

    CV_PROP_RW bool enableDataChannel;
    CV_PROP_RW std::string dataChannelLabel;
};

struct CV_EXPORTS_W IngestParams {
    CV_WRAP IngestParams();

    CV_PROP_RW IngestMode mode;
    CV_PROP_RW int width;
    CV_PROP_RW int height;
    CV_PROP_RW int framerate;
    CV_PROP_RW bool useProvidedTimestamps;
};

// --- Callbacks ---------------------------------------------------------------

struct WebRtcCallbacks {
    std::function<void(const Sdp& local)> onLocalDescription;
    std::function<void(const IceCandidate& cand)> onIceCandidate;
    std::function<void(ConnState state)> onConnectionState;
    std::function<void(const std::string& msg)> onDataMessage;
    std::function<void(const char* where, int err)> onError;
};

// --- Peer (one browser client) ----------------------------------------------

class CV_EXPORTS_W WebRtcPeer {
public:
    CV_WRAP WebRtcPeer();
    ~WebRtcPeer();

    CV_WRAP bool open(const WebRtcParams& params, const IngestParams& ingest, const WebRtcCallbacks& cb);
    CV_WRAP void close();
    CV_WRAP bool isOpen() const;

    CV_WRAP bool setRemoteDescription(const Sdp& remote);
    CV_WRAP bool addRemoteIceCandidate(const IceCandidate& cand);
    CV_WRAP bool createOffer();

    CV_WRAP bool pushRawFrame(const cv::Mat& frame, int64_t ptsNs = -1);
    CV_WRAP bool pushH264(const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs = -1);

    CV_WRAP void requestKeyframe();
    CV_WRAP void setTargetBitrate(int bps);
    CV_WRAP void setFramerate(int fps);

    CV_WRAP std::map<std::string, std::string> getStats() const;

    WebRtcPeer(const WebRtcPeer&) = delete;
    WebRtcPeer& operator=(const WebRtcPeer&) = delete;
    WebRtcPeer(WebRtcPeer&&) noexcept;
    WebRtcPeer& operator=(WebRtcPeer&&) noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl;
};

// --- Utilities ---------------------------------------------------------------

CV_EXPORTS_W bool parseSdpFromJson(const std::string& json, Sdp& out);
CV_EXPORTS_W bool parseIceFromJson(const std::string& json, IceCandidate& out);
CV_EXPORTS_W std::string makeSdpJson(const Sdp& in);
CV_EXPORTS_W std::string makeIceJson(const IceCandidate& in);

CV_EXPORTS_W void ensureInit();
CV_EXPORTS_W void shutdown();

} // namespace stream
} // namespace cv

#endif // OPENCV_WEBRTC_HPP
