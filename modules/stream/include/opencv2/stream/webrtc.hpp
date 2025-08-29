#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
#ifndef OPENCV_WEBRTC_HPP
#define OPENCV_WEBRTC_HPP

#include "opencv2/core/cvdef.h"
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
    CV_WRAP IceCandidate() : sdpMLineIndex(0) {}
};

// --- Configuration -----------------------------------------------------------

struct CV_EXPORTS_W IceServer {
    CV_PROP_RW std::string uri;        // "stun:host:port" | "turn:host:port"
    CV_PROP_RW std::string username;
    CV_PROP_RW std::string credential;
};

// In this variant, ONLY pre-encoded elementary streams are accepted.
// Raw frame ingestion is intentionally not supported.

enum class VideoCodec {
    H264,
    VP8,
    AV1
};

enum class ConnState {
    New,
    Connecting,
    Connected,
    Disconnected,
    Failed,
    Closed
};

struct CV_EXPORTS_W WebRtcParams {
    CV_WRAP WebRtcParams()
        : trickleIce(true),
          initialBitrate(0), minBitrate(0), maxBitrate(0),
          mtu(1200), lowLatency(true),
          enableDataChannel(false),
          dataChannelLabel("cv")
    {
        // negotiation preference (first match wins)
        preferredCodecs.push_back(VideoCodec::H264);
        preferredCodecs.push_back(VideoCodec::VP8);
        preferredCodecs.push_back(VideoCodec::AV1);
    }

    CV_PROP_RW std::vector<IceServer> iceServers;
    CV_PROP_RW bool trickleIce;

    // codec negotiation preference (first match wins)
    CV_PROP_RW std::vector<VideoCodec> preferredCodecs;

    // bitrate hints (bps); 0 = unset
    CV_PROP_RW int initialBitrate;
    CV_PROP_RW int minBitrate;
    CV_PROP_RW int maxBitrate;

    // transport tuning
    CV_PROP_RW int  mtu;          // RTP MTU
    CV_PROP_RW bool lowLatency;   // favor low-latency pipeline

    // datachannel (optional)
    CV_PROP_RW bool        enableDataChannel;
    CV_PROP_RW std::string dataChannelLabel;

    // backpressure
    CV_PROP_RW size_t maxQueueBytes = 4 * 1024 * 1024;
};

// Pre-encoded ingest only. Supply codec-specific AUs/OBUs via pushEncoded(...).
// Width/height are derived from the bitstream; not required here.
struct CV_EXPORTS_W IngestParams {
    CV_WRAP IngestParams()
        : framerate(0), useProvidedTimestamps(false)
    {}

    // Used only if pts are not provided when pushing AUs (simple pacing).
    CV_PROP_RW int  framerate;              // fps; 0 = do not pace
    CV_PROP_RW bool useProvidedTimestamps;  // pts are ns (monotonic). If false and framerate>0, pts are generated.
};

// --- Callbacks ---------------------------------------------------------------

struct WebRtcCallbacks {
    std::function<void(const Sdp& local)> onLocalDescription;
    std::function<void(const IceCandidate& cand)> onIceCandidate;
    std::function<void(ConnState state)> onConnectionState;

    // receiver feedback & adaptation
    std::function<void()> onKeyframeRequested;      // FIR/PLI
    std::function<void(int /*bps*/)> onBitrateSuggested; // REMB/TMMBR

    // datachannel
    std::function<void(const std::string& msg)> onDataMessage; // text/binary (UTF-8 if text)

    // errors
    std::function<void(const char* where, int err)> onError;
};

// --- Peer (one browser client) ----------------------------------------------

class CV_EXPORTS_W WebRtcPeer {
public:
    CV_WRAP WebRtcPeer();
    ~WebRtcPeer();

    // lifetime
    CV_WRAP bool open(const WebRtcParams& params, const IngestParams& ingest, const WebRtcCallbacks& cb);
    CV_WRAP void close();
    CV_WRAP bool isOpen() const;

    // signaling
    CV_WRAP bool createOffer();                       // triggers onLocalDescription
    CV_WRAP bool createAnswer();                      // if remote is an offer
    CV_WRAP bool setRemoteDescription(const Sdp& remote);
    CV_WRAP bool addRemoteIceCandidate(const IceCandidate& cand);

    // ingest (PRE-ENCODED ONLY)
    // Provide codec elementary units:
    //  - H.264: Annex-B AU (start-code delimited), keyFrame=true on IDR
    //  - VP8: full encoded frame payload
    //  - AV1: OBUs for a frame (Annex-B or length-delimited accepted by implementation)
    CV_WRAP bool pushEncoded(VideoCodec codec,
                             const uint8_t* data, size_t bytes,
                             bool keyFrame, int64_t ptsNs = -1);

    // convenience for H.264 byte-stream (Annex-B) AUs
    CV_WRAP bool pushH264(const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs = -1);

    // control
    CV_WRAP void forceKeyframe();           // sends FIR/PLI to upstream encoder
    CV_WRAP void setTargetBitrate(int bps);
    CV_WRAP void setFramerate(int fps);     // pacing hint if pts are not provided
    CV_WRAP void setWriteQueueLimitBytes(size_t bytes);

    // datachannel
    CV_WRAP bool sendDataMessage(const void* data, size_t nBytes, bool binary = false);

    // stats / state
    CV_WRAP std::map<std::string, std::string> getStats() const;
    CV_WRAP ConnState getConnectionState() const;
    CV_WRAP VideoCodec getNegotiatedVideoCodec() const;

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

// global init (idempotent)
CV_EXPORTS_W void ensureInit();
CV_EXPORTS_W void shutdown();

// factory
CV_EXPORTS_W std::unique_ptr<WebRtcPeer> createWebRtcPeer();

} // namespace stream
} // namespace cv

#endif // OPENCV_WEBRTC_HPP
#endif
