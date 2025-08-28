#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)

// webrtc.cpp
#include <opencv2/stream/webrtc.hpp>

#include <atomic>
#include <cassert>
#include <cstdio>
#include <mutex>
#include <thread>
#include <utility>

// GStreamer (private to impl – not exposed in public headers)
#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/sdp/gstsdpmessage.h>
#include <gst/webrtc/webrtc.h>

namespace cv {
namespace stream {

// ===============================
// Utilities (private)
// ===============================

namespace {

inline const char* toString(VideoCodec c) {
    switch (c) {
        case VideoCodec::H264: return "H264";
        case VideoCodec::VP8:  return "VP8";
        case VideoCodec::AV1:  return "AV1";
    }
    return "UNKNOWN";
}

inline ConnState toConnState(GstWebRTCPeerConnectionState st) {
    switch (st) {
        case GST_WEBRTC_PEER_CONNECTION_STATE_NEW:          return ConnState::New;
        case GST_WEBRTC_PEER_CONNECTION_STATE_CONNECTING:   return ConnState::Connecting;
        case GST_WEBRTC_PEER_CONNECTION_STATE_CONNECTED:    return ConnState::Connected;
        case GST_WEBRTC_PEER_CONNECTION_STATE_DISCONNECTED: return ConnState::Disconnected;
        case GST_WEBRTC_PEER_CONNECTION_STATE_FAILED:       return ConnState::Failed;
        case GST_WEBRTC_PEER_CONNECTION_STATE_CLOSED:       return ConnState::Closed;
        default:                                            return ConnState::New;
    }
}

inline GstWebRTCSDPType toGstType(const std::string& t) {
    if (t == "offer")  return GST_WEBRTC_SDP_TYPE_OFFER;
    if (t == "answer") return GST_WEBRTC_SDP_TYPE_ANSWER;
    // fallthrough – treat others as answer
    return GST_WEBRTC_SDP_TYPE_ANSWER;
}

inline std::string escapeJson(const std::string& s) {
    std::string out;
    out.reserve(s.size() + 16);
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"':  out += "\\\""; break;
            case '\n': out += "\\n";  break;
            case '\r': out += "\\r";  break;
            case '\t': out += "\\t";  break;
            default:   out += c;      break;
        }
    }
    return out;
}

inline bool extractJsonString(const std::string& json, const std::string& key, std::string& out) {
    // Tiny ad-hoc parser sufficient for {"key":"value"} payloads we emit.
    const std::string pat = "\"" + key + "\"";
    size_t p = json.find(pat);
    if (p == std::string::npos) return false;
    p = json.find(':', p);
    if (p == std::string::npos) return false;
    p = json.find('"', p);
    if (p == std::string::npos) return false;
    size_t q = p + 1;
    std::string val;
    bool esc = false;
    for (; q < json.size(); ++q) {
        char c = json[q];
        if (esc) { // rudimentary unescape
            switch (c) {
                case 'n': val.push_back('\n'); break;
                case 'r': val.push_back('\r'); break;
                case 't': val.push_back('\t'); break;
                case '"': val.push_back('"');  break;
                case '\\': val.push_back('\\'); break;
                default: val.push_back(c); break;
            }
            esc = false;
            continue;
        }
        if (c == '\\') { esc = true; continue; }
        if (c == '"') break;
        val.push_back(c);
    }
    if (q >= json.size()) return false;
    out.swap(val);
    return true;
}

inline GstCaps* makeEncodedCaps(VideoCodec codec, const IngestParams& ingest) {
    GstCaps* caps = nullptr;
    switch (codec) {
        case VideoCodec::H264: {
            caps = gst_caps_new_simple(
                "video/x-h264",
                "stream-format", G_TYPE_STRING, "byte-stream",
                "alignment",     G_TYPE_STRING, "au",
                nullptr);
            break;
        }
        case VideoCodec::VP8: {
            caps = gst_caps_new_simple("video/x-vp8", nullptr, nullptr);
            break;
        }
        case VideoCodec::AV1: {
            caps = gst_caps_new_simple("video/x-av1", nullptr, nullptr);
            break;
        }
    }
    if (!caps) return nullptr;
    if (ingest.width > 0)
        gst_caps_set_simple(caps, "width", G_TYPE_INT, ingest.width, nullptr);
    if (ingest.height > 0)
        gst_caps_set_simple(caps, "height", G_TYPE_INT, ingest.height, nullptr);
    if (ingest.framerate > 0)
        gst_caps_set_simple(caps, "framerate", GST_TYPE_FRACTION, ingest.framerate, 1, nullptr);
    return caps;
}

inline GstElement* makeEncoderFor(VideoCodec codec, const WebRtcParams& params, const IngestParams& ingest) {
    // Minimal encoders with low-latency knobs; availability depends on the system.
    switch (codec) {
        case VideoCodec::H264: {
            // Try hardware first, fallback to x264
            const char* candidates[] = { "nvh264enc", "x264enc", "vaapih264enc", "vtenc_h264", "qsvh264enc", "openh264enc" };
            for (auto name : candidates) {
                GstElement* e = gst_element_factory_make(name, nullptr);
                if (!e) continue;
                // common low-latency options when available
                g_object_set(e,
                    "tune", 4 /*zerolatency*/ , nullptr); // x264enc
                g_object_set(e, "speed-preset", 1 /*ultrafast*/, nullptr);
                g_object_set(e, "byte-stream", TRUE, nullptr);
                if (ingest.framerate > 0) g_object_set(e, "key-int-max", ingest.framerate, nullptr);
                if (params.initialBitrate > 0) g_object_set(e, "bitrate", params.initialBitrate / 1000, nullptr); // kbps
                return e;
            }
            break;
        }
        case VideoCodec::VP8: {
            GstElement* e = gst_element_factory_make("vp8enc", nullptr);
            if (e) {
                g_object_set(e, "deadline", 1, "cpu-used", 8, nullptr);
                if (params.initialBitrate > 0) g_object_set(e, "target-bitrate", params.initialBitrate, nullptr);
            }
            return e;
        }
        case VideoCodec::AV1: {
            // Try common AV1 encoders
            const char* candidates[] = { "nvav1enc", "svtav1enc", "av1enc" /*libaom*/ };
            for (auto name : candidates) {
                GstElement* e = gst_element_factory_make(name, nullptr);
                if (!e) continue;
                if (params.initialBitrate > 0) g_object_set(e, "bitrate", params.initialBitrate, nullptr);
                return e;
            }
            break;
        }
    }
    return nullptr;
}

inline GstElement* makePayloaderFor(VideoCodec codec) {
    switch (codec) {
        case VideoCodec::H264: return gst_element_factory_make("rtph264pay", nullptr);
        case VideoCodec::VP8:  return gst_element_factory_make("rtpvp8pay", nullptr);
        case VideoCodec::AV1:  return gst_element_factory_make("rtpav1pay", nullptr);
    }
    return nullptr;
}

inline void setPayloaderDefaults(GstElement* pay) {
    if (!pay) return;
    g_object_set(pay, "pt", 96, nullptr);
    // H.264: insert SPS/PPS regularly for late-joiners
    if (g_str_has_prefix(G_OBJECT_TYPE_NAME(pay), "GstRtpH264Pay"))
        g_object_set(pay, "config-interval", 1, nullptr);
}

} // namespace

// ===============================
// Global init / shutdown
// ===============================

static std::once_flag g_onceInit;
static std::atomic<int> g_initCount{0};

void ensureInit() {
    std::call_once(g_onceInit, []() {
        gst_init(nullptr, nullptr);
    });
    ++g_initCount;
}

void shutdown() {
    // GStreamer has global state; generally apps don't deinit. Keep no-op.
    int c = --g_initCount;
    (void)c;
}

// ===============================
// JSON helpers
// ===============================

bool parseSdpFromJson(const std::string& json, Sdp& out) {
    std::string t, s;
    if (!extractJsonString(json, "type", t)) return false;
    if (!extractJsonString(json, "sdp", s)) return false;
    out.type = t;
    out.sdp  = s;
    return true;
}

bool parseIceFromJson(const std::string& json, IceCandidate& out) {
    std::string cand, mid;
    std::string mlineStr;
    if (!extractJsonString(json, "candidate", cand)) return false;
    extractJsonString(json, "sdpMid", mid);
    out.candidate = cand;
    out.sdpMid = mid;
    // sdpMLineIndex (int) – optional; try to parse primitive
    size_t p = json.find("\"sdpMLineIndex\"");
    if (p != std::string::npos) {
        p = json.find(':', p);
        if (p != std::string::npos) {
            size_t q = p + 1;
            while (q < json.size() && (json[q] == ' ' || json[q] == '\t')) ++q;
            int idx = 0;
            bool neg = false;
            if (q < json.size() && json[q] == '-') { neg = true; ++q; }
            while (q < json.size() && json[q] >= '0' && json[q] <= '9') {
                idx = idx * 10 + (json[q] - '0'); ++q;
            }
            out.sdpMLineIndex = neg ? -idx : idx;
        }
    }
    return true;
}

std::string makeSdpJson(const Sdp& in) {
    return std::string("{\"type\":\"") + escapeJson(in.type) + "\",\"sdp\":\"" + escapeJson(in.sdp) + "\"}";
}

std::string makeIceJson(const IceCandidate& in) {
    std::string s = "{\"candidate\":\"" + escapeJson(in.candidate) + "\"";
    if (!in.sdpMid.empty()) s += ",\"sdpMid\":\"" + escapeJson(in.sdpMid) + "\"";
    s += ",\"sdpMLineIndex\":" + std::to_string(in.sdpMLineIndex) + "}";
    return s;
}

// ===============================
// WebRtcPeer implementation
// ===============================

class WebRtcPeer::Impl {
public:
    Impl()
        : loop_(nullptr),
          context_(nullptr),
          pipeline_(nullptr),
          appsrc_(nullptr),
          pay_(nullptr),
          webrtc_(nullptr),
          data_channel_(nullptr),
          opened_(false),
          negotiatedCodec_(VideoCodec::H264),
          state_(ConnState::New),
          pts_gen_ns_(0),
          frame_duration_ns_(0)
    {}

    ~Impl() { close(); }

    bool open(const WebRtcParams& p, const IngestParams& ing, const WebRtcCallbacks& cb);
    void close();
    bool isOpen() const { return opened_; }

    bool createOffer();
    bool createAnswer();
    bool setRemoteDescription(const Sdp& remote);
    bool addRemoteIceCandidate(const IceCandidate& c);

    bool pushRawFrame(const cv::Mat& frame, int64_t ptsNs);
    bool pushEncoded(VideoCodec codec, const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs);
    bool pushH264(const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs) {
        return pushEncoded(VideoCodec::H264, data, bytes, keyFrame, ptsNs);
    }

    void forceKeyframe();
    void setTargetBitrate(int bps);
    void setFramerate(int fps);
    void setWriteQueueLimitBytes(size_t bytes);
    bool sendDataMessage(const void* data, size_t nBytes, bool binary);

    std::map<std::string, std::string> getStats() const;
    ConnState getConnectionState() const { return state_; }
    VideoCodec getNegotiatedVideoCodec() const { return negotiatedCodec_; }

private:
    bool buildPipeline();
    bool linkRtpToWebrtc();
    void runMain();
    void changeState(ConnState s);
    static void onBusMessage(GstBus* bus, GstMessage* msg, gpointer user);

    // Helpers
    VideoCodec selectCodec(const WebRtcParams& p) const;

    // Members
    WebRtcParams   params_;
    IngestParams   ingest_;
    WebRtcCallbacks cb_;

    GMainLoop*   loop_;
    GMainContext* context_;
    std::thread  loop_thread_;

    GstElement* pipeline_;
    GstElement* appsrc_;
    GstElement* conv_;   // optional
    GstElement* enc_;    // optional
    GstElement* parse_;  // optional
    GstElement* pay_;
    GstElement* webrtc_;
    GstWebRTCDataChannel* data_channel_;

    std::atomic<bool> opened_;
    VideoCodec negotiatedCodec_;
    std::atomic<ConnState> state_;

    std::mutex push_mtx_;
    guint64 pts_gen_ns_;
    guint64 frame_duration_ns_;
};

VideoCodec WebRtcPeer::Impl::selectCodec(const WebRtcParams& p) const {
    for (auto c : p.preferredCodecs) {
        switch (c) {
            case VideoCodec::H264:
            case VideoCodec::VP8:
            case VideoCodec::AV1:
                return c;
        }
    }
    // Fallback
    return VideoCodec::H264;
}

bool WebRtcPeer::Impl::buildPipeline() {
    pipeline_ = gst_pipeline_new(nullptr);
    if (!pipeline_) return false;

    appsrc_ = gst_element_factory_make("appsrc", "src");
    if (!appsrc_) return false;

    // Live timestamps & backpressure
    g_object_set(appsrc_,
                 "is-live", TRUE,
                 "format", GST_FORMAT_TIME,
                 "block", FALSE,
                 nullptr);
    if (params_.maxQueueBytes > 0)
        g_object_set(appsrc_, "max-bytes", (guint64)params_.maxQueueBytes, nullptr);

    negotiatedCodec_ = selectCodec(params_);

    // Build branch depending on ingest mode
    if (ingest_.mode == IngestMode::PreEncodedElementary) {
        // caps for encoded stream
        GstCaps* caps = makeEncodedCaps(negotiatedCodec_, ingest_);
        if (!caps) return false;
        g_object_set(appsrc_, "caps", caps, nullptr);
        gst_caps_unref(caps);

        // optional parser
        parse_ = nullptr;
        if (negotiatedCodec_ == VideoCodec::H264) {
            parse_ = gst_element_factory_make("h264parse", nullptr);
            if (parse_) g_object_set(parse_, "config-interval", 1, nullptr);
        } else if (negotiatedCodec_ == VideoCodec::AV1) {
            parse_ = gst_element_factory_make("av1parse", nullptr);
        }

        pay_ = makePayloaderFor(negotiatedCodec_);
        if (!pay_) return false;
        setPayloaderDefaults(pay_);

        webrtc_ = gst_element_factory_make("webrtcbin", "webrtcbin");
        if (!webrtc_) return false;

        gst_bin_add_many(GST_BIN(pipeline_), appsrc_, nullptr);
        if (parse_) gst_bin_add(GST_BIN(pipeline_), parse_);
        gst_bin_add_many(GST_BIN(pipeline_), pay_, webrtc_, nullptr);

        bool ok = true;
        if (parse_) ok = gst_element_link(appsrc_, parse_) && gst_element_link(parse_, pay_);
        else        ok = gst_element_link(appsrc_, pay_);
        if (!ok) return false;

        // webrtcbin RTP sink pad linking (request pad "send_rtp_sink_0")
        if (!linkRtpToWebrtc()) return false;

    } else { // AutoEncodeRawBGR
        // appsrc caps for raw BGR
        GstCaps* rawCaps = gst_caps_new_simple("video/x-raw",
                                               "format", G_TYPE_STRING, "BGR",
                                               "width",  G_TYPE_INT, ingest_.width,
                                               "height", G_TYPE_INT, ingest_.height,
                                               "framerate", GST_TYPE_FRACTION, ingest_.framerate > 0 ? ingest_.framerate : 0, 1,
                                               nullptr);
        g_object_set(appsrc_, "caps", rawCaps, nullptr);
        gst_caps_unref(rawCaps);

        conv_ = gst_element_factory_make("videoconvert", nullptr);
        enc_  = makeEncoderFor(negotiatedCodec_, params_, ingest_);
        pay_  = makePayloaderFor(negotiatedCodec_);
        webrtc_ = gst_element_factory_make("webrtcbin", "webrtcbin");
        if (!conv_ || !enc_ || !pay_ || !webrtc_) return false;
        setPayloaderDefaults(pay_);

        gst_bin_add_many(GST_BIN(pipeline_), appsrc_, conv_, enc_, pay_, webrtc_, nullptr);

        if (!gst_element_link(appsrc_, conv_)) return false;
        if (!gst_element_link(conv_, enc_)) return false;
        // Insert h264parse for H.264 encoders to ensure Annex-B + SPS/PPS emission
        if (negotiatedCodec_ == VideoCodec::H264) {
            parse_ = gst_element_factory_make("h264parse", nullptr);
            if (!parse_) return false;
            g_object_set(parse_, "config-interval", 1, nullptr);
            gst_bin_add(GST_BIN(pipeline_), parse_);
            if (!gst_element_link(enc_, parse_)) return false;
            if (!gst_element_link(parse_, pay_)) return false;
        } else {
            if (!gst_element_link(enc_, pay_)) return false;
        }
        if (!linkRtpToWebrtc()) return false;
    }

    // Configure webrtcbin (STUN/TURN, MTU, BUNDLE, etc.)
    if (params_.mtu > 0) g_object_set(webrtc_, "mtu", params_.mtu, nullptr);

    // STUN/TURN via IceServer list
    GPtrArray* servers = g_ptr_array_new_with_free_func(g_free);
    for (size_t i = 0; i < params_.iceServers.size(); ++i) {
        const IceServer& s = params_.iceServers[i];
        if (s.uri.empty()) continue;
        // Format: "stun://host:port" or "turns://user:pass@host:port?transport=udp"
        // webrtcbin expects RFC7064/7065 URIs; pass through.
        g_ptr_array_add(servers, g_strdup(s.uri.c_str()));
    }
    g_object_set(webrtc_, "stun-server", nullptr, nullptr); // ensure default clear
    g_object_set(webrtc_, "turn-server", nullptr, nullptr); // legacy single-string; multiple via "ice-servers"
    g_object_set(webrtc_, "ice-servers", servers, nullptr);
    g_ptr_array_unref(servers);

    // Data channel
    if (params_.enableDataChannel) {
        GstWebRTCDataChannel* dc = nullptr;
        g_signal_emit_by_name(webrtc_, "create-data-channel", params_.dataChannelLabel.c_str(), nullptr, &dc);
        if (dc) {
            data_channel_ = dc;
            // onmessage (string)
            g_signal_connect(dc, "on-message-string",
                G_CALLBACK(+[](GstWebRTCDataChannel* /*dc*/, gchar* msg, gpointer u) {
                    Impl* self = static_cast<Impl*>(u);
                    if (self->cb_.onDataMessage) self->cb_.onDataMessage(std::string(msg ? msg : ""));
                }), this);
            // onmessage (binary)
            g_signal_connect(dc, "on-message-data",
                G_CALLBACK(+[](GstWebRTCDataChannel* /*dc*/, GBytes* bytes, gpointer u) {
                    Impl* self = static_cast<Impl*>(u);
                    if (!self->cb_.onDataMessage) return;
                    gsize n = 0;
                    const guint8* p = static_cast<const guint8*>(g_bytes_get_data(bytes, &n));
                    self->cb_.onDataMessage(std::string(reinterpret_cast<const char*>(p), reinterpret_cast<const char*>(p) + n));
                }), this);
        }
    }

    // Bus handler (errors, keyframe requests via element messages)
    GstBus* bus = gst_pipeline_get_bus(GST_PIPELINE(pipeline_));
    gst_bus_add_signal_watch(bus);
    g_signal_connect(bus, "message", G_CALLBACK(&Impl::onBusMessage), this);
    gst_object_unref(bus);

    return true;
}

bool WebRtcPeer::Impl::linkRtpToWebrtc() {
    // pay ! webrtcbin (request pad)
    GstPad* pay_src = gst_element_get_static_pad(pay_, "src");
    if (!pay_src) return false;

    GstPad* rtp_sink = gst_element_get_request_pad(webrtc_, "send_rtp_sink_0");
    if (!rtp_sink) {
        gst_object_unref(pay_src);
        return false;
    }
    if (gst_pad_link(pay_src, rtp_sink) != GST_PAD_LINK_OK) {
        gst_object_unref(pay_src);
        gst_object_unref(rtp_sink);
        return false;
    }
    gst_object_unref(pay_src);
    gst_object_unref(rtp_sink);
    return true;
}

void WebRtcPeer::Impl::runMain() {
    loop_ = g_main_loop_new(context_, FALSE);
    gst_element_set_state(pipeline_, GST_STATE_PLAYING);
    g_main_loop_run(loop_);
    gst_element_set_state(pipeline_, GST_STATE_NULL);
    if (loop_) {
        g_main_loop_unref(loop_);
        loop_ = nullptr;
    }
}

void WebRtcPeer::Impl::changeState(ConnState s) {
    state_ = s;
    if (cb_.onConnectionState) cb_.onConnectionState(s);
}

void WebRtcPeer::Impl::onBusMessage(GstBus* /*bus*/, GstMessage* msg, gpointer user) {
    Impl* self = static_cast<Impl*>(user);
    switch (GST_MESSAGE_TYPE(msg)) {
        case GST_MESSAGE_ERROR: {
            GError* err = nullptr; gchar* dbg = nullptr;
            gst_message_parse_error(msg, &err, &dbg);
            if (self->cb_.onError) self->cb_.onError("gst-error", err ? err->code : -1);
            if (err) g_error_free(err);
            if (dbg) g_free(dbg);
            break;
        }
        case GST_MESSAGE_ELEMENT: {
            // Heuristic: detect upstream keyframe requests (FIR/PLI) posted as element messages.
            const GstStructure* s = gst_message_get_structure(msg);
            if (!s) break;
            const gchar* name = gst_structure_get_name(s);
            if (name && (g_str_has_prefix(name, "GstForceKeyUnit") || g_str_has_prefix(name, "GstVideoForceKeyUnit"))) {
                if (self->cb_.onKeyframeRequested) self->cb_.onKeyframeRequested();
            }
            break;
        }
        default: break;
    }
}

bool WebRtcPeer::Impl::open(const WebRtcParams& p, const IngestParams& ing, const WebRtcCallbacks& cb) {
    if (opened_) return true;
    ensureInit();

    params_ = p;
    ingest_ = ing;
    cb_     = cb;

    // Dedicated GLib context per peer to keep things isolated.
    context_ = g_main_context_new();

    if (!buildPipeline()) {
        if (cb_.onError) cb_.onError("build-pipeline", -1);
        g_main_context_unref(context_);
        context_ = nullptr;
        return false;
    }

    // webrtcbin signals

    // on-ice-candidate
    g_signal_connect(webrtc_, "on-ice-candidate",
        G_CALLBACK(+[](GstElement* /*webrtc*/, guint mline, gchar* candidate, gpointer u) {
            Impl* self = static_cast<Impl*>(u);
            if (self->cb_.onIceCandidate) {
                IceCandidate ic;
                ic.candidate = candidate ? candidate : "";
                ic.sdpMid = "video"; // browsers may ignore; mline index is the key
                ic.sdpMLineIndex = static_cast<int>(mline);
                self->cb_.onIceCandidate(ic);
            }
        }), this);

    // on-connection-state
    g_signal_connect(webrtc_, "on-connection-state",
        G_CALLBACK(+[](GstElement* /*webrtc*/, GstWebRTCPeerConnectionState st, gpointer u) {
            Impl* self = static_cast<Impl*>(u);
            self->changeState(toConnState(st));
        }), this);

    // Start mainloop thread
    loop_thread_ = std::thread([this]() {
        // Attach context to this thread
        g_main_context_push_thread_default(context_);
        runMain();
        g_main_context_pop_thread_default(context_);
    });

    // Timestamp generation defaults
    if (ingest_.framerate > 0)
        frame_duration_ns_ = (guint64)(1000000000LL / ingest_.framerate);

    opened_ = true;
    return true;
}

void WebRtcPeer::Impl::close() {
    if (!opened_) return;
    opened_ = false;

    if (context_) g_main_context_wakeup(context_);
    if (loop_) g_main_loop_quit(loop_);

    if (loop_thread_.joinable()) loop_thread_.join();

    if (pipeline_) {
        gst_object_unref(pipeline_);
        pipeline_ = nullptr;
    }
    appsrc_ = conv_ = enc_ = parse_ = pay_ = webrtc_ = nullptr;
    data_channel_ = nullptr;

    if (context_) {
        g_main_context_unref(context_);
        context_ = nullptr;
    }
    changeState(ConnState::Closed);
}


    bool WebRtcPeer::Impl::createOffer() {
    if (!webrtc_) return false;

    GstPromise* promise = gst_promise_new_with_change_func(
        +[](GstPromise* p, gpointer u) {
            Impl* self = static_cast<Impl*>(u);

            const GstStructure* reply = gst_promise_get_reply(p);  // OK
            GstWebRTCSessionDescription* offer = nullptr;
            gst_structure_get(reply, "offer",
                              GST_TYPE_WEBRTC_SESSION_DESCRIPTION, &offer, nullptr);
            if (!offer) { gst_promise_unref(p); return; }

            g_signal_emit_by_name(self->webrtc_, "set-local-description", offer, nullptr);

            gchar* sdp_str = gst_sdp_message_as_text(offer->sdp);
            if (self->cb_.onLocalDescription) {
                Sdp sdp; sdp.type = "offer"; sdp.sdp = sdp_str ? sdp_str : "";
                self->cb_.onLocalDescription(sdp);
            }
            g_free(sdp_str);
            gst_webrtc_session_description_free(offer);
            gst_promise_unref(p);
        },
        this,
        /*notify*/ nullptr); // <-- 3rd param required

    // options must be passed (NULL is fine)
    g_signal_emit_by_name(webrtc_, "create-offer", /*options*/ nullptr, promise);
    return true;
}


    bool WebRtcPeer::Impl::createAnswer() {
    if (!webrtc_) return false;

    GstPromise* promise = gst_promise_new_with_change_func(
        +[](GstPromise* p, gpointer u) {
            Impl* self = static_cast<Impl*>(u);

            const GstStructure* reply = gst_promise_get_reply(p);  // OK
            GstWebRTCSessionDescription* answer = nullptr;
            gst_structure_get(reply, "answer",
                              GST_TYPE_WEBRTC_SESSION_DESCRIPTION, &answer, nullptr);
            if (!answer) { gst_promise_unref(p); return; }

            g_signal_emit_by_name(self->webrtc_, "set-local-description", answer, nullptr);

            gchar* sdp_str = gst_sdp_message_as_text(answer->sdp);
            if (self->cb_.onLocalDescription) {
                Sdp sdp; sdp.type = "answer"; sdp.sdp = sdp_str ? sdp_str : "";
                self->cb_.onLocalDescription(sdp);
            }
            g_free(sdp_str);
            gst_webrtc_session_description_free(answer);
            gst_promise_unref(p);
        },
        this,
        /*notify*/ nullptr); // <-- 3rd param required

    g_signal_emit_by_name(webrtc_, "create-answer", /*options*/ nullptr, promise);
    return true;
}

bool WebRtcPeer::Impl::setRemoteDescription(const Sdp& remote) {
    if (!webrtc_) return false;

    GstSDPMessage* sdp = nullptr;
    if (gst_sdp_message_new(&sdp) != GST_SDP_OK) return false;
    if (gst_sdp_message_parse_buffer((guint8*)remote.sdp.c_str(), remote.sdp.size(), sdp) != GST_SDP_OK) {
        gst_sdp_message_free(sdp);
        return false;
    }
    GstWebRTCSessionDescription* desc = gst_webrtc_session_description_new(toGstType(remote.type), sdp);

    GstPromise* p = gst_promise_new();
    g_signal_emit_by_name(webrtc_, "set-remote-description", desc, p);
    gst_promise_interrupt(p);
    gst_promise_unref(p);

    gst_webrtc_session_description_free(desc);

    // If remote was an offer, app may call createAnswer() next.
    return true;
}

bool WebRtcPeer::Impl::addRemoteIceCandidate(const IceCandidate& c) {
    if (!webrtc_) return false;
    g_signal_emit_by_name(webrtc_, "add-ice-candidate", c.sdpMLineIndex, c.candidate.c_str());
    return true;
}

bool WebRtcPeer::Impl::pushRawFrame(const cv::Mat& frame, int64_t ptsNs) {
    if (!opened_ || !appsrc_) return false;
    if (frame.empty() || frame.type() != CV_8UC3) return false; // BGR8 expected

    const size_t sz = static_cast<size_t>(frame.total() * frame.elemSize());
    GstBuffer* buf = gst_buffer_new_and_alloc(sz);
    if (!buf) return false;

    // Copy; if you own the memory, you could wrap with GstMemory instead.
    GstMapInfo map;
    gst_buffer_map(buf, &map, GST_MAP_WRITE);
    std::memcpy(map.data, frame.data, sz);
    gst_buffer_unmap(buf, &map);

    guint64 pts = 0;
    guint64 dur = 0;
    if (ingest_.useProvidedTimestamps && ptsNs >= 0) {
        pts = (guint64)ptsNs;
        dur = (ingest_.framerate > 0) ? frame_duration_ns_ : 0;
    } else {
        // simple generator
        pts = pts_gen_ns_;
        dur = frame_duration_ns_;
        pts_gen_ns_ += dur ? dur : 0;
    }
    GST_BUFFER_PTS(buf) = pts;
    GST_BUFFER_DTS(buf) = GST_CLOCK_TIME_NONE;
    GST_BUFFER_DURATION(buf) = dur;

    GstFlowReturn ret;
    g_signal_emit_by_name(appsrc_, "push-buffer", buf, &ret);
    gst_buffer_unref(buf);
    return ret == GST_FLOW_OK;
}

bool WebRtcPeer::Impl::pushEncoded(VideoCodec codec, const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs) {
    if (!opened_ || !appsrc_ || !data || bytes == 0) return false;
    if (codec != negotiatedCodec_) {
        // Different from negotiated path – reject to avoid mismatched caps/pt
        return false;
    }

    GstBuffer* buf = gst_buffer_new_and_alloc(bytes);
    if (!buf) return false;

    GstMapInfo map;
    gst_buffer_map(buf, &map, GST_MAP_WRITE);
    std::memcpy(map.data, data, bytes);
    gst_buffer_unmap(buf, &map);

    guint64 pts = 0;
    guint64 dur = 0;
    if (ingest_.useProvidedTimestamps && ptsNs >= 0) {
        pts = (guint64)ptsNs;
        dur = (ingest_.framerate > 0) ? frame_duration_ns_ : 0;
    } else {
        pts = pts_gen_ns_;
        dur = frame_duration_ns_;
        pts_gen_ns_ += dur ? dur : 0;
    }
    GST_BUFFER_PTS(buf) = pts;
    GST_BUFFER_DTS(buf) = GST_CLOCK_TIME_NONE;
    GST_BUFFER_DURATION(buf) = dur;

    if (!keyFrame) GST_BUFFER_FLAG_SET(buf, GST_BUFFER_FLAG_DELTA_UNIT);
    else           GST_BUFFER_FLAG_UNSET(buf, GST_BUFFER_FLAG_DELTA_UNIT);

    GstFlowReturn ret;
    g_signal_emit_by_name(appsrc_, "push-buffer", buf, &ret);
    gst_buffer_unref(buf);
    return ret == GST_FLOW_OK;
}

void WebRtcPeer::Impl::forceKeyframe() {
    if (!webrtc_) return;
    // Push a ForceKeyUnit event upstream from payloader side
    GstStructure* s = gst_structure_new("GstForceKeyUnit",
                                        "all-headers", G_TYPE_BOOLEAN, TRUE,
                                        "count",       G_TYPE_UINT, 0,
                                        NULL);
    GstEvent* ev = gst_event_new_custom(GST_EVENT_CUSTOM_UPSTREAM, s);
    if (pay_) {
        GstPad* sink = gst_element_get_static_pad(pay_, "sink");
        if (sink) {
            gst_pad_send_event(sink, ev);
            gst_object_unref(sink);
            return;
        }
    }
    gst_event_unref(ev);
}

void WebRtcPeer::Impl::setTargetBitrate(int bps) {
    // Best-effort: set on encoder if present; otherwise ignore.
    if (enc_) {
        // Common property name "bitrate" (bps or kbps depending on encoder)
        // Try both interpretations without failing if unknown.
        g_object_set(enc_, "bitrate", bps, nullptr);
        if (bps >= 1000) g_object_set(enc_, "bitrate", bps / 1000, nullptr);
    }
}

void WebRtcPeer::Impl::setFramerate(int fps) {
    if (fps <= 0) return;
    frame_duration_ns_ = (guint64)(1000000000LL / fps);
}

void WebRtcPeer::Impl::setWriteQueueLimitBytes(size_t bytes) {
    if (appsrc_) g_object_set(appsrc_, "max-bytes", (guint64)bytes, nullptr);
}

    bool WebRtcPeer::Impl::sendDataMessage(const void* data, size_t nBytes, bool binary) {
    if (!data_channel_) return false;

    // Check property: "ready-state" (enum GstWebRTCDataChannelState)
    GstWebRTCDataChannelState st = GST_WEBRTC_DATA_CHANNEL_STATE_CLOSED;
    g_object_get(data_channel_, "ready-state", &st, nullptr);
    if (st != GST_WEBRTC_DATA_CHANNEL_STATE_OPEN)
        return false;

#if GST_CHECK_VERSION(1,22,0)
    if (binary) {
        GBytes* b = g_bytes_new(data, nBytes);
        GError* err = nullptr;
        gboolean ok = gst_webrtc_data_channel_send_data_full(data_channel_, b, &err);
        g_bytes_unref(b);
        if (!ok) { if (err) g_error_free(err); return false; }
        if (err) g_error_free(err);
        return true;
    } else {
        GError* err = nullptr;
        gboolean ok = gst_webrtc_data_channel_send_string_full(data_channel_,
                                                               std::string(static_cast<const char*>(data),
                                                                           static_cast<const char*>(data)+nBytes).c_str(),
                                                               &err);
        if (!ok) { if (err) g_error_free(err); return false; }
        if (err) g_error_free(err);
        return true;
    }
#else
    // Legacy APIs (void return). We already checked OPEN state.
    if (binary) {
        GBytes* b = g_bytes_new(data, nBytes);
        gst_webrtc_data_channel_send_data(data_channel_, b);
        g_bytes_unref(b);
    } else {
        std::string s(static_cast<const char*>(data),
                      static_cast<const char*>(data) + nBytes);
        gst_webrtc_data_channel_send_string(data_channel_, s.c_str());
    }
    return true;
#endif
}

std::map<std::string, std::string> WebRtcPeer::Impl::getStats() const {
    std::map<std::string, std::string> out;
    // Minimal placeholder; richer stats require parsing webrtcbin get-stats promise
    // which varies by GStreamer version. Provide something stable.
    out["state"] = (state_ == ConnState::Connected ? "connected" :
                    state_ == ConnState::Connecting ? "connecting" :
                    state_ == ConnState::Failed ? "failed" :
                    state_ == ConnState::Disconnected ? "disconnected" :
                    state_ == ConnState::Closed ? "closed" : "new");
    out["codec"] = toString(negotiatedCodec_);
    return out;
}

// ===============================
// WebRtcPeer (public) wrappers
// ===============================

WebRtcPeer::WebRtcPeer() : pimpl(new Impl) {}
WebRtcPeer::~WebRtcPeer() {}

bool WebRtcPeer::open(const WebRtcParams& params, const IngestParams& ingest, const WebRtcCallbacks& cb) {
    return pimpl->open(params, ingest, cb);
}
void WebRtcPeer::close() { pimpl->close(); }
bool WebRtcPeer::isOpen() const { return pimpl->isOpen(); }

bool WebRtcPeer::createOffer() { return pimpl->createOffer(); }
bool WebRtcPeer::createAnswer() { return pimpl->createAnswer(); }
bool WebRtcPeer::setRemoteDescription(const Sdp& remote) { return pimpl->setRemoteDescription(remote); }
bool WebRtcPeer::addRemoteIceCandidate(const IceCandidate& cand) { return pimpl->addRemoteIceCandidate(cand); }

bool WebRtcPeer::pushRawFrame(const cv::Mat& frame, int64_t ptsNs) { return pimpl->pushRawFrame(frame, ptsNs); }
bool WebRtcPeer::pushEncoded(VideoCodec codec, const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs) {
    return pimpl->pushEncoded(codec, data, bytes, keyFrame, ptsNs);
}
bool WebRtcPeer::pushH264(const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs) {
    return pimpl->pushH264(data, bytes, keyFrame, ptsNs);
}

void WebRtcPeer::forceKeyframe() { pimpl->forceKeyframe(); }
void WebRtcPeer::setTargetBitrate(int bps) { pimpl->setTargetBitrate(bps); }
void WebRtcPeer::setFramerate(int fps) { pimpl->setFramerate(fps); }
void WebRtcPeer::setWriteQueueLimitBytes(size_t bytes) { pimpl->setWriteQueueLimitBytes(bytes); }

bool WebRtcPeer::sendDataMessage(const void* data, size_t nBytes, bool binary) {
    return pimpl->sendDataMessage(data, nBytes, binary);
}

std::map<std::string, std::string> WebRtcPeer::getStats() const { return pimpl->getStats(); }
ConnState WebRtcPeer::getConnectionState() const { return pimpl->getConnectionState(); }
VideoCodec WebRtcPeer::getNegotiatedVideoCodec() const { return pimpl->getNegotiatedVideoCodec(); }

// Factory
std::unique_ptr<WebRtcPeer> createWebRtcPeer() {
    return std::unique_ptr<WebRtcPeer>(new WebRtcPeer());
}

} // namespace stream
} // namespace cv
#endif