#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)

// webrtc.cpp (encoded-only)
// Build: requires GStreamer (core + webrtc + sdp + app + debugutils)
//
// This variant **does not accept raw frames**. You must feed pre-encoded
// H.264 / VP8 / AV1 AUs/OBUs via pushEncoded()/pushH264().
// (e.g., encode with FFmpeg/Encoder first, then pass the encoded units here.)

#include <opencv2/stream/webrtc.hpp>

#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <utility>
#include <string>
#include <vector>
#include <cstring>

// GStreamer
#include <cinttypes>
#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/sdp/gstsdpmessage.h>
#include <gst/webrtc/webrtc.h>
#include <gst/gstdebugutils.h> // gst_debug_bin_to_dot_file_with_ts

namespace cv {
namespace stream {

// ============================================================================
// Logging helpers
// ============================================================================

static inline void wrtc_log(const char* fmt, ...) {
    std::fprintf(stderr, "[webrtc] ");
    va_list ap; va_start(ap, fmt);
    std::vfprintf(stderr, fmt, ap);
    va_end(ap);
    std::fprintf(stderr, "\n");
}

static inline std::string capsToStr(GstCaps* caps) {
    if (!caps) return "(null)";
    gchar* s = gst_caps_to_string(caps);
    std::string out = s ? s : "(?)";
    if (s) g_free(s);
    return out;
}

static inline const char* msgTypeName(GstMessageType t) {
    return gst_message_type_get_name(t);
}

static inline void dot_dump(GstElement* pipeline, const char* why) {
    const char* en = std::getenv("CV_WRTC_DOT");
    if (!en || !*en) return;
#if !defined(GST_DISABLE_GST_DEBUG)
    wrtc_log("DOT: dumping pipeline graph (%s)", why ? why : "");
    gst_debug_bin_to_dot_file_with_ts(GST_BIN(pipeline),
                                      GST_DEBUG_GRAPH_SHOW_ALL, why ? why : "webrtc");
#endif
}

// small utils
static inline bool has_prop(GObject* obj, const char* name) {
    if (!obj || !name) return false;
    GObjectClass* cls = G_OBJECT_GET_CLASS(obj);
    return g_object_class_find_property(cls, name) != nullptr;
}

// ============================================================================
// ICE server helper
// ============================================================================

static void configureIceServers(GstElement* webrtc, const std::vector<IceServer>& servers) {
    if (!webrtc || servers.empty()) return;

    if (has_prop(G_OBJECT(webrtc), "ice-servers")) {
        GPtrArray* arr = g_ptr_array_new_with_free_func(g_free);
        for (const auto& s : servers) {
            if (!s.uri.empty())
                g_ptr_array_add(arr, g_strdup(s.uri.c_str()));
        }
        g_object_set(webrtc, "ice-servers", arr, nullptr);
        g_ptr_array_unref(arr);
        wrtc_log("webrtcbin(modern): configured %zu ICE server URI(s)", servers.size());
        return;
    }

    // Legacy path (older webrtcbin)
    for (const auto& s : servers) {
        if (s.uri.rfind("stun:", 0) == 0 || s.uri.rfind("stuns:", 0) == 0) {
            std::string stun = (s.uri.rfind("stuns:",0)==0)
                ? ("stuns://" + s.uri.substr(6))
                : ("stun://"  + s.uri.substr(5));
            if (has_prop(G_OBJECT(webrtc), "stun-server")) {
                wrtc_log("webrtcbin: legacy stun-server=%s", stun.c_str());
                g_object_set(webrtc, "stun-server", stun.c_str(), nullptr);
            }
        } else if (s.uri.rfind("turn", 0) == 0) {
            if (has_prop(G_OBJECT(webrtc), "turn-server")) {
                wrtc_log("webrtcbin: legacy turn-server=%s", s.uri.c_str());
                g_object_set(webrtc, "turn-server", s.uri.c_str(), nullptr);
            }
        }
    }
}

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
    return GST_WEBRTC_SDP_TYPE_ANSWER;
}

inline std::string escapeJson(const std::string& s) {
    std::string out; out.reserve(s.size() + 16);
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
    const std::string pat = "\"" + key + "\"";
    size_t p = json.find(pat);
    if (p == std::string::npos) return false;
    p = json.find(':', p);
    if (p == std::string::npos) return false;
    p = json.find('"', p);
    if (p == std::string::npos) return false;
    size_t q = p + 1;
    std::string val; bool esc = false;
    for (; q < json.size(); ++q) {
        char c = json[q];
        if (esc) {
            switch (c) {
                case 'n': val.push_back('\n'); break;
                case 'r': val.push_back('\r'); break;
                case 't': val.push_back('\t'); break;
                case '"': val.push_back('"');  break;
                case '\\': val.push_back('\\'); break;
                default: val.push_back(c); break;
            }
            esc = false; continue;
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
        case VideoCodec::H264:
            caps = gst_caps_new_simple("video/x-h264",
                                       "stream-format", G_TYPE_STRING, "byte-stream",
                                       "alignment",     G_TYPE_STRING, "au",
                                       nullptr);
            break;
        case VideoCodec::VP8:
            caps = gst_caps_new_simple("video/x-vp8", nullptr, nullptr);
            break;
        case VideoCodec::AV1:
            caps = gst_caps_new_simple("video/x-av1", nullptr, nullptr);
            break;
    }
    if (!caps) return nullptr;
    if (ingest.framerate > 0) {
        gst_caps_set_simple(caps, "framerate", GST_TYPE_FRACTION, ingest.framerate, 1, nullptr);
    }
    wrtc_log("caps (encoded): %s", capsToStr(caps).c_str());
    return caps;
}

inline GstElement* makePayloaderFor(VideoCodec codec) {
    const char* name = nullptr;
    switch (codec) {
        case VideoCodec::H264: name = "rtph264pay"; break;
        case VideoCodec::VP8:  name = "rtpvp8pay";  break;
        case VideoCodec::AV1:  name = "rtpav1pay";  break;
    }
    if (!name) return nullptr;
    wrtc_log("payloader: creating '%s'...", name);
    GstElement* e = gst_element_factory_make(name, nullptr);
    if (e) wrtc_log("  -> created %s", G_OBJECT_TYPE_NAME(e));
    else   wrtc_log("  -> not available");
    return e;
}

inline void setPayloaderDefaults(GstElement* pay) {
    if (!pay) return;
    g_object_set(pay, "pt", 96, nullptr);
    if (g_str_has_prefix(G_OBJECT_TYPE_NAME(pay), "GstRtpH264Pay")) {
        g_object_set(pay, "config-interval", 1, nullptr);
    }
}

} // namespace

// ============================================================================
// Global init / shutdown
// ============================================================================

static std::once_flag g_onceInit;
static std::atomic<int> g_initCount{0};

void ensureInit() {
    std::call_once(g_onceInit, []() {
        wrtc_log("gst_init()");
        gst_init(nullptr, nullptr);

        const char* must_have[] = { "webrtcbin", "rtph264pay", "h264parse" };
        for (const char* n : must_have) {
            GstElementFactory* f = gst_element_factory_find(n);
            wrtc_log("probe factory '%s' -> %s", n, f ? "OK" : "MISSING");
            if (f) gst_object_unref(f);
        }
    });
    ++g_initCount;
    wrtc_log("ensureInit: initCount=%d", (int)g_initCount.load());
}

void shutdown() {
    int c = --g_initCount;
    wrtc_log("shutdown: initCount=%d (no-op for GStreamer)", c);
}

// ============================================================================
// JSON helpers
// ============================================================================

bool parseSdpFromJson(const std::string& json, Sdp& out) {
    std::string t, s;
    if (!extractJsonString(json, "type", t)) return false;
    if (!extractJsonString(json, "sdp",  s)) return false;
    out.type = t; out.sdp = s;
    wrtc_log("parseSdpFromJson: type=%s, sdp_len=%zu", out.type.c_str(), out.sdp.size());
    return true;
}

bool parseIceFromJson(const std::string& json, IceCandidate& out) {
    std::string cand, mid;
    if (!extractJsonString(json, "candidate", cand)) return false;
    extractJsonString(json, "sdpMid", mid);
    out.candidate = cand;
    out.sdpMid = mid;
    size_t p = json.find("\"sdpMLineIndex\"");
    if (p != std::string::npos) {
        p = json.find(':', p);
        if (p != std::string::npos) {
            size_t q = p + 1;
            while (q < json.size() && (json[q] == ' ' || json[q] == '\t')) ++q;
            int idx = 0; bool neg = false;
            if (q < json.size() && json[q] == '-') { neg = true; ++q; }
            while (q < json.size() && json[q] >= '0' && json[q] <= '9') { idx = idx * 10 + (json[q] - '0'); ++q; }
            out.sdpMLineIndex = neg ? -idx : idx;
        }
    }
    wrtc_log("parseIceFromJson: mid=%s, mline=%d, cand_len=%zu",
             out.sdpMid.c_str(), out.sdpMLineIndex, out.candidate.size());
    return true;
}

std::string makeSdpJson(const Sdp& in) {
    std::string s = std::string("{\"type\":\"") + escapeJson(in.type) + "\",\"sdp\":\"" + escapeJson(in.sdp) + "\"}";
    wrtc_log("makeSdpJson: type=%s, sdp_len=%zu", in.type.c_str(), in.sdp.size());
    return s;
}

std::string makeIceJson(const IceCandidate& in) {
    std::string s = "{\"candidate\":\"" + escapeJson(in.candidate) + "\"";
    if (!in.sdpMid.empty()) s += ",\"sdpMid\":\"" + escapeJson(in.sdpMid) + "\"";
    s += ",\"sdpMLineIndex\":" + std::to_string(in.sdpMLineIndex) + "}";
    wrtc_log("makeIceJson: mid=%s, mline=%d, cand_len=%zu",
             in.sdpMid.c_str(), in.sdpMLineIndex, in.candidate.size());
    return s;
}

// ============================================================================
// WebRtcPeer implementation (encoded-only)
// ============================================================================

class WebRtcPeer::Impl {
public:
    Impl()
        : loop_(nullptr),
          context_(nullptr),
          pipeline_(nullptr),
          appsrc_(nullptr),
          parse_(nullptr),
          pay_(nullptr),
          webrtc_(nullptr),
          data_channel_(nullptr),
          opened_(false),
          negotiatedCodec_(VideoCodec::H264),
          state_(ConnState::New),
          pts_gen_ns_(0),
          frame_duration_ns_(0) {
        wrtc_log("Impl::Impl()");
    }

    ~Impl() { wrtc_log("Impl::~Impl()"); close(); }

    bool open(const WebRtcParams& p, const IngestParams& ing, const WebRtcCallbacks& cb);
    void close();
    bool isOpen() const { return opened_; }

    bool createOffer();
    bool createAnswer();
    bool setRemoteDescription(const Sdp& remote);
    bool addRemoteIceCandidate(const IceCandidate& c);

    // ENCODED ONLY
    bool pushEncoded(VideoCodec codec, const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs);
    bool pushH264(const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs) {
        return pushEncoded(VideoCodec::H264, data, bytes, keyFrame, ptsNs);
    }

    void forceKeyframe();                 // best-effort; upstream is appsrc/parse
    void setTargetBitrate(int bps);       // no-op in encoded-only mode
    void setFramerate(int fps);           // pacing only if PTS not provided
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

    VideoCodec selectCodec(const WebRtcParams& p) const;

    // Members
    WebRtcParams    params_;
    IngestParams    ingest_;
    WebRtcCallbacks cb_;

    GMainLoop*     loop_;
    GMainContext*  context_;
    std::thread    loop_thread_;

    GstElement* pipeline_;
    GstElement* appsrc_;
    GstElement* parse_;
    GstElement* pay_;
    GstElement* webrtc_;
    GstWebRTCDataChannel* data_channel_;

    std::atomic<bool> opened_;
    VideoCodec negotiatedCodec_;
    std::atomic<ConnState> state_;

    std::mutex push_mtx_;
    guint64 pts_gen_ns_;
    guint64 frame_duration_ns_;

    std::atomic<uint64_t> enc_push_count_{0};
};

inline VideoCodec WebRtcPeer::Impl::selectCodec(const WebRtcParams& p) const {
    wrtc_log("selectCodec: preferred list size=%zu", p.preferredCodecs.size());
    for (auto c : p.preferredCodecs) {
        wrtc_log("  pref: %s", toString(c));
        switch (c) {
            case VideoCodec::H264:
            case VideoCodec::VP8:
            case VideoCodec::AV1:
                wrtc_log("  -> selecting %s", toString(c));
                return c;
        }
    }
    wrtc_log("  -> fallback H264");
    return VideoCodec::H264;
}

bool WebRtcPeer::Impl::buildPipeline() {
    auto fail = [this](const char* where) {
        wrtc_log("buildPipeline: FAIL at %s", where);
        if (cb_.onError) cb_.onError(where, -1);
        if (pipeline_) dot_dump(pipeline_, "fail");
        return false;
    };

    wrtc_log("buildPipeline: begin (encoded-only)");

    pipeline_ = gst_pipeline_new(nullptr);
    if (!pipeline_) return fail("new-pipeline");

    appsrc_ = gst_element_factory_make("appsrc", "src");
    if (!appsrc_) return fail("make-appsrc");

    wrtc_log("appsrc: created");
    g_object_set(appsrc_,
        "is-live", TRUE,
        "format", GST_FORMAT_TIME,
        "block", TRUE,
        "max-buffers", 2,
        "max-bytes", (guint64)(params_.maxQueueBytes ? params_.maxQueueBytes : (1u<<20)),
        "max-time", (gint64)0,
        nullptr);
    if (!ingest_.useProvidedTimestamps) g_object_set(appsrc_, "do-timestamp", TRUE, nullptr);

    negotiatedCodec_ = selectCodec(params_);
    wrtc_log("negotiated codec preference = %s", toString(negotiatedCodec_));

    // Set caps for encoded input
    {
        GstCaps* caps = makeEncodedCaps(negotiatedCodec_, ingest_);
        if (!caps) return fail("encoded-caps");
        g_object_set(appsrc_, "caps", caps, nullptr);
        gst_caps_unref(caps);
    }

    // Optional parser for some codecs
    parse_ = nullptr;
    if (negotiatedCodec_ == VideoCodec::H264) {
        parse_ = gst_element_factory_make("h264parse", nullptr);
        if (!parse_) return fail("make-h264parse");
        g_object_set(parse_, "config-interval", 1, nullptr);
    } else if (negotiatedCodec_ == VideoCodec::AV1) {
        parse_ = gst_element_factory_make("av1parse", nullptr);
        if (!parse_) return fail("make-av1parse");
    }

    pay_ = makePayloaderFor(negotiatedCodec_);
    if (!pay_) return fail("make-payloader");
    setPayloaderDefaults(pay_);

    webrtc_ = gst_element_factory_make("webrtcbin", "webrtcbin");
    if (!webrtc_) return fail("make-webrtcbin");

    // Assemble
    gst_bin_add(GST_BIN(pipeline_), appsrc_);
    if (parse_) gst_bin_add(GST_BIN(pipeline_), parse_);
    gst_bin_add_many(GST_BIN(pipeline_), pay_, webrtc_, nullptr);

    // Some versions of webrtcbin behave better if pipeline is READY before pad requests
    gst_element_set_state(pipeline_, GST_STATE_READY);

    GstElement* q = gst_element_factory_make("queue", "q_pay");
    if (!q) return fail("make-queue");
    g_object_set(q,
        "leaky", 2,                 // downstream (drop oldest)
        "max-size-buffers", 1,
        "max-size-bytes", 0,
        "max-size-time", 0,
        nullptr);
    gst_bin_add(GST_BIN(pipeline_), q);

    if (parse_) {
        wrtc_log("link: appsrc -> parse -> q -> pay");
        if (!gst_element_link_many(appsrc_, parse_, q, pay_, nullptr)) return fail("link-appsrc-parse-q-pay");
    } else {
        wrtc_log("link: appsrc -> q -> pay");
        if (!gst_element_link_many(appsrc_, q, pay_, nullptr)) return fail("link-appsrc-q-pay");
    }


    if (!linkRtpToWebrtc()) return fail("link-pay-webrtc");

    /* MTU + latency + ICE */
    if (params_.mtu > 0 && has_prop(G_OBJECT(webrtc_), "mtu")) {
        wrtc_log("webrtcbin: set mtu=%d", params_.mtu);
        g_object_set(webrtc_, "mtu", params_.mtu, nullptr);
    } else if (params_.mtu > 0) {
        wrtc_log("webrtcbin: 'mtu' property not present (legacy)");
    }
    if (has_prop(G_OBJECT(webrtc_), "latency")) {
        g_object_set(webrtc_, "latency", 50, nullptr); // try 20–80
    }
    if (!params_.iceServers.empty()) {
        wrtc_log("webrtcbin: configuring %zu ICE server(s)", params_.iceServers.size());
        configureIceServers(webrtc_, params_.iceServers);
    }
    /* payloader MTU (if supported) */
    if (has_prop(G_OBJECT(pay_), "mtu")) {
        g_object_set(pay_, "mtu", params_.mtu > 0 ? params_.mtu : 1200, nullptr);
    }


    // Bus watch
    GstBus* bus = gst_pipeline_get_bus(GST_PIPELINE(pipeline_));
    gst_bus_add_signal_watch(bus);
    g_signal_connect(bus, "message", G_CALLBACK(&Impl::onBusMessage), this);
    gst_object_unref(bus);

    dot_dump(pipeline_, "built");
    wrtc_log("buildPipeline: OK");
    return true;
}

bool WebRtcPeer::Impl::linkRtpToWebrtc() {
    wrtc_log("linkRtpToWebrtc: begin");

    GstCaps* rtp_caps = nullptr;
    switch (negotiatedCodec_) {
        case VideoCodec::H264:
            rtp_caps = gst_caps_new_simple("application/x-rtp",
                                           "media",         G_TYPE_STRING, "video",
                                           "encoding-name", G_TYPE_STRING, "H264",
                                           "payload",       G_TYPE_INT,    96,
                                           "clock-rate",    G_TYPE_INT,    90000,
                                           nullptr);
            break;
        case VideoCodec::VP8:
            rtp_caps = gst_caps_new_simple("application/x-rtp",
                                           "media",         G_TYPE_STRING, "video",
                                           "encoding-name", G_TYPE_STRING, "VP8",
                                           "payload",       G_TYPE_INT,    96,
                                           "clock-rate",    G_TYPE_INT,    90000,
                                           nullptr);
            break;
        case VideoCodec::AV1:
            rtp_caps = gst_caps_new_simple("application/x-rtp",
                                           "media",         G_TYPE_STRING, "video",
                                           "encoding-name", G_TYPE_STRING, "AV1",
                                           "payload",       G_TYPE_INT,    96,
                                           "clock-rate",    G_TYPE_INT,    90000,
                                           nullptr);
            break;
    }
    wrtc_log("  -> filtered link caps: %s", capsToStr(rtp_caps).c_str());

    gboolean ok = gst_element_link_pads_filtered(pay_, "src", webrtc_, "sink_%u", rtp_caps);
    if (rtp_caps) gst_caps_unref(rtp_caps);

    wrtc_log("linkRtpToWebrtc: %s", ok ? "OK" : "FAIL");
    return ok;
}

void WebRtcPeer::Impl::runMain() {
    wrtc_log("runMain: entering main loop");
    loop_ = g_main_loop_new(context_, FALSE);
    GstStateChangeReturn ret = gst_element_set_state(pipeline_, GST_STATE_PLAYING);
    wrtc_log("set_state(PLAYING) -> %d", ret);
    dot_dump(pipeline_, "playing");
    g_main_loop_run(loop_);
    wrtc_log("runMain: main loop exited");
    gst_element_set_state(pipeline_, GST_STATE_NULL);
    dot_dump(pipeline_, "stopped");
    if (loop_) { g_main_loop_unref(loop_); loop_ = nullptr; }
}

void WebRtcPeer::Impl::changeState(ConnState s) {
    wrtc_log("conn-state: %d -> %d", (int)state_.load(), (int)s);
    state_ = s;
    if (cb_.onConnectionState) cb_.onConnectionState(s);
}

void WebRtcPeer::Impl::onBusMessage(GstBus* /*bus*/, GstMessage* msg, gpointer user) {
    Impl* self = static_cast<Impl*>(user);
    if (!msg) return;
    GstMessageType t = GST_MESSAGE_TYPE(msg);
    wrtc_log("bus: %s", msgTypeName(t));

    switch (t) {
        case GST_MESSAGE_ERROR: {
            GError* err = nullptr; gchar* dbg = nullptr;
            gst_message_parse_error(msg, &err, &dbg);
            wrtc_log("ERROR: code=%d domain=%u msg=%s", err?err->code:-1, err?err->domain:0u, err&&err->message?err->message:"(nil)");
            if (dbg) wrtc_log("DEBUG: %s", dbg);
            if (self->cb_.onError) self->cb_.onError("gst-error", err ? err->code : -1);
            if (err) g_error_free(err);
            if (dbg) g_free(dbg);
            dot_dump(self->pipeline_, "bus_error");
            break;
        }
        case GST_MESSAGE_WARNING: {
            GError* err = nullptr; gchar* dbg = nullptr;
            gst_message_parse_warning(msg, &err, &dbg);
            wrtc_log("WARNING: code=%d domain=%u msg=%s", err?err->code:-1, err?err->domain:0u, err&&err->message?err->message:"(nil)");
            if (dbg) wrtc_log("DEBUG: %s", dbg);
            if (err) g_error_free(err);
            if (dbg) g_free(dbg);
            break;
        }
        case GST_MESSAGE_INFO: {
            GError* err = nullptr; gchar* dbg = nullptr;
            gst_message_parse_info(msg, &err, &dbg);
            wrtc_log("INFO: %s", err&&err->message?err->message:"(nil)");
            if (dbg) wrtc_log("DEBUG: %s", dbg);
            if (err) g_error_free(err);
            if (dbg) g_free(dbg);
            break;
        }
        case GST_MESSAGE_STATE_CHANGED: {
            if (GST_MESSAGE_SRC(msg) == GST_OBJECT(self->pipeline_)) {
                GstState old_s, new_s, pending;
                gst_message_parse_state_changed(msg, &old_s, &new_s, &pending);
                wrtc_log("state-changed: pipeline %d -> %d (pending %d)", old_s, new_s, pending);
            }
            break;
        }
        case GST_MESSAGE_ELEMENT: {
            const GstStructure* s = gst_message_get_structure(msg);
            if (!s) break;
            const gchar* name = gst_structure_get_name(s);
            wrtc_log("element-msg: %s", name ? name : "(nil)");
            if (name && (g_str_has_prefix(name, "GstForceKeyUnit") || g_str_has_prefix(name, "GstVideoForceKeyUnit"))) {
                if (self->cb_.onKeyframeRequested) self->cb_.onKeyframeRequested();
            }
            break;
        }
        default: break;
    }
}

bool WebRtcPeer::Impl::open(const WebRtcParams& p, const IngestParams& ing, const WebRtcCallbacks& cb) {
    if (opened_) { wrtc_log("open: already opened"); return true; }
    ensureInit();

    params_ = p;
    ingest_ = ing;
    cb_     = cb;

    wrtc_log("open: ingest (ENCODED-ONLY, fps=%d, usePTS=%d, maxQ=%zu)",
             ingest_.framerate, (int)ingest_.useProvidedTimestamps, params_.maxQueueBytes);
    wrtc_log("open: bitrate hints (init=%d, min=%d, max=%d), mtu=%d, lowLat=%d, dataCh=%d label='%s'",
             params_.initialBitrate, params_.minBitrate, params_.maxBitrate,
             params_.mtu, (int)params_.lowLatency,
             (int)params_.enableDataChannel, params_.dataChannelLabel.c_str());
    wrtc_log("open: preferred codecs: %zu", params_.preferredCodecs.size());
    for (auto c : params_.preferredCodecs) wrtc_log("  - %s", toString(c));

    context_ = g_main_context_new();
    wrtc_log("g_main_context_new -> %p", (void*)context_);

    if (!buildPipeline()) {
        if (cb_.onError) cb_.onError("build-pipeline", -1);
        g_main_context_unref(context_);
        context_ = nullptr;
        wrtc_log("open: buildPipeline failed");
        return false;
    }

    // webrtcbin signals
    g_signal_connect(webrtc_, "on-ice-candidate",
        G_CALLBACK(+[](GstElement* /*webrtc*/, guint mline, gchar* candidate, gpointer u) {
            Impl* self = static_cast<Impl*>(u);
            wrtc_log("signal: on-ice-candidate (mline=%u, cand_len=%zu)", mline, candidate?std::strlen(candidate):0u);
            if (self->cb_.onIceCandidate) {
                IceCandidate ic;
                ic.candidate = candidate ? candidate : "";
                ic.sdpMid.clear();                 // keep legacy compatibility: mid may be empty
                ic.sdpMLineIndex = (int)mline;
                self->cb_.onIceCandidate(ic);
            }
        }), this);

    if (g_signal_lookup("on-connection-state", G_OBJECT_TYPE(webrtc_))) {
        g_signal_connect(webrtc_, "on-connection-state",
            G_CALLBACK(+[](GstElement*, GstWebRTCPeerConnectionState st, gpointer u) {
                Impl* self = static_cast<Impl*>(u);
                wrtc_log("signal: on-connection-state -> %d", (int)st);
                self->changeState(toConnState(st));
            }), this);
    } else if (g_signal_lookup("on-ice-connection-state", G_OBJECT_TYPE(webrtc_))) {
        g_signal_connect(webrtc_, "on-ice-connection-state",
            G_CALLBACK(+[](GstElement*, GstWebRTCICEConnectionState st, gpointer u) {
                Impl* self = static_cast<Impl*>(u);
                wrtc_log("signal: on-ice-connection-state -> %d", (int)st);
                ConnState s = ConnState::New;
                switch (st) {
                    case GST_WEBRTC_ICE_CONNECTION_STATE_CHECKING:     s = ConnState::Connecting;    break;
                    case GST_WEBRTC_ICE_CONNECTION_STATE_CONNECTED:
                    case GST_WEBRTC_ICE_CONNECTION_STATE_COMPLETED:    s = ConnState::Connected;     break;
                    case GST_WEBRTC_ICE_CONNECTION_STATE_FAILED:       s = ConnState::Failed;        break;
                    case GST_WEBRTC_ICE_CONNECTION_STATE_DISCONNECTED: s = ConnState::Disconnected;  break;
                    default: break;
                }
                self->changeState(s);
            }), this);
    } else {
        wrtc_log("webrtcbin: no connection-state signals found (very old)");
    }

    // negotiation (modern path)
    if (g_signal_lookup("on-negotiation-needed", G_OBJECT_TYPE(webrtc_))) {
        g_signal_connect(webrtc_, "on-negotiation-needed",
            G_CALLBACK(+[](GstElement* webrtc, gpointer u) {
                Impl* self = static_cast<Impl*>(u);
                wrtc_log("signal: on-negotiation-needed");
                GstPromise* promise = gst_promise_new_with_change_func(
                    +[](GstPromise* p, gpointer u2) {
                        Impl* s = static_cast<Impl*>(u2);
                        wrtc_log("createOffer(neg-need): promise changed");
                        const GstStructure* reply = gst_promise_get_reply(p);
                        GstWebRTCSessionDescription* offer = nullptr;
                        gst_structure_get(reply, "offer", GST_TYPE_WEBRTC_SESSION_DESCRIPTION, &offer, nullptr);
                        if (!offer) { wrtc_log("createOffer(neg-need): no offer"); gst_promise_unref(p); return; }
                        g_signal_emit_by_name(s->webrtc_, "set-local-description", offer, nullptr);
                        gchar* sdp_str = gst_sdp_message_as_text(offer->sdp);
                        if (s->cb_.onLocalDescription) {
                            Sdp sdp; sdp.type = "offer"; sdp.sdp = sdp_str ? sdp_str : "";
                            s->cb_.onLocalDescription(sdp);
                        }
                        g_free(sdp_str);
                        gst_webrtc_session_description_free(offer);
                        gst_promise_unref(p);
                    }, self, nullptr);
                g_signal_emit_by_name(webrtc, "create-offer", /*options*/ nullptr, promise);
            }), this);
    } else {
        // legacy: kick it off now
        wrtc_log("legacy path: no on-negotiation-needed -> createOffer()");
        createOffer();
    }

    // datachannel (optional)
    if (params_.enableDataChannel) {
        wrtc_log("datachannel: creating label='%s'", params_.dataChannelLabel.c_str());
        GstWebRTCDataChannel* dc = nullptr;
        g_signal_emit_by_name(webrtc_, "create-data-channel", params_.dataChannelLabel.c_str(), nullptr, &dc);
        if (dc) {
            data_channel_ = dc;
            wrtc_log("datachannel: created %p", (void*)dc);
            g_signal_connect(dc, "on-message-string",
                G_CALLBACK(+[](GstWebRTCDataChannel* /*dc*/, gchar* msg, gpointer u) {
                    Impl* self = static_cast<Impl*>(u);
                    wrtc_log("datachannel: on-message-string len=%zu", msg?std::strlen(msg):0u);
                    if (self->cb_.onDataMessage) self->cb_.onDataMessage(std::string(msg ? msg : ""));
                }), this);
            g_signal_connect(dc, "on-message-data",
                G_CALLBACK(+[](GstWebRTCDataChannel* /*dc*/, GBytes* bytes, gpointer u) {
                    Impl* self = static_cast<Impl*>(u);
                    gsize n = 0; g_bytes_get_data(bytes, &n);
                    wrtc_log("datachannel: on-message-data n=%zu", (size_t)n);
                    if (!self->cb_.onDataMessage) return;
                    const guint8* p = static_cast<const guint8*>(g_bytes_get_data(bytes, &n));
                    self->cb_.onDataMessage(std::string(reinterpret_cast<const char*>(p),
                                                        reinterpret_cast<const char*>(p) + n));
                }), this);
        } else {
            wrtc_log("datachannel: create failed (null)");
        }
    }

    // Start GLib main loop thread
    loop_thread_ = std::thread([this]() {
        wrtc_log("loop thread: start");
        g_main_context_push_thread_default(context_);
        runMain();
        g_main_context_pop_thread_default(context_);
        wrtc_log("loop thread: end");
    });

    // Timestamp generation defaults
    if (ingest_.framerate > 0) {
        frame_duration_ns_ = (guint64)(1000000000LL / ingest_.framerate);
        wrtc_log("frame_duration_ns = %lu", (unsigned long)frame_duration_ns_);
    }

    opened_ = true;
    wrtc_log("open: OK");
    return true;
}

void WebRtcPeer::Impl::close() {
    wrtc_log("close: begin (opened=%d)", (int)opened_.load());
    if (!opened_) return;
    opened_ = false;

    if (context_) g_main_context_wakeup(context_);
    if (loop_)    g_main_loop_quit(loop_);

    if (loop_thread_.joinable()) {
        wrtc_log("close: joining loop thread...");
        loop_thread_.join();
    }

    if (pipeline_) {
        wrtc_log("close: unref pipeline");
        gst_object_unref(pipeline_);
        pipeline_ = nullptr;
    }
    appsrc_ = parse_ = pay_ = webrtc_ = nullptr;
    data_channel_ = nullptr;

    if (context_) {
        wrtc_log("close: unref main context");
        g_main_context_unref(context_);
        context_ = nullptr;
    }
    changeState(ConnState::Closed);
    wrtc_log("close: done");
}

bool WebRtcPeer::Impl::createOffer() {
    if (!webrtc_) { wrtc_log("createOffer: webrtc_ null"); return false; }
    wrtc_log("createOffer: begin");

    GstPromise* promise = gst_promise_new_with_change_func(
        +[](GstPromise* p, gpointer u) {
            Impl* self = static_cast<Impl*>(u);
            wrtc_log("createOffer: promise changed");
            const GstStructure* reply = gst_promise_get_reply(p);
            GstWebRTCSessionDescription* offer = nullptr;
            gst_structure_get(reply, "offer", GST_TYPE_WEBRTC_SESSION_DESCRIPTION, &offer, nullptr);
            if (!offer) { wrtc_log("createOffer: no offer in reply"); gst_promise_unref(p); return; }

            g_signal_emit_by_name(self->webrtc_, "set-local-description", offer, nullptr);

            gchar* sdp_str = gst_sdp_message_as_text(offer->sdp);
            wrtc_log("createOffer: SDP len=%zu", sdp_str?std::strlen(sdp_str):0u);
            if (self->cb_.onLocalDescription) {
                Sdp sdp; sdp.type = "offer"; sdp.sdp = sdp_str ? sdp_str : "";
                self->cb_.onLocalDescription(sdp);
            }
            g_free(sdp_str);
            gst_webrtc_session_description_free(offer);
            gst_promise_unref(p);
        },
        this, nullptr);

    g_signal_emit_by_name(webrtc_, "create-offer", /*options*/ nullptr, promise);
    wrtc_log("createOffer: requested");
    return true;
}

bool WebRtcPeer::Impl::createAnswer() {
    if (!webrtc_) { wrtc_log("createAnswer: webrtc_ null"); return false; }
    wrtc_log("createAnswer: begin");

    GstPromise* promise = gst_promise_new_with_change_func(
        +[](GstPromise* p, gpointer u) {
            Impl* self = static_cast<Impl*>(u);
            wrtc_log("createAnswer: promise changed");
            const GstStructure* reply = gst_promise_get_reply(p);
            GstWebRTCSessionDescription* answer = nullptr;
            gst_structure_get(reply, "answer", GST_TYPE_WEBRTC_SESSION_DESCRIPTION, &answer, nullptr);
            if (!answer) { wrtc_log("createAnswer: no answer in reply"); gst_promise_unref(p); return; }

            g_signal_emit_by_name(self->webrtc_, "set-local-description", answer, nullptr);

            gchar* sdp_str = gst_sdp_message_as_text(answer->sdp);
            wrtc_log("createAnswer: SDP len=%zu", sdp_str?std::strlen(sdp_str):0u);
            if (self->cb_.onLocalDescription) {
                Sdp sdp; sdp.type = "answer"; sdp.sdp = sdp_str ? sdp_str : "";
                self->cb_.onLocalDescription(sdp);
            }
            g_free(sdp_str);
            gst_webrtc_session_description_free(answer);
            gst_promise_unref(p);
        },
        this, nullptr);

    g_signal_emit_by_name(webrtc_, "create-answer", /*options*/ nullptr, promise);
    wrtc_log("createAnswer: requested");
    return true;
}

bool WebRtcPeer::Impl::setRemoteDescription(const Sdp& remote) {
    if (!webrtc_) { wrtc_log("setRemoteDescription: webrtc_ null"); return false; }
    wrtc_log("setRemoteDescription: type=%s, sdp_len=%zu", remote.type.c_str(), remote.sdp.size());

    GstSDPMessage* sdp = nullptr;
    if (gst_sdp_message_new(&sdp) != GST_SDP_OK) { wrtc_log("  -> sdp_message_new failed"); return false; }
    if (gst_sdp_message_parse_buffer(reinterpret_cast<const guint8*>(remote.sdp.c_str()),
                                     remote.sdp.size(), sdp) != GST_SDP_OK) {
        wrtc_log("  -> parse_buffer failed");
        gst_sdp_message_free(sdp);
        return false;
    }

    GstWebRTCSessionDescription* desc = gst_webrtc_session_description_new(toGstType(remote.type), sdp);
    GstPromise* p = gst_promise_new();
    wrtc_log("  -> emitting set-remote-description");
    g_signal_emit_by_name(webrtc_, "set-remote-description", desc, p);
    gst_promise_unref(p);
    gst_webrtc_session_description_free(desc);
    wrtc_log("setRemoteDescription: done");
    return true;
}

bool WebRtcPeer::Impl::addRemoteIceCandidate(const IceCandidate& c) {
    if (!webrtc_) { wrtc_log("addRemoteIceCandidate: webrtc_ null"); return false; }
    wrtc_log("addRemoteIceCandidate: mline=%d cand_len=%zu", c.sdpMLineIndex, c.candidate.size());

    // End-of-candidates: pass NULL on legacy webrtcbin
    if (c.candidate.empty()) {
        g_signal_emit_by_name(webrtc_, "add-ice-candidate", c.sdpMLineIndex, (const gchar*)nullptr);
        return true;
    }
    g_signal_emit_by_name(webrtc_, "add-ice-candidate", c.sdpMLineIndex, c.candidate.c_str());
    return true;
}

bool WebRtcPeer::Impl::pushEncoded(VideoCodec codec, const uint8_t* data, size_t bytes, bool keyFrame, int64_t ptsNs) {
    if (!opened_ || !appsrc_ || !data || bytes == 0) return false;
    if (codec != negotiatedCodec_) {
        wrtc_log("pushEncoded: codec mismatch (got %s, negotiated %s)", toString(codec), toString(negotiatedCodec_));
        return false;
    }

    GstBuffer* buf = gst_buffer_new_and_alloc(bytes);
    if (!buf) return false;

    GstMapInfo map;
    gst_buffer_map(buf, &map, GST_MAP_WRITE);
    std::memcpy(map.data, data, bytes);
    gst_buffer_unmap(buf, &map);

    guint64 pts = 0, dur = 0;
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

    uint64_t ccount = ++enc_push_count_;
    if ((ccount & 0xFF) == 0) wrtc_log("pushEncoded: count=%" PRIu64 " key=%d bytes=%zu pts=%" PRIu64 " flow=%d",
                                       ccount, (int)keyFrame, bytes, pts, ret);
    return ret == GST_FLOW_OK;
}

void WebRtcPeer::Impl::forceKeyframe() {
    // Best effort: post force-key-unit upstream to payloader sink (propagates back)
    if (!pay_) return;
    wrtc_log("forceKeyframe: posting upstream event");
    GstStructure* s = gst_structure_new("GstForceKeyUnit",
                                        "all-headers", G_TYPE_BOOLEAN, TRUE,
                                        "count",       G_TYPE_UINT, 0,
                                        NULL);
    GstEvent* ev = gst_event_new_custom(GST_EVENT_CUSTOM_UPSTREAM, s);
    GstPad* sink = gst_element_get_static_pad(pay_, "sink");
    if (sink) {
        gst_pad_send_event(sink, ev);
        gst_object_unref(sink);
    } else {
        gst_event_unref(ev);
    }
}

void WebRtcPeer::Impl::setTargetBitrate(int bps) {
    // No encoder in this path. You should reconfigure your upstream encoder.
    wrtc_log("setTargetBitrate(%d): no-op (encoded-only path; adjust upstream encoder)", bps);
}

void WebRtcPeer::Impl::setFramerate(int fps) {
    wrtc_log("setFramerate: %d", fps);
    if (fps > 0) frame_duration_ns_ = (guint64)(1000000000LL / fps);
}

void WebRtcPeer::Impl::setWriteQueueLimitBytes(size_t bytes) {
    wrtc_log("setWriteQueueLimitBytes: %zu", bytes);
    if (appsrc_) g_object_set(appsrc_, "max-bytes", (guint64)bytes, nullptr);
}

bool WebRtcPeer::Impl::sendDataMessage(const void* data, size_t nBytes, bool binary) {
    if (!data_channel_) { wrtc_log("sendDataMessage: no data channel"); return false; }

    GstWebRTCDataChannelState st = GST_WEBRTC_DATA_CHANNEL_STATE_CLOSED;
    g_object_get(data_channel_, "ready-state", &st, nullptr);
    wrtc_log("sendDataMessage: state=%d size=%zu binary=%d", (int)st, nBytes, (int)binary);
    if (st != GST_WEBRTC_DATA_CHANNEL_STATE_OPEN) return false;

#if GST_CHECK_VERSION(1,22,0)
    if (binary) {
        GBytes* b = g_bytes_new(data, nBytes);
        GError* err = nullptr;
        gboolean ok = gst_webrtc_data_channel_send_data_full(data_channel_, b, &err);
        g_bytes_unref(b);
        if (!ok) { wrtc_log("  -> send_data_full failed"); if (err) g_error_free(err); return false; }
        if (err) g_error_free(err);
        return true;
    } else {
        std::string s(static_cast<const char*>(data), static_cast<const char*>(data)+nBytes);
        GError* err = nullptr;
        gboolean ok = gst_webrtc_data_channel_send_string_full(data_channel_, s.c_str(), &err);
        if (!ok) { wrtc_log("  -> send_string_full failed"); if (err) g_error_free(err); return false; }
        if (err) g_error_free(err);
        return true;
    }
#else
    if (binary) {
        GBytes* b = g_bytes_new(data, nBytes);
        gst_webrtc_data_channel_send_data(data_channel_, b);
        g_bytes_unref(b);
    } else {
        std::string s(static_cast<const char*>(data), static_cast<const char*>(data) + nBytes);
        gst_webrtc_data_channel_send_string(data_channel_, s.c_str());
    }
    return true;
#endif
}

std::map<std::string, std::string> WebRtcPeer::Impl::getStats() const {
    std::map<std::string, std::string> out;
    out["state"] = (state_ == ConnState::Connected ? "connected" :
                    state_ == ConnState::Connecting ? "connecting" :
                    state_ == ConnState::Failed ? "failed" :
                    state_ == ConnState::Disconnected ? "disconnected" :
                    state_ == ConnState::Closed ? "closed" : "new");
    out["codec"] = toString(negotiatedCodec_);
    return out;
}

// ============================================================================
// WebRtcPeer (public) wrappers
// ============================================================================

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
