// stream.cpp
#include <opencv2/stream/stream.hpp>

#include <opencv2/core.hpp>
#include <opencv2/core/utils/logger.hpp>   // << Logging
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <atomic>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <unordered_map>
#include <deque>
#include <sstream>
#include <iomanip>
#include <typeinfo>
#if !defined(_WIN32)
  #include <unistd.h>  // for ::write
#endif

// ===== Super-verbose logging helpers =========================================
using cv::utils::logging::LogLevel;

static cv::utils::logging::LogTag kStreamLogTag(
    "cv.stream.stream",
    LogLevel::LOG_LEVEL_VERBOSE
);
static cv::utils::logging::LogTag* kTag = &kStreamLogTag;

static inline unsigned long long tid() {
    return (unsigned long long)std::hash<std::thread::id>{}(std::this_thread::get_id());
}
static inline const void* pvoid(const void* p) { return p; }
static inline int64_t monotonicNs() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now().time_since_epoch()).count();
}
struct TraceScope {
    const char* name;
    std::string extra;
    int64_t startNs;
    TraceScope(const char* n, std::string e = std::string())
        : name(n), extra(std::move(e)), startNs(monotonicNs()) {
        CV_LOG_VERBOSE(kTag, 0, "[TRACE ENTER] " << name
            << " tid=0x" << std::hex << tid() << std::dec
            << (extra.empty() ? "" : (" | " + extra)));
    }
    ~TraceScope() {
        int64_t endNs = monotonicNs();
        CV_LOG_VERBOSE(kTag, 0, "[TRACE EXIT ] " << name
            << " tid=0x" << std::hex << tid() << std::dec
            << " | dt_ns=" << (endNs - startNs));
    }
}
;

#define LOG_PTR(lbl, ptr) \
    CV_LOG_VERBOSE(kTag, 4, lbl << "=0x" << std::hex << (uintptr_t)(ptr) << std::dec)

#define LOG_KV(k, v) \
    CV_LOG_VERBOSE(kTag, 5, k << "=" << v)

#define LOG_LINE_HERE() \
    CV_LOG_VERBOSE(kTag, 5, "[line] " << __FILE__ << ":" << __LINE__)

// =============================================================================

using namespace std;

namespace cv {
namespace stream {

// ---- small time helper ------------------------------------------------------
static inline int64_t nowNs() {
    using clock = std::chrono::steady_clock;
    return std::chrono::duration_cast<std::chrono::nanoseconds>(clock::now().time_since_epoch()).count();
}

// ============================================================================
// Stream::Impl
// ============================================================================

class Stream::Impl {
public:
    Impl()
        : secure(false),
          running(false),
          nextId(1),
          rootIndexEnabled(false),
          rootMount("/"),
          rootTitle("OpenCV Stream") {
        TraceScope ts("Stream::Impl::Impl");
        CV_LOG_VERBOSE(kTag, 4, "Constructed Impl @0x" << std::hex << (uintptr_t)this << std::dec);
    }

    ~Impl() {
        TraceScope ts("Stream::Impl::~Impl");
        stop();
        CV_LOG_VERBOSE(kTag, 4, "Destroyed Impl @0x" << std::hex << (uintptr_t)this << std::dec);
    }

    // process
    bool start(const std::string& /*bindAddress*/, int port, bool secureMode, int numThreads);
    void stop();
    bool isRunning() const { return running; }

    void setSecureMode(bool on) { secure = on; }
    bool secureMode() const { return secure; }

    void enableRootIndex(bool on, const std::string& mount, const std::string& title);

    // endpoints
    EndpointHandle addRaw (const std::string& path, const FrameSource& src, const RawOptions& opts);
    EndpointHandle addFmp4(const std::string& path, const FrameSource& src, const Fmp4Options& opts);
#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
    EndpointHandle addWebRtc(const std::string& path, const EncodedSource& encoded, const WebRtcOptions& opts);
#endif
    void remove(const EndpointHandle& h);
    void removeByPath(const std::string& path);

    // pages
    void mountEmbedded(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view);
    void mountJupyter (const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view);

    // tokens
    bool allowAccess(const std::string& endpointPath, const std::string& token, AccessRole role, int ttlSec);
    void removeAccess(const std::string& endpointPath, const std::string& token);
    void clearAccess (const std::string& endpointPath);

    // params
    bool setParam(const EndpointHandle& h, const std::string& id, const std::string& value);
    bool getParam(const EndpointHandle& h, const std::string& id, std::string& outValue) const;
    std::map<std::string,std::string> listParams(const EndpointHandle& h) const;

    bool setParamByPath(const std::string& path, const std::string& id, const std::string& value);
    bool getParamByPath(const std::string& path, const std::string& id, std::string& outValue) const;
    std::map<std::string,std::string> listParamsByPath(const std::string& path) const;

    // recording
    std::string startRecording(const EndpointHandle& h, const std::string& dst);
    std::string stopRecording (const EndpointHandle& h);
    bool configureRecording(const EndpointHandle& h, const RecordingParams& rp);

    std::string startRecordingByPath(const std::string& path, const std::string& dst);
    std::string stopRecordingByPath (const std::string& path);
    bool configureRecordingByPath (const std::string& path, const RecordingParams& rp);
    bool splitRecordingSegment(const EndpointHandle& h);
    bool splitRecordingSegmentByPath(const std::string& path);

    // introspection
    std::vector<std::string> listEndpoints() const;
    EndpointKind kindOf(const std::string& path) const;

private:
    // --- per-endpoint state --------------------------------------------------
    struct TokenEntry {
        std::string tok;
        int64_t     expiresNs;   // <=0 => no expiry
        AccessRole  role;
    };

    struct SessionCtx {
        WebSocketSession* ws = NULL;
        bool authed = false;
        AccessRole role = AccessRole::ReadOnly;
        // pending init segment for fMP4
        bool fmp4InitSent = false;
    };

    struct Endpoint {
        EndpointKind kind;
        std::string  path;
        bool         editor;

        // param store
        std::map<std::string,std::string> params;
        mutable std::mutex paramMtx;

        // token gate
        std::vector<TokenEntry> tokens; // under gateMtx
        mutable std::mutex gateMtx;

        // sessions
        std::vector<SessionCtx> sessions;
        mutable std::mutex sessMtx;

        // sources
        FrameSource   rawSource;  // used by Raw and Fmp4 endpoints
        EncodedSource encSource;  // used by WebRTC (encoded-only)

        // encoder / recording (fMP4)
#if defined(HAVE_STREAM_COMPRESSION)
        std::unique_ptr<Encoder> encoder;
        RecordingParams recCfg;
        std::string      lastDest;
#endif

        // worker thread
        std::thread worker;
        std::atomic<bool> run{false};

        // raw pacing
        int fps = 0;

        // fMP4 init cache
        std::vector<uint8_t> fmp4Init;

        Endpoint() : kind(EndpointKind::Raw), editor(false), fps(0) {
            CV_LOG_VERBOSE(kTag, 5, "Endpoint() ctor @" << std::hex << (uintptr_t)this << std::dec);
        }
        ~Endpoint() {
            CV_LOG_VERBOSE(kTag, 5, "~Endpoint() dtor @" << std::hex << (uintptr_t)this << std::dec);
        }
    };

    // server & routing
    std::unique_ptr<Server> srv;
    bool secure;
    std::atomic<bool> running;
    int nextId;

    // endpoints
    std::map<int, std::shared_ptr<Endpoint> > byId;
    std::unordered_map<std::string,int> byPath; // path -> id
    mutable std::mutex epMtx;

    // root index
    bool        rootIndexEnabled;
    std::string rootMount;
    std::string rootTitle;

private:
    // helpers
    std::shared_ptr<Endpoint> getByHandle(const EndpointHandle& h) const;
    std::shared_ptr<Endpoint> getByPath  (const std::string& path) const;
    static bool tokenAllowed(const Endpoint& ep, const std::string& tok, AccessRole* outRole);
    static void dropExpiredTokens(Endpoint& ep);

    void attachRawWS (Endpoint& ep);
    void attachFmp4WS(Endpoint& ep);
#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
    void attachWebRtcWS(const WebRtcOptions& opts, const EncodedSource& enc, Endpoint& ep);
#endif

    void ensureRootPage();
    static void writeSimpleHtml(Response& res, const std::string& html);
};

// ============================================================================
// Impl: server lifecycle
// ============================================================================

bool Stream::Impl::start(const std::string& /*bindAddress*/, int port, bool secureMode, int numThreads) {
    TraceScope ts("Stream::Impl::start",
        (std::ostringstream() << "port=" << port << " secureMode=" << (int)secureMode
                              << " threads=" << numThreads).str());
    if (running) {
        CV_LOG_VERBOSE(kTag, 3, "Already running. srv=0x" << std::hex << (uintptr_t)srv.get() << std::dec);
        return true;
    }
    secure = secureMode;

    srv = createServer();
    LOG_PTR("srv", srv.get());
    if (!srv) {
        CV_LOG_ERROR(kTag, "createServer() failed (nullptr)");
        CV_Error(cv::Error::StsError, "stream::createServer() failed");
    }
    CV_LOG_VERBOSE(kTag, 4, "Calling Server::start(port=" << port << ", threads=" << numThreads << ")");
    if (!srv->start(port, numThreads)) {
        CV_LOG_ERROR(kTag, "Server::start() returned false");
        srv.reset();
        CV_Error(cv::Error::StsError, "Failed to start stream::Server");
    }

    running = true;
    CV_LOG_INFO(kTag, "Server started. port=" << port << " secure=" << secureMode
                   << " srv=0x" << std::hex << (uintptr_t)srv.get() << std::dec);

    // Root index mounting is deferred; user calls enableRootIndex().
    return true;
}

void Stream::Impl::stop() {
    TraceScope ts("Stream::Impl::stop");
    if (!running) {
        CV_LOG_VERBOSE(kTag, 4, "Not running; nothing to stop.");
        return;
    }

    // Stop workers
    {
        std::lock_guard<std::mutex> lock(epMtx);
        CV_LOG_VERBOSE(kTag, 4, "Stopping " << byId.size() << " endpoints...");
        for (auto& kv : byId) {
            auto& ep = *kv.second;
            CV_LOG_VERBOSE(kTag, 4, "Stop endpoint id=" << kv.first << " path=" << ep.path
                               << " worker.joinable=" << (int)ep.worker.joinable());
            ep.run = false;
            if (ep.worker.joinable()) ep.worker.join();
        }
        byId.clear();
        byPath.clear();
    }

    if (srv) {
        CV_LOG_VERBOSE(kTag, 4, "Stopping server @0x" << std::hex << (uintptr_t)srv.get() << std::dec);
        srv->stop();
        srv.reset();
    }
    running = false;
    CV_LOG_INFO(kTag, "Server stopped");
}

void Stream::Impl::enableRootIndex(bool on, const std::string& mount, const std::string& title) {
    TraceScope ts("Stream::Impl::enableRootIndex",
        (std::ostringstream() << "on=" << (int)on << " mount='" << mount << "' title='" << title << "'").str());
    rootIndexEnabled = on;
    rootMount = mount.empty() ? "/" : mount;
    rootTitle = title.empty() ? "OpenCV Stream" : title;

    LOG_KV("rootIndexEnabled", on);
    LOG_KV("rootMount", rootMount);
    LOG_KV("rootTitle", rootTitle);

    if (!srv) {
        CV_LOG_VERBOSE(kTag, 4, "No server; returning.");
        return;
    }
    if (on) {
        CV_LOG_VERBOSE(kTag, 4, "Ensuring root page at '" << rootMount << "'");
        ensureRootPage();
    } else {
        CV_LOG_VERBOSE(kTag, 4, "Unregistering root endpoint '" << rootMount << "'");
        srv->unregisterEndpoint(rootMount);
    }
}

// ============================================================================
// Impl: tokens
// ============================================================================

bool Stream::Impl::tokenAllowed(const Endpoint& ep, const std::string& tok, AccessRole* outRole) {
    TraceScope ts("Stream::Impl::tokenAllowed",
        (std::ostringstream() << "path=" << ep.path << " tok.len=" << tok.size()).str());
    int64_t t = nowNs();
    size_t i = 0;
    for (const auto& e : ep.tokens) {
        CV_LOG_VERBOSE(kTag, 6, "scan token[" << i++ << "] role=" << (int)e.role
                            << " exp_ns=" << e.expiresNs << " now_ns=" << t);
        if (e.tok == tok) {
            if (e.expiresNs <= 0 || e.expiresNs > t) {
                if (outRole) *outRole = e.role;
                CV_LOG_VERBOSE(kTag, 4, "tokenAllowed -> true");
                return true;
            }
            // expired, but keep scanning other entries
            CV_LOG_VERBOSE(kTag, 5, "Matched expired token; continue scanning.");
        }
    }
    CV_LOG_VERBOSE(kTag, 4, "tokenAllowed -> false");
    return false;
}

void Stream::Impl::dropExpiredTokens(Endpoint& ep) {
    TraceScope ts("Stream::Impl::dropExpiredTokens", ep.path);
    int64_t t = nowNs();
    std::vector<TokenEntry> keep;
    keep.reserve(ep.tokens.size());
    for (auto& e : ep.tokens) {
        bool ok = (e.expiresNs <= 0 || e.expiresNs > t);
        CV_LOG_VERBOSE(kTag, 6, "token e.tok.len=" << e.tok.size()
                            << " exp=" << e.expiresNs << " now=" << t
                            << " keep=" << (int)ok);
        if (ok) keep.push_back(e);
    }
    size_t dropped = ep.tokens.size() - keep.size();
    ep.tokens.swap(keep);
    CV_LOG_VERBOSE(kTag, 4, "Expired tokens dropped: " << dropped << " remain=" << ep.tokens.size());
}

bool Stream::Impl::allowAccess(const std::string& endpointPath, const std::string& token, AccessRole role, int ttlSec) {
    TraceScope ts("Stream::Impl::allowAccess",
        (std::ostringstream() << "path=" << endpointPath << " tok.len=" << token.size()
                              << " role=" << (int)role << " ttlSec=" << ttlSec).str());
    auto ep = getByPath(endpointPath);
    if (!ep) {
        CV_LOG_WARNING(kTag, "allowAccess: endpoint not found: " << endpointPath);
        return false;
    }
    std::lock_guard<std::mutex> lock(ep->gateMtx);
    int64_t exp = (ttlSec > 0) ? (nowNs() + (int64_t)ttlSec * 1000000000LL) : 0;
    // refresh if existing
    for (auto& e : ep->tokens) {
        if (e.tok == token) { e.expiresNs = exp; e.role = role; CV_LOG_VERBOSE(kTag, 4, "Refreshed token"); return true; }
    }
    ep->tokens.push_back(TokenEntry{token, exp, role});
    CV_LOG_VERBOSE(kTag, 4, "Added token; count=" << ep->tokens.size());
    return true;
}

void Stream::Impl::removeAccess(const std::string& endpointPath, const std::string& token) {
    TraceScope ts("Stream::Impl::removeAccess",
        (std::ostringstream() << "path=" << endpointPath << " tok.len=" << token.size()).str());
    auto ep = getByPath(endpointPath);
    if (!ep) { CV_LOG_WARNING(kTag, "removeAccess: endpoint not found: " << endpointPath); return; }
    std::lock_guard<std::mutex> lock(ep->gateMtx);
    std::vector<TokenEntry> keep;
    for (auto& e : ep->tokens) if (e.tok != token) keep.push_back(e);
    size_t dropped = ep->tokens.size() - keep.size();
    ep->tokens.swap(keep);
    CV_LOG_VERBOSE(kTag, 4, "Removed token(s): " << dropped);
}

void Stream::Impl::clearAccess(const std::string& endpointPath) {
    TraceScope ts("Stream::Impl::clearAccess", endpointPath);
    auto ep = getByPath(endpointPath);
    if (!ep) { CV_LOG_WARNING(kTag, "clearAccess: endpoint not found: " << endpointPath); return; }
    std::lock_guard<std::mutex> lock(ep->gateMtx);
    size_t n = ep->tokens.size();
    ep->tokens.clear();
    CV_LOG_VERBOSE(kTag, 4, "Cleared all tokens: " << n);
}

// ============================================================================
// Impl: params
// ============================================================================

bool Stream::Impl::setParam(const EndpointHandle& h, const std::string& id, const std::string& value) {
    TraceScope ts("Stream::Impl::setParam",
        (std::ostringstream() << "hid=" << h.id << " id='" << id << "' val.len=" << value.size()).str());
    auto ep = getByHandle(h);
    if (!ep) { CV_LOG_WARNING(kTag, "setParam: bad handle " << h.id); return false; }
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    ep->params[id] = value;
    CV_LOG_VERBOSE(kTag, 5, "Param set; total=" << ep->params.size());
    return true;
}

bool Stream::Impl::getParam(const EndpointHandle& h, const std::string& id, std::string& outValue) const {
    TraceScope ts("Stream::Impl::getParam",
        (std::ostringstream() << "hid=" << h.id << " id='" << id << "'").str());
    auto ep = getByHandle(h);
    if (!ep) { CV_LOG_WARNING(kTag, "getParam: bad handle " << h.id); return false; }
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    auto it = ep->params.find(id);
    if (it == ep->params.end()) { CV_LOG_VERBOSE(kTag, 5, "Param not found"); return false; }
    outValue = it->second;
    CV_LOG_VERBOSE(kTag, 5, "Param value.len=" << outValue.size());
    return true;
}

std::map<std::string,std::string> Stream::Impl::listParams(const EndpointHandle& h) const {
    TraceScope ts("Stream::Impl::listParams", (std::ostringstream() << "hid=" << h.id).str());
    std::map<std::string,std::string> out;
    auto ep = getByHandle(h);
    if (!ep) { CV_LOG_WARNING(kTag, "listParams: bad handle " << h.id); return out; }
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    out = ep->params;
    CV_LOG_VERBOSE(kTag, 5, "Param count=" << out.size());
    return out;
}

bool Stream::Impl::setParamByPath(const std::string& path, const std::string& id, const std::string& value) {
    TraceScope ts("Stream::Impl::setParamByPath",
        (std::ostringstream() << "path=" << path << " id=" << id << " val.len=" << value.size()).str());
    auto ep = getByPath(path);
    if (!ep) { CV_LOG_WARNING(kTag, "setParamByPath: endpoint not found: " << path); return false; }
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    ep->params[id] = value;
    return true;
}
bool Stream::Impl::getParamByPath(const std::string& path, const std::string& id, std::string& outValue) const {
    TraceScope ts("Stream::Impl::getParamByPath", (std::ostringstream() << "path=" << path << " id=" << id).str());
    auto ep = getByPath(path);
    if (!ep) { CV_LOG_WARNING(kTag, "getParamByPath: endpoint not found: " << path); return false; }
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    auto it = ep->params.find(id);
    if (it == ep->params.end()) return false;
    outValue = it->second;
    return true;
}
std::map<std::string,std::string> Stream::Impl::listParamsByPath(const std::string& path) const {
    TraceScope ts("Stream::Impl::listParamsByPath", path);
    std::map<std::string,std::string> out;
    auto ep = getByPath(path);
    if (!ep) { CV_LOG_WARNING(kTag, "listParamsByPath: endpoint not found: " << path); return out; }
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    out = ep->params;
    CV_LOG_VERBOSE(kTag, 5, "Param count=" << out.size());
    return out;
}

// ============================================================================
// Impl: recording
// ============================================================================

std::string Stream::Impl::startRecording(const EndpointHandle& h, const std::string& dst) {
    TraceScope ts("Stream::Impl::startRecording",
        (std::ostringstream() << "hid=" << h.id << " dst='" << dst << "'").str());
    auto ep = getByHandle(h);
    if (!ep) return std::string();

    if (ep->kind != EndpointKind::Fmp4) {
        CV_LOG_ERROR(kTag, "startRecording: not an Fmp4 endpoint; kind=" << (int)ep->kind);
        CV_Error(cv::Error::StsNotImplemented, "Recording is only implemented for fMP4 endpoints");
    }

#if defined(HAVE_STREAM_COMPRESSION)
    if (!ep->encoder) {
        CV_LOG_ERROR(kTag, "startRecording: encoder not initialized");
        CV_Error(cv::Error::StsError, "Encoder not initialized for this endpoint");
    }
    if (!dst.empty()) {
        RecordingParams rp = ep->recCfg;
        rp.destination = dst;
        ep->recCfg = rp;
        bool ok = ep->encoder->configureRecording(ep->recCfg);
        CV_LOG_VERBOSE(kTag, 4, "Configured rec to '" << rp.destination << "' ok=" << (int)ok);
        ep->lastDest = dst;
    } else if (ep->recCfg.destination.size()) {
        bool ok = ep->encoder->configureRecording(ep->recCfg);
        CV_LOG_VERBOSE(kTag, 4, "Configured rec to existing '" << ep->recCfg.destination << "' ok=" << (int)ok);
        ep->lastDest = ep->recCfg.destination;
    }
    if (!ep->encoder->startRecording()) {
        CV_LOG_WARNING(kTag, "startRecording: encoder->startRecording() returned false");
        return std::string();
    }
    CV_LOG_INFO(kTag, "Recording started to '" << ep->lastDest << "'");
    return ep->lastDest;
#else
    CV_LOG_ERROR(kTag, "Recording not supported in this build");
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return std::string();
#endif
}

std::string Stream::Impl::stopRecording(const EndpointHandle& h) {
    TraceScope ts("Stream::Impl::stopRecording", (std::ostringstream() << "hid=" << h.id).str());
    auto ep = getByHandle(h);
    if (!ep) return std::string();

    if (ep->kind != EndpointKind::Fmp4) {
        CV_LOG_ERROR(kTag, "stopRecording: not an Fmp4 endpoint; kind=" << (int)ep->kind);
        CV_Error(cv::Error::StsNotImplemented, "Recording is only implemented for fMP4 endpoints");
    }
#if defined(HAVE_STREAM_COMPRESSION)
    if (!ep->encoder) { CV_LOG_WARNING(kTag, "stopRecording: encoder is null"); return std::string(); }
    ep->encoder->stopRecording();
    CV_LOG_INFO(kTag, "Recording stopped; lastDest='" << ep->lastDest << "'");
    return ep->lastDest;
#else
    CV_LOG_ERROR(kTag, "Recording not supported in this build");
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return std::string();
#endif
}

bool Stream::Impl::configureRecording(const EndpointHandle& h, const RecordingParams& rp) {
    TraceScope ts("Stream::Impl::configureRecording",
        (std::ostringstream() << "hid=" << h.id << " dst='" << rp.destination << "'").str());
    auto ep = getByHandle(h);
    if (!ep) return false;
    if (ep->kind != EndpointKind::Fmp4) {
        CV_LOG_ERROR(kTag, "configureRecording: not an Fmp4 endpoint; kind=" << (int)ep->kind);
        CV_Error(cv::Error::StsNotImplemented, "Recording is only implemented for fMP4 endpoints");
    }
#if defined(HAVE_STREAM_COMPRESSION)
    if (!ep->encoder) { CV_LOG_WARNING(kTag, "configureRecording: encoder null"); return false; }
    ep->recCfg = rp;
    ep->lastDest = rp.destination;
    bool ok = ep->encoder->configureRecording(ep->recCfg);
    CV_LOG_VERBOSE(kTag, 4, "encoder->configureRecording() -> " << (int)ok);
    return ok;
#else
    CV_LOG_ERROR(kTag, "Recording not supported in this build");
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return false;
#endif
}

std::string Stream::Impl::startRecordingByPath(const std::string& path, const std::string& dst) {
    TraceScope ts("Stream::Impl::startRecordingByPath", (std::ostringstream() << "path=" << path << " dst=" << dst).str());
    auto ep = getByPath(path);
    if (!ep) return std::string();
    return startRecording(EndpointHandle(byPath[path]), dst);
}
std::string Stream::Impl::stopRecordingByPath(const std::string& path) {
    TraceScope ts("Stream::Impl::stopRecordingByPath", (std::ostringstream() << "path=" << path).str());
    auto ep = getByPath(path);
    if (!ep) return std::string();
    return stopRecording(EndpointHandle(byPath[path]));
}
bool Stream::Impl::configureRecordingByPath(const std::string& path, const RecordingParams& rp) {
    TraceScope ts("Stream::Impl::configureRecordingByPath", (std::ostringstream() << "path=" << path << " dst=" << rp.destination).str());
    auto ep = getByPath(path);
    if (!ep) return false;
    return configureRecording(EndpointHandle(byPath[path]), rp);
}
bool Stream::Impl::splitRecordingSegment(const EndpointHandle& h) {
    TraceScope ts("Stream::Impl::splitRecordingSegment", (std::ostringstream() << "hid=" << h.id).str());
    auto ep = getByHandle(h);
    if (!ep) return false;
#if defined(HAVE_STREAM_COMPRESSION)
    if (ep->encoder) { bool ok = ep->encoder->splitSegment(); CV_LOG_VERBOSE(kTag, 4, "splitSegment -> " << (int)ok); return ok; }
    CV_LOG_WARNING(kTag, "splitRecordingSegment: encoder null");
    return false;
#else
    CV_LOG_ERROR(kTag, "Recording not supported in this build");
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return false;
#endif
}
bool Stream::Impl::splitRecordingSegmentByPath(const std::string& path) {
    TraceScope ts("Stream::Impl::splitRecordingSegmentByPath", (std::ostringstream() << "path=" << path).str());
    auto ep = getByPath(path);
    if (!ep) return false;
    return splitRecordingSegment(EndpointHandle(byPath[path]));
}

// ============================================================================
// Impl: endpoints (add/remove) and workers
// ============================================================================

std::shared_ptr<Stream::Impl::Endpoint> Stream::Impl::getByHandle(const EndpointHandle& h) const {
    TraceScope ts("Stream::Impl::getByHandle", (std::ostringstream() << "hid=" << h.id).str());
    std::lock_guard<std::mutex> lock(epMtx);
    auto it = byId.find(h.id);
    if (it == byId.end()) { CV_LOG_VERBOSE(kTag, 5, "getByHandle: not found"); return nullptr; }
    CV_LOG_VERBOSE(kTag, 6, "getByHandle: found ep@" << std::hex << (uintptr_t)it->second.get() << std::dec
                         << " path=" << it->second->path);
    return it->second;
}
std::shared_ptr<Stream::Impl::Endpoint> Stream::Impl::getByPath(const std::string& path) const {
    TraceScope ts("Stream::Impl::getByPath", (std::ostringstream() << "path=" << path).str());
    std::lock_guard<std::mutex> lock(epMtx);
    auto jt = byPath.find(path);
    if (jt == byPath.end()) { CV_LOG_VERBOSE(kTag, 5, "getByPath: not found"); return nullptr; }
    auto it = byId.find(jt->second);
    if (it == byId.end()) { CV_LOG_VERBOSE(kTag, 5, "getByPath: id missing in byId"); return nullptr; }
    CV_LOG_VERBOSE(kTag, 6, "getByPath: found id=" << jt->second << " ep@" << std::hex << (uintptr_t)it->second.get() << std::dec);
    return it->second;
}

EndpointHandle Stream::Impl::addRaw(const std::string& path, const FrameSource& src, const RawOptions& opts) {
    TraceScope ts("Stream::Impl::addRaw",
        (std::ostringstream() << "path=" << path << " fps=" << opts.framerate
            << " editor=" << (int)opts.editor << " title='" << opts.title << "'").str());
    if (!srv) { CV_LOG_ERROR(kTag, "addRaw: server not started"); CV_Error(cv::Error::StsError, "Server not started"); }

    std::shared_ptr<Endpoint> ep(new Endpoint());
    ep->kind = EndpointKind::Raw;
    ep->path = path;
    ep->editor = !secure && opts.editor;
    ep->rawSource = src;
    ep->fps = opts.framerate;

    // prime params from schema if present
    if (opts.ctrl && opts.numControls) {
        std::lock_guard<std::mutex> lock(ep->paramMtx);
        for (size_t i = 0; i < opts.numControls; ++i) {
            const auto& c = opts.ctrl[i];
            if (c.id && c.defaultValue) ep->params[c.id] = c.defaultValue;
        }
        CV_LOG_VERBOSE(kTag, 5, "Primed " << opts.numControls << " control defaults");
    }

    // register WS route
    attachRawWS(*ep);

    // launch worker (simple broadcaster)
    ep->run = true;
    ep->worker = std::thread([ep]() {
        TraceScope wts("RawWorker", (std::ostringstream() << "path=" << ep->path << " fps=" << ep->fps).str());
        const int64_t frameDur = (ep->fps > 0) ? (1000000000LL / ep->fps) : 0;
        int64_t nextNs = nowNs();
        size_t frameCount = 0;
        while (ep->run) {
            cv::Mat f; int64_t pts = -1;
            if (!ep->rawSource || !ep->rawSource(f, pts)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            if (f.empty()) continue;

            // Very simple wire format: text header "RAW WxH\n" once per frame followed by bytes
            std::vector<uint8_t> pkt;
            {
                std::ostringstream os;
                os << "RAW " << f.cols << "x" << f.rows << "\n";
                const std::string hdr = os.str();
                pkt.resize(hdr.size() + f.total()*f.elemSize());
                std::memcpy(pkt.data(), hdr.data(), hdr.size());
                std::memcpy(pkt.data()+hdr.size(), f.data, f.total()*f.elemSize());
            }

            if ((frameCount++ % 30) == 0) {
                CV_LOG_VERBOSE(kTag, 6, "[raw] push frame#" << frameCount
                                      << " size=" << f.cols << "x" << f.rows
                                      << " pkt=" << pkt.size()
                                      << " sessions=" << ep->sessions.size());
            }

            // broadcast
            std::lock_guard<std::mutex> lk(ep->sessMtx);
            for (auto& s : ep->sessions) {
                if (!s.authed || !s.ws || !s.ws->isOpen()) continue;
                s.ws->send(pkt.data(), pkt.size(), /*binary=*/true);
            }

            // pacing
            if (frameDur > 0) {
                nextNs += frameDur;
                int64_t now = nowNs();
                if (nextNs > now) {
                    std::this_thread::sleep_for(std::chrono::nanoseconds(nextNs - now));
                } else {
                    nextNs = now;
                }
            }
        }
        CV_LOG_VERBOSE(kTag, 4, "Raw worker exit. frames=" << frameCount);
    });

    // store
    int id;
    {
        std::lock_guard<std::mutex> lock(epMtx);
        id = nextId++;
        byId[id] = ep;
        byPath[path] = id;
    }
    CV_LOG_INFO(kTag, "Added RAW endpoint id=" << id << " path=" << path);

    // auto webview (lab mode)
    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::Raw; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }

    return EndpointHandle(id);
}

EndpointHandle Stream::Impl::addFmp4(const std::string& path, const FrameSource& src, const Fmp4Options& opts) {
    TraceScope ts("Stream::Impl::addFmp4",
        (std::ostringstream() << "path=" << path
                              << " editor=" << (int)opts.editor
                              << " title='" << opts.title << "'").str());
    if (!srv) { CV_LOG_ERROR(kTag, "addFmp4: server not started"); CV_Error(cv::Error::StsError, "Server not started"); }
#if !defined(HAVE_STREAM_COMPRESSION)
    CV_LOG_ERROR(kTag, "addFmp4: compression disabled at build");
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return EndpointHandle();
#else
    std::shared_ptr<Endpoint> ep(new Endpoint());
    ep->kind = EndpointKind::Fmp4;
    ep->path = path;
    ep->editor = !secure && opts.editor;
    ep->rawSource = src;

    // params from schema
    if (opts.ctrl && opts.numControls) {
        std::lock_guard<std::mutex> lock(ep->paramMtx);
        for (size_t i = 0; i < opts.numControls; ++i) {
            const auto& c = opts.ctrl[i];
            if (c.id && c.defaultValue) ep->params[c.id] = c.defaultValue;
        }
        CV_LOG_VERBOSE(kTag, 5, "Primed " << opts.numControls << " control defaults");
    }

    // encoder
    ep->encoder.reset(new Encoder());
    LOG_PTR("encoder", ep->encoder.get());
    if (!ep->encoder->open(opts.encoder)) {
        CV_LOG_ERROR(kTag, "Failed to open encoder");
        CV_Error(cv::Error::StsError, "Failed to open encoder");
    }
    ep->recCfg = RecordingParams(); // default
    ep->lastDest.clear();

    // WS route
    attachFmp4WS(*ep);

    // worker: feed encoder and broadcast fragments
    ep->run = true;
    ep->worker = std::thread([ep]() {
        TraceScope wts("Fmp4Worker", (std::ostringstream() << "path=" << ep->path).str());
        // init segment cache once available
        bool initReady = false;
        size_t fragCount = 0, initCount = 0;

        while (ep->run) {
            // 1) ingest frame → encoder
            if (ep->rawSource) {
                cv::Mat f; int64_t pts = -1;
                if (ep->rawSource(f, pts) && !f.empty()) {
                    ep->encoder->push(f);
                    if ((fragCount % 60) == 0) {
                        CV_LOG_VERBOSE(kTag, 6, "[fmp4] push frame " << f.cols << "x" << f.rows << " pts=" << pts);
                    }
                }
            }

            // 2) init segment
            if (!initReady) {
                std::vector<uint8_t> init;
                if (ep->encoder->getFmp4InitializationSegment(init) && !init.empty()) {
                    std::lock_guard<std::mutex> lk(ep->sessMtx);
                    ep->fmp4Init = init; // cache
                    // send to already-authenticated clients that haven't received it
                    for (auto& s : ep->sessions) {
                        if (s.authed && s.ws && s.ws->isOpen() && !s.fmp4InitSent) {
                            s.ws->send(init.data(), init.size(), /*binary=*/true);
                            s.fmp4InitSent = true;
                            ++initCount;
                        }
                    }
                    initReady = true;
                    CV_LOG_VERBOSE(kTag, 5, "[fmp4] init ready; cached bytes=" << ep->fmp4Init.size()
                                           << " sentTo=" << initCount);
                }
            }

            // 3) fragments
            std::vector<uint8_t> frag;
            if (ep->encoder->pullAsFmp4(frag) && !frag.empty()) {
                std::lock_guard<std::mutex> lk(ep->sessMtx);
                for (auto& s : ep->sessions) {
                    if (!s.authed || !s.ws || !s.ws->isOpen()) continue;
                    // late joiner: send init first
                    if (!s.fmp4InitSent && !ep->fmp4Init.empty()) {
                        s.ws->send(ep->fmp4Init.data(), ep->fmp4Init.size(), /*binary=*/true);
                        s.fmp4InitSent = true;
                    }
                    s.ws->send(frag.data(), frag.size(), /*binary=*/true);
                    ++fragCount;
                }
            } else {
                // small idle
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
        CV_LOG_VERBOSE(kTag, 4, "Fmp4 worker exit. initCount=" << initCount << " fragCount=" << fragCount);
    });

    int id;
    {
        std::lock_guard<std::mutex> lock(epMtx);
        id = nextId++;
        byId[id] = ep;
        byPath[path] = id;
    }

    CV_LOG_INFO(kTag, "Added FMP4 endpoint id=" << id << " path=" << path);

    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::Fmp4; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }

    return EndpointHandle(id);
#endif
}

#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
EndpointHandle Stream::Impl::addWebRtc(const std::string& path, const EncodedSource& encoded, const WebRtcOptions& opts) {
    TraceScope ts("Stream::Impl::addWebRtc",
        (std::ostringstream() << "path=" << path << " title='" << opts.title << "'").str());
    if (!srv) { CV_LOG_ERROR(kTag, "addWebRtc: server not started"); CV_Error(cv::Error::StsError, "Server not started"); }

    std::shared_ptr<Endpoint> ep(new Endpoint());
    ep->kind = EndpointKind::WebRTC;
    ep->path = path;
    ep->editor = false; // no auto UI controls for WebRTC at this layer
    ep->encSource = encoded;

    attachWebRtcWS(opts, ep->encSource, *ep);

    int id;
    { std::lock_guard<std::mutex> lock(epMtx);
      id = nextId++; byId[id] = ep; byPath[path] = id; }

    CV_LOG_INFO(kTag, "Added WebRTC endpoint id=" << id << " path=" << path);

    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::WebRTC; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }
    return EndpointHandle(id);
}
#endif

void Stream::Impl::remove(const EndpointHandle& h) {
    TraceScope ts("Stream::Impl::remove", (std::ostringstream() << "hid=" << h.id).str());
    auto ep = getByHandle(h);
    if (!ep) { CV_LOG_WARNING(kTag, "remove: bad handle " << h.id); return; }

    // unregister WS/HTTP
    if (srv) {
        CV_LOG_VERBOSE(kTag, 4, "Unregister WS/HTTP for path '" << ep->path << "'");
        srv->unregisterWebSocketEndpoint(ep->path);
        srv->unregisterEndpoint(ep->path + "/view");
    }

    // stop worker
    ep->run = false;
    if (ep->worker.joinable()) ep->worker.join();

    // purge maps
    std::lock_guard<std::mutex> lock(epMtx);
    auto it = byId.find(h.id);
    if (it != byId.end()) {
        byPath.erase(it->second->path);
        byId.erase(it);
        CV_LOG_VERBOSE(kTag, 4, "Removed endpoint id=" << h.id);
    }
}

void Stream::Impl::removeByPath(const std::string& path) {
    TraceScope ts("Stream::Impl::removeByPath", (std::ostringstream() << "path=" << path).str());
    auto ep = getByPath(path);
    if (!ep) { CV_LOG_WARNING(kTag, "removeByPath: not found: " << path); return; }
    remove(EndpointHandle(byPath[path]));
}

// ============================================================================
// WS routes
// ============================================================================

void Stream::Impl::attachRawWS(Endpoint& ep) {
    TraceScope ts("Stream::Impl::attachRawWS", ep.path);
    WebSocketHandler h;

    h.onOpen = [this,&ep](WebSocketSession& s) {
        TraceScope ts2("RAW.onOpen", ep.path);
        SessionCtx ctx; ctx.ws = &s; ctx.role = AccessRole::ReadOnly;
        if (!secure) {
            // Lab mode: auto-auth immediately
            ctx.authed = true;
            const char ok[] = "OK";
            s.send(ok, sizeof(ok)-1, /*binary=*/false);
            CV_LOG_VERBOSE(kTag, 4, "[raw] open " << s.remoteAddress() << " (lab) -> authed");
        } else {
            ctx.authed = false;
            CV_LOG_VERBOSE(kTag, 4, "[raw] open " << s.remoteAddress() << " (secure) -> awaiting token");
        }
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        ep.sessions.push_back(ctx);
        CV_LOG_VERBOSE(kTag, 5, "[raw] sessions=" << ep.sessions.size());
    };

    h.onMessage = [this,&ep](WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        TraceScope ts2("RAW.onMessage",
            (std::ostringstream() << ep.path << " from=" << s.remoteAddress() << " n=" << n).str());
        // Token handshake: first message must be ASCII "T <token>" (secure mode only)
        if (!secure) return; // already authed in lab mode
        std::string msg(reinterpret_cast<const char*>(data), reinterpret_cast<const char*>(data)+n);
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        for (auto& it : ep.sessions) if (it.ws == &s) {
            if (!it.authed) {
                std::string tok;
                if (msg.size() > 2 && msg[0]=='T' && msg[1]==' ') tok = msg.substr(2);
                {
                    std::lock_guard<std::mutex> g(ep.gateMtx);
                    dropExpiredTokens(ep);
                    AccessRole role;
                    if (tokenAllowed(ep, tok, &role)) {
                        it.authed = true; it.role = role;
                        const char ok[] = "OK";
                        s.send(ok, sizeof(ok)-1, false);
                        CV_LOG_VERBOSE(kTag, 4, "[raw] " << s.remoteAddress() << " authed (secure)");
                    } else {
                        s.close(WsCloseCode::PolicyViolation, "unauthorized");
                        CV_LOG_WARNING(kTag, "[raw] " << s.remoteAddress() << " unauthorized (secure)");
                    }
                }
            }
            break;
        }
    };

    h.onClose = [this,&ep](WebSocketSession& s, int code, const std::string& reason) {
        TraceScope ts2("RAW.onClose",
            (std::ostringstream() << ep.path << " from=" << s.remoteAddress()
                                  << " code=" << code << " reason='" << reason << "'").str());
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        std::vector<SessionCtx> keep;
        for (auto& it : ep.sessions) if (it.ws != &s) keep.push_back(it);
        ep.sessions.swap(keep);
        CV_LOG_VERBOSE(kTag, 5, "[raw] sessions=" << ep.sessions.size());
    };

    srv->registerWebSocketEndpoint(ep.path, h);
    CV_LOG_VERBOSE(kTag, 4, "Registered RAW WS endpoint '" << ep.path << "'");
}

void Stream::Impl::attachFmp4WS(Endpoint& ep) {
    TraceScope ts("Stream::Impl::attachFmp4WS", ep.path);
    WebSocketHandler h;

    h.onOpen = [this,&ep](WebSocketSession& s) {
        TraceScope ts2("FMP4.onOpen", ep.path);
        SessionCtx ctx; ctx.ws = &s; ctx.role = AccessRole::ReadOnly; ctx.fmp4InitSent = false;
        if (!secure) {
            // Lab mode: auto-auth + send init if available
            ctx.authed = true;
            const char ok[] = "OK";
            s.send(ok, sizeof(ok)-1, /*binary=*/false);
            if (!ep.fmp4Init.empty()) {
                s.send(ep.fmp4Init.data(), ep.fmp4Init.size(), /*binary=*/true);
                ctx.fmp4InitSent = true;
            }
            CV_LOG_VERBOSE(kTag, 4, "[fmp4] open " << s.remoteAddress()
                               << " (lab) -> authed (initSent=" << (int)ctx.fmp4InitSent << ")");
        } else {
            ctx.authed = false;
            CV_LOG_VERBOSE(kTag, 4, "[fmp4] open " << s.remoteAddress() << " (secure) -> awaiting token");
        }
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        ep.sessions.push_back(ctx);
        CV_LOG_VERBOSE(kTag, 5, "[fmp4] sessions=" << ep.sessions.size());
    };

    h.onMessage = [this,&ep](WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        TraceScope ts2("FMP4.onMessage",
            (std::ostringstream() << ep.path << " from=" << s.remoteAddress() << " n=" << n).str());
        if (!secure) return; // already authed in lab mode
        std::string msg(reinterpret_cast<const char*>(data), reinterpret_cast<const char*>(data)+n);
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        for (auto& it : ep.sessions) if (it.ws == &s) {
            if (!it.authed) {
                std::string tok;
                if (msg.size() > 2 && msg[0]=='T' && msg[1]==' ') tok = msg.substr(2);
                {
                    std::lock_guard<std::mutex> g(ep.gateMtx);
                    dropExpiredTokens(ep);
                    AccessRole role;
                    if (tokenAllowed(ep, tok, &role)) {
                        it.authed = true; it.role = role;
                        if (!ep.fmp4Init.empty() && !it.fmp4InitSent) {
                            it.ws->send(ep.fmp4Init.data(), ep.fmp4Init.size(), true);
                            it.fmp4InitSent = true;
                        }
                        const char ok[] = "OK";
                        s.send(ok, sizeof(ok)-1, false);
                        CV_LOG_VERBOSE(kTag, 4, "[fmp4] " << s.remoteAddress() << " authed (secure)");
                    } else {
                        s.close(WsCloseCode::PolicyViolation, "unauthorized");
                        CV_LOG_WARNING(kTag, "[fmp4] " << s.remoteAddress() << " unauthorized (secure)");
                    }
                }
            }
            break;
        }
    };

    h.onClose = [this,&ep](WebSocketSession& s, int code, const std::string& reason) {
        TraceScope ts2("FMP4.onClose",
            (std::ostringstream() << ep.path << " from=" << s.remoteAddress()
                                  << " code=" << code << " reason='" << reason << "'").str());
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        std::vector<SessionCtx> keep;
        for (auto& it : ep.sessions) if (it.ws != &s) keep.push_back(it);
        ep.sessions.swap(keep);
        CV_LOG_VERBOSE(kTag, 5, "[fmp4] sessions=" << ep.sessions.size());
    };

    srv->registerWebSocketEndpoint(ep.path, h);
    CV_LOG_VERBOSE(kTag, 4, "Registered FMP4 WS endpoint '" << ep.path << "'");
}

#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
void Stream::Impl::attachWebRtcWS(const WebRtcOptions& opts,
                                  const EncodedSource& enc,
                                  Endpoint& ep)
{
    TraceScope ts("Stream::Impl::attachWebRtcWS", ep.path);
    WebSocketHandler h;

    struct PeerState {
        std::unique_ptr<WebRtcPeer> peer;
        std::thread worker;
        std::atomic<bool> run{false};
        bool authed = false;
        AccessRole role = AccessRole::ReadOnly;
    };

    // Lifetime must outlive this function and captured lambdas
    auto peers    = std::make_shared<
        std::unordered_map<WebSocketSession*, std::shared_ptr<PeerState>>>();
    auto peersMtx = std::make_shared<std::mutex>();

    // Keep a copy of options alive for lambdas
    auto optsPtr = std::make_shared<WebRtcOptions>(opts);

    // helper to bootstrap a peer once authenticated
    auto begin_peer = [optsPtr,&enc](std::shared_ptr<PeerState> st, WebSocketSession& s) {
        TraceScope ts3("WebRTC.begin_peer");
        st->peer = createWebRtcPeer();

        WebRtcCallbacks cb;
        cb.onLocalDescription = [&s](const Sdp& local) {
            const std::string json = makeSdpJson(local);
            s.send(json.data(), json.size(), /*binary=*/false);
        };
        cb.onIceCandidate = [&s](const IceCandidate& ic) {
            const std::string json = makeIceJson(ic);
            s.send(json.data(), json.size(), /*binary=*/false);
        };
        cb.onError = [&s](const char* where, int err) {
            std::ostringstream os; os << "{\"error\":\"" << where << "\",\"code\":" << err << "}";
            const std::string js = os.str();
            s.send(js.data(), js.size(), false);
        };

        // Open peer with provided (encoded-only) ingest options.
        if (!st->peer->open(optsPtr->webrtc, optsPtr->ingest, cb)) {
            const char* er = "{\"error\":\"build-pipeline\",\"code\":-1}";
            s.send(er, std::strlen(er), /*binary=*/false);
            s.close(WsCloseCode::InternalError, "webrtc open failed");
            CV_LOG_WARNING(kTag, "[webrtc] peer open failed; remote=" << s.remoteAddress());
            return;
        }

        if (optsPtr->autostartOffer) st->peer->createOffer();

        // Worker: pull pre-encoded AUs/OBUs and push into WebRTC
        st->run = true;
        st->worker = std::thread([st,&enc]() {
            TraceScope tsw("WebRTC.worker");
            // Use the codec selected by the peer (driven by webrtc params)
            // This is decided during pipeline build (first preferred match).
            auto selected_codec = st->peer ? st->peer->getNegotiatedVideoCodec() : VideoCodec::H264;

            size_t auCount = 0;
            while (st->run) {
                std::vector<uint8_t> au; bool key = false; int64_t pts = -1;
                if (!enc(au, key, pts)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                    continue;
                }
                if (st->peer) {
                    st->peer->pushEncoded(selected_codec, au.data(), au.size(), key, pts);
                    if ((auCount++ % 60) == 0) {
                        CV_LOG_VERBOSE(kTag, 6, "[webrtc] push au#" << auCount << " size=" << au.size() << " key=" << (int)key);
                    }
                }
            }
            CV_LOG_VERBOSE(kTag, 4, "[webrtc] worker exit auCount=" << auCount);
        });
    };

    // capture begin_peer **by value** (copy), not by reference
    h.onOpen = [this,&ep,peers,peersMtx,begin_peer](WebSocketSession& s) {
        TraceScope ts2("WebRTC.onOpen", ep.path);
        auto st = std::make_shared<PeerState>();
        {
            std::lock_guard<std::mutex> lk(*peersMtx);
            (*peers)[&s] = st;
        }

        if (!secure) {
            st->authed = true;
            const char ok[] = "OK";
            s.send(ok, sizeof(ok)-1, /*binary=*/false);
            CV_LOG_VERBOSE(kTag, 4, "[webrtc] open " << s.remoteAddress() << " (lab) -> authed");
            begin_peer(st, s);
        } else {
            CV_LOG_VERBOSE(kTag, 4, "[webrtc] open " << s.remoteAddress() << " (secure) -> awaiting token/SDP");
        }
    };

    h.onMessage = [this,&ep,peers,peersMtx,begin_peer](WebSocketSession& s,
                                                       const uint8_t* data, size_t n, bool /*binary*/) {
        TraceScope ts2("WebRTC.onMessage",
            (std::ostringstream() << ep.path << " from=" << s.remoteAddress() << " n=" << n).str());
        const std::string msg(reinterpret_cast<const char*>(data),
                              reinterpret_cast<const char*>(data) + n);

        std::shared_ptr<PeerState> st;
        {
            std::lock_guard<std::mutex> lk(*peersMtx);
            auto it = peers->find(&s);
            if (it != peers->end()) st = it->second;
        }
        if (!st) { CV_LOG_WARNING(kTag, "[webrtc] onMessage: no PeerState for session"); return; }

        if (!st->authed) {
            std::string tok;
            if (msg.size() > 2 && msg[0] == 'T' && msg[1] == ' ') tok = msg.substr(2);

            {
                std::lock_guard<std::mutex> g(ep.gateMtx);
                dropExpiredTokens(ep);
                AccessRole role;
                if (!secure || tokenAllowed(ep, tok, &role)) {
                    st->authed = true;
                    st->role = secure ? role : AccessRole::ReadOnly;
                    const char ok[] = "OK";
                    s.send(ok, sizeof(ok)-1, false);
                    CV_LOG_VERBOSE(kTag, 4, "[webrtc] " << s.remoteAddress() << " authed (secure)");
                    begin_peer(st, s);
                } else {
                    s.close(WsCloseCode::PolicyViolation, "unauthorized");
                    CV_LOG_WARNING(kTag, "[webrtc] " << s.remoteAddress() << " unauthorized (secure)");
                }
            }
            return;
        }

        // after auth: SDP/ICE as JSON strings
        Sdp sdp;
        IceCandidate ice;
        if (parseSdpFromJson(msg, sdp)) {
            if (st->peer) {
                st->peer->setRemoteDescription(sdp);
                CV_LOG_VERBOSE(kTag, 5, "[webrtc] setRemoteDescription");
            }
            return;
        }
        if (parseIceFromJson(msg, ice)) {
            if (st->peer) {
                st->peer->addRemoteIceCandidate(ice);
                CV_LOG_VERBOSE(kTag, 5, "[webrtc] addRemoteIceCandidate");
            }
            return;
        }
        CV_LOG_VERBOSE(kTag, 5, "[webrtc] onMessage: unrecognized payload");
    };

    h.onClose = [peers,peersMtx](WebSocketSession& s, int code, const std::string& reason) {
        TraceScope ts2("WebRTC.onClose",
            (std::ostringstream() << "from=" << s.remoteAddress()
                                  << " code=" << code << " reason='" << reason << "'").str());
        std::shared_ptr<PeerState> st;
        {
            std::lock_guard<std::mutex> lk(*peersMtx);
            auto it = peers->find(&s);
            if (it != peers->end()) {
                st = it->second;
                peers->erase(it);
            }
        }
        if (st) {
            st->run = false;
            if (st->worker.joinable()) st->worker.join();
            if (st->peer) st->peer->close();
        }
    };

    srv->registerWebSocketEndpoint(ep.path, h);
    CV_LOG_VERBOSE(kTag, 4, "Registered WebRTC WS endpoint '" << ep.path << "'");
}
#endif



// ============================================================================
// Impl: web pages
// ============================================================================

void Stream::Impl::writeSimpleHtml(Response& res, const std::string& html) {
    TraceScope ts("Stream::Impl::writeSimpleHtml", (std::ostringstream() << "html.len=" << html.size()).str());
    res.setStatusCode(200);
    res.setHeader("Content-Type", webpage::kContentTypeHtml);
    size_t wrote = (size_t)res.write(html.c_str(), html.size());
    if (wrote != html.size()) {
        CV_LOG_WARNING(kTag, "writeSimpleHtml: short write wrote=" << wrote << " expected=" << html.size());
    }
}

void Stream::Impl::ensureRootPage() {
    TraceScope ts("Stream::Impl::ensureRootPage",
                  (std::ostringstream() << "mount=" << rootMount << " title='" << rootTitle << "'").str());
    if (!srv) { CV_LOG_WARNING(kTag, "ensureRootPage: srv=null"); return; }

    const std::string mount = rootMount.empty() ? "/" : rootMount;
    const std::string title = rootTitle.empty() ? "OpenCV Stream" : rootTitle;
    Stream::Impl* self = this; // capture explicitly by value

    srv->registerEndpoint(mount, [self, title](const Request& rq, Response& rs) {
        // ultra-early raw sentinel (logger-independent)
    #if !defined(_WIN32)
        ::write(2, "[http] rootIndex ENTER\n", 23);
    #else
        std::fwrite("[http] rootIndex ENTER\n", 1, 23, stderr); std::fflush(stderr);
    #endif
        try {
            std::string meth = "<unknown>", path = "<unknown>";
            try { meth = rq.getMethod(); path = rq.getPath(); } catch (...) {} // don't trust early access

            TraceScope tsH("HTTP.rootIndex",
                (std::ostringstream() << "method=" << meth << " path=" << path).str());

            std::ostringstream os;
            os << "<!doctype html><html><head><meta charset='utf-8'><title>"
               << title << "</title></head><body><h1>" << title << "</h1><ul>";

            {
                std::lock_guard<std::mutex> lock(self->epMtx);
                for (auto& kv : self->byId) {
                    const auto& ep = *kv.second;
                    os << "<li><code>" << ep.path << "</code> ("
                       << (ep.kind==EndpointKind::Raw ? "raw"
                           : ep.kind==EndpointKind::Fmp4 ? "fmp4" : "webrtc")
                       << ") — <a href='" << ep.path << "/view'>view</a></li>";
                }
            }

            os << "</ul></body></html>";
            self->writeSimpleHtml(rs, os.str());
        } catch (const std::exception& e) {
            CV_LOG_ERROR(kTag, "rootIndex handler exception: " << e.what());
            rs.setStatusCode(500);
        } catch (...) {
            CV_LOG_ERROR(kTag, "rootIndex handler unknown exception");
            rs.setStatusCode(500);
        }
    });

    CV_LOG_VERBOSE(kTag, 4, "Registered HTTP index at '" << mount << "'");
}


void Stream::Impl::mountEmbedded(const std::string& pagePath,
                                 const std::string& streamPath,
                                 const WebviewOptions& view)
{
    printf("LAMBDA START\n");
    TraceScope ts("Stream::Impl::mountEmbedded",
        (std::ostringstream() << "pagePath=" << pagePath
                              << " streamPath=" << streamPath
                              << " view.title='" << view.title << "'"
                              << " view.videoClient=" << (int)view.videoClient).str());
    if (!srv) { CV_LOG_WARNING(kTag, "mountEmbedded: srv=null"); return; }
    printf("LAMBDA END\n");
    // Wrap the user handler in a defensive wrapper
    srv->registerEndpoint(pagePath, [streamPath, view](const Request& rq, Response& rs)
    {
        // ultra-early raw sentinel (optional)
    #if !defined(_WIN32)
        ::write(2, "[http] mountEmbedded ENTER (raw)\n", 33);
    #else
        std::fwrite("[http] mountEmbedded ENTER (raw)\n", 1, 33, stderr); std::fflush(stderr);
    #endif

        try {
            std::string meth = "<unknown>", path = "<unknown>";
            try { meth = rq.getMethod(); path = rq.getPath(); } catch (...) {}

            webpage::VideoClient vc = view.videoClient;
            if (vc == webpage::VideoClient::Auto) {
                vc = (streamPath.find("webrtc") != std::string::npos) ? webpage::VideoClient::WebRTC
                   : (streamPath.find("fmp4")   != std::string::npos) ? webpage::VideoClient::Fmp4
                                                                      : webpage::VideoClient::Raw;
            }

            const char* title = view.title.empty() ? "OpenCV Stream" : view.title.c_str();

            // HEAP buffer instead of large stack array
            std::vector<char> buf(64 * 1024);
            size_t n = webpage::embedded_video_page(buf.data(), buf.size(),
                                                    streamPath.c_str(), vc, title);

            if (n >= buf.size()) {
                // optional warn; content will be truncated below
            }

            std::string html(buf.data(), buf.data() + std::min(n, buf.size() - 1));

            rs.setStatusCode(200);
            rs.setHeader("Content-Type", webpage::kContentTypeHtml);
            (void)rs.write(html.c_str(), html.size());
        }
        catch (const std::exception& e) {
            rs.setStatusCode(500);
        }
        catch (...) {
            rs.setStatusCode(500);
        }
    });

    CV_LOG_INFO(kTag, "Registered embedded page '" << pagePath << "' -> stream '" << streamPath << "'");
}

void Stream::Impl::mountJupyter(const std::string& pagePath,
                                const std::string& streamPath,
                                const WebviewOptions& view)
{
    TraceScope ts("Stream::Impl::mountJupyter",
        (std::ostringstream() << "pagePath=" << pagePath
                              << " streamPath=" << streamPath
                              << " view.title='" << view.title << "'"
                              << " view.videoClient=" << (int)view.videoClient).str());
    if (!srv) { CV_LOG_WARNING(kTag, "mountJupyter: srv=null"); return; }

    srv->registerEndpoint(pagePath, [streamPath, view](const Request& rq, Response& rs)
    {
    #if !defined(_WIN32)
        ::write(2, "[http] mountJupyter ENTER (raw)\n", 32);
    #else
        std::fwrite("[http] mountJupyter ENTER (raw)\n", 1, 32, stderr); std::fflush(stderr);
    #endif
        try {
            std::string meth = "<unknown>", path = "<unknown>";
            try { meth = rq.getMethod(); path = rq.getPath(); } catch (...) {}
            TraceScope tsH("HTTP.mountJupyter.handler",
                (std::ostringstream() << "method=" << meth
                                      << " path=" << path
                                      << " streamPath=" << streamPath
                                      << " view.title='" << view.title << "'").str());

            // Build Jupyter-style page into a HEAP buffer (avoid large stack objects)
            std::vector<char> buf(64 * 1024);
            size_t n = webpage::webview_jupyter_page(buf.data(), buf.size(), "/", streamPath.c_str());
            if (n >= buf.size()) {
                CV_LOG_VERBOSE(kTag, 4, "webview_jupyter_page filled buffer (n=" << n << " cap=" << buf.size() << ")");
            }

            // Clamp length and form std::string safely (ensure trailing '\0' not required)
            const size_t used = std::min(n, buf.size() - 1);
            std::string html(buf.data(), buf.data() + used);

            writeSimpleHtml(rs, html);
        }
        catch (const std::exception& e) {
            CV_LOG_ERROR(kTag, "mountJupyter handler exception: " << e.what());
            rs.setStatusCode(500);
        }
        catch (...) {
            CV_LOG_ERROR(kTag, "mountJupyter handler unknown exception");
            rs.setStatusCode(500);
        }
    });

    CV_LOG_INFO(kTag, "Registered jupyter page '" << pagePath << "' -> stream '" << streamPath << "'");
}


// ============================================================================
// Impl: introspection
// ============================================================================

std::vector<std::string> Stream::Impl::listEndpoints() const {
    TraceScope ts("Stream::Impl::listEndpoints");
    std::vector<std::string> out;
    std::lock_guard<std::mutex> lock(epMtx);
    out.reserve(byPath.size());
    for (auto& kv : byPath) out.push_back(kv.first);
    CV_LOG_VERBOSE(kTag, 5, "listEndpoints -> " << out.size());
    return out;
}

EndpointKind Stream::Impl::kindOf(const std::string& path) const {
    TraceScope ts("Stream::Impl::kindOf", path);
    auto ep = getByPath(path);
    if (!ep) return EndpointKind::Raw;
    CV_LOG_VERBOSE(kTag, 5, "kindOf('" << path << "') -> " << (int)ep->kind);
    return ep->kind;
}

// ============================================================================
// Public wrappers
// ============================================================================

Stream::Stream() : pimpl(new Impl) { CV_LOG_VERBOSE(kTag, 4, "Stream() @" << std::hex << (uintptr_t)this << std::dec); }
Stream::~Stream() { CV_LOG_VERBOSE(kTag, 4, "~Stream() @" << std::hex << (uintptr_t)this << std::dec); }

bool Stream::start(const std::string& bindAddress, int port, bool secureMode, int numThreads) {
    TraceScope ts("Stream::start",
        (std::ostringstream() << "bindAddress=" << bindAddress << " port=" << port
                              << " secure=" << (int)secureMode << " threads=" << numThreads).str());
    // Note: Server::start() does not accept bindAddress in this backend;
    // callers should run behind a reverse proxy to manage exposure.
    if (!pimpl) return false;
    if (!secureMode && bindAddress != "127.0.0.1") {
        // Smoky warning for lab mode on non-loopback.
        fprintf(stderr, "[cv::stream] WARNING: starting in insecure/lab mode on %s:%d. "
                        "Do NOT expose publicly.\n", bindAddress.c_str(), port);
    }
    return pimpl->start(bindAddress, port, secureMode, numThreads);
}

void Stream::stop() { TraceScope ts("Stream::stop"); if (pimpl) pimpl->stop(); }
bool Stream::isRunning() const { return pimpl && pimpl->isRunning(); }
void Stream::setSecureMode(bool on) { TraceScope ts("Stream::setSecureMode", (std::ostringstream() << "on=" << (int)on).str()); if (pimpl) pimpl->setSecureMode(on); }
bool Stream::secureMode() const { return pimpl && pimpl->secureMode(); }
void Stream::enableRootIndex(bool on, const std::string& mountPath, const std::string& title) {
    TraceScope ts("Stream::enableRootIndex",
        (std::ostringstream() << "on=" << (int)on << " mountPath=" << mountPath << " title='" << title << "'").str());
    if (pimpl) pimpl->enableRootIndex(on, mountPath, title);
}

EndpointHandle Stream::addRaw(const std::string& path, const FrameSource& src, const RawOptions& opts) {
    TraceScope ts("Stream::addRaw", (std::ostringstream() << "path=" << path).str());
    if (!pimpl) return EndpointHandle();
    return pimpl->addRaw(path, src, opts);
}
EndpointHandle Stream::addFmp4(const std::string& path, const FrameSource& src, const Fmp4Options& opts) {
    TraceScope ts("Stream::addFmp4", (std::ostringstream() << "path=" << path).str());
    if (!pimpl) return EndpointHandle();
    return pimpl->addFmp4(path, src, opts);
}
#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
EndpointHandle Stream::addWebRtc(const std::string& path, const EncodedSource& encoded, const WebRtcOptions& opts) {
    TraceScope ts("Stream::addWebRtc", (std::ostringstream() << "path=" << path).str());
    if (!pimpl) return EndpointHandle();
    return pimpl->addWebRtc(path, encoded, opts);
}
#endif

void Stream::remove(const EndpointHandle& h) { TraceScope ts("Stream::remove", (std::ostringstream() << "hid=" << h.id).str()); if (pimpl) pimpl->remove(h); }
void Stream::removeByPath(const std::string& path) { TraceScope ts("Stream::removeByPath", (std::ostringstream() << "path=" << path).str()); if (pimpl) pimpl->removeByPath(path); }

void Stream::mountEmbedded(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view) {
    TraceScope ts("Stream::mountEmbedded",
        (std::ostringstream() << "pagePath=" << pagePath << " streamPath=" << streamPath).str());
    if (pimpl) pimpl->mountEmbedded(pagePath, streamPath, view);
}
void Stream::mountJupyter(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view) {
    TraceScope ts("Stream::mountJupyter",
        (std::ostringstream() << "pagePath=" << pagePath << " streamPath=" << streamPath).str());
    if (pimpl) pimpl->mountJupyter(pagePath, streamPath, view);
}

bool Stream::allowAccess(const std::string& endpointPath, const std::string& token, AccessRole role, int ttlSeconds) {
    TraceScope ts("Stream::allowAccess", (std::ostringstream() << "path=" << endpointPath << " ttl=" << ttlSeconds).str());
    return pimpl && pimpl->allowAccess(endpointPath, token, role, ttlSeconds);
}
void Stream::removeAccess(const std::string& endpointPath, const std::string& token) {
    TraceScope ts("Stream::removeAccess", (std::ostringstream() << "path=" << endpointPath).str());
    if (pimpl) pimpl->removeAccess(endpointPath, token);
}
void Stream::clearAccess(const std::string& endpointPath) {
    TraceScope ts("Stream::clearAccess", (std::ostringstream() << "path=" << endpointPath).str());
    if (pimpl) pimpl->clearAccess(endpointPath);
}

bool Stream::setParam(const EndpointHandle& h, const std::string& id, const std::string& value) {
    TraceScope ts("Stream::setParam", (std::ostringstream() << "hid=" << h.id << " id=" << id).str());
    return pimpl && pimpl->setParam(h, id, value);
}
bool Stream::getParam(const EndpointHandle& h, const std::string& id, std::string& outValue) const {
    TraceScope ts("Stream::getParam", (std::ostringstream() << "hid=" << h.id << " id=" << id).str());
    return pimpl && pimpl->getParam(h, id, outValue);
}
std::map<std::string,std::string> Stream::listParams(const EndpointHandle& h) const {
    TraceScope ts("Stream::listParams", (std::ostringstream() << "hid=" << h.id).str());
    return pimpl ? pimpl->listParams(h) : std::map<std::string,std::string>();
}

bool Stream::setParamByPath(const std::string& path, const std::string& id, const std::string& value) {
    TraceScope ts("Stream::setParamByPath", (std::ostringstream() << "path=" << path << " id=" << id).str());
    return pimpl && pimpl->setParamByPath(path, id, value);
}
bool Stream::getParamByPath(const std::string& path, const std::string& id, std::string& outValue) const {
    TraceScope ts("Stream::getParamByPath", (std::ostringstream() << "path=" << path << " id=" << id).str());
    return pimpl && pimpl->getParamByPath(path, id, outValue);
}
std::map<std::string,std::string> Stream::listParamsByPath(const std::string& path) const {
    TraceScope ts("Stream::listParamsByPath", (std::ostringstream() << "path=" << path).str());
    return pimpl ? pimpl->listParamsByPath(path) : std::map<std::string,std::string>();
}

std::string Stream::startRecording(const EndpointHandle& h, const std::string& destination) {
    TraceScope ts("Stream::startRecording", (std::ostringstream() << "hid=" << h.id << " dst=" << destination).str());
    return pimpl ? pimpl->startRecording(h, destination) : std::string();
}
std::string Stream::stopRecording(const EndpointHandle& h) {
    TraceScope ts("Stream::stopRecording", (std::ostringstream() << "hid=" << h.id).str());
    return pimpl ? pimpl->stopRecording(h) : std::string();
}
bool Stream::configureRecording(const EndpointHandle& h, const RecordingParams& params) {
    TraceScope ts("Stream::configureRecording", (std::ostringstream() << "hid=" << h.id << " dst=" << params.destination).str());
    return pimpl && pimpl->configureRecording(h, params);
}

std::string Stream::startRecordingByPath(const std::string& path, const std::string& destination) {
    TraceScope ts("Stream::startRecordingByPath", (std::ostringstream() << "path=" << path << " dst=" << destination).str());
    return pimpl ? pimpl->startRecordingByPath(path, destination) : std::string();
}
std::string Stream::stopRecordingByPath(const std::string& path) {
    TraceScope ts("Stream::stopRecordingByPath", (std::ostringstream() << "path=" << path).str());
    return pimpl ? pimpl->stopRecordingByPath(path) : std::string();
}
bool Stream::configureRecordingByPath(const std::string& path, const RecordingParams& params) {
    TraceScope ts("Stream::configureRecordingByPath", (std::ostringstream() << "path=" << path << " dst=" << params.destination).str());
    return pimpl && pimpl->configureRecordingByPath(path, params);
}
bool Stream::splitRecordingSegment(const EndpointHandle& h) {
    TraceScope ts("Stream::splitRecordingSegment", (std::ostringstream() << "hid=" << h.id).str());
    return pimpl && pimpl->splitRecordingSegment(h);
}
bool Stream::splitRecordingSegmentByPath(const std::string& path) {
    TraceScope ts("Stream::splitRecordingSegmentByPath", (std::ostringstream() << "path=" << path).str());
    return pimpl && pimpl->splitRecordingSegmentByPath(path);
}

std::vector<std::string> Stream::listEndpoints() const {
    TraceScope ts("Stream::listEndpoints");
    return pimpl ? pimpl->listEndpoints() : std::vector<std::string>();
}
EndpointKind Stream::kindOf(const std::string& path) const {
    TraceScope ts("Stream::kindOf", (std::ostringstream() << "path=" << path).str());
    return pimpl ? pimpl->kindOf(path) : EndpointKind::Raw;
}

std::unique_ptr<Stream> createStream() { CV_LOG_VERBOSE(kTag, 4, "createStream()"); return std::unique_ptr<Stream>(new Stream()); }

} // namespace stream
} // namespace cv
