// stream.cpp
#include <opencv2/stream/stream.hpp>

#include <opencv2/core.hpp>
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
          rootTitle("OpenCV Stream") {}

    ~Impl() { stop(); }

    // process
    bool start(const std::string& /*bindAddress*/, int port, bool secureMode, int numThreads);
    void stop();
    bool isRunning() const { return running; }

    void setSecureMode(bool on) { secure = on; }
    bool secureMode() const { return secure; }

    void enableRootIndex(bool on, const std::string& mount, const std::string& title);

    // endpoints
    EndpointHandle addRaw(const std::string& path, const FrameSource& src, const RawOptions& opts);
    EndpointHandle addFmp4(const std::string& path, const FrameSource& src, const Fmp4Options& opts);
#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
    EndpointHandle addWebRtc(const std::string& path, const EncodedSource& encoded, const WebRtcOptions& opts);
    EndpointHandle addWebRtcRaw(const std::string& path, const FrameSource& raw, const WebRtcOptions& opts);
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
        FrameSource   rawSource;
        EncodedSource encSource;

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

        Endpoint() : kind(EndpointKind::Raw), editor(false), fps(0) {}
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
    void attachWebRtcWS(const WebRtcOptions& opts, const EncodedSource* enc, const FrameSource* raw, Endpoint& ep);
#endif

    void ensureRootPage();
    static void writeSimpleHtml(Response& res, const std::string& html);
};

// ============================================================================
// Impl: server lifecycle
// ============================================================================

bool Stream::Impl::start(const std::string& /*bindAddress*/, int port, bool secureMode, int numThreads) {
    if (running) return true;
    secure = secureMode;

    srv = createServer();
    if (!srv) {
        CV_Error(cv::Error::StsError, "stream::createServer() failed");
    }
    if (!srv->start(port, numThreads)) {
        srv.reset();
        CV_Error(cv::Error::StsError, "Failed to start stream::Server");
    }

    running = true;

    // Root index mounting is deferred; user calls enableRootIndex().
    return true;
}

void Stream::Impl::stop() {
    if (!running) return;

    // Stop workers
    {
        std::lock_guard<std::mutex> lock(epMtx);
        for (auto& kv : byId) {
            auto& ep = *kv.second;
            ep.run = false;
            if (ep.worker.joinable()) ep.worker.join();
        }
        byId.clear();
        byPath.clear();
    }

    if (srv) {
        srv->stop();
        srv.reset();
    }
    running = false;
}

void Stream::Impl::enableRootIndex(bool on, const std::string& mount, const std::string& title) {
    rootIndexEnabled = on;
    rootMount = mount.empty() ? "/" : mount;
    rootTitle = title.empty() ? "OpenCV Stream" : title;

    if (!srv) return;
    if (on) {
        ensureRootPage();
    } else {
        srv->unregisterEndpoint(rootMount);
    }
}

// ============================================================================
// Impl: tokens
// ============================================================================

bool Stream::Impl::tokenAllowed(const Endpoint& ep, const std::string& tok, AccessRole* outRole) {
    int64_t t = nowNs();
    for (const auto& e : ep.tokens) {
        if (e.tok == tok) {
            if (e.expiresNs <= 0 || e.expiresNs > t) {
                if (outRole) *outRole = e.role;
                return true;
            }
            // expired, but keep scanning other entries
        }
    }
    return false;
}

void Stream::Impl::dropExpiredTokens(Endpoint& ep) {
    int64_t t = nowNs();
    std::vector<TokenEntry> keep;
    keep.reserve(ep.tokens.size());
    for (auto& e : ep.tokens) {
        if (e.expiresNs <= 0 || e.expiresNs > t) keep.push_back(e);
    }
    ep.tokens.swap(keep);
}

bool Stream::Impl::allowAccess(const std::string& endpointPath, const std::string& token, AccessRole role, int ttlSec) {
    auto ep = getByPath(endpointPath);
    if (!ep) return false;
    std::lock_guard<std::mutex> lock(ep->gateMtx);
    int64_t exp = (ttlSec > 0) ? (nowNs() + (int64_t)ttlSec * 1000000000LL) : 0;
    // refresh if existing
    for (auto& e : ep->tokens) {
        if (e.tok == token) { e.expiresNs = exp; e.role = role; return true; }
    }
    ep->tokens.push_back(TokenEntry{token, exp, role});
    return true;
}

void Stream::Impl::removeAccess(const std::string& endpointPath, const std::string& token) {
    auto ep = getByPath(endpointPath);
    if (!ep) return;
    std::lock_guard<std::mutex> lock(ep->gateMtx);
    std::vector<TokenEntry> keep;
    for (auto& e : ep->tokens) if (e.tok != token) keep.push_back(e);
    ep->tokens.swap(keep);
}

void Stream::Impl::clearAccess(const std::string& endpointPath) {
    auto ep = getByPath(endpointPath);
    if (!ep) return;
    std::lock_guard<std::mutex> lock(ep->gateMtx);
    ep->tokens.clear();
}

// ============================================================================
// Impl: params
// ============================================================================

bool Stream::Impl::setParam(const EndpointHandle& h, const std::string& id, const std::string& value) {
    auto ep = getByHandle(h);
    if (!ep) return false;
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    ep->params[id] = value;
    return true;
}

bool Stream::Impl::getParam(const EndpointHandle& h, const std::string& id, std::string& outValue) const {
    auto ep = getByHandle(h);
    if (!ep) return false;
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    auto it = ep->params.find(id);
    if (it == ep->params.end()) return false;
    outValue = it->second;
    return true;
}

std::map<std::string,std::string> Stream::Impl::listParams(const EndpointHandle& h) const {
    std::map<std::string,std::string> out;
    auto ep = getByHandle(h);
    if (!ep) return out;
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    out = ep->params;
    return out;
}

bool Stream::Impl::setParamByPath(const std::string& path, const std::string& id, const std::string& value) {
    auto ep = getByPath(path);
    if (!ep) return false;
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    ep->params[id] = value;
    return true;
}
bool Stream::Impl::getParamByPath(const std::string& path, const std::string& id, std::string& outValue) const {
    auto ep = getByPath(path);
    if (!ep) return false;
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    auto it = ep->params.find(id);
    if (it == ep->params.end()) return false;
    outValue = it->second;
    return true;
}
std::map<std::string,std::string> Stream::Impl::listParamsByPath(const std::string& path) const {
    std::map<std::string,std::string> out;
    auto ep = getByPath(path);
    if (!ep) return out;
    std::lock_guard<std::mutex> lock(ep->paramMtx);
    out = ep->params;
    return out;
}

// ============================================================================
// Impl: recording
// ============================================================================

std::string Stream::Impl::startRecording(const EndpointHandle& h, const std::string& dst) {
    auto ep = getByHandle(h);
    if (!ep) return std::string();

    if (ep->kind != EndpointKind::Fmp4) {
        CV_Error(cv::Error::StsNotImplemented, "Recording is only implemented for fMP4 endpoints");
    }

#if defined(HAVE_STREAM_COMPRESSION)
    if (!ep->encoder) {
        CV_Error(cv::Error::StsError, "Encoder not initialized for this endpoint");
    }
    if (!dst.empty()) {
        RecordingParams rp = ep->recCfg;
        rp.destination = dst;
        ep->recCfg = rp;
        ep->encoder->configureRecording(ep->recCfg);
        ep->lastDest = dst;
    } else if (ep->recCfg.destination.size()) {
        ep->encoder->configureRecording(ep->recCfg);
        ep->lastDest = ep->recCfg.destination;
    }
    if (!ep->encoder->startRecording()) {
        return std::string();
    }
    return ep->lastDest;
#else
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return std::string();
#endif
}

std::string Stream::Impl::stopRecording(const EndpointHandle& h) {
    auto ep = getByHandle(h);
    if (!ep) return std::string();

    if (ep->kind != EndpointKind::Fmp4) {
        CV_Error(cv::Error::StsNotImplemented, "Recording is only implemented for fMP4 endpoints");
    }
#if defined(HAVE_STREAM_COMPRESSION)
    if (!ep->encoder) return std::string();
    ep->encoder->stopRecording();
    return ep->lastDest;
#else
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return std::string();
#endif
}

bool Stream::Impl::configureRecording(const EndpointHandle& h, const RecordingParams& rp) {
    auto ep = getByHandle(h);
    if (!ep) return false;
    if (ep->kind != EndpointKind::Fmp4) {
        CV_Error(cv::Error::StsNotImplemented, "Recording is only implemented for fMP4 endpoints");
    }
#if defined(HAVE_STREAM_COMPRESSION)
    if (!ep->encoder) return false;
    ep->recCfg = rp;
    ep->lastDest = rp.destination;
    return ep->encoder->configureRecording(ep->recCfg);
#else
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return false;
#endif
}

std::string Stream::Impl::startRecordingByPath(const std::string& path, const std::string& dst) {
    auto ep = getByPath(path);
    if (!ep) return std::string();
    return startRecording(EndpointHandle(byPath[path]), dst);
}
std::string Stream::Impl::stopRecordingByPath(const std::string& path) {
    auto ep = getByPath(path);
    if (!ep) return std::string();
    return stopRecording(EndpointHandle(byPath[path]));
}
bool Stream::Impl::configureRecordingByPath(const std::string& path, const RecordingParams& rp) {
    auto ep = getByPath(path);
    if (!ep) return false;
    return configureRecording(EndpointHandle(byPath[path]), rp);
}
bool Stream::Impl::splitRecordingSegment(const EndpointHandle& h) {
    auto ep = getByHandle(h);
    if (!ep) return false;
#if defined(HAVE_STREAM_COMPRESSION)
    if (ep->encoder) return ep->encoder->splitSegment();
    return false;
#else
    CV_Error(cv::Error::StsNotImplemented, "This build does not include compression/FFmpeg support");
    return false;
#endif
}
bool Stream::Impl::splitRecordingSegmentByPath(const std::string& path) {
    auto ep = getByPath(path);
    if (!ep) return false;
    return splitRecordingSegment(EndpointHandle(byPath[path]));
}

// ============================================================================
// Impl: endpoints (add/remove) and workers
// ============================================================================

std::shared_ptr<Stream::Impl::Endpoint> Stream::Impl::getByHandle(const EndpointHandle& h) const {
    std::lock_guard<std::mutex> lock(epMtx);
    auto it = byId.find(h.id);
    if (it == byId.end()) return nullptr;
    return it->second;
}
std::shared_ptr<Stream::Impl::Endpoint> Stream::Impl::getByPath(const std::string& path) const {
    std::lock_guard<std::mutex> lock(epMtx);
    auto jt = byPath.find(path);
    if (jt == byPath.end()) return nullptr;
    auto it = byId.find(jt->second);
    if (it == byId.end()) return nullptr;
    return it->second;
}

EndpointHandle Stream::Impl::addRaw(const std::string& path, const FrameSource& src, const RawOptions& opts) {
    if (!srv) CV_Error(cv::Error::StsError, "Server not started");

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
    }

    // register WS route
    attachRawWS(*ep);

    // launch worker (simple broadcaster)
    ep->run = true;
    ep->worker = std::thread([ep]() {
        const int64_t frameDur = (ep->fps > 0) ? (1000000000LL / ep->fps) : 0;
        int64_t nextNs = nowNs();
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
    });

    // store
    int id;
    {
        std::lock_guard<std::mutex> lock(epMtx);
        id = nextId++;
        byId[id] = ep;
        byPath[path] = id;
    }

    // auto webview (lab mode)
    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::Raw; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }

    return EndpointHandle(id);
}

EndpointHandle Stream::Impl::addFmp4(const std::string& path, const FrameSource& src, const Fmp4Options& opts) {
    if (!srv) CV_Error(cv::Error::StsError, "Server not started");
#if !defined(HAVE_STREAM_COMPRESSION)
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
    }

    // encoder
    ep->encoder.reset(new Encoder());
    if (!ep->encoder->open(opts.encoder)) {
        CV_Error(cv::Error::StsError, "Failed to open encoder");
    }
    ep->recCfg = RecordingParams(); // default
    ep->lastDest.clear();

    // WS route
    attachFmp4WS(*ep);

    // worker: feed encoder and broadcast fragments
    ep->run = true;
    ep->worker = std::thread([ep]() {
        // init segment cache once available
        bool initReady = false;

        while (ep->run) {
            // 1) ingest frame → encoder
            if (ep->rawSource) {
                cv::Mat f; int64_t pts = -1;
                if (ep->rawSource(f, pts) && !f.empty()) {
                    ep->encoder->push(f);
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
                        }
                    }
                    initReady = true;
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
                }
            } else {
                // small idle
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        }
    });

    int id;
    {
        std::lock_guard<std::mutex> lock(epMtx);
        id = nextId++;
        byId[id] = ep;
        byPath[path] = id;
    }

    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::Fmp4; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }

    return EndpointHandle(id);
#endif
}

#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
EndpointHandle Stream::Impl::addWebRtc(const std::string& path, const EncodedSource& encoded, const WebRtcOptions& opts) {
    if (!srv) CV_Error(cv::Error::StsError, "Server not started");

    std::shared_ptr<Endpoint> ep(new Endpoint());
    ep->kind = EndpointKind::WebRTC;
    ep->path = path;
    ep->editor = false; // no auto UI controls for WebRTC at this layer
    ep->encSource = encoded;

    attachWebRtcWS(opts, &ep->encSource, /*raw*/NULL, *ep);

    int id;
    { std::lock_guard<std::mutex> lock(epMtx);
      id = nextId++; byId[id] = ep; byPath[path] = id; }

    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::WebRTC; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }
    return EndpointHandle(id);
}

EndpointHandle Stream::Impl::addWebRtcRaw(const std::string& path, const FrameSource& raw, const WebRtcOptions& opts) {
    if (!srv) CV_Error(cv::Error::StsError, "Server not started");

    std::shared_ptr<Endpoint> ep(new Endpoint());
    ep->kind = EndpointKind::WebRTC;
    ep->path = path;
    ep->editor = false;
    ep->rawSource = raw;

    attachWebRtcWS(opts, /*enc*/NULL, &ep->rawSource, *ep);

    int id;
    { std::lock_guard<std::mutex> lock(epMtx);
      id = nextId++; byId[id] = ep; byPath[path] = id; }

    if (!secure && opts.title.size()) {
        WebviewOptions v; v.videoClient = webpage::VideoClient::WebRTC; v.title = opts.title;
        mountEmbedded(path + "/view", path, v);
    }
    return EndpointHandle(id);
}
#endif

void Stream::Impl::remove(const EndpointHandle& h) {
    auto ep = getByHandle(h);
    if (!ep) return;

    // unregister WS/HTTP
    if (srv) {
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
    }
}

void Stream::Impl::removeByPath(const std::string& path) {
    auto ep = getByPath(path);
    if (!ep) return;
    remove(EndpointHandle(byPath[path]));
}

// ============================================================================
// WS routes
// ============================================================================

void Stream::Impl::attachRawWS(Endpoint& ep) {
    WebSocketHandler h;
    h.onOpen = [this,&ep](WebSocketSession& s) {
        SessionCtx ctx; ctx.ws = &s; ctx.authed = false; ctx.role = AccessRole::ReadOnly;
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        ep.sessions.push_back(ctx);
    };
    h.onMessage = [this,&ep](WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        // Token handshake: first message must be ASCII "T <token>"
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
                    if (!secure || tokenAllowed(ep, tok, &role)) {
                        it.authed = true; it.role = secure ? role : AccessRole::ReadOnly;
                        // ack
                        const char* ok = "OK";
                        s.send(ok, 2, false);
                    } else {
                        s.close(WsCloseCode::PolicyViolation, "unauthorized");
                    }
                }
            }
            break;
        }
    };
    h.onClose = [this,&ep](WebSocketSession& s, int /*code*/, const std::string& /*reason*/) {
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        std::vector<SessionCtx> keep;
        for (auto& it : ep.sessions) if (it.ws != &s) keep.push_back(it);
        ep.sessions.swap(keep);
    };
    srv->registerWebSocketEndpoint(ep.path, h);
}

void Stream::Impl::attachFmp4WS(Endpoint& ep) {
    WebSocketHandler h;
    h.onOpen = [this,&ep](WebSocketSession& s) {
        SessionCtx ctx; ctx.ws = &s; ctx.authed = false; ctx.role = AccessRole::ReadOnly; ctx.fmp4InitSent = false;
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        ep.sessions.push_back(ctx);
    };
    h.onMessage = [this,&ep](WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        // Token handshake
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
                    if (!secure || tokenAllowed(ep, tok, &role)) {
                        it.authed = true; it.role = secure ? role : AccessRole::ReadOnly;
                        // send init if cached
                        if (!ep.fmp4Init.empty()) {
                            it.ws->send(ep.fmp4Init.data(), ep.fmp4Init.size(), true);
                            it.fmp4InitSent = true;
                        }
                        const char* ok = "OK";
                        s.send(ok, 2, false);
                    } else {
                        s.close(WsCloseCode::PolicyViolation, "unauthorized");
                    }
                }
            }
            break;
        }
    };
    h.onClose = [this,&ep](WebSocketSession& s, int /*code*/, const std::string& /*reason*/) {
        std::lock_guard<std::mutex> lk(ep.sessMtx);
        std::vector<SessionCtx> keep;
        for (auto& it : ep.sessions) if (it.ws != &s) keep.push_back(it);
        ep.sessions.swap(keep);
    };
    srv->registerWebSocketEndpoint(ep.path, h);
}

#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
void Stream::Impl::attachWebRtcWS(const WebRtcOptions& opts, const EncodedSource* enc, const FrameSource* raw, Endpoint& ep) {
    WebSocketHandler h;

    // Per-WS peer state (one peer per signaling session).
    struct PeerState {
        std::unique_ptr<WebRtcPeer> peer;
        std::thread worker;
        std::atomic<bool> run{false};
        bool authed = false;
        AccessRole role = AccessRole::ReadOnly;
    };
    // Keep small map in endpoint (session* -> state)
    std::unordered_map<WebSocketSession*, std::shared_ptr<PeerState> > peers;
    std::mutex peersMtx;

    h.onOpen = [&ep,&peers,&peersMtx](WebSocketSession& s) {
        auto st = std::shared_ptr<PeerState>(new PeerState());
        std::lock_guard<std::mutex> lk(peersMtx);
        peers[&s] = st;
        // token gating will happen on first message
    };

    h.onMessage = [this,&ep,&peers,&peersMtx,&opts,enc,raw](WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        std::string msg(reinterpret_cast<const char*>(data), reinterpret_cast<const char*>(data)+n);

        std::shared_ptr<PeerState> st;
        { std::lock_guard<std::mutex> lk(peersMtx);
          auto it = peers.find(&s); if (it != peers.end()) st = it->second; }
        if (!st) return;

        if (!st->authed) {
            std::string tok;
            if (msg.size() > 2 && msg[0]=='T' && msg[1]==' ') tok = msg.substr(2);
            {
                std::lock_guard<std::mutex> g(ep.gateMtx);
                dropExpiredTokens(ep);
                AccessRole role;
                if (!secure || tokenAllowed(ep, tok, &role)) {
                    st->authed = true; st->role = secure ? role : AccessRole::ReadOnly;
                    const char* ok = "OK"; s.send(ok, 2, false);

                    // Create peer now
                    st->peer = createWebRtcPeer();
                    WebRtcCallbacks cb;
                    cb.onLocalDescription = [&s](const Sdp& local) {
                        std::string json = makeSdpJson(local);
                        s.send(json.data(), json.size(), /*binary=*/false);
                    };
                    cb.onIceCandidate = [&s](const IceCandidate& ic) {
                        std::string json = makeIceJson(ic);
                        s.send(json.data(), json.size(), /*binary=*/false);
                    };
                    cb.onError = [&s](const char* where, int err) {
                        std::ostringstream os; os << "{\"error\":\"" << where << "\"," << "\"code\":" << err << "}";
                        const std::string js = os.str();
                        s.send(js.data(), js.size(), false);
                    };

                    if (!st->peer->open(opts.webrtc, opts.ingest, cb)) {
                        s.close(WsCloseCode::InternalError, "webrtc open failed");
                        return;
                    }
                    if (opts.autostartOffer) st->peer->createOffer();

                    // media worker
                    st->run = true;
                    if (enc) {
                        st->worker = std::thread([st,enc]() {
                            while (st->run) {
                                std::vector<uint8_t> au; bool key=false; int64_t pts=-1;
                                if (!(*enc)(au, key, pts)) { std::this_thread::sleep_for(std::chrono::milliseconds(2)); continue; }
                                if (st->peer) st->peer->pushEncoded(VideoCodec::H264, au.data(), au.size(), key, pts);
                            }
                        });
                    } else if (raw) {
                        st->worker = std::thread([st,raw]() {
                            while (st->run) {
                                cv::Mat f; int64_t pts=-1;
                                if (!(*raw)(f, pts) || f.empty()) { std::this_thread::sleep_for(std::chrono::milliseconds(2)); continue; }
                                if (st->peer) st->peer->pushRawFrame(f, pts);
                            }
                        });
                    }
                } else {
                    s.close(WsCloseCode::PolicyViolation, "unauthorized");
                }
            }
            return;
        }

        // After auth: SDP/ICE messages as JSON
        Sdp sdp;
        IceCandidate ice;
        if (parseSdpFromJson(msg, sdp)) {
            if (st->peer) {
                st->peer->setRemoteDescription(sdp);
                // If remote was an offer and autostartOffer==false, app can call createAnswer via client msg,
                // but we auto-answer here if type==offer.
                if (sdp.type == "offer") st->peer->createAnswer();
            }
            return;
        }
        if (parseIceFromJson(msg, ice)) {
            if (st->peer) st->peer->addRemoteIceCandidate(ice);
            return;
        }
        // Unknown message ignored.
    };

    h.onClose = [&peers,&peersMtx](WebSocketSession& s, int /*code*/, const std::string& /*reason*/) {
        std::shared_ptr<PeerState> st;
        {
            std::lock_guard<std::mutex> lk(peersMtx);
            auto it = peers.find(&s); if (it != peers.end()) { st = it->second; peers.erase(it); }
        }
        if (st) {
            st->run = false;
            if (st->worker.joinable()) st->worker.join();
            if (st->peer) st->peer->close();
            st.reset();
        }
    };

    srv->registerWebSocketEndpoint(ep.path, h);
}
#endif

// ============================================================================
// Impl: web pages
// ============================================================================

void Stream::Impl::writeSimpleHtml(Response& res, const std::string& html) {
    res.setStatusCode(200);
    res.setHeader("Content-Type", webpage::kContentTypeHtml);
    (void)res.write(html.c_str(), html.size());
}

void Stream::Impl::ensureRootPage() {
    if (!srv) return;
    srv->registerEndpoint(rootMount, [this](const Request& /*rq*/, Response& rs) {
        // Build a tiny index with links to endpoints and /view pages.
        std::ostringstream os;
        os << "<!doctype html><html><head><meta charset='utf-8'><title>"
           << rootTitle << "</title></head><body><h1>" << rootTitle << "</h1><ul>";
        {
            std::lock_guard<std::mutex> lock(epMtx);
            for (auto& kv : byId) {
                const auto& ep = *kv.second;
                os << "<li><code>" << ep.path << "</code> (";
                os << (ep.kind==EndpointKind::Raw?"raw": ep.kind==EndpointKind::Fmp4?"fmp4":"webrtc");
                os << ") — <a href='" << ep.path << "/view'>view</a></li>";
            }
        }
        os << "</ul></body></html>";
        writeSimpleHtml(rs, os.str());
    });
}

void Stream::Impl::mountEmbedded(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view) {
    if (!srv) return;
    srv->registerEndpoint(pagePath, [streamPath,view](const Request& /*rq*/, Response& rs) {
        // Pick client runtime
        webpage::VideoClient vc = view.videoClient;
        char buf[64*1024];
        size_t n = 0;

        if (vc == webpage::VideoClient::Auto) {
            // heuristic: presence of "/webrtc" in path => webrtc
            if (streamPath.find("webrtc") != std::string::npos) vc = webpage::VideoClient::WebRTC;
            else if (streamPath.find("fmp4") != std::string::npos) vc = webpage::VideoClient::Fmp4;
            else vc = webpage::VideoClient::Raw;
        }

        // Use the simple embedded_video_page(...) with explicit runtime
        n = webpage::embedded_video_page(buf, sizeof(buf), streamPath.c_str(), vc,
                                         view.title.empty()? "OpenCV Stream" : view.title.c_str());
        std::string html(buf, buf + std::min(n, sizeof(buf)-1));
        writeSimpleHtml(rs, html);
    });
}

void Stream::Impl::mountJupyter(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view) {
    if (!srv) return;
    srv->registerEndpoint(pagePath, [streamPath,view](const Request& /*rq*/, Response& rs) {
        char buf[64*1024];
        size_t n = 0;
        // Jupyter variant (video-only back-compat)
        if (view.videoClient == webpage::VideoClient::WebRTC) {
            n = webpage::webview_jupyter_page(buf, sizeof(buf), "/", streamPath.c_str());
        } else if (view.videoClient == webpage::VideoClient::Fmp4) {
            n = webpage::webview_jupyter_page(buf, sizeof(buf), "/", streamPath.c_str());
        } else {
            n = webpage::webview_jupyter_page(buf, sizeof(buf), "/", streamPath.c_str());
        }
        std::string html(buf, buf + std::min(n, sizeof(buf)-1));
        writeSimpleHtml(rs, html);
    });
}

// ============================================================================
// Impl: introspection
// ============================================================================

std::vector<std::string> Stream::Impl::listEndpoints() const {
    std::vector<std::string> out;
    std::lock_guard<std::mutex> lock(epMtx);
    out.reserve(byPath.size());
    for (auto& kv : byPath) out.push_back(kv.first);
    return out;
}

EndpointKind Stream::Impl::kindOf(const std::string& path) const {
    auto ep = getByPath(path);
    if (!ep) return EndpointKind::Raw;
    return ep->kind;
}

// ============================================================================
// Public wrappers
// ============================================================================

Stream::Stream() : pimpl(new Impl) {}
Stream::~Stream() {}

bool Stream::start(const std::string& bindAddress, int port, bool secureMode, int numThreads) {
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

void Stream::stop() { if (pimpl) pimpl->stop(); }
bool Stream::isRunning() const { return pimpl && pimpl->isRunning(); }
void Stream::setSecureMode(bool on) { if (pimpl) pimpl->setSecureMode(on); }
bool Stream::secureMode() const { return pimpl && pimpl->secureMode(); }
void Stream::enableRootIndex(bool on, const std::string& mountPath, const std::string& title) {
    if (pimpl) pimpl->enableRootIndex(on, mountPath, title);
}

EndpointHandle Stream::addRaw(const std::string& path, const FrameSource& src, const RawOptions& opts) {
    if (!pimpl) return EndpointHandle();
    return pimpl->addRaw(path, src, opts);
}
EndpointHandle Stream::addFmp4(const std::string& path, const FrameSource& src, const Fmp4Options& opts) {
    if (!pimpl) return EndpointHandle();
    return pimpl->addFmp4(path, src, opts);
}
#if defined(HAVE_STREAM_WEBRTC_GSTREAMER)
EndpointHandle Stream::addWebRtc(const std::string& path, const EncodedSource& encoded, const WebRtcOptions& opts) {
    if (!pimpl) return EndpointHandle();
    return pimpl->addWebRtc(path, encoded, opts);
}
EndpointHandle Stream::addWebRtcRaw(const std::string& path, const FrameSource& raw, const WebRtcOptions& opts) {
    if (!pimpl) return EndpointHandle();
    return pimpl->addWebRtcRaw(path, raw, opts);
}
#endif

void Stream::remove(const EndpointHandle& h) { if (pimpl) pimpl->remove(h); }
void Stream::removeByPath(const std::string& path) { if (pimpl) pimpl->removeByPath(path); }

void Stream::mountEmbedded(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view) {
    if (pimpl) pimpl->mountEmbedded(pagePath, streamPath, view);
}
void Stream::mountJupyter(const std::string& pagePath, const std::string& streamPath, const WebviewOptions& view) {
    if (pimpl) pimpl->mountJupyter(pagePath, streamPath, view);
}

bool Stream::allowAccess(const std::string& endpointPath, const std::string& token, AccessRole role, int ttlSeconds) {
    return pimpl && pimpl->allowAccess(endpointPath, token, role, ttlSeconds);
}
void Stream::removeAccess(const std::string& endpointPath, const std::string& token) {
    if (pimpl) pimpl->removeAccess(endpointPath, token);
}
void Stream::clearAccess(const std::string& endpointPath) {
    if (pimpl) pimpl->clearAccess(endpointPath);
}

bool Stream::setParam(const EndpointHandle& h, const std::string& id, const std::string& value) {
    return pimpl && pimpl->setParam(h, id, value);
}
bool Stream::getParam(const EndpointHandle& h, const std::string& id, std::string& outValue) const {
    return pimpl && pimpl->getParam(h, id, outValue);
}
std::map<std::string,std::string> Stream::listParams(const EndpointHandle& h) const {
    return pimpl ? pimpl->listParams(h) : std::map<std::string,std::string>();
}

bool Stream::setParamByPath(const std::string& path, const std::string& id, const std::string& value) {
    return pimpl && pimpl->setParamByPath(path, id, value);
}
bool Stream::getParamByPath(const std::string& path, const std::string& id, std::string& outValue) const {
    return pimpl && pimpl->getParamByPath(path, id, outValue);
}
std::map<std::string,std::string> Stream::listParamsByPath(const std::string& path) const {
    return pimpl ? pimpl->listParamsByPath(path) : std::map<std::string,std::string>();
}

std::string Stream::startRecording(const EndpointHandle& h, const std::string& destination) {
    return pimpl ? pimpl->startRecording(h, destination) : std::string();
}
std::string Stream::stopRecording(const EndpointHandle& h) {
    return pimpl ? pimpl->stopRecording(h) : std::string();
}
bool Stream::configureRecording(const EndpointHandle& h, const RecordingParams& params) {
    return pimpl && pimpl->configureRecording(h, params);
}

std::string Stream::startRecordingByPath(const std::string& path, const std::string& destination) {
    return pimpl ? pimpl->startRecordingByPath(path, destination) : std::string();
}
std::string Stream::stopRecordingByPath(const std::string& path) {
    return pimpl ? pimpl->stopRecordingByPath(path) : std::string();
}
bool Stream::configureRecordingByPath(const std::string& path, const RecordingParams& params) {
    return pimpl && pimpl->configureRecordingByPath(path, params);
}
bool Stream::splitRecordingSegment(const EndpointHandle& h) {
    return pimpl && pimpl->splitRecordingSegment(h);
}
bool Stream::splitRecordingSegmentByPath(const std::string& path) {
    return pimpl && pimpl->splitRecordingSegmentByPath(path);
}

std::vector<std::string> Stream::listEndpoints() const {
    return pimpl ? pimpl->listEndpoints() : std::vector<std::string>();
}
EndpointKind Stream::kindOf(const std::string& path) const {
    return pimpl ? pimpl->kindOf(path) : EndpointKind::Raw;
}

std::unique_ptr<Stream> createStream() { return std::unique_ptr<Stream>(new Stream()); }

} // namespace stream
} // namespace cv
