// server_civetweb.cpp — LOGGING-ONLY INSTRUMENTATION (super-verbose, crash breadcrumbs)
// Toggle this to 1 if you also want a crash-time backtrace + breadcrumb ring dump.
#ifndef STREAM_CIVETWEB_CRASH_HOOK
#define STREAM_CIVETWEB_CRASH_HOOK 1
#endif

#if defined(HAVE_STREAM_HTTP_CIVETWEB)

#include "opencv2/stream/server.hpp"

#include <opencv2/core/utils/logger.hpp>
#include <civetweb.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdarg>     // va_list, va_start, va_end
#include <cstring>     // strstr, memchr
#include <deque>
#include <functional>  // std::hash<std::thread::id>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <condition_variable>
#include <iomanip>

#if defined(_WIN32)
  #include <string.h>
  #define strcasecmp _stricmp
#else
  #include <strings.h>
  #include <unistd.h>  // write()
  #if STREAM_CIVETWEB_CRASH_HOOK
    #include <signal.h>
    #include <execinfo.h>
  #endif
#endif

#ifndef MG_WEBSOCKET_OPCODE_TEXT
#define MG_WEBSOCKET_OPCODE_TEXT 0x1
#endif
#ifndef MG_WEBSOCKET_OPCODE_BINARY
#define MG_WEBSOCKET_OPCODE_BINARY 0x2
#endif

namespace cv {
namespace stream {

namespace {

// --- safe, portable bounded strlen (no strnlen dependency) ---
static inline size_t cstrnlen(const char* s, size_t max) {
    if (!s) return 0;
    if (max == 0) return 0;
    const void* p = std::memchr(s, '\0', max);
    return p ? static_cast<size_t>(static_cast<const char*>(p) - s) : max;
}

// ---------- raw breadcrumbs that always print (write(2)) + tiny ring buffer ----------
struct _DbgRing {
    static const int kSlots = 1024;
    static const int kLine  = 256;

    std::atomic<unsigned long> idx{0};
    char  msg[kSlots][kLine];
    size_t len[kSlots];

    void put(const char* s) {
        unsigned long i = idx.fetch_add(1, std::memory_order_relaxed) % kSlots;
        std::snprintf(msg[i], kLine, "%s", s);
        len[i] = cstrnlen(msg[i], kLine);
    #if !defined(_WIN32)
        ::write(STDERR_FILENO, msg[i], len[i]);
        ::write(STDERR_FILENO, "\n", 1);
    #else
        std::fwrite(msg[i], 1, len[i], stderr);
        std::fwrite("\n", 1, 1, stderr);
        std::fflush(stderr);
    #endif
    }

    void dump() {
        unsigned long end = idx.load(std::memory_order_relaxed);
        unsigned long n   = end > (unsigned long)kSlots ? kSlots : end;

        static const char hdr[] = "\n==== civetweb crashlog (most recent first) ====\n";
    #if !defined(_WIN32)
        ::write(STDERR_FILENO, hdr, sizeof(hdr)-1);
    #else
        std::fwrite(hdr, 1, sizeof(hdr)-1, stderr);
        std::fflush(stderr);
    #endif

        for (unsigned long j = 0; j < n; ++j) {
            unsigned long p = (end - 1 - j) % kSlots;
        #if !defined(_WIN32)
            ::write(STDERR_FILENO, msg[p], len[p]);
            ::write(STDERR_FILENO, "\n", 1);
        #else
            std::fwrite(msg[p], 1, len[p], stderr);
            std::fwrite("\n", 1, 1, stderr);
        #endif
        }
    #if defined(_WIN32)
        std::fflush(stderr);
    #endif
    }
};

static _DbgRing& _ring() { static _DbgRing R; return R; }

static inline void _bc(char (&buf)[512], const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    _ring().put(buf);
}

#if STREAM_CIVETWEB_CRASH_HOOK && !defined(_WIN32)
static void _segv(int sig, siginfo_t*, void*) {
    const char head[] = "\n*** SIGSEGV (server_civetweb) ***\n";
    ::write(STDERR_FILENO, head, sizeof(head)-1);
    _ring().dump();
    void* bt[128]; int n = backtrace(bt, 128);
    const char bthdr[] = "---- backtrace ----\n";
    ::write(STDERR_FILENO, bthdr, sizeof(bthdr)-1);
    backtrace_symbols_fd(bt, n, STDERR_FILENO);
    _exit(128 + sig);
}
static void _install_segv_once() {
    static std::atomic<bool> once{false};
    bool expected=false;
    if (once.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        struct sigaction sa{};
        sa.sa_sigaction = &_segv;
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = SA_SIGINFO | SA_RESETHAND;
        sigaction(SIGSEGV, &sa, nullptr);
        char buf[512]; _bc(buf, "[CRASHHOOK] SIGSEGV handler installed");
    }
}
#else
static inline void _install_segv_once() {}
#endif

using cv::utils::logging::LogLevel;
static cv::utils::logging::LogTag kStreamLogTag(
    "cv.stream.server",
    LogLevel::LOG_LEVEL_VERBOSE
);
cv::utils::logging::LogTag* kTag = &kStreamLogTag;

struct LogScope {
    const char*      name;
    const char*      file;
    int              line;
    std::thread::id  tid;
    size_t           tid_hash;

    LogScope(const char* n, const char* f, int l)
        : name(n), file(f), line(l), tid(std::this_thread::get_id()),
          tid_hash(std::hash<std::thread::id>{}(tid)) {
        CV_LOG_VERBOSE(kTag, 5, "[ENTER] " << name << " @" << file << ":" << line << " tid=" << tid);
        char b[512]; _bc(b, "[ENTER] %s %s:%d tid=%zu", name, file, line, (size_t)tid_hash);
    }
    ~LogScope() {
        CV_LOG_VERBOSE(kTag, 5, "[EXIT ] " << name << " @" << file << ":" << line << " tid=" << tid);
        char b[512]; _bc(b, "[EXIT ] %s %s:%d tid=%zu", name, file, line, (size_t)tid_hash);
    }
};

#define SCOPE_TRACE(name) LogScope _cv_scope__(name, __FILE__, __LINE__)

template <typename T>
static inline std::string ptr(T* p) { std::ostringstream o; o << (const void*)p; return o.str(); }

static inline std::string to_string_u64(uint64_t v) { std::ostringstream o; o << v; return o.str(); }
static inline std::string httpStatusText(int code) {
    switch (code) { case 200: return "OK"; case 404: return "Not Found"; case 426: return "Upgrade Required"; case 500: return "Internal Server Error"; case 503: return "Service Unavailable"; default: return "OK"; }
}
static inline bool ci_eq(const char* a, const char* b) {
    if (!a || !b) return false;
    while (*a && *b) {
        char ca = (*a >= 'A' && *a <= 'Z') ? (*a - 'A' + 'a') : *a;
        char cb = (*b >= 'A' && *b <= 'Z') ? (*b - 'A' + 'a') : *b;
        if (ca != cb) return false; ++a; ++b;
    } return *a == *b;
}
static inline bool is_ws_upgrade(struct mg_connection* conn) {
    SCOPE_TRACE("is_ws_upgrade");
    const char* up = mg_get_header(conn, "Upgrade");
    const char* cn = mg_get_header(conn, "Connection");
    CV_LOG_VERBOSE(kTag, 4, "Upgrade=" << (up?up:"(null)") << " Connection=" << (cn?cn:"(null)") << " conn=" << ptr(conn));
    return up && cn && ci_eq(up, "websocket") && (std::strstr(cn, "Upgrade") || std::strstr(cn, "upgrade"));
}
static inline std::string remote_string(const mg_request_info* ri) {
    if (!ri) return std::string();
    std::string ip = ri->remote_addr ? ri->remote_addr : "";
    if (ri->remote_port > 0) { char tmp[32]; std::snprintf(tmp, sizeof(tmp), ":%d", ri->remote_port); ip += tmp; }
    return ip;
}
static inline void dump_request_info(uint64_t rid, const mg_request_info* ri) {
    CV_LOG_VERBOSE(kTag, 4, "[RID " << rid << "] ri=" << ptr((void*)ri));
    char b[512];
    if (!ri) { _bc(b, "[RID %llu] ri=null", (unsigned long long)rid); return; }
    _bc(b, "[RID %llu] method='%s' request_uri='%s' local_uri='%s' query='%s' ver='%s' remote='%s'",
        (unsigned long long)rid,
        ri->request_method?ri->request_method:"(null)",
        ri->request_uri?ri->request_uri:"(null)",
        ri->local_uri?ri->local_uri:"(null)",
        ri->query_string?ri->query_string:"(null)",
        ri->http_version?ri->http_version:"(null)",
        remote_string(ri).c_str());
#if defined(MG_MAX_HEADERS)
    for (int i=0;i<ri->num_headers;++i) {
        CV_LOG_VERBOSE(kTag, 4, "[RID " << rid << "] H["<<i<<"] "
                          << (ri->http_headers[i].name?ri->http_headers[i].name:"(null)") << ": "
                          << (ri->http_headers[i].value?ri->http_headers[i].value:"(null)"));
    }
#endif
}
static inline std::string preview(const char* data, size_t n, size_t maxn=64) {
    std::ostringstream o; o << "\"";
    size_t lim = std::min(n, maxn);
    for (size_t i=0;i<lim;++i) {
        unsigned char c = (unsigned char)data[i];
        if (c>=32 && c!=127) o<<char(c); else { o<<"\\x"<<std::hex<<std::setw(2)<<std::setfill('0')<<(unsigned)c<<std::dec; }
    }
    if (n>lim) o<<"\"…("<<(n-lim)<<" more)"; else o<<"\"";
    return o.str();
}

} // namespace

// ============================================================================
// Request / Response wrappers
// ============================================================================

class CivetRequest final : public Request {
public:
    explicit CivetRequest(struct mg_connection* c)
        : conn_(c), ri_(mg_get_request_info(c)) { SCOPE_TRACE("CivetRequest::ctor"); }
    std::string getMethod() const CV_OVERRIDE { SCOPE_TRACE("CivetRequest::getMethod"); return (ri_&&ri_->request_method)?ri_->request_method:std::string(); }
    std::string getPath()   const CV_OVERRIDE { SCOPE_TRACE("CivetRequest::getPath"); if (!ri_) return {}; if (ri_->local_uri) return ri_->local_uri; if (ri_->request_uri) return ri_->request_uri; return {}; }
    bool isWebSocketUpgrade() const CV_OVERRIDE { SCOPE_TRACE("CivetRequest::isWebSocketUpgrade"); return is_ws_upgrade(conn_); }
    struct mg_connection* raw() const { return conn_; }
private:
    struct mg_connection* conn_;
    const mg_request_info* ri_;
};

class CivetResponse final : public Response {
public:
    explicit CivetResponse(struct mg_connection* c) : conn_(c) { SCOPE_TRACE("CivetResponse::ctor"); }
    ~CivetResponse() CV_OVERRIDE {
        SCOPE_TRACE("CivetResponse::dtor");
        if (started_ && chunked_) {
            int rc1 = mg_printf(conn_, "0\r\n\r\n");
            CV_LOG_VERBOSE(kTag, 4, "final-chunk rc=" << rc1 << " conn=" << ptr(conn_));
            char b[512]; _bc(b, "[HTTP] final-chunk rc=%d conn=%p", rc1, (void*)conn_);
        }
    }
    void setStatusCode(int code) CV_OVERRIDE { SCOPE_TRACE("CivetResponse::setStatusCode"); if (!started_) statusCode_=code; }
    void setHeader(const std::string& k, const std::string& v) CV_OVERRIDE { SCOPE_TRACE("CivetResponse::setHeader"); if (!started_) headers_.push_back(std::make_pair(k,v)); }

    bool write(const char* data, size_t size) CV_OVERRIDE {
        SCOPE_TRACE("CivetResponse::write");
        if (!conn_) { CV_LOG_WARNING(kTag, "write: conn=null"); return false; }
        if (!started_) {
            bool hasCL=false, hasTE=false, hasCT=false, hasConn=false;
            for (size_t i=0;i<headers_.size();++i) {
                const std::string& h = headers_[i].first;
                if (!hasCL  && strcasecmp(h.c_str(),"Content-Length")==0) hasCL=true;
                if (!hasTE  && strcasecmp(h.c_str(),"Transfer-Encoding")==0) hasTE=true;
                if (!hasCT  && strcasecmp(h.c_str(),"Content-Type")==0) hasCT=true;
                if (!hasConn&& strcasecmp(h.c_str(),"Connection")==0) hasConn=true;
            }
            // Force "Connection: close" to immediately release the CivetWeb worker after the response.
            chunked_ = !(hasCL || hasTE);
            CV_LOG_VERBOSE(kTag, 3, "HTTP start: status="<<statusCode_<<" chunked="<<chunked_
                                <<" hasCL="<<hasCL<<" hasTE="<<hasTE<<" hasCT="<<hasCT<<" hasConn="<<hasConn);
            char b0[512]; _bc(b0, "[HTTP] start status=%d chunked=%d hasCL=%d hasTE=%d hasCT=%d hasConn=%d",
                              statusCode_, (int)chunked_, (int)hasCL, (int)hasTE, (int)hasCT, (int)hasConn);
            int rc = mg_printf(conn_, "HTTP/1.1 %d %s\r\n", statusCode_, httpStatusText(statusCode_).c_str());
            CV_LOG_VERBOSE(kTag, 4, "write status rc="<<rc);
            if (!hasCT)   { rc = mg_printf(conn_, "Content-Type: application/octet-stream\r\n"); }
            // Always close to free the thread (avoid keep-alive stalls)
            rc = mg_printf(conn_, "Connection: close\r\n");
            if (chunked_) { rc = mg_printf(conn_, "Transfer-Encoding: chunked\r\n"); }
            rc = mg_printf(conn_, "Cache-Control: no-cache, no-store, must-revalidate\r\n");
            rc = mg_printf(conn_, "Pragma: no-cache\r\n");
            for (size_t i=0;i<headers_.size();++i) { rc = mg_printf(conn_, "%s: %s\r\n", headers_[i].first.c_str(), headers_[i].second.c_str()); }
            rc = mg_printf(conn_, "\r\n");
            started_ = true;
        }
        if (chunked_) {
            int rcw = mg_printf(conn_, "%zx\r\n", size);
            if (size) { (void)mg_write(conn_, data, size); }
            mg_printf(conn_, "\r\n");
            char b[512]; _bc(b, "[HTTP] chunk sz=%zu rcw=%d", size, rcw);
        } else {
            if (size) { int rc = mg_write(conn_, data, size); char b[512]; _bc(b, "[HTTP] body rc=%d sz=%zu", rc, size); }
        }
        return true;
    }

    bool acceptWebSocket(const WebSocketHandler&, const std::vector<std::string>&) CV_OVERRIDE {
        SCOPE_TRACE("CivetResponse::acceptWebSocket");
        CV_LOG_WARNING(kTag, "acceptWebSocket not supported on civetweb HTTP path");
        char b[512]; _bc(b, "[HTTP WARN] acceptWebSocket called on HTTP path");
        return false;
    }

private:
    struct mg_connection* conn_;
    int statusCode_ = 200;
    std::vector<std::pair<std::string,std::string> > headers_;
    bool started_ = false;
    bool chunked_ = false;
};

// ============================================================================
// WebSocket session (instrumented)
// ============================================================================

class CivetWebSocketSession final : public WebSocketSession,
                                    public std::enable_shared_from_this<CivetWebSocketSession> {
public:
    CivetWebSocketSession(struct mg_connection* c, std::string path, const ServerConfig& cfg)
        : conn_(c), path_(std::move(path)), cfg_(cfg), open_(true), rng_(static_cast<unsigned>(std::random_device()())) {
        SCOPE_TRACE("CivetWebSocketSession::ctor");
        CV_LOG_VERBOSE(kTag, 3, "WS session @"<<ptr(this)<<" conn="<<ptr(conn_)<<" path="<<path_);
        char b[512]; _bc(b, "[WS OPEN] sess=%p conn=%p path='%s'", (void*)this, (void*)conn_, path_.c_str());
    }
    ~CivetWebSocketSession() CV_OVERRIDE { SCOPE_TRACE("CivetWebSocketSession::dtor"); stop_keepalive_(); }

    bool send(const void* data, size_t size, bool binary) CV_OVERRIDE {
        SCOPE_TRACE("CivetWebSocketSession::send");
        if (!isOpen()) return false;
        std::lock_guard<std::mutex> lk(send_mx_);
        if (!isOpen()) return false;
        int opcode = binary ? MG_WEBSOCKET_OPCODE_BINARY : MG_WEBSOCKET_OPCODE_TEXT;
        int rc = mg_websocket_write(conn_, opcode, static_cast<const char*>(data), size);
        CV_LOG_VERBOSE(kTag, 4, "mg_websocket_write rc="<<rc<<" size="<<size<<" bin="<<binary);
        char b[512]; _bc(b, "[WS WRITE] rc=%d size=%zu bin=%d", rc, size, (int)binary);
        return rc >= 0;
    }
    void close(WsCloseCode /*code*/, const std::string& reason) CV_OVERRIDE {
        SCOPE_TRACE("CivetWebSocketSession::close");
        CV_LOG_INFO(kTag, "WS close @"<<ptr(this)<<" reason="<<reason);
        char b[512]; _bc(b, "[WS CLOSE] sess=%p reason='%s'", (void*)this, reason.c_str());
        std::lock_guard<std::mutex> lk(state_mx_);
        if (!open_) return;
        open_ = false; mg_close_connection(conn_); cv_.notify_all();
    }
    bool isOpen() const CV_OVERRIDE { return open_.load(std::memory_order_acquire); }
    std::string remoteAddress() const CV_OVERRIDE { return remote_string(mg_get_request_info(conn_)); }
    void setWriteQueueLimit(size_t) CV_OVERRIDE {}
    size_t queuedBytes() const CV_OVERRIDE { return 0; }
    void setNoDelay(bool) CV_OVERRIDE {}
    void enableCompression(bool) CV_OVERRIDE {}
    const std::string& path() const { return path_; }

    void on_open(const WebSocketHandler& h) {
        SCOPE_TRACE("CivetWebSocketSession::on_open");
        handler_ = h; open_.store(true, std::memory_order_release);
        if (handler_.onOpen) handler_.onOpen(*this);
        if (cfg_.ws.enabled) start_keepalive_();
    }
    void on_data(int bits, char* data, size_t len) {
        SCOPE_TRACE("CivetWebSocketSession::on_data");
        bool isBinary = ((bits & 0x0F) == MG_WEBSOCKET_OPCODE_BINARY);
        bool isText   = ((bits & 0x0F) == MG_WEBSOCKET_OPCODE_TEXT);
        CV_LOG_VERBOSE(kTag, 4, "bits=0x"<<std::hex<<bits<<std::dec<<" len="<<len<<" bin="<<isBinary<<" txt="<<isText<<" preview="<<preview(data,std::min<size_t>(len,64)));
        if (handler_.onMessage && (isBinary || isText)) {
            handler_.onMessage(*this, reinterpret_cast<const uint8_t*>(data), len, isBinary);
        }
    }
    void on_close_normal() {
        SCOPE_TRACE("CivetWebSocketSession::on_close_normal");
        { std::lock_guard<std::mutex> lk(state_mx_); if (!open_) return; open_ = false; }
        stop_keepalive_();
        if (handler_.onClose) handler_.onClose(*this, static_cast<int>(WsCloseCode::Normal), std::string());
        cv_.notify_all();
    }
    void on_error(int ec, const char* where) {
        SCOPE_TRACE("CivetWebSocketSession::on_error");
        CV_LOG_ERROR(kTag, "WS error ec="<<ec<<" where="<<(where?where:"(null)"));
    }

private:
    void start_keepalive_() {
        SCOPE_TRACE("CivetWebSocketSession::start_keepalive_");
        if (cfg_.ws.challengeInterval.count() <= 0) return;
        ka_stop_.store(false, std::memory_order_release);
        ka_thread_ = std::thread([self = shared_from_this()](){ self->keepalive_loop_(); });
    }
    void stop_keepalive_() {
        SCOPE_TRACE("CivetWebSocketSession::stop_keepalive_");
        ka_stop_.store(true, std::memory_order_release); cv_.notify_all();
        if (ka_thread_.joinable()) ka_thread_.join();
    }
    std::string make_token_(std::size_t n) {
        static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, (int)sizeof(alphabet)-2);
        std::string s; s.reserve(n);
        for (std::size_t i=0;i<n;++i) s.push_back(alphabet[(std::size_t)dist(rng_)]);
        return s;
    }
    void keepalive_loop_() {
        SCOPE_TRACE("CivetWebSocketSession::keepalive_loop_");
        CV_LOG_VERBOSE(kTag, 2, "Keepalive loop start remote="<<remoteAddress());
        while (!ka_stop_.load(std::memory_order_acquire) && isOpen()) {
            { std::unique_lock<std::mutex> lk(ack_mx_); if (cv_.wait_for(lk, cfg_.ws.challengeInterval, [this]{ return ka_stop_.load() || !isOpen(); })) break; }
            if (!isOpen() || ka_stop_.load()) break;
            const std::string token = make_token_(cfg_.ws.challengeSize);
            bool sent = send(token.data(), token.size(), /*binary*/false);
            if (!sent) break;
            bool timed_out=false;
            { std::unique_lock<std::mutex> lk(ack_mx_);
              if (!cv_.wait_for(lk, cfg_.ws.clientTimeout, [this]{ return !isOpen() || ka_stop_.load(); })) {
                  timed_out = true;
              }
            }
            if (timed_out) { close(WsCloseCode::PolicyViolation, "keepalive timeout"); break; }
        }
        CV_LOG_VERBOSE(kTag, 2, "Keepalive loop end remote="<<remoteAddress());
    }

private:
    struct mg_connection* conn_;
    std::string           path_;
    ServerConfig          cfg_;
    WebSocketHandler      handler_;

    mutable std::mutex    send_mx_;
    mutable std::mutex    state_mx_;
    std::atomic<bool>     open_;

    std::thread           ka_thread_;
    std::atomic<bool>     ka_stop_{false};
    std::mt19937          rng_;

    std::mutex            ack_mx_;
    std::condition_variable cv_;
    std::string           expected_ack_;
    bool                  ack_received_ = false;
};

// ============================================================================
// Server::ServerImpl (CivetWeb) — ULTRA VERBOSE PATH TRACE
// ============================================================================

class Server::ServerImpl {
public:
    ServerImpl() : ctx_(NULL) {
        SCOPE_TRACE("ServerImpl::ctor");
        CV_LOG_INFO(kTag, "ServerImpl created @"<<ptr(this));
        char b[512]; _bc(b, "[SVR] ctor self=%p", (void*)this);
    }
    ~ServerImpl() { SCOPE_TRACE("ServerImpl::dtor"); stop(); }

    bool start(int port, int num_threads) {
        SCOPE_TRACE("ServerImpl::start");
        _install_segv_once();
        char b1[512]; _bc(b1, "[SVR] start port=%d threads=%d ctx=%p", port, num_threads, (void*)ctx_);
        CV_LOG_INFO(kTag, "start(port="<<port<<", threads="<<num_threads<<") ctx="<<ptr(ctx_));
        if (ctx_) return true;

        port_str_    = to_string_u64((unsigned short)port);
        threads_str_ = to_string_u64((unsigned)std::max(1, num_threads));
        thread_limit_ = std::max(1, num_threads);

        // IMPORTANT: Disable HTTP keep-alive so each HTTP request frees its worker immediately.
        // Also set finite request timeout; leave websocket timeout unlimited (0).
        const char* options[] = {
            "listening_ports",      port_str_.c_str(),
            "num_threads",          threads_str_.c_str(),
            "enable_keep_alive",    "no",          // <<< key fix for CivetWeb worker exhaustion
            "request_timeout_ms",   "10000",       // don't block workers forever on HTTP
            "websocket_timeout_ms", "0",           // WS lifetime controlled by app
            0
        };

        std::memset(&callbacks_, 0, sizeof(callbacks_));
        callbacks_.begin_request = &ServerImpl::beginRequestFallback_;
        CV_LOG_VERBOSE(kTag, 3, "civetweb version: " << mg_version() << " callbacks@" << ptr(&callbacks_));
        char b2[512]; _bc(b2, "[SVR] mg_start opts@%p callbacks@%p", (void*)options, (void*)&callbacks_);

        ctx_ = mg_start(&callbacks_, this, options);
        CV_LOG_VERBOSE(kTag, 3, "mg_start ctx="<<ptr(ctx_)<<" user_data="<<ptr(this));
        char b3[512]; _bc(b3, "[SVR] mg_start ctx=%p user_data=%p", (void*)ctx_, (void*)this);
        if (!ctx_) { CV_LOG_ERROR(kTag, "civetweb mg_start failed"); _bc(b3, "[SVR] mg_start FAILED"); return false; }

        installHandlers_();

        // Proactive warning about CivetWeb capacity (thread-per-connection model).
        CV_LOG_WARNING(kTag,
            "Using CivetWeb backend will only allow for " << thread_limit_
            << " number of (ws + http) connections in total (thread-per-connection).");
        {
            char b[512];
            std::snprintf(b, sizeof(b),
                "[SVR WARN] using civetweb backend will only allow for %d number of ws+http connections in total",
                thread_limit_);
            _ring().put(b);
        }

        CV_LOG_INFO(kTag, "Server started (civetweb) on *:"<<port<<" threads="<<num_threads);
        return true;
    }

    void stop() {
        SCOPE_TRACE("ServerImpl::stop");
        if (!ctx_) { CV_LOG_VERBOSE(kTag, 3, "stop: ctx=null"); char b[512]; _bc(b, "[SVR] stop ctx=null"); return; }

        std::vector<std::shared_ptr<CivetWebSocketSession> > sessions;
        {
            std::lock_guard<std::mutex> lk(mx_);
            for (auto it=ws_sessions_.begin(); it!=ws_sessions_.end(); ++it)
                for (auto s=it->second.begin(); s!=it->second.end(); ++s) if (*s) sessions.push_back(*s);
        }
        for (size_t i=0;i<sessions.size();++i) sessions[i]->close(WsCloseCode::GoingAway, "Server shutdown");

        mg_stop(ctx_); ctx_=NULL;
        {
            std::lock_guard<std::mutex> lk(mx_);
            http_routes_.clear(); ws_routes_.clear(); ws_sessions_.clear(); route_data_.clear();
            installed_http_.clear(); installed_ws_.clear();
        }
        ws_count_.store(0, std::memory_order_release);
        CV_LOG_INFO(kTag, "Server stopped (civetweb)");
        char b[512]; _bc(b, "[SVR] stopped");
    }

    bool isRunning() const { return ctx_ != NULL; }

    // Configuration
    void setConfig(const ServerConfig& c) {
        SCOPE_TRACE("ServerImpl::setConfig");
        std::lock_guard<std::mutex> lk(cfg_mx_); config_ = c;
        CV_LOG_VERBOSE(kTag, 3, "cfg: 404.ct="<<config_.notFound.contentType
                             <<" 404.len="<<config_.notFound.body.size()
                             <<" ws.enabled="<<config_.ws.enabled
                             <<" ws.interval="<<config_.ws.challengeInterval.count()
                             <<" ws.timeout="<<config_.ws.clientTimeout.count()
                             <<" ws.chsize="<<config_.ws.challengeSize
                             <<" ws.ackPrefix=\""<<config_.ws.ackPrefix<<"\"");
    }
    ServerConfig getConfig() const { std::lock_guard<std::mutex> lk(cfg_mx_); return config_; }

    // HTTP routes
    void registerEndpoint(const std::string& path, const RequestHandler& h) {
        SCOPE_TRACE("ServerImpl::registerEndpoint");
        CV_LOG_INFO(kTag, "registerEndpoint path=\""<<path<<"\" ctx="<<ptr(ctx_)<<" cached(before)="<<http_routes_.size());
        char b[512]; _bc(b, "[SVR] registerEndpoint path='%s' ctx=%p cached(before)=%zu target='%s' empty=%d",
                         path.c_str(), (void*)ctx_, http_routes_.size(), h ? h.target_type().name() : "<empty>", !h);
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_[path] = h;
        CV_LOG_VERBOSE(kTag, 3, "cached path=\""<<path<<"\" cached(now)="<<http_routes_.size());
        if (!ctx_) { CV_LOG_VERBOSE(kTag, 3, "ctx null; will install on start()"); return; }
        addHttpRoute_(path);
    }
    void unregisterEndpoint(const std::string& path) {
        SCOPE_TRACE("ServerImpl::unregisterEndpoint");
        CV_LOG_INFO(kTag, "unregisterEndpoint path=\""<<path<<"\" ctx="<<ptr(ctx_));
        char b[512]; _bc(b, "[SVR] unregisterEndpoint path='%s' ctx=%p", path.c_str(), (void*)ctx_);
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_.erase(path);
        if (ctx_) {
            mg_set_request_handler(ctx_, path.c_str(), 0, 0);
            installed_http_.erase(path);
            eraseRouteDataForPath_(path);
        }
    }

    // WS routes
    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& h) {
        SCOPE_TRACE("ServerImpl::registerWebSocketEndpoint");
        CV_LOG_INFO(kTag, "registerWebSocketEndpoint path=\""<<path<<"\" ctx="<<ptr(ctx_));
        char b[512]; _bc(b, "[SVR] registerWebSocketEndpoint path='%s' ctx=%p", path.c_str(), (void*)ctx_);
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_[path] = h;
        if (!ctx_) { CV_LOG_VERBOSE(kTag, 3, "ctx null; will install on start()"); return; }
        addWsRoute_(path, h);
    }
    void unregisterWebSocketEndpoint(const std::string& path) {
        SCOPE_TRACE("ServerImpl::unregisterWebSocketEndpoint");
        CV_LOG_INFO(kTag, "unregisterWebSocketEndpoint path=\""<<path<<"\" ctx="<<ptr(ctx_));
        char b[512]; _bc(b, "[SVR] unregisterWebSocketEndpoint path='%s' ctx=%p", path.c_str(), (void*)ctx_);
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_.erase(path); ws_sessions_.erase(path);
        if (ctx_) {
            mg_set_websocket_handler(ctx_, path.c_str(), 0, 0, 0, 0, 0);
            installed_ws_.erase(path);
            eraseRouteDataForPath_(path);
        }
    }

    size_t broadcast(const std::string& path, const void* data, size_t n, bool binary) {
        SCOPE_TRACE("ServerImpl::broadcast");
        std::vector<std::shared_ptr<CivetWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(mx_);
            auto it = ws_sessions_.find(path);
            if (it==ws_sessions_.end()) return 0;
            copy.assign(it->second.begin(), it->second.end());
        }
        size_t ok=0; for (size_t i=0;i<copy.size();++i) ok += copy[i]->send(data,n,binary)?1:0;
        return ok;
    }
    void forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) {
        SCOPE_TRACE("ServerImpl::forEachWebSocket");
        std::vector<std::shared_ptr<CivetWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(mx_);
            auto it = ws_sessions_.find(path);
            if (it==ws_sessions_.end()) return;
            copy.assign(it->second.begin(), it->second.end());
        }
        for (size_t i=0;i<copy.size();++i) fn(*copy[i]);
    }
    size_t numWebSocketClients(const std::string& path) const {
        SCOPE_TRACE("ServerImpl::numWebSocketClients");
        std::lock_guard<std::mutex> lk(mx_);
        auto it = ws_sessions_.find(path);
        return (it==ws_sessions_.end())?0u:it->second.size();
    }

private:
    struct RouteData { ServerImpl* impl; std::string path; WebSocketHandler ws_h; };
    typedef std::unordered_set<std::shared_ptr<CivetWebSocketSession> > SessionSet;

    void installHandlers_() {
        SCOPE_TRACE("ServerImpl::installHandlers_");
        std::lock_guard<std::mutex> lk(mx_);
        route_data_.clear();
        for (auto it = http_routes_.begin(); it!=http_routes_.end(); ++it) addHttpRoute_(it->first);
        for (auto it = ws_routes_.begin(); it!=ws_routes_.end(); ++it) addWsRoute_(it->first, it->second);
    }
    void addHttpRoute_(const std::string& path) {
        SCOPE_TRACE("ServerImpl::addHttpRoute_");
        std::unique_ptr<RouteData> rd(new RouteData()); rd->impl=this; rd->path=path;
        mg_set_request_handler(ctx_, path.c_str(), &ServerImpl::httpHandler_, rd.get());
        installed_http_.insert(path);
        CV_LOG_VERBOSE(kTag, 3, "HTTP installed path=\"" << path << "\" installed_http_.size=" << installed_http_.size());
        char b[512]; _bc(b, "[SVR] HTTP install path='%s' cbdata=%p installed=%zu", path.c_str(), (void*)rd.get(), (size_t)installed_http_.size());
        route_data_.push_back(std::move(rd));
    }
    void addWsRoute_(const std::string& path, const WebSocketHandler& h) {
        SCOPE_TRACE("ServerImpl::addWsRoute_");
        std::unique_ptr<RouteData> rd(new RouteData()); rd->impl=this; rd->path=path; rd->ws_h=h;
        mg_set_websocket_handler(ctx_, path.c_str(), &ServerImpl::wsConnect_, &ServerImpl::wsReady_, &ServerImpl::wsData_, &ServerImpl::wsClose_, rd.get());
        installed_ws_.insert(path);
        CV_LOG_VERBOSE(kTag, 3, "WS installed path=\"" << path << "\" installed_ws_.size=" << installed_ws_.size());
        char b[512]; _bc(b, "[SVR]  WS  install path='%s' cbdata=%p installed=%zu", path.c_str(), (void*)rd.get(), (size_t)installed_ws_.size());
        route_data_.push_back(std::move(rd));
    }
    void eraseRouteDataForPath_(const std::string& path) {
        SCOPE_TRACE("ServerImpl::eraseRouteDataForPath_");
        size_t before = route_data_.size();
        route_data_.erase(std::remove_if(route_data_.begin(), route_data_.end(),
            [&](const std::unique_ptr<RouteData>& p){ return p && p->path==path; }), route_data_.end());
        CV_LOG_VERBOSE(kTag, 3, "eraseRouteDataForPath_ \""<<path<<"\" removed="<<(before-route_data_.size()));
        char b[512]; _bc(b, "[SVR] eraseRouteData path='%s' removed=%zu", path.c_str(), (size_t)(before-route_data_.size()));
    }

    // Request ID generator
    static uint64_t nextRid_() { static std::atomic<uint64_t> c(1); return c.fetch_add(1, std::memory_order_relaxed); }

    // Capacity helpers
    inline int usedWorkersApprox_() const {
        return active_http_.load(std::memory_order_acquire) + ws_count_.load(std::memory_order_acquire);
    }
    inline bool capacityAvailable_() const {
        return usedWorkersApprox_() < thread_limit_;
    }
    void warnRejected_(const char* kind, const mg_request_info* ri, const char* path) {
        CV_LOG_WARNING(kTag, "("<<kind<<") connection rejected due to thread shortage "
                           << "[limit="<<thread_limit_<<", used~"<<usedWorkersApprox_()<<"] "
                           << "remote="<<remote_string(ri)<<" path="<<(path?path:""));
        char b[512];
        std::snprintf(b, sizeof(b), "[SVR WARN] (%s) connection rejected due to thread shortage (limit=%d used~%d) remote='%s' path='%s'",
                      kind, thread_limit_, usedWorkersApprox_(), remote_string(ri).c_str(), path?path:"");
        _ring().put(b);
    }

    // Fallback: central path-resolution trace
    static int beginRequestFallback_(struct mg_connection* conn) {
        SCOPE_TRACE("ServerImpl::beginRequestFallback_");
        uint64_t rid = nextRid_();
        mg_context* ctx = mg_get_context(conn);
        ServerImpl* self = reinterpret_cast<ServerImpl*>(mg_get_user_data(ctx));
        char b0[512]; _bc(b0, "[RID %llu] beginRequest conn=%p ctx=%p self=%p",
                          (unsigned long long)rid, (void*)conn, (void*)ctx, (void*)self);
        if (!self) return 0;

        const mg_request_info* ri = mg_get_request_info(conn);
        dump_request_info(rid, ri);

        std::string local = (ri && ri->local_uri)   ? ri->local_uri   : std::string();
        std::string req   = (ri && ri->request_uri) ? ri->request_uri : std::string();
        std::string path  = !local.empty() ? local : req;
        bool isWS = is_ws_upgrade(conn);

        bool cachedHttp=false, cachedWs=false, installedHttp=false, installedWs=false;
        {
            std::lock_guard<std::mutex> lk(self->mx_);
            cachedHttp = self->http_routes_.find(path) != self->http_routes_.end();
            cachedWs   = self->ws_routes_.find(path)   != self->ws_routes_.end();
            installedHttp = self->installed_http_.find(path) != self->installed_http_.end();
            installedWs   = self->installed_ws_.find(path   ) != self->installed_ws_.end();
        }
        char b1[512]; _bc(b1, "[RID %llu] PATH='%s' isWS=%d cachedHttp=%d cachedWs=%d installedHttp=%d installedWs=%d",
                          (unsigned long long)rid, path.c_str(), (int)isWS, (int)cachedHttp, (int)cachedWs, (int)installedHttp, (int)installedWs);

        // If this is an HTTP request for an installed handler and we're out of workers, reject early with 503.
        if (!isWS && installedHttp && !self->capacityAvailable_()) {
            self->warnRejected_("http", ri, path.c_str());
            std::ostringstream oss;
            static const char* body = "503 Service Unavailable (thread shortage)";
            oss << "HTTP/1.1 503 " << httpStatusText(503) << "\r\n"
                << "Content-Type: text/plain; charset=utf-8\r\n"
                << "Content-Length: " << std::strlen(body) << "\r\n"
                << "Connection: close\r\n\r\n"
                << body;
            const std::string resp = oss.str();
            (void)mg_write(conn, resp.c_str(), resp.size());
            return 1; // handled here
        }

        if ((!isWS && installedHttp) || (isWS && installedWs)) {
            CV_LOG_VERBOSE(kTag, 3, "[RID "<<rid<<"] handoff to installed handler");
            char b2[512]; _bc(b2, "[RID %llu] handoff to installed handler", (unsigned long long)rid);
            return 0;
        }

        const ServerConfig cfg = self->getConfig();
        std::ostringstream oss;
        oss << "HTTP/1.1 404 " << httpStatusText(404) << "\r\n";
        oss << "Content-Type: " << cfg.notFound.contentType << "\r\n";
        for (size_t i=0;i<cfg.defaultNotFoundHeaders.size();++i)
            oss << cfg.defaultNotFoundHeaders[i].first << ": " << cfg.defaultNotFoundHeaders[i].second << "\r\n";
        oss << "Content-Length: " << cfg.notFound.body.size() << "\r\n";
        oss << "Connection: close\r\n\r\n";
        const std::string head = oss.str();

        CV_LOG_WARNING(kTag, "[RID "<<rid<<"] 404 for \""<<path<<"\" (WS="<<(isWS?"yes":"no")
                                <<") remote="<<remote_string(ri)
                                <<" head="<<head.size()<<" body="<<cfg.notFound.body.size());
        char b3[512]; _bc(b3, "[RID %llu] 404 for '%s' (WS=%d) remote='%s'",
                          (unsigned long long)rid, path.c_str(), (int)isWS, remote_string(ri).c_str());

        (void)mg_write(conn, head.c_str(), head.size());
        if (!cfg.notFound.body.empty())
            (void)mg_write(conn, cfg.notFound.body.data(), cfg.notFound.body.size());
        return 1; // handled
    }

    // HTTP handler
    static int httpHandler_(struct mg_connection* conn, void* cbdata) {
        SCOPE_TRACE("ServerImpl::httpHandler_");
        uint64_t rid = nextRid_();
        RouteData* rd = static_cast<RouteData*>(cbdata);
        char b0[512]; _bc(b0, "[RID %llu] httpHandler conn=%p rd=%p", (unsigned long long)rid, (void*)conn, (void*)rd);
        if (!rd || !rd->impl) { CV_LOG_WARNING(kTag, "[RID "<<rid<<"] rd/impl null"); return 0; }

        // Track active HTTP workers while we are handling a request
        struct ActiveGuard {
            std::atomic<int>& ref;
            ActiveGuard(std::atomic<int>& r) : ref(r) { ref.fetch_add(1, std::memory_order_acq_rel); }
            ~ActiveGuard() { ref.fetch_sub(1, std::memory_order_acq_rel); }
        } active(rd->impl->active_http_);

        RequestHandler handler;
        {
            std::lock_guard<std::mutex> lk(rd->impl->mx_);
            auto it = rd->impl->http_routes_.find(rd->path);
            CV_LOG_VERBOSE(kTag, 3, "[RID "<<rid<<"] lookup \""<<rd->path<<"\" found="<<(it!=rd->impl->http_routes_.end()));
            char b1[512]; _bc(b1, "[RID %llu] lookup path='%s' found=%d",
                              (unsigned long long)rid, rd->path.c_str(), (int)(it!=rd->impl->http_routes_.end()));
            if (it == rd->impl->http_routes_.end()) return 0;
            handler = it->second; // copy
        }

        const mg_request_info* ri = mg_get_request_info(conn);
        dump_request_info(rid, ri);
        CV_LOG_VERBOSE(kTag, 3, "[RID "<<rid<<"] DISPATCH path=\""<<rd->path<<"\" remote="<<remote_string(ri));
        char b2[512]; _bc(b2, "[RID %llu] DISPATCH path='%s' handler.empty=%d type='%s'",
                          (unsigned long long)rid, rd->path.c_str(), !handler, handler ? handler.target_type().name() : "<empty>");

        CivetRequest req(conn);
        CivetResponse res(conn);

        // Ultra-early breadcrumb before operator() to catch closure issues
        char b3[512]; _bc(b3, "[RID %llu] BEFORE CALL handler@%p req@%p res@%p",
                          (unsigned long long)rid, (void*)&handler, (void*)&req, (void*)&res);

        try {
            handler(req, res);
            CV_LOG_VERBOSE(kTag, 3, "[RID "<<rid<<"] handler returned path=\""<<rd->path<<"\"");
            char b4[512]; _bc(b4, "[RID %llu] AFTER CALL ok", (unsigned long long)rid);
        } catch (const std::exception& e) {
            CV_LOG_ERROR(kTag, "[RID "<<rid<<"] handler exception: "<<e.what());
            char b5[512]; _bc(b5, "[RID %llu] EXCEPTION: %s", (unsigned long long)rid, e.what());
            const char* msg = "Internal Server Error";
            mg_printf(conn, "HTTP/1.1 500 %s\r\n"
                            "Content-Type: text/plain; charset=utf-8\r\n"
                            "Connection: close\r\n"
                            "Content-Length: %zu\r\n\r\n%s",
                            httpStatusText(500).c_str(), std::strlen(msg), msg);
        } catch (...) {
            CV_LOG_ERROR(kTag, "[RID "<<rid<<"] handler unknown exception");
            char b6[512]; _bc(b6, "[RID %llu] EXCEPTION: unknown", (unsigned long long)rid);
            const char* msg = "Internal Server Error";
            mg_printf(conn, "HTTP/1.1 500 %s\r\n"
                            "Content-Type: text/plain; charset=utf-8\r\n"
                            "Connection: close\r\n"
                            "Content-Length: %zu\r\n\r\n%s",
                            httpStatusText(500).c_str(), std::strlen(msg), msg);
        }
        return 1;
    }

    // WS handlers
    static int  wsConnect_(const struct mg_connection* cconn, void* cbdata) {
        // Capacity gate *before* accepting WS upgrade.
        RouteData* rd = (RouteData*)cbdata;
        if (!rd || !rd->impl) return 1;
        // NOTE: mg_request_info is safe to read here
        const mg_request_info* ri = mg_get_request_info(cconn);
        if (!rd->impl->capacityAvailable_()) {
            rd->impl->warnRejected_("ws", ri, rd->path.c_str());
            // Reject upgrade. (We don't try to write a response here; browser will see handshake failure.)
            return 1;
        }
        return 0;
    }
    static void wsReady_(struct mg_connection* conn, void* cbdata) {
        RouteData* rd = static_cast<RouteData*>(cbdata); if (!rd||!rd->impl) return;
        const ServerConfig cfg = rd->impl->getConfig();
        const mg_request_info* ri = mg_get_request_info(conn);
        std::shared_ptr<CivetWebSocketSession> session(new CivetWebSocketSession(conn, rd->path, cfg));
        { std::lock_guard<std::mutex> lk(rd->impl->mx_); rd->impl->ws_sessions_[rd->path].insert(session); }
        rd->impl->ws_count_.fetch_add(1, std::memory_order_acq_rel);
        mg_set_user_connection_data(conn, session.get());
        CV_LOG_INFO(kTag, "WS OPEN path="<<rd->path<<" remote="<<remote_string(ri)<<" session="<<ptr(session.get()));
        char b[512]; _bc(b, "[WS READY] path='%s' sess=%p", rd->path.c_str(), (void*)session.get());
        session->on_open(rd->ws_h);
    }
    static int  wsData_(struct mg_connection* conn, int bits, char* data, size_t len, void* cbdata) {
        RouteData* rd = static_cast<RouteData*>(cbdata); if (!rd||!rd->impl) return 0;
        CivetWebSocketSession* sess = static_cast<CivetWebSocketSession*>(mg_get_user_connection_data(conn)); if (!sess) return 1;
        sess->on_data(bits, data, len); return 1;
    }
    static void wsClose_(const struct mg_connection* conn, void* cbdata) {
        RouteData* rd = static_cast<RouteData*>(cbdata); if (!rd||!rd->impl) return;
        CivetWebSocketSession* raw = static_cast<CivetWebSocketSession*>(mg_get_user_connection_data(conn));
        if (raw) raw->on_close_normal();
        std::lock_guard<std::mutex> lk(rd->impl->mx_);
        auto it = rd->impl->ws_sessions_.find(rd->path);
        if (it!=rd->impl->ws_sessions_.end()) {
            for (auto s=it->second.begin(); s!=it->second.end(); ) { if (s->get()==raw) s=it->second.erase(s); else ++s; }
            if (it->second.empty()) rd->impl->ws_sessions_.erase(it);
        }
        rd->impl->ws_count_.fetch_sub(1, std::memory_order_acq_rel);
        CV_LOG_INFO(kTag, "WS CLOSED path="<<rd->path);
        char b[512]; _bc(b, "[WS CLOSE] path='%s' raw=%p", rd->path.c_str(), (void*)raw);
    }

private:
    struct mg_context*  ctx_;
    struct mg_callbacks callbacks_;

    mutable std::mutex mx_;
    std::unordered_map<std::string, RequestHandler>   http_routes_;
    std::unordered_map<std::string, WebSocketHandler> ws_routes_;
    std::unordered_map<std::string, SessionSet>       ws_sessions_;
    std::vector<std::unique_ptr<RouteData> >          route_data_;

    // for logging — handlers actually installed in civetweb
    std::unordered_set<std::string> installed_http_;
    std::unordered_set<std::string> installed_ws_;

    mutable std::mutex cfg_mx_;
    ServerConfig       config_;

    std::string port_str_;
    std::string threads_str_;

    // Capacity tracking
    int thread_limit_ = 1;
    std::atomic<int> active_http_{0}; // in-flight HTTP handlers (keep-alive disabled, so equals worker usage for HTTP)
    std::atomic<int> ws_count_{0};    // active WS sessions (each ties up a worker)
};

// ============================================================================
// Server (public)
// ============================================================================

Server::Server() : pimpl(new ServerImpl) {}
Server::~Server() {}

bool Server::start(int port, int num_threads) { return pimpl->start(port, num_threads); }
void Server::stop() { pimpl->stop(); }
bool Server::isRunning() const { return pimpl->isRunning(); }

void Server::setConfig(const ServerConfig& cfg) { pimpl->setConfig(cfg); }
const ServerConfig& Server::getConfig() const { static ServerConfig snap; snap = pimpl->getConfig(); return snap; }

void Server::registerEndpoint(const std::string& path, const RequestHandler& handler) { pimpl->registerEndpoint(path, handler); }
void Server::unregisterEndpoint(const std::string& path) { pimpl->unregisterEndpoint(path); }
void Server::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) { pimpl->registerWebSocketEndpoint(path, handler); }
void Server::unregisterWebSocketEndpoint(const std::string& path) { pimpl->unregisterWebSocketEndpoint(path); }

size_t Server::broadcast(const std::string& path, const void* data, size_t size, bool binary) { return pimpl->broadcast(path, data, size, binary); }
void Server::forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) { pimpl->forEachWebSocket(path, fn); }
size_t Server::numWebSocketClients(const std::string& path) const { return pimpl->numWebSocketClients(path); }

Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;

std::unique_ptr<Server> createServer() { return std::unique_ptr<Server>(new Server()); }
std::unique_ptr<Server> createServer(const ServerConfig& cfg) { auto s = createServer(); s->setConfig(cfg); return s; }

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_HTTP_CIVETWEB
