#if defined(HAVE_STREAM_HTTP_MONGOOSE)

#include "opencv2/stream/server.hpp"
#include "mongoose.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <cctype>

#include "opencv2/core/utils/logger.hpp"

namespace {
    using cv::utils::logging::LogLevel;
    static cv::utils::logging::LogTag kStreamLogTag(
        "cv.stream.server",
        LogLevel::LOG_LEVEL_VERBOSE
    );
    cv::utils::logging::LogTag* kTag = &kStreamLogTag;

    template <typename T> static inline std::string to_string(const T& v) { std::ostringstream o; o << v; return o.str(); }

    // Short preview for logging payloads
    static inline std::string preview_payload(const void* data, size_t size, bool binary) {
        const char* s = reinterpret_cast<const char*>(data);
        if (!binary) {
            std::string out; out.reserve(size);
            for (size_t i = 0; i < size; ++i) {
                unsigned char ch = static_cast<unsigned char>(s[i]);
                out.push_back((ch >= 32 && ch != 127) ? char(ch) : '.');
            }
            if (out.size() > 1024) { out.resize(1024); out += "…"; }
            return std::string("TEXT(") + to_string(size) + "B): \"" + out + "\"";
        }
        const std::size_t kMax = 256;
        std::ostringstream oss; oss << "BIN(" << size << "B) [first " << std::min(size, kMax) << " bytes]: ";
        oss << std::hex;
        const std::size_t lim = std::min(size, kMax);
        for (std::size_t i = 0; i < lim; ++i) {
            unsigned v = static_cast<unsigned>(static_cast<unsigned char>(s[i]));
            if (i) oss << ' ';
            oss << std::setw(2) << std::setfill('0') << v;
        }
        if (size > kMax) oss << " …";
        return oss.str();
    }

    // Weak-pointer set helpers
    template <class T>
    struct WeakPtrHash {
        std::size_t operator()(const std::weak_ptr<T>& wp) const {
            if (auto sp = wp.lock()) return std::hash<const void*>()(sp.get());
            return 0u;
        }
    };
    template <class T>
    struct WeakPtrEqual {
        bool operator()(const std::weak_ptr<T>& a, const std::weak_ptr<T>& b) const {
            return !a.owner_before(b) && !b.owner_before(a);
        }
    };

    // Case-insensitive compare mg_str with literal
    static inline bool mg_str_ieq(const mg_str& s, const char* lit) {
        const size_t n = std::strlen(lit);
        if (s.len != n) return false;
        for (size_t i = 0; i < n; ++i) {
            unsigned char a = static_cast<unsigned char>(s.buf[i]);
            unsigned char b = static_cast<unsigned char>(lit[i]);
            if (std::tolower(a) != std::tolower(b)) return false;
        }
        return true;
    }

    static inline std::string mg_str_to_std(const mg_str& s) {
        return std::string(s.buf ? s.buf : "", s.len);
    }

    static inline bool mg_is_ws_upgrade(const mg_http_message* hm) {
        mg_http_message* ncc = const_cast<mg_http_message*>(hm);
        const mg_str* u = mg_http_get_header(ncc, "Upgrade");   // 7.x: non-const
        return u && mg_str_ieq(*u, "websocket");
    }

    // mg_ntoa() exists in 7.x; provide a tiny fallback if it’s unavailable in some amalgamations
    static inline std::string addr_to_string(const mg_addr& a) {
    #if defined(__has_include)
    # if __has_include("mongoose.h")
        char buf[64] = {0};
        #ifdef mg_ntoa
        mg_ntoa(&a, buf, sizeof(buf));
        return buf;
        #else
        if (a.is_ip6 == 0) {
            const uint8_t* ip = reinterpret_cast<const uint8_t*>(&a.ip);
            std::ostringstream oss; oss
                << unsigned(ip[0]) << '.'
                << unsigned(ip[1]) << '.'
                << unsigned(ip[2]) << '.'
                << unsigned(ip[3]) << ':' << mg_ntohs(a.port);
            return oss.str();
        }
        return std::string();
        #endif
    # else
        (void)a; return std::string();
    # endif
    #else
        (void)a; return std::string();
    #endif
    }
}

namespace cv {
namespace stream {

// =====================================================================================
// Request/Response wrappers
// =====================================================================================

class MongooseRequest final : public Request {
public:
    explicit MongooseRequest(const mg_http_message* hm)
        : method_(mg_str_to_std(hm->method))
        , path_(mg_str_to_std(hm->uri))
        , is_ws_(mg_is_ws_upgrade(hm)) {}

    std::string getMethod() const CV_OVERRIDE { return method_; }
    std::string getPath()   const CV_OVERRIDE { return path_; }
    bool isWebSocketUpgrade() const CV_OVERRIDE { return is_ws_; }
private:
    std::string method_;
    std::string path_;
    bool        is_ws_;
};

class MongooseResponse final : public Response {
public:
    MongooseResponse(struct mg_connection* c, const mg_http_message* hm)
        : c_(c), hm_(hm) {}

    ~MongooseResponse() CV_OVERRIDE {
        if (sent_) return;
        do_send();
    }

    void setStatusCode(int code) CV_OVERRIDE { status_ = code; }
    void setHeader(const std::string& key, const std::string& value) CV_OVERRIDE {
        headers_.push_back(std::make_pair(key, value));
    }

    bool write(const char* data, size_t size) CV_OVERRIDE {
        if (!data || size == 0) return true;
        body_.write(data, static_cast<std::streamsize>(size));
        return true;
    }

    bool acceptWebSocket(const WebSocketHandler&, const std::vector<std::string>&) CV_OVERRIDE {
        // WS upgrade is handled at event layer for Mongoose
        return false;
    }

    void mark_sent() { sent_ = true; }

private:
    void do_send() {
        if (!c_) return;
        std::string hdrs;
        for (size_t i = 0; i < headers_.size(); ++i) {
            hdrs += headers_[i].first;
            hdrs += ": ";
            hdrs += headers_[i].second;
            hdrs += "\r\n";
        }
        const std::string body = body_.str();
        mg_http_reply(c_, status_, hdrs.c_str(), "%.*s", (int)body.size(), body.data());
        sent_ = true;
    }

private:
    struct mg_connection*           c_ = nullptr;
    const struct mg_http_message*   hm_ = nullptr;
    int                             status_ = 200;
    std::vector<std::pair<std::string, std::string> > headers_;
    std::ostringstream              body_;
    bool                            sent_ = false;
};

// =====================================================================================
// WebSocket session
// =====================================================================================

class MongooseWebSocketSession
    : public WebSocketSession
    , public std::enable_shared_from_this<MongooseWebSocketSession> {
public:
    MongooseWebSocketSession(Server* owner,
                             struct mg_connection* c,
                             std::string path,
                             const WebSocketHandler& handler,
                             const ServerConfig& cfg)
        : owner_(owner)
        , path_(std::move(path))
        , handler_(handler)
        , cfg_(cfg)
        , rng_(static_cast<unsigned>(std::random_device()())) {
        c_.store(c, std::memory_order_release);
    }

    ~MongooseWebSocketSession() CV_OVERRIDE {}

    // Thread-safe: queue only. Actual socket writes are performed from the Mongoose thread.
    bool send(const void* data, size_t size, bool binary) CV_OVERRIDE {
        struct mg_connection* lc = c_.load(std::memory_order_acquire);
        if (!lc) return false;  // not bound (already closed)
        auto buf = std::make_shared<std::vector<uint8_t>>(
            static_cast<const uint8_t*>(data),
            static_cast<const uint8_t*>(data) + size);
        {
            std::lock_guard<std::mutex> lk(out_mtx_);
            outq_.push_back({ buf, binary });
        }
        return true;
    }

    void close(WsCloseCode code, const std::string& reason) CV_OVERRIDE {
        struct mg_connection* lc = c_.load(std::memory_order_acquire);
        if (!lc) return;
        uint16_t cc = static_cast<uint16_t>(static_cast<int>(code));
        uint8_t payload[2 + 125];
        payload[0] = static_cast<uint8_t>((cc >> 8) & 0xff);
        payload[1] = static_cast<uint8_t>(cc & 0xff);
        size_t rlen = reason.size();
        if (rlen > sizeof(payload) - 2) rlen = sizeof(payload) - 2;
        if (rlen) std::memcpy(payload + 2, reason.data(), rlen);
        mg_ws_send(lc, payload, (size_t) (2 + rlen), WEBSOCKET_OP_CLOSE);
        open_.store(false, std::memory_order_release);
    }

    bool isOpen() const CV_OVERRIDE { return open_.load(std::memory_order_acquire); }
    std::string remoteAddress() const CV_OVERRIDE { return remote_; }

    // ---- driven by Mongoose events (Mongoose thread) ----
    void on_open() {
        struct mg_connection* lc = c_.load(std::memory_order_acquire);
        remote_.clear();
        if (lc) remote_ = addr_to_string(lc->rem);
        open_.store(true, std::memory_order_release);
        CV_LOG_INFO(kTag, "WS OPEN path=" << path_ << " remote=" << remote_);
        if (handler_.onOpen) handler_.onOpen(*this);

        // Defer the first keepalive challenge until after challengeInterval.
        // This avoids sending a frame during/just-after the HTTP upgrade and
        // matches the Boost server behavior the test expects.
        if (cfg_.ws.enabled) {
            expected_ack_.clear();
            ack_deadline_tp_ = TimePoint();
            auto now = Clock::now();
            next_challenge_tp_ = (cfg_.ws.challengeInterval.count() > 0)
                                 ? now + cfg_.ws.challengeInterval
                                 : TimePoint();
        }

        flush_queued();
    }


    void on_msg(const struct mg_ws_message* wm) {
        const bool is_bin = ((wm->flags & 0x0F) == WEBSOCKET_OP_BINARY);
        const uint8_t* p = reinterpret_cast<const uint8_t*>(wm->data.buf);
        const size_t n = wm->data.len;

        CV_LOG_DEBUG(kTag, "WS recv " << n << "B " << (is_bin ? "BIN" : "TEXT")
                       << " from " << remote_ << " | " << preview_payload(p, n, is_bin));

        // Keepalive ACK check (text frames)
        if (!is_bin && !expected_ack_.empty()) {
            std::string payload(wm->data.buf, wm->data.len);
            if (payload == expected_ack_) {
                CV_LOG_DEBUG(kTag, "Keepalive ACK ok from " << remote_);
                expected_ack_.clear();
                ack_deadline_tp_ = TimePoint();
            }
        }

        if (handler_.onMessage) handler_.onMessage(*this, p, n, is_bin);

        // Critical: flush immediately after onMessage to avoid first-reply races
        flush_queued();
    }

    void on_ctl(const struct mg_ws_message* wm) {
        const uint8_t op = (wm->flags & 0x0F);
        if (op == WEBSOCKET_OP_PING) {
            if (handler_.onPing) handler_.onPing(*this,
                reinterpret_cast<const uint8_t*>(wm->data.buf), wm->data.len);
            CV_LOG_DEBUG(kTag, "WS ping from " << remote_);
        } else if (op == WEBSOCKET_OP_PONG) {
            if (handler_.onPong) handler_.onPong(*this,
                reinterpret_cast<const uint8_t*>(wm->data.buf), wm->data.len);
            CV_LOG_DEBUG(kTag, "WS pong from " << remote_);
        }
    }

    // In MongooseWebSocketSession
    void on_poll() {
        if (cfg_.ws.enabled && isOpen()) {
            auto now = Clock::now();

            // 1) Timeout check FIRST. If a previous challenge wasn't ACKed within clientTimeout, close.
            if (!expected_ack_.empty() &&
                ack_deadline_tp_.time_since_epoch().count() != 0 &&
                now >= ack_deadline_tp_) {
                CV_LOG_WARNING(kTag, "Keepalive timeout" << (remote_.empty() ? "" : (" from " + remote_)) << "; closing");
                close(WsCloseCode::PolicyViolation, "keepalive timeout");
                } else {
                    // 2) Only issue a new challenge if there is NO outstanding ACK.
                    if (expected_ack_.empty() &&
                        cfg_.ws.challengeInterval.count() > 0 &&
                        next_challenge_tp_.time_since_epoch().count() != 0 &&
                        now >= next_challenge_tp_) {
                        issue_challenge(now);
                        next_challenge_tp_ = now + cfg_.ws.challengeInterval;
                        }
                }
        }

        // Periodic flush, including any just-queued keepalive frames
        flush_queued();
    }


    void on_close() {
        bool wasOpen = open_.exchange(false, std::memory_order_acq_rel);
        if (wasOpen && handler_.onClose) handler_.onClose(*this,
            static_cast<int>(WsCloseCode::Normal), "closed");
        CV_LOG_INFO(kTag, "WS CLOSED path=" << path_ << (remote_.empty() ? "" : (" remote=" + remote_)));
    }

    void bind_connection(struct mg_connection* c) { c_.store(c, std::memory_order_release); }
    const std::string& path() const { return path_; }

private:
    struct OutMsg { std::shared_ptr<std::vector<uint8_t>> buf; bool bin; };

    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;

    // Called only on the Mongoose thread
    void flush_queued() {
        struct mg_connection* lc = c_.load(std::memory_order_acquire);
        if (!lc) return;

        std::vector<OutMsg> local;
        {
            std::lock_guard<std::mutex> lk(out_mtx_);
            if (outq_.empty()) return;
            local.swap(outq_);
        }
        for (const auto& m : local) {
            mg_ws_send(lc, m.buf->data(), m.buf->size(),
                       m.bin ? WEBSOCKET_OP_BINARY : WEBSOCKET_OP_TEXT);
        }
    }

    void issue_challenge(Clock::time_point now) {
        if (!cfg_.ws.enabled) return;
        const std::string token = make_token(cfg_.ws.challengeSize);
        expected_ack_ = cfg_.ws.ackPrefix + token;

        const bool ok = send(token.data(), token.size(), /*binary*/false);
        CV_LOG_DEBUG(kTag, "Keepalive challenge -> " << remote_
                                                     << " token=\"" << token << "\" ("
                                                     << (ok ? "queued" : "drop") << ")");
        ack_deadline_tp_ = now + cfg_.ws.clientTimeout;

        flush_queued();
    }


    std::string make_token(std::size_t n) {
        static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, static_cast<int>(sizeof(alphabet) - 2));
        std::string s; s.reserve(n);
        for (std::size_t i = 0; i < n; ++i) s.push_back(alphabet[static_cast<std::size_t>(dist(rng_))]);
        return s;
    }

private:
    Server*                                     owner_ = nullptr;
    std::atomic<struct mg_connection*>          c_{nullptr};
    std::string                                 path_;
    WebSocketHandler                            handler_;
    ServerConfig                                cfg_;
    std::atomic<bool>                           open_{false};
    std::string                                 remote_;
    std::mutex                                  out_mtx_;
    std::vector<OutMsg>                         outq_;

    // Keepalive
    TimePoint                                   next_challenge_tp_{};
    TimePoint                                   ack_deadline_tp_{};
    std::string                                 expected_ack_;
    std::mt19937                                rng_;
};

// =====================================================================================
// Server::ServerImpl (Mongoose)
// =====================================================================================

class Server::ServerImpl {
public:
    ServerImpl()
        : running_(false) {
        mg_mgr_init(&mgr_);
        CV_LOG_INFO(kTag, "ServerImpl(Mongoose) created");
    }

    ~ServerImpl() {
        stop();
        mg_mgr_free(&mgr_);
    }

    bool start(int port, int num_threads) {
        if (running_.load()) return true;
        (void) num_threads; // single poll thread for mgr
        const std::string url = std::string("http://0.0.0.0:") + to_string(port);
        listener_ = mg_http_listen(&mgr_, url.c_str(), &ServerImpl::static_ev_handler, this /*fn_data*/);
        if (!listener_) {
            CV_LOG_ERROR(kTag, "mg_http_listen failed on " << url);
            return false;
        }

        running_.store(true);
        poll_thread_ = std::thread([this, port]() {
            CV_LOG_INFO(kTag, "Poll thread run() port=" << port);
            while (running_.load()) {
                mg_mgr_poll(&mgr_, 50);
            }
            CV_LOG_INFO(kTag, "Poll thread stopped");
        });

        CV_LOG_INFO(kTag, "Server started on *:" << port << " (Mongoose) threads=1");
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;

        if (listener_) listener_ = nullptr;
        if (poll_thread_.joinable()) poll_thread_.join();

        {
            std::lock_guard<std::mutex> lk(ws_mtx_);
            ws_sessions_.clear();
        }
        {
            std::lock_guard<std::mutex> lk(ws_conn_mtx_);
            ws_by_conn_.clear();
        }
        CV_LOG_INFO(kTag, "Server stopped (Mongoose)");
    }

    bool isRunning() const { return running_.load(); }

    void setConfig(const ServerConfig& cfg) {
        std::lock_guard<std::mutex> lk(cfg_mtx_);
        config_ = cfg;
    }
    ServerConfig getConfig() const {
        std::lock_guard<std::mutex> lk(cfg_mtx_);
        return config_;
    }

    void registerEndpoint(const std::string& path, const RequestHandler& handler) {
        std::lock_guard<std::mutex> lk(http_mtx_); http_routes_[path] = handler;
    }
    void unregisterEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(http_mtx_); http_routes_.erase(path);
    }

    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) {
        std::lock_guard<std::mutex> lk(ws_h_mtx_); ws_handlers_[path] = handler;
    }
    void unregisterWebSocketEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(ws_h_mtx_); ws_handlers_.erase(path);
    }

    size_t broadcast(const std::string& path, const void* data, size_t size, bool binary) {
        std::vector<std::shared_ptr<MongooseWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(ws_mtx_);
            auto it = ws_sessions_.find(path);
            if (it == ws_sessions_.end()) return 0;
            copy.reserve(it->second.size());
            for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
                if (auto sp = sit->lock()) copy.push_back(sp);
            }
        }
        size_t count = 0;
        for (size_t i = 0; i < copy.size(); ++i) {
            count += copy[i]->send(data, size, binary) ? 1 : 0;
        }
        return count;
    }

    void forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) {
        std::vector<std::shared_ptr<MongooseWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(ws_mtx_);
            auto it = ws_sessions_.find(path);
            if (it == ws_sessions_.end()) return;
            copy.reserve(it->second.size());
            for (auto sit = it->second.begin(); sit != it->second.end(); ++sit) {
                if (auto sp = sit->lock()) copy.push_back(sp);
            }
        }
        for (size_t i = 0; i < copy.size(); ++i) fn(*copy[i]);
    }

    size_t numWebSocketClients(const std::string& path) const {
        std::lock_guard<std::mutex> lk(ws_mtx_);
        auto it = ws_sessions_.find(path);
        return (it == ws_sessions_.end()) ? 0u : it->second.size();
    }

    RequestHandler findHttpHandler(const std::string& path) {
        std::lock_guard<std::mutex> lk(http_mtx_);
        auto it = http_routes_.find(path);
        return (it == http_routes_.end()) ? RequestHandler() : it->second;
    }

    std::pair<WebSocketHandler, bool> findWebSocketHandler(const std::string& path) {
        std::lock_guard<std::mutex> lk(ws_h_mtx_);
        auto it = ws_handlers_.find(path);
        return (it == ws_handlers_.end()) ? std::make_pair(WebSocketHandler(), false) : std::make_pair(it->second, true);
    }

    void addWebSocketSession(const std::string& path, const std::shared_ptr<MongooseWebSocketSession>& s) {
        std::lock_guard<std::mutex> lk(ws_mtx_);
        ws_sessions_[path].insert(std::weak_ptr<MongooseWebSocketSession>(s));
    }
    void removeWebSocketSession(const std::string& path, const std::shared_ptr<MongooseWebSocketSession>& s) {
        std::lock_guard<std::mutex> lk(ws_mtx_);
        auto it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) return;
        it->second.erase(std::weak_ptr<MongooseWebSocketSession>(s));
        if (it->second.empty()) ws_sessions_.erase(it);
    }

private:
    mutable std::mutex ws_conn_mtx_;
    std::unordered_map<mg_connection*, std::shared_ptr<MongooseWebSocketSession>> ws_by_conn_;

    // NOTE: 7.18 handler signature is (c, ev, ev_data). Pass ServerImpl* via fn_data!
    static void static_ev_handler(mg_connection* c, int ev, void* ev_data) {
        ServerImpl* self = static_cast<ServerImpl*>(c->fn_data);
        if (!self) return;
        self->ev_handler(c, ev, ev_data);
    }

    void ev_handler(struct mg_connection* c, int ev, void* ev_data) {
        switch (ev) {
        case MG_EV_HTTP_MSG: {
            mg_http_message* hm = static_cast<mg_http_message*>(ev_data);
            handle_http(c, hm);
            break;
        }
                // server_mongoose.cpp — in ev_handler(...)
            case MG_EV_WS_OPEN: {
            // Let the official MG_EV_WS_OPEN be the single source of truth for "open"
            if (auto sp = session_from_conn(c)) sp->on_open();
            break;
            }

        case MG_EV_WS_MSG: {
            mg_ws_message* wm = static_cast<mg_ws_message*>(ev_data);
            auto sp = session_from_conn(c);
            if (sp) sp->on_msg(wm);
            break;
        }
        case MG_EV_WS_CTL: {
            mg_ws_message* wm = static_cast<mg_ws_message*>(ev_data);
            auto sp = session_from_conn(c);
            if (sp) sp->on_ctl(wm);
            break;
        }
        case MG_EV_POLL: {
            auto sp = session_from_conn(c);
            if (sp) sp->on_poll();
            break;
        }
        case MG_EV_CLOSE: {
            auto sp = session_from_conn(c);
            if (sp) {
                sp->on_close();
                removeWebSocketSession(sp->path(), sp);
                clear_session_on_conn(c);
            }
            break;
        }
        default: break;
        }
    }

    void handle_http(struct mg_connection* c, const mg_http_message* hm) {
        const std::string path = mg_str_to_std(hm->uri);
        const bool is_ws_req = mg_is_ws_upgrade(hm);
        CV_LOG_DEBUG(kTag, "HTTP " << mg_str_to_std(hm->method) << " " << path
                        << (is_ws_req ? " (WS upgrade?)" : ""));

        // server_mongoose.cpp — inside handle_http(), WS path
        // server_mongoose.cpp — inside handle_http(), WS path
        if (is_ws_req) {
            auto ws_h = findWebSocketHandler(path);
            if (ws_h.second) {
                // Create and register the session BEFORE the upgrade so MG_EV_WS_OPEN
                // can resolve it immediately.
                auto sess = std::make_shared<MongooseWebSocketSession>(owner_, c, path, ws_h.first, getConfig());
                sess->bind_connection(c);
                set_session_on_conn(c, sess);
                addWebSocketSession(path, sess);

                // Perform the WebSocket upgrade. MG_EV_WS_OPEN will follow and call sp->on_open().
                mg_ws_upgrade(c, const_cast<mg_http_message*>(hm), NULL);  // 7.x non-const

                // Do NOT call on_open() here — calling both here and in MG_EV_WS_OPEN
                // can double-emit KA challenges and race the first ACK/timeout.
                return;
            } else {
                const ServerConfig cfg = getConfig();
                std::string hdrs = "Content-Type: " + cfg.notFound.contentType + "\r\n";
                for (size_t i = 0; i < cfg.defaultNotFoundHeaders.size(); ++i) {
                    hdrs += cfg.defaultNotFoundHeaders[i].first + ": " + cfg.defaultNotFoundHeaders[i].second + "\r\n";
                }
                mg_http_reply(c, 404, hdrs.c_str(), "%.*s",
                              (int)cfg.notFound.body.size(), cfg.notFound.body.data());
                CV_LOG_WARNING(kTag, "No WS handler for path " << path << " -> 404");
                return;
            }
        }



        RequestHandler http_h = findHttpHandler(path);
        if (http_h) {
            try {
                MongooseRequest req(hm);
                MongooseResponse res(c, hm);
                http_h(req, res);
            } catch (const std::exception& e) {
                CV_LOG_ERROR(kTag, "HTTP handler exception: " << e.what());
                mg_http_reply(c, 500, "Content-Type: text/plain; charset=utf-8\r\n", "Internal Server Error in request handler.");
            } catch (...) {
                CV_LOG_ERROR(kTag, "HTTP handler unknown exception");
                mg_http_reply(c, 500, "Content-Type: text/plain; charset=utf-8\r\n", "Internal Server Error in request handler.");
            }
            return;
        } else {
            const ServerConfig cfg = getConfig();
            std::string hdrs = "Content-Type: " + cfg.notFound.contentType + "\r\n";
            for (size_t i = 0; i < cfg.defaultNotFoundHeaders.size(); ++i) {
                hdrs += cfg.defaultNotFoundHeaders[i].first + ": " + cfg.defaultNotFoundHeaders[i].second + "\r\n";
            }
            mg_http_reply(c, 404, hdrs.c_str(), "%.*s",
                          (int)cfg.notFound.body.size(), cfg.notFound.body.data());
            CV_LOG_INFO(kTag, "No HTTP handler for path " << path << " -> 404");
            return;
        }
    }

    // Keep fn_data as ServerImpl*; store session in our own map (no poking mg_connection::data).
    void set_session_on_conn(struct mg_connection* c,
                             const std::shared_ptr<MongooseWebSocketSession>& s) {
        std::lock_guard<std::mutex> lk(ws_conn_mtx_);
        ws_by_conn_[c] = s;
    }

    std::shared_ptr<MongooseWebSocketSession> session_from_conn(struct mg_connection* c) {
        std::lock_guard<std::mutex> lk(ws_conn_mtx_);
        auto it = ws_by_conn_.find(c);
        return (it == ws_by_conn_.end()) ? nullptr : it->second;
    }

    void clear_session_on_conn(struct mg_connection* c) {
        std::lock_guard<std::mutex> lk(ws_conn_mtx_);
        ws_by_conn_.erase(c);
    }

private:
    // Config
    mutable std::mutex         cfg_mtx_;
    ServerConfig               config_;

    // Mongoose core
    struct mg_mgr              mgr_{};
    struct mg_connection*      listener_ = nullptr;
    std::thread                poll_thread_;
    std::atomic<bool>          running_;

    // HTTP routes
    mutable std::mutex                         http_mtx_;
    std::unordered_map<std::string, RequestHandler> http_routes_;

    // WS handlers
    mutable std::mutex                         ws_h_mtx_;
    std::unordered_map<std::string, WebSocketHandler> ws_handlers_;

    // WS sessions (by path)
    using SessionSet = std::unordered_set<std::weak_ptr<MongooseWebSocketSession>, WeakPtrHash<MongooseWebSocketSession>, WeakPtrEqual<MongooseWebSocketSession>>;
    mutable std::mutex                         ws_mtx_;
    std::unordered_map<std::string, SessionSet> ws_sessions_;

    Server* owner_ = nullptr;

    friend class Server;
};

// =====================================================================================
// Server (public API) – Mongoose wiring
// =====================================================================================

Server::Server() : pimpl(new ServerImpl) { pimpl->owner_ = this; }
Server::~Server() {}
bool Server::start(int port, int num_threads) { return pimpl->start(port, num_threads); }
void Server::stop() { pimpl->stop(); }
bool Server::isRunning() const { return pimpl->isRunning(); }

void Server::setConfig(const ServerConfig& cfg) { pimpl->setConfig(cfg); }
const ServerConfig& Server::getConfig() const {
    static ServerConfig snap;
    snap = pimpl->getConfig();
    return snap;
}

void Server::registerEndpoint(const std::string& path, const RequestHandler& handler) { pimpl->registerEndpoint(path, handler); }
void Server::unregisterEndpoint(const std::string& path) { pimpl->unregisterEndpoint(path); }
void Server::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) { pimpl->registerWebSocketEndpoint(path, handler); }
void Server::unregisterWebSocketEndpoint(const std::string& path) { pimpl->unregisterWebSocketEndpoint(path); }
size_t Server::broadcast(const std::string& path, const void* data, size_t size, bool binary) { return pimpl->broadcast(path, data, size, binary); }
void Server::forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) { pimpl->forEachWebSocket(path, fn); }
size_t Server::numWebSocketClients(const std::string& path) const { return pimpl->numWebSocketClients(path); }

Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;

std::unique_ptr<Server> createServer() {
    return std::unique_ptr<Server>(new Server());
}
std::unique_ptr<Server> createServer(const ServerConfig& cfg) {
    auto s = createServer();
    s->setConfig(cfg);
    return s;
}

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_HTTP_MONGOOSE
