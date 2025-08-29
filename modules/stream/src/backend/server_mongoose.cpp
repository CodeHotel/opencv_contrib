#if defined(HAVE_STREAM_HTTP_MONGOOSE)

#include "opencv2/stream/server.hpp"
#include "mongoose.h"

#include <opencv2/core/utils/logger.hpp>

#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <mutex>
#include <memory>
#include <string>
#include <vector>
#include <deque>
#include <atomic>
#include <cassert>
#include <cstring>
#include <cstdio>
#include <thread>
#include <cctype>
#include <random>
#include <sstream>
#include <chrono>

namespace cv {
namespace stream {

namespace {
using cv::utils::logging::LogLevel;
static cv::utils::logging::LogTag kStreamLogTag(
    "cv.stream.server",
    LogLevel::LOG_LEVEL_VERBOSE
);
cv::utils::logging::LogTag* kTag = &kStreamLogTag;
}

// ============================================================================
// Utilities
// ============================================================================

static inline std::string httpStatusText(int code) {
    switch (code) {
        case 200: return "OK";
        case 201: return "Created";
        case 202: return "Accepted";
        case 204: return "No Content";
        case 301: return "Moved Permanently";
        case 302: return "Found";
        case 304: return "Not Modified";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 409: return "Conflict";
        case 413: return "Payload Too Large";
        case 415: return "Unsupported Media Type";
        case 426: return "Upgrade Required";
        case 429: return "Too Many Requests";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 503: return "Service Unavailable";
        default:  return "OK";
    }
}

static inline bool mg_str_ieq(const mg_str& s, const char* cstr) {
    if (!cstr) return false;
    size_t n = s.len;
    if (!s.buf) return *cstr == '\0';
    for (size_t i = 0; i < n && cstr[i]; ++i) {
        unsigned char ca = (unsigned char) s.buf[i];
        unsigned char cb = (unsigned char) cstr[i];
        char a = (char)((ca >= 'A' && ca <= 'Z') ? (ca - 'A' + 'a') : ca);
        char b = (char)((cb >= 'A' && cb <= 'Z') ? (cb - 'A' + 'a') : cb);
        if (a != b) return false;
        if (i + 1 == n && cstr[i + 1] != '\0') return false;
    }
    return n == strlen(cstr);
}

static inline std::string mg_str_to_string(const mg_str& s) {
    return std::string(s.buf ? s.buf : "", s.len);
}

static inline const mg_str* http_get_header(const mg_http_message* hm, const char* name) {
    for (size_t i = 0; i < MG_MAX_HTTP_HEADERS && hm->headers[i].name.buf; ++i) {
        if (mg_str_ieq(hm->headers[i].name, name)) return &hm->headers[i].value;
    }
    return NULL;
}

static inline bool is_ws_upgrade(const mg_http_message* hm) {
    const mg_str* up = http_get_header(hm, "Upgrade");
    const mg_str* conn = http_get_header(hm, "Connection");
    bool up_ws = up && mg_str_ieq(*up, "websocket");
    bool has_upgrade_token = false;
    if (conn) {
        std::string v = mg_str_to_string(*conn);
        for (size_t i = 0; i < v.size(); ++i)
            v[i] = (char) std::tolower((unsigned char) v[i]);
        has_upgrade_token = v.find("upgrade") != std::string::npos;
    }
    return up_ws && has_upgrade_token;
}

template <typename T> static inline std::string to_string_generic(const T& v) { std::ostringstream o; o << v; return o.str(); }

// ============================================================================
// Request / Response wrappers
// ============================================================================

class MongooseRequest final : public Request {
public:
    MongooseRequest(mg_connection* c, const mg_http_message* hm)
        : conn_(c), hm_(hm) {}

    std::string getMethod() const CV_OVERRIDE { return mg_str_to_string(hm_->method); }
    std::string getPath()   const CV_OVERRIDE { return mg_str_to_string(hm_->uri); }
    bool isWebSocketUpgrade() const CV_OVERRIDE { return is_ws_upgrade(hm_); }

    mg_connection* raw() const { return conn_; }
    const mg_http_message* http() const { return hm_; }

private:
    mg_connection* conn_;
    const mg_http_message* hm_;
};

class MongooseResponse final : public Response {
public:
    explicit MongooseResponse(mg_connection* c)
        : conn_(c) {}

    ~MongooseResponse() CV_OVERRIDE {
        // If chunked started, ensure terminating zero-length chunk
        if (started_ && chunked_) mg_http_write_chunk(conn_, "", 0);
    }

    void setStatusCode(int code) CV_OVERRIDE {
        if (!started_) statusCode_ = code;
    }
    void setHeader(const std::string& key, const std::string& value) CV_OVERRIDE {
        if (!started_) headers_.push_back(std::make_pair(key, value));
    }

    bool write(const char* data, size_t size) CV_OVERRIDE {
        if (!conn_) return false;
        if (!started_) {
            bool hasCL = false, hasTE = false, hasCT = false, hasConn = false;
            for (size_t i = 0; i < headers_.size(); ++i) {
                const std::string& k = headers_[i].first;
                if (!hasCL  && strcasecmp(k.c_str(), "Content-Length")    == 0) hasCL  = true;
                if (!hasTE  && strcasecmp(k.c_str(), "Transfer-Encoding") == 0) hasTE  = true;
                if (!hasCT  && strcasecmp(k.c_str(), "Content-Type")      == 0) hasCT  = true;
                if (!hasConn&& strcasecmp(k.c_str(), "Connection")        == 0) hasConn= true;
            }
            chunked_ = !(hasCL || hasTE);

            mg_printf(conn_, "HTTP/1.1 %d %s\r\n", statusCode_, httpStatusText(statusCode_).c_str());
            if (chunked_) mg_printf(conn_, "Transfer-Encoding: chunked\r\n");
            if (!hasCT)   mg_printf(conn_, "Content-Type: application/octet-stream\r\n");
            if (!hasConn) mg_printf(conn_, "Connection: keep-alive\r\n");
            // No-cache headers for safety; handlers may override
            mg_printf(conn_, "Cache-Control: no-cache, no-store, must-revalidate\r\n");
            mg_printf(conn_, "Pragma: no-cache\r\n");
            for (size_t i = 0; i < headers_.size(); ++i) {
                mg_printf(conn_, "%s: %s\r\n", headers_[i].first.c_str(), headers_[i].second.c_str());
            }
            mg_printf(conn_, "\r\n");
            started_ = true;
        }
        if (chunked_) mg_http_write_chunk(conn_, data, size);
        else if (size > 0) mg_send(conn_, data, size);
        return true;
    }

    bool acceptWebSocket(const WebSocketHandler&, const std::vector<std::string>&) CV_OVERRIDE {
        // HTTP->WS upgrade is driven fully by the event handler; no-op here.
        return false;
    }

private:
    mg_connection* conn_;
    int statusCode_ = 200;
    std::vector<std::pair<std::string, std::string> > headers_;
    bool started_ = false;
    bool chunked_ = false;
};

// ============================================================================
// WebSocket session
// ============================================================================

class MongooseWebSocketSession final : public WebSocketSession,
                                       public std::enable_shared_from_this<MongooseWebSocketSession> {
public:
    MongooseWebSocketSession(class Server::ServerImpl* impl,
                             mg_connection* c,
                             std::string path)
        : impl_(impl), conn_(c), path_(std::move(path)), open_(true),
          rng_(static_cast<unsigned>(std::random_device()())) {}

    ~MongooseWebSocketSession() CV_OVERRIDE {}

    bool send(const void* data, size_t size, bool binary) CV_OVERRIDE {
        if (!isOpen()) return false;
        std::lock_guard<std::mutex> lk(send_mx_);
        if (!isOpen()) return false;
        int op = binary ? WEBSOCKET_OP_BINARY : WEBSOCKET_OP_TEXT;
        mg_ws_send(conn_, (const char*)data, size, op);
        return true;
    }

    void close(WsCloseCode /*code*/ = WsCloseCode::Normal,
               const std::string& /*reason*/ = std::string()) CV_OVERRIDE {
        std::lock_guard<std::mutex> lk(state_mx_);
        if (!open_.exchange(false)) return;
        mg_close_conn(conn_);
    }

    bool isOpen() const CV_OVERRIDE { return open_.load(std::memory_order_acquire); }
    std::string remoteAddress() const CV_OVERRIDE { return std::string(); } // portable no-op

    void setWriteQueueLimit(size_t) CV_OVERRIDE {}
    size_t queuedBytes() const CV_OVERRIDE { return 0; }
    void setNoDelay(bool) CV_OVERRIDE {}
    void enableCompression(bool) CV_OVERRIDE {}

    const std::string& path() const { return path_; }
    mg_connection* raw() const { return conn_; }

    // Called by event loop when MG_EV_CLOSE fires
    void markClosed() {
        std::lock_guard<std::mutex> lk(state_mx_);
        open_.store(false, std::memory_order_release);
    }

    // Keepalive policy
    void startKeepalive(const WsKeepalivePolicy& policy) {
        std::lock_guard<std::mutex> lk(ka_mx_);
        ka_enabled_ = policy.enabled;
        ack_prefix_ = policy.ackPrefix;
        challenge_size_ = policy.challengeSize;
        if (policy.challengeInterval.count() > 0) {
            next_challenge_due_ = std::chrono::steady_clock::now() + policy.challengeInterval;
        } else {
            next_challenge_due_ = timepoint::max();
        }
        ack_deadline_ = timepoint::min();
        CV_LOG_VERBOSE(kTag, 2, "WS KA init path=" << path_ << " enabled=" << (ka_enabled_ ? 1 : 0));
    }

    // Poll thread tick
    void tick(const WsKeepalivePolicy& policy, const std::chrono::steady_clock::time_point& now) {
        if (!isOpen()) return;
        if (!ka_enabled_) return;

        // Send periodic challenge
        if (policy.challengeInterval.count() > 0 && now >= next_challenge_due_) {
            const std::string token = make_token(challenge_size_);
            {
                std::lock_guard<std::mutex> lk(ka_mx_);
                expected_ack_ = ack_prefix_ + token;
                ack_deadline_ = now + policy.clientTimeout;
            }
            const bool ok = send(token.data(), token.size(), /*binary*/false);
            CV_LOG_DEBUG(kTag, "Keepalive challenge -> path=" << path_
                           << " token=\"" << token << "\" (" << (ok ? "sent" : "drop") << ")");
            // Schedule next
            next_challenge_due_ = now + policy.challengeInterval;
        }

        // Enforce ACK timeout
        bool timeout = false;
        {
            std::lock_guard<std::mutex> lk(ka_mx_);
            timeout = (!expected_ack_.empty() && now >= ack_deadline_);
        }
        if (timeout) {
            CV_LOG_WARNING(kTag, "Keepalive timeout path=" << path_ << "; closing");
            close(WsCloseCode::PolicyViolation, "keepalive timeout");
        }
    }

    // Called by MG_EV_WS_MSG with TEXT frames
    void onTextMessage(const char* data, size_t len) {
        if (!ka_enabled_) return;
        std::string payload(data, len);
        bool matched = false;
        {
            std::lock_guard<std::mutex> lk(ka_mx_);
            if (!expected_ack_.empty() && payload == expected_ack_) {
                expected_ack_.clear();
                ack_deadline_ = timepoint::min();
                matched = true;
            }
        }
        if (matched) CV_LOG_DEBUG(kTag, "Keepalive ACK ok path=" << path_);
    }

private:
    using timepoint = std::chrono::steady_clock::time_point;

    std::string make_token(std::size_t n) {
        static const char alphabet[] =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        std::uniform_int_distribution<int> dist(0, (int)sizeof(alphabet) - 2);
        std::string s; s.reserve(n);
        for (std::size_t i = 0; i < n; ++i) s.push_back(alphabet[(std::size_t) dist(rng_)]);
        return s;
    }

    // State
    class Server::ServerImpl* impl_;
    mg_connection*                       conn_;
    std::string                          path_;
    mutable std::mutex                   send_mx_;
    mutable std::mutex                   state_mx_;
    std::atomic<bool>                    open_{true};

    // Keepalive
    mutable std::mutex                   ka_mx_;
    bool                                 ka_enabled_{false};
    std::string                          ack_prefix_;
    std::size_t                          challenge_size_{12};
    std::string                          expected_ack_;
    timepoint                            next_challenge_due_{timepoint::max()};
    timepoint                            ack_deadline_{timepoint::min()};
    std::mt19937                         rng_;
};

// Per-connection state kept in c->fn_data
struct ConnState {
    std::string ws_path;
    std::shared_ptr<MongooseWebSocketSession> session;
};

// ============================================================================
// Server::ServerImpl
// ============================================================================

class Server::ServerImpl {
public:
    ServerImpl() = default;
    ~ServerImpl() { stop(); }

    bool start(int port, int /*num_threads*/) {
        if (running_.load()) return true;

        char url[64];
        std::snprintf(url, sizeof(url), "http://0.0.0.0:%d", port);

        mg_mgr_init(&mgr_);
        mgr_.userdata = this;
        listener_ = mg_http_listen(&mgr_, url, &ServerImpl::evHandler, NULL);
        if (!listener_) {
            CV_LOG_ERROR(kTag, "Mongoose listen failed on " << url);
            mg_mgr_free(&mgr_);
            return false;
        }
        running_.store(true);
        CV_LOG_INFO(kTag, "Server listening at " << url << " (mgr=" << (void*)&mgr_ << ", listener=" << (void*)listener_ << ")");

        // Single poll thread; Mongoose APIs are not thread-safe across multiple polls
        poll_thread_ = std::thread([this]() {
            CV_LOG_INFO(kTag, "Poll thread started");
            while (running_.load()) {
                mg_mgr_poll(&mgr_, 50);     // 50ms
                // Keepalive tick for WS sessions
                tick_keepalive_();
            }
            CV_LOG_INFO(kTag, "Poll thread exiting");
        });
        return true;
    }

    void stop() {
        if (!running_.exchange(false)) return;

        CV_LOG_INFO(kTag, "Stopping server (mgr=" << (void*)&mgr_ << ")");
        if (poll_thread_.joinable()) poll_thread_.join();

        {
            std::lock_guard<std::mutex> lk(mx_);
            for (std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.begin();
                 it != ws_sessions_.end(); ++it) {
                for (SessionSet::iterator sit = it->second.begin(); sit != it->second.end(); ++sit) {
                    if (*sit) {
                        mg_connection* c = (*sit)->raw();
                        if (c && c->fn_data) {
                            ConnState* st = static_cast<ConnState*>(c->fn_data);
                            delete st;
                            c->fn_data = NULL;
                        }
                    }
                }
                 }

            ws_sessions_.clear();
            http_routes_.clear();
            ws_routes_.clear();
        }

        mg_mgr_free(&mgr_);
        listener_ = NULL;

        CV_LOG_INFO(kTag, "Server stopped");
    }


    bool running() const { return running_.load(); }

    // ---- Config ----
    void setConfig(const ServerConfig& cfg) {
        std::lock_guard<std::mutex> lk(cfg_mx_);
        config_ = cfg;
    }
    ServerConfig getConfig() const {
        std::lock_guard<std::mutex> lk(cfg_mx_);
        return config_;
    }

    // ---- HTTP ----
    void registerEndpoint(const std::string& path, const RequestHandler& h) {
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_[path] = h;
        CV_LOG_INFO(kTag, "Registered HTTP route '" << path << "'");
    }
    void unregisterEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_.erase(path);
        CV_LOG_INFO(kTag, "Unregistered HTTP route '" << path << "'");
    }

    // ---- WebSocket ----
    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& h) {
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_[path] = h;
        CV_LOG_INFO(kTag, "Registered WS route '" << path << "'");
    }
    void unregisterWebSocketEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_.erase(path);
        ws_sessions_.erase(path);
        CV_LOG_INFO(kTag, "Unregistered WS route '" << path << "'");
    }

    size_t broadcast(const std::string& path, const void* data, size_t n, bool binary) {
        std::vector<std::shared_ptr<MongooseWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(mx_);
            typename std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.find(path);
            if (it == ws_sessions_.end()) return 0;
            copy.reserve(it->second.size());
            for (SessionSet::iterator s = it->second.begin(); s != it->second.end(); ++s) {
                if (*s && (*s)->isOpen()) copy.push_back(*s);
            }
        }
        size_t ok = 0;
        for (size_t i = 0; i < copy.size(); ++i) ok += copy[i]->send(data, n, binary) ? 1 : 0;
        CV_LOG_VERBOSE(kTag, 2, "Broadcast to " << ok << " clients on '" << path << "'");
        return ok;
    }

    void forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) {
        std::vector<std::shared_ptr<MongooseWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(mx_);
            typename std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.find(path);
            if (it == ws_sessions_.end()) return;
            copy.reserve(it->second.size());
            for (SessionSet::iterator s = it->second.begin(); s != it->second.end(); ++s) {
                if (*s && (*s)->isOpen()) copy.push_back(*s);
            }
        }
        for (size_t i = 0; i < copy.size(); ++i) fn(*copy[i]);
    }

    size_t numWebSocketClients(const std::string& path) const {
        std::lock_guard<std::mutex> lk(mx_);
        typename std::unordered_map<std::string, SessionSet>::const_iterator it = ws_sessions_.find(path);
        const size_t n = (it == ws_sessions_.end()) ? 0u : it->second.size();
        CV_LOG_VERBOSE(kTag, 3, "WS count for '" << path << "': " << n);
        return n;
    }

private:
    // ---- Event handler ----
    static void evHandler(mg_connection* c, int ev, void* ev_data) {
        Server::ServerImpl* self = static_cast<Server::ServerImpl*>(c->mgr->userdata);
        if (!self) { CV_LOG_WARNING(kTag, "No ServerImpl in mgr->userdata"); return; }

        switch (ev) {
            case MG_EV_OPEN: {
                CV_LOG_VERBOSE(kTag, 3, "EV_OPEN conn=" << (void*)c);
                break;
            }

            case MG_EV_ACCEPT: {
                CV_LOG_VERBOSE(kTag, 3, "EV_ACCEPT conn=" << (void*)c);
                break;
            }

            case MG_EV_HTTP_MSG: {
                mg_http_message* hm = static_cast<mg_http_message*>(ev_data);
                const std::string path = mg_str_to_string(hm->uri);
                const bool wants_upgrade = is_ws_upgrade(hm);
                CV_LOG_VERBOSE(kTag, 2, "HTTP path='" << path << "' upgrade=" << (wants_upgrade ? "Y" : "N"));

                // WebSocket upgrade flow
                if (wants_upgrade) {
                    if (!self->isWebSocketRoute_(path)) {
                        // Unknown WS path: reply HTTP 404 (do not upgrade)
                        self->reply_404_(c);
                        // Explicitly close after response for simplicity
                        mg_printf(c, "Connection: close\r\n\r\n");
                        return;
                    }
                    // Prepare per-connection state for WS
                    ConnState* st = static_cast<ConnState*>(c->fn_data);
                    if (!st) { st = new ConnState(); c->fn_data = st; }
                    st->ws_path = path;

                    // Disable any framework default idle timeouts: not applicable in Mongoose,
                    // but we avoid extra timers. Just upgrade immediately.
                    mg_ws_upgrade(c, hm, NULL);
                    return;
                }

                // If route is WS but no upgrade headers -> 426 Upgrade Required
                if (self->isWebSocketRoute_(path)) {
                    const char* hdrs = "Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\n";
                    mg_http_reply(c, 426, hdrs, "Upgrade Required");
                    return;
                }

                // Regular HTTP endpoint
                RequestHandler handler;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    std::unordered_map<std::string, RequestHandler>::iterator it = self->http_routes_.find(path);
                    if (it != self->http_routes_.end()) handler = it->second;
                }

                if (handler) {
                    MongooseRequest req(c, hm);
                    MongooseResponse res(c);
                    try {
                        handler(req, res);
                    } catch (const std::exception& e) {
                        CV_LOG_ERROR(kTag, "HTTP handler exception: " << e.what());
                        mg_http_reply(c, 500, "Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\n",
                                      "Internal Server Error");
                    } catch (...) {
                        CV_LOG_ERROR(kTag, "HTTP handler unknown exception");
                        mg_http_reply(c, 500, "Content-Type: text/plain; charset=utf-8\r\nConnection: close\r\n",
                                      "Internal Server Error");
                    }
                } else {
                    self->reply_404_(c);
                }
                break;
            }

            case MG_EV_WS_OPEN: {
                ConnState* st = static_cast<ConnState*>(c->fn_data);
                const std::string path = st ? st->ws_path : std::string();
                CV_LOG_INFO(kTag, "WS OPEN path='" << path << "' conn=" << (void*)c);

                WebSocketHandler cb;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    std::unordered_map<std::string, WebSocketHandler>::iterator it = self->ws_routes_.find(path);
                    if (it != self->ws_routes_.end()) cb = it->second;
                }

                // Create session and store
                std::shared_ptr<MongooseWebSocketSession> session(new MongooseWebSocketSession(self, c, path));
                if (st) st->session = session;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    self->ws_sessions_[path].insert(session);
                }

                // Start keepalive with current policy
                ServerConfig cfg = self->getConfig();
                if (cfg.ws.enabled) session->startKeepalive(cfg.ws);

                if (cb.onOpen) {
                    try { cb.onOpen(*session); }
                    catch (const std::exception& e) { CV_LOG_ERROR(kTag, "WS onOpen exception: " << e.what()); }
                    catch (...) { CV_LOG_ERROR(kTag, "WS onOpen unknown exception"); }
                }
                break;
            }

            case MG_EV_WS_MSG: {
                mg_ws_message* wm = static_cast<mg_ws_message*>(ev_data);
                ConnState* st = static_cast<ConnState*>(c->fn_data);
                std::shared_ptr<MongooseWebSocketSession> session = st ? st->session : std::shared_ptr<MongooseWebSocketSession>();
                if (!session) { CV_LOG_WARNING(kTag, "WS_MSG but no session"); break; }

                WebSocketHandler cb;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    std::unordered_map<std::string, WebSocketHandler>::iterator it = self->ws_routes_.find(session->path());
                    if (it != self->ws_routes_.end()) cb = it->second;
                }

                const bool binary = (wm->flags & WEBSOCKET_OP_BINARY) != 0;

                // ---- NEW: print received payload preview ----
                if (binary) {
                    CV_LOG_VERBOSE(kTag, 3, "WS MSG bin len=" << wm->data.len);
                } else {
                    std::string preview(wm->data.buf, wm->data.len);
                    for (size_t i = 0; i < preview.size(); ++i) {
                        unsigned char ch = (unsigned char) preview[i];
                        if (ch < 32 || ch == 127) preview[i] = '.';
                    }
                    if (preview.size() > 1024) { preview.resize(1024); preview += "…"; }
                    CV_LOG_VERBOSE(kTag, 3, "WS MSG text len=" << wm->data.len << " \"" << preview << "\"");
                }
                // ---------------------------------------------

                if (!binary) {
                    session->onTextMessage(wm->data.buf, wm->data.len);
                }

                if (cb.onMessage) {
                    try { cb.onMessage(*session, (const uint8_t*)wm->data.buf, wm->data.len, binary); }
                    catch (const std::exception& e) { CV_LOG_ERROR(kTag, "WS onMessage exception: " << e.what()); }
                    catch (...) { CV_LOG_ERROR(kTag, "WS onMessage unknown exception"); }
                }
                break;
            }

            case MG_EV_CLOSE: {
                ConnState* st = static_cast<ConnState*>(c->fn_data);
                if (st && st->session) {
                    std::shared_ptr<MongooseWebSocketSession> session = st->session;
                    session->markClosed();

                    WebSocketHandler cb;
                    {
                        std::lock_guard<std::mutex> lk(self->mx_);
                        std::unordered_map<std::string, WebSocketHandler>::iterator it = self->ws_routes_.find(session->path());
                        if (it != self->ws_routes_.end()) cb = it->second;

                        typename std::unordered_map<std::string, SessionSet>::iterator sit = self->ws_sessions_.find(session->path());
                        if (sit != self->ws_sessions_.end()) {
                            for (SessionSet::iterator it2 = sit->second.begin(); it2 != sit->second.end();) {
                                if (it2->get() == session.get()) it2 = sit->second.erase(it2);
                                else ++it2;
                            }
                            if (sit->second.empty()) self->ws_sessions_.erase(sit);
                        }
                    }

                    if (cb.onClose) {
                        try { cb.onClose(*session, (int) WsCloseCode::Normal, std::string()); }
                        catch (const std::exception& e) { CV_LOG_ERROR(kTag, "WS onClose exception: " << e.what()); }
                        catch (...) { CV_LOG_ERROR(kTag, "WS onClose unknown exception"); }
                    }
                    st->session.reset();
                }
                if (st) { delete st; c->fn_data = NULL; }
                CV_LOG_VERBOSE(kTag, 2, "EV_CLOSE conn=" << (void*)c);
                break;
            }

            default:
                break;
        }
    }

    // ---- Helpers ----
    bool isWebSocketRoute_(const std::string& path) const {
        std::lock_guard<std::mutex> lk(mx_);
        const bool exists = ws_routes_.find(path) != ws_routes_.end();
        if (!exists) {
            // Detailed route inventory on verbose
            std::string keys;
            for (std::unordered_map<std::string, WebSocketHandler>::const_iterator it = ws_routes_.begin();
                 it != ws_routes_.end(); ++it) {
                if (!keys.empty()) keys += ", ";
                keys += it->first;
            }
            CV_LOG_VERBOSE(kTag, 4, "WS route check for '" << path << "': false; known=[" << keys << "]");
        }
        return exists;
    }

    void reply_404_(mg_connection* c) {
        ServerConfig cfg = getConfig();
        std::string hdr;
        hdr.reserve(128 + cfg.defaultNotFoundHeaders.size() * 32 + cfg.notFound.contentType.size());
        hdr += "Content-Type: "; hdr += cfg.notFound.contentType; hdr += "\r\n";
        for (size_t i = 0; i < cfg.defaultNotFoundHeaders.size(); ++i) {
            hdr += cfg.defaultNotFoundHeaders[i].first; hdr += ": ";
            hdr += cfg.defaultNotFoundHeaders[i].second; hdr += "\r\n";
        }
        // Use "%.*s" to avoid interpreting '%' in body
        mg_http_reply(c, 404, hdr.c_str(), "%.*s",
                      (int)cfg.notFound.body.size(), cfg.notFound.body.data());
        CV_LOG_WARNING(kTag, "HTTP 404 for unknown path (uniform NotFoundPage used)");
    }

    void tick_keepalive_() {
        // Snapshot policy once per tick
        ServerConfig cfg = getConfig();
        if (!cfg.ws.enabled && !cfg.ws.disableFrameworkIdleTimeout) return;

        std::vector<std::shared_ptr<MongooseWebSocketSession> > copy;
        {
            std::lock_guard<std::mutex> lk(mx_);
            for (std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.begin();
                 it != ws_sessions_.end(); ++it) {
                for (SessionSet::iterator s = it->second.begin(); s != it->second.end(); ++s) {
                    if (*s && (*s)->isOpen()) copy.push_back(*s);
                }
            }
        }

        const std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
        for (size_t i = 0; i < copy.size(); ++i) {
            copy[i]->tick(cfg.ws, now);
        }
    }

    // Types / storage
    typedef std::unordered_set<std::shared_ptr<MongooseWebSocketSession> > SessionSet;

    // Mongoose core
    mg_mgr           mgr_;
    mg_connection*   listener_ = NULL;
    std::thread      poll_thread_;
    std::atomic<bool> running_{false};

    // Config
    mutable std::mutex cfg_mx_;
    ServerConfig       config_;

    // Routes & sessions
    mutable std::mutex mx_;
    std::unordered_map<std::string, RequestHandler>  http_routes_;
    std::unordered_map<std::string, WebSocketHandler> ws_routes_;
    std::unordered_map<std::string, SessionSet>       ws_sessions_;
};

// ============================================================================
// Server (public API)
// ============================================================================

Server::Server() : pimpl(new ServerImpl) {}
Server::~Server() {}

bool Server::start(int port, int num_threads) { return pimpl->start(port, num_threads); }
void Server::stop() { pimpl->stop(); }
bool Server::isRunning() const { return pimpl->running(); }

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
    std::unique_ptr<Server> s = createServer();
    s->setConfig(cfg);
    return s;
}

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_HTTP_MONGOOSE
