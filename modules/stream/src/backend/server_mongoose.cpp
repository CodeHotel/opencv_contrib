#if defined(HAVE_STREAM_HTTP_MONGOOSE)

#include "opencv2/stream/server.hpp"
#include "mongoose.h"

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
#include <cctype>   // tolower
#include <iostream>

// Mongoose 7.18 notes:
// - mg_http_listen(mgr, url, handler, fn_data) -> 4 args
// - mg_event_handler_t: void (*)(mg_connection*, int ev, void* ev_data) -> 3 args
// - mg_str has fields .buf and .len
// - mg_http_write_chunk(c, buf, len) -> 3 args

namespace cv {
namespace stream {

static inline void SLOG(const char* tag, const char* fmt, ...) {
    std::fprintf(stderr, "[SERVER:%s] ", tag);
    va_list ap; va_start(ap, fmt);
    std::vfprintf(stderr, fmt, ap);
    va_end(ap);
    std::fprintf(stderr, "\n");
    std::fflush(stderr);
}

// ==============================
// Helpers
// ==============================

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

static inline const mg_str* my_http_get_header(const mg_http_message* hm, const char* name) {
    for (size_t i = 0; i < MG_MAX_HTTP_HEADERS && hm->headers[i].name.buf; ++i) {
        if (mg_str_ieq(hm->headers[i].name, name)) return &hm->headers[i].value;
    }
    return nullptr;
}

static inline bool is_ws_upgrade(const mg_http_message* hm) {
    auto* up = my_http_get_header(hm, "Upgrade");
    auto* conn = my_http_get_header(hm, "Connection");
    bool up_ws = up && mg_str_ieq(*up, "websocket");
    bool has_upgrade_token = false;
    if (conn) {
        std::string v = mg_str_to_string(*conn);
        for (auto& ch : v) ch = (char) std::tolower((unsigned char)ch);
        has_upgrade_token = v.find("upgrade") != std::string::npos;
    }
    return up_ws && has_upgrade_token;
}

// ==============================
// Request / Response wrappers
// ==============================

class MongooseRequest final : public Request {
public:
    MongooseRequest(mg_connection* c, const mg_http_message* hm)
        : conn_(c), hm_(hm) {}

    std::string getMethod() const override { return mg_str_to_string(hm_->method); }
    std::string getPath() const override { return mg_str_to_string(hm_->uri); }
    bool isWebSocketUpgrade() const override { return is_ws_upgrade(hm_); }

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

    ~MongooseResponse() override {
        if (started_ && chunked_) {
            mg_http_write_chunk(conn_, "", 0); // end of chunks
        }
    }

    void setStatusCode(int code) override {
        if (started_) return;
        statusCode_ = code;
    }

    void setHeader(const std::string& key, const std::string& value) override {
        if (started_) return;
        headers_.emplace_back(key, value);
    }

    bool write(const char* data, size_t size) override {
        if (!conn_) return false;
        if (!started_) {
            bool hasCL = false, hasTE = false, hasCT = false, hasConn = false;
            for (auto& kv : headers_) {
                if (!hasCL  && strcasecmp(kv.first.c_str(), "Content-Length")     == 0) hasCL  = true;
                if (!hasTE  && strcasecmp(kv.first.c_str(), "Transfer-Encoding")  == 0) hasTE  = true;
                if (!hasCT  && strcasecmp(kv.first.c_str(), "Content-Type")       == 0) hasCT  = true;
                if (!hasConn&& strcasecmp(kv.first.c_str(), "Connection")         == 0) hasConn= true;
            }
            chunked_ = !(hasCL || hasTE);

            mg_printf(conn_, "HTTP/1.1 %d %s\r\n", statusCode_, httpStatusText(statusCode_).c_str());
            if (chunked_) mg_printf(conn_, "Transfer-Encoding: chunked\r\n");
            if (!hasCT)   mg_printf(conn_, "Content-Type: application/octet-stream\r\n");
            if (!hasConn) mg_printf(conn_, "Connection: keep-alive\r\n");
            mg_printf(conn_, "Cache-Control: no-cache, no-store, must-revalidate\r\n");
            mg_printf(conn_, "Pragma: no-cache\r\n");
            for (auto& kv : headers_) {
                mg_printf(conn_, "%s: %s\r\n", kv.first.c_str(), kv.second.c_str());
            }
            mg_printf(conn_, "\r\n");
            started_ = true;
        }

        if (chunked_) {
            mg_http_write_chunk(conn_, data, size);
        } else {
            if (size > 0) mg_send(conn_, data, size);
        }
        return true;
    }

    bool acceptWebSocket(const WebSocketHandler&, const std::vector<std::string>&) override {
        return false;
    }

private:
    mg_connection* conn_{nullptr};
    int statusCode_{200};
    std::vector<std::pair<std::string, std::string>> headers_;
    bool started_{false};
    bool chunked_{false};
};

// ==============================
// WebSocket Session
// ==============================

class MongooseWebSocketSession final : public WebSocketSession,
                                       public std::enable_shared_from_this<MongooseWebSocketSession> {
public:
    MongooseWebSocketSession(mg_connection* c, std::string path)
        : conn_(c), path_(std::move(path)), open_(true) {}

    ~MongooseWebSocketSession() override = default;

    bool send(const void* data, size_t size, bool binary) override {
        if (!isOpen()) return false;
        std::lock_guard<std::mutex> lk(send_mx_);
        if (!isOpen()) return false;
        int op = binary ? WEBSOCKET_OP_BINARY : WEBSOCKET_OP_TEXT;
        mg_ws_send(conn_, (const char*)data, size, op);
        return true;
    }

    void close(WsCloseCode /*code*/ = WsCloseCode::Normal, const std::string& /*reason*/ = std::string()) override {
        std::lock_guard<std::mutex> lk(state_mx_);
        open_.store(false, std::memory_order_release);
        mg_close_conn(conn_);
    }

    bool isOpen() const override { return open_.load(std::memory_order_acquire); }

    std::string remoteAddress() const override {
        return std::string(); // keep portable across builds
    }

    void setWriteQueueLimit(size_t) override {}
    size_t queuedBytes() const override { return 0; }
    void setNoDelay(bool) override {}
    void enableCompression(bool) override {}

    mg_connection* raw() const { return conn_; }
    const std::string& path() const { return path_; }

    void _markClosed() {
        std::lock_guard<std::mutex> lk(state_mx_);
        open_.store(false, std::memory_order_release);
    }

private:
    mg_connection* conn_{nullptr};
    std::string path_;
    mutable std::mutex send_mx_;
    mutable std::mutex state_mx_;
    std::atomic<bool> open_;
};

// Per-connection state for HTTP->WS flow and cleanup
struct ConnState {
    std::string ws_path; // set at HTTP stage before mg_ws_upgrade
    std::shared_ptr<MongooseWebSocketSession> session; // set at WS_OPEN
};

// ==============================
// Server::ServerImpl
// ==============================

class Server::ServerImpl {
public:
    ServerImpl() = default;
    ~ServerImpl() { stop(); }

    bool start(int port, int num_threads) {
        (void)num_threads; // single poll thread here
        if (running_) return true;

        char url[64];
        std::snprintf(url, sizeof(url), "http://0.0.0.0:%d", port);

        mg_mgr_init(&mgr_);
        mgr_.userdata = this;  // access via c->mgr->userdata in handler
        listener_ = mg_http_listen(&mgr_, url, &ServerImpl::evHandler, nullptr);
        if (!listener_) {
            mg_mgr_free(&mgr_);
            return false;
        }
        running_ = true;

        SLOG("start", "Listening at %s (mgr=%p, listener=%p)", url, (void*)&mgr_, (void*)listener_);

        // poll thread
        poll_thread_ = std::thread([this]() {
            while (running_) {
                mg_mgr_poll(&mgr_, 50);
            }
            SLOG("poll", "Poll thread exiting");
        });
        return true;
    }

    void stop() {
        if (!running_) return;
        running_ = false;

        SLOG("stop", "Stopping server (mgr=%p)", (void*)&mgr_);
        if (poll_thread_.joinable()) poll_thread_.join();
        mg_mgr_free(&mgr_);
        {
            std::lock_guard<std::mutex> lk(mx_);
            http_routes_.clear();
            ws_routes_.clear();
            ws_sessions_.clear();
        }
        listener_ = nullptr;
        SLOG("stop", "Stopped");
    }

    bool running() const { return running_; }

    // ---- HTTP ----
    void registerEndpoint(const std::string& path, const RequestHandler& h) {
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_[path] = h; // dispatch happens dynamically in handler
        SLOG("reg-http", "Registered HTTP route '%s'", path.c_str());
    }

    void unregisterEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_.erase(path);
        SLOG("unreg-http", "Unregistered HTTP route '%s'", path.c_str());
    }

    // ---- WebSocket ----
    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& h) {
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_[path] = h;
        SLOG("reg-ws", "Registered WS route '%s'", path.c_str());
    }

    void unregisterWebSocketEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_.erase(path);
        ws_sessions_.erase(path);
        SLOG("unreg-ws", "Unregistered WS route '%s'", path.c_str());
    }

    size_t broadcast(const std::string& path, const void* data, size_t n, bool binary) {
        std::lock_guard<std::mutex> lk(mx_);
        size_t ok = 0;
        auto it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) return 0;
        for (auto iter = it->second.begin(); iter != it->second.end();) {
            const auto& sp = *iter;
            if (!sp || !sp->isOpen()) { iter = it->second.erase(iter); continue; }
            if (sp->send(data, n, binary)) ++ok;
            ++iter;
        }
        SLOG("broadcast", "Sent to %zu clients on '%s'", ok, path.c_str());
        return ok;
    }

    void forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) {
        std::vector<std::shared_ptr<MongooseWebSocketSession>> copy;
        {
            std::lock_guard<std::mutex> lk(mx_);
            auto it = ws_sessions_.find(path);
            if (it == ws_sessions_.end()) return;
            copy.assign(it->second.begin(), it->second.end());
        }
        for (auto& s : copy) fn(*s);
    }

    size_t numWebSocketClients(const std::string& path) const {
        std::lock_guard<std::mutex> lk(mx_);
        auto it = ws_sessions_.find(path);
        size_t n = it == ws_sessions_.end() ? 0 : it->second.size();
        SLOG("ws-count", "'%s' has %zu clients", path.c_str(), n);
        return n;
    }

private:
    static void evHandler(mg_connection* c, int ev, void* ev_data) {
        // Always recover ServerImpl from the manager only.
        auto* self = static_cast<ServerImpl*>(c->mgr->userdata);
        if (!self) {
            SLOG("ev", "No self for ev=%d conn=%p (mgr->userdata=null)", ev, (void*) c);
            return;
        }

        switch (ev) {
            case MG_EV_ACCEPT: {
                SLOG("ACCEPT", "conn=%p", (void*) c);
                // NOTE: MG_EV_ACCEPT is delivered to the listening socket
                break;
            }

            case MG_EV_OPEN: {
                // First event for a new connection. We do not touch fn_data here.
                SLOG("OPEN", "new conn=%p", (void*) c);
                break;
            }

            case MG_EV_READ: {
                long n = ev_data ? *(long*)ev_data : 0;     // ✅ correct type
                SLOG("READ", "conn=%p got %ld bytes", (void*) c, n);
                break;
            }

            case MG_EV_HTTP_MSG: {
                auto* hm = static_cast<mg_http_message*>(ev_data);
                std::string path = mg_str_to_string(hm->uri);

                const mg_str* up = my_http_get_header(hm, "Upgrade");
                const mg_str* con = my_http_get_header(hm, "Connection");
                SLOG("HTTP", "HTTP_MSG path='%s' upgrade='%s' connection='%s'",
                     path.c_str(),
                     up ? mg_str_to_string(*up).c_str() : "(none)",
                     con ? mg_str_to_string(*con).c_str() : "(none)");

                // WS upgrade?
                if (self->isWebSocketRoute_(path) && is_ws_upgrade(hm)) {
                    SLOG("HTTP", "Upgrading to WS for path '%s'", path.c_str());
                    // Stash per-connection WS state in fn_data (NOT self!)
                    auto* st = static_cast<ConnState*>(c->fn_data);
                    if (!st) { st = new ConnState(); c->fn_data = st; }
                    st->ws_path = path;
                    mg_ws_upgrade(c, hm, NULL);
                    return;
                } else if (self->isWebSocketRoute_(path)) {
                    SLOG("HTTP", "WS route matched, but Upgrade headers missing -> 426");
                    mg_http_reply(c, 426, "Content-Type: text/plain\r\nConnection: close\r\n", "Upgrade Required");
                    return;
                }

                // HTTP route
                RequestHandler handler;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    auto it = self->http_routes_.find(path);
                    if (it != self->http_routes_.end()) handler = it->second;
                }
                if (handler) {
                    MongooseRequest req(c, hm);
                    MongooseResponse res(c);
                    try {
                        handler(req, res);
                    } catch (...) {
                        mg_http_reply(c, 500, "Content-Type: text/plain\r\nConnection: close\r\n",
                                      "Internal Server Error");
                    }
                } else {
                    SLOG("HTTP", "No route for '%s' -> 404", path.c_str());
                    mg_http_reply(c, 404, "Content-Type: text/plain\r\nConnection: close\r\n", "Not Found");
                }
                break;
            }

            case MG_EV_WS_OPEN: {
                auto* st = static_cast<ConnState*>(c->fn_data);
                std::string path = st ? st->ws_path : std::string();
                SLOG("WS", "WS_OPEN path='%s' conn=%p", path.c_str(), (void*) c);

                WebSocketHandler cb;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    auto it = self->ws_routes_.find(path);
                    if (it != self->ws_routes_.end()) cb = it->second;
                }
                if (!cb.onOpen && !cb.onMessage && !cb.onClose && !cb.onError && !cb.onPing && !cb.onPong) {
                    SLOG("WS", "No handler installed for '%s' -> closing", path.c_str());
                    mg_close_conn(c);
                    break;
                }

                auto session = std::make_shared<MongooseWebSocketSession>(c, path);
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    self->ws_sessions_[path].insert(session);
                }
                if (st) st->session = session;

                if (cb.onOpen) cb.onOpen(*session);
                break;
            }

            case MG_EV_WS_MSG: {
                auto* wm = static_cast<mg_ws_message*>(ev_data);
                auto* st = static_cast<ConnState*>(c->fn_data);
                std::shared_ptr<MongooseWebSocketSession> session = st ? st->session : nullptr;
                if (!session) { SLOG("WS", "WS_MSG but no session"); break; }

                WebSocketHandler cb;
                {
                    std::lock_guard<std::mutex> lk(self->mx_);
                    auto it = self->ws_routes_.find(session->path());
                    if (it != self->ws_routes_.end()) cb = it->second;
                }
                if (!cb.onMessage) break;

                bool binary = (wm->flags & WEBSOCKET_OP_BINARY) != 0;
                SLOG("WS", "WS_MSG len=%zu bin=%d", wm->data.len, (int) binary);
                cb.onMessage(*session, (const uint8_t*) wm->data.buf, wm->data.len, binary);
                break;
            }

            case MG_EV_CLOSE: {
                auto* st = static_cast<ConnState*>(c->fn_data);
                SLOG("WS", "EV_CLOSE conn=%p st=%p", (void*) c, (void*) st);
                if (st) {
                    if (st->session) {
                        st->session->_markClosed();
                        WebSocketHandler cb;
                        {
                            std::lock_guard<std::mutex> lk(self->mx_);
                            auto it = self->ws_routes_.find(st->session->path());
                            if (it != self->ws_routes_.end()) cb = it->second;

                            auto sit = self->ws_sessions_.find(st->session->path());
                            if (sit != self->ws_sessions_.end()) {
                                for (auto iter = sit->second.begin(); iter != sit->second.end();) {
                                    if (iter->get() == st->session.get()) iter = sit->second.erase(iter);
                                    else ++iter;
                                }
                            }
                        }
                        if (cb.onClose) cb.onClose(*st->session, (int) WsCloseCode::Normal, std::string());
                        st->session.reset();
                    }
                    delete st;
                    c->fn_data = nullptr;
                }
                break;
            }

            default:
                break;
        }
    }


    bool isWebSocketRoute_(const std::string& path) const {
        std::lock_guard<std::mutex> lk(mx_);
        bool exists = ws_routes_.find(path) != ws_routes_.end();
        if (!exists) {
            // Extra debug: list known WS routes
            std::string keys;
            for (auto& kv : ws_routes_) {
                if (!keys.empty()) keys += ", ";
                keys += kv.first;
            }
            SLOG("route", "isWebSocketRoute_('%s') -> false; known WS routes: [%s]",
                 path.c_str(), keys.c_str());
        }
        return exists;
    }

    // Mongoose core
    mg_mgr mgr_{};
    mg_connection* listener_{nullptr};
    std::thread poll_thread_;
    std::atomic<bool> running_{false};

    // Route storage
    mutable std::mutex mx_;
    std::unordered_map<std::string, RequestHandler> http_routes_;
    std::unordered_map<std::string, WebSocketHandler> ws_routes_;
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<MongooseWebSocketSession>>> ws_sessions_;
};

// ==============================
// Server API
// ==============================

Server::Server() : pimpl(new ServerImpl) {}
Server::~Server() = default;

bool Server::start(int port, int num_threads) { return pimpl->start(port, num_threads); }
void Server::stop() { pimpl->stop(); }
bool Server::isRunning() const { return pimpl->running(); }

void Server::registerEndpoint(const std::string& path, const RequestHandler& handler) {
    pimpl->registerEndpoint(path, handler);
}
void Server::unregisterEndpoint(const std::string& path) {
    pimpl->unregisterEndpoint(path);
}

void Server::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) {
    pimpl->registerWebSocketEndpoint(path, handler);
}
void Server::unregisterWebSocketEndpoint(const std::string& path) {
    pimpl->unregisterWebSocketEndpoint(path);
}

size_t Server::broadcast(const std::string& path, const void* data, size_t size, bool binary) {
    return pimpl->broadcast(path, data, size, binary);
}
void Server::forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) {
    pimpl->forEachWebSocket(path, fn);
}
size_t Server::numWebSocketClients(const std::string& path) const {
    return pimpl->numWebSocketClients(path);
}

Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;

std::unique_ptr<Server> createServer() {
    return std::unique_ptr<Server>(new Server());
}

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_BACKEND_MONGOOSE
