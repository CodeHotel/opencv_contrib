#if defined(HAVE_STREAM_BACKEND_CIVETWEB)

#include <opencv2/stream/server.hpp>
#include <civetweb.h>

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

#if defined(_WIN32)
  #include <string.h>
  #define strcasecmp _stricmp
#else
  #include <strings.h>  // strcasecmp on POSIX
#endif

// CivetWeb RFC6455 opcodes (fallbacks if headers don't define them)
#ifndef MG_WEBSOCKET_OPCODE_TEXT
#define MG_WEBSOCKET_OPCODE_TEXT 0x1
#endif
#ifndef MG_WEBSOCKET_OPCODE_BINARY
#define MG_WEBSOCKET_OPCODE_BINARY 0x2
#endif

namespace cv {
namespace stream {

// ==============================
// Forward / Helpers
// ==============================

class CivetRequest;
class CivetResponse;
class CivetWebSocketSession;

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

static inline bool iequals(const char* a, const char* b) {
    if (!a || !b) return false;
    while (*a && *b) {
        char ca = (*a >= 'A' && *a <= 'Z') ? (*a - 'A' + 'a') : *a;
        char cb = (*b >= 'A' && *b <= 'Z') ? (*b - 'A' + 'a') : *b;
        if (ca != cb) return false;
        ++a; ++b;
    }
    return *a == *b;
}

// ==============================
// Request / Response
// ==============================

class CivetRequest final : public Request {
public:
    explicit CivetRequest(struct mg_connection* c)
        : conn_(c), ri_(mg_get_request_info(c)) {}

    std::string getMethod() const override {
        return ri_ && ri_->request_method ? ri_->request_method : "";
    }
    std::string getPath() const override {
        return ri_ && ri_->local_uri ? ri_->local_uri : (ri_ && ri_->request_uri ? ri_->request_uri : "");
    }
    bool isWebSocketUpgrade() const override {
        const char* up = mg_get_header(conn_, "Upgrade");
        const char* connHdr = mg_get_header(conn_, "Connection");
        // Basic check: Upgrade: websocket AND Connection: Upgrade
        return (up && iequals(up, "websocket")) &&
               (connHdr && (std::strstr(connHdr, "Upgrade") || std::strstr(connHdr, "upgrade")));
    }

    struct mg_connection* raw() const { return conn_; }
    const struct mg_request_info* info() const { return ri_; }

private:
    struct mg_connection* conn_{nullptr};
    const struct mg_request_info* ri_{nullptr};
};

class CivetResponse final : public Response {
public:
    explicit CivetResponse(struct mg_connection* c)
        : conn_(c) {}

    ~CivetResponse() override {
        // If we started chunked transfer but didn't finish, terminate it.
        if (started_ && chunked_) {
            // last-chunk
            mg_printf(conn_, "0\r\n\r\n");
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
            // Decide transfer mode: if user didn't set Content-Length, use chunked
            chunked_ = true;
            // Compose status line and headers
            mg_printf(conn_, "HTTP/1.1 %d %s\r\n", statusCode_, httpStatusText(statusCode_).c_str());

            bool hasCL = false;
            bool hasTE = false;
            for (auto& kv : headers_) {
                if (!hasCL && strcasecmp(kv.first.c_str(), "Content-Length") == 0) hasCL = true;
                if (!hasTE && strcasecmp(kv.first.c_str(), "Transfer-Encoding") == 0) hasTE = true;
            }
            if (!hasCL && !hasTE) {
                mg_printf(conn_, "Transfer-Encoding: chunked\r\n");
            } else {
                chunked_ = !hasCL; // if Content-Length present, not chunked
            }

            // Reasonable defaults for streaming
            bool hasCT = false;
            bool hasConn = false;
            for (auto& kv : headers_) {
                if (!hasCT && strcasecmp(kv.first.c_str(), "Content-Type") == 0) hasCT = true;
                if (!hasConn && strcasecmp(kv.first.c_str(), "Connection") == 0) hasConn = true;
            }
            if (!hasCT) mg_printf(conn_, "Content-Type: application/octet-stream\r\n");
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
            mg_printf(conn_, "%zx\r\n", size);
            if (size > 0) mg_write(conn_, data, size);
            mg_printf(conn_, "\r\n");
        } else {
            if (size > 0) mg_write(conn_, data, size);
        }
        return true;
    }

    bool acceptWebSocket(const WebSocketHandler&, const std::vector<std::string>&) override {
        // CivetWeb uses dedicated route registration for WS; upgrading here is non-trivial.
        // Keep API but return false to indicate unsupported in this backend.
        return false;
    }

private:
    struct mg_connection* conn_{nullptr};
    int statusCode_{200};
    std::vector<std::pair<std::string, std::string>> headers_;
    bool started_{false};
    bool chunked_{false};
};

// ==============================
// WebSocket Session
// ==============================

class CivetWebSocketSession final : public WebSocketSession,
                                    public std::enable_shared_from_this<CivetWebSocketSession> {
public:
    CivetWebSocketSession(struct mg_connection* c, std::string path)
        : conn_(c), path_(std::move(path)), open_(true) {}

    ~CivetWebSocketSession() override = default;

    bool send(const void* data, size_t size, bool binary) override {
        if (!isOpen()) return false;
        std::lock_guard<std::mutex> lk(send_mx_);
        if (!isOpen()) return false;
        int opcode = binary ? MG_WEBSOCKET_OPCODE_BINARY : MG_WEBSOCKET_OPCODE_TEXT;
        // mg_websocket_write returns number of bytes written, or <0 on error.
        int rc = mg_websocket_write(conn_, opcode, static_cast<const char*>(data), size);
        return rc >= 0;
    }

    void close(WsCloseCode /*code*/, const std::string& /*reason*/) override {
        std::lock_guard<std::mutex> lk(state_mx_);
        open_ = false;
        mg_close_connection(conn_);
    }

    bool isOpen() const override {
        return open_.load(std::memory_order_acquire);
    }

    std::string remoteAddress() const override {
        const mg_request_info* ri = mg_get_request_info(conn_);
        if (!ri) return {};
        std::string ip = ri->remote_addr ? ri->remote_addr : "";
        if (ri->remote_port > 0) {
            char tail[32];
            std::snprintf(tail, sizeof(tail), ":%d", ri->remote_port);
            ip += tail;
        }
        return ip;
    }

    void setWriteQueueLimit(size_t) override {}
    size_t queuedBytes() const override { return 0; }
    void setNoDelay(bool) override {}
    void enableCompression(bool) override {}

    struct mg_connection* raw() const { return conn_; }
    const std::string& path() const { return path_; }

    void _markClosed() {
        std::lock_guard<std::mutex> lk(state_mx_);
        open_ = false;
    }

private:
    struct mg_connection* conn_{nullptr};
    std::string path_;
    mutable std::mutex send_mx_;
    mutable std::mutex state_mx_;
    std::atomic<bool> open_;
};

// ==============================
// Server::ServerImpl
// ==============================

class Server::ServerImpl {
public:
    ServerImpl() = default;
    ~ServerImpl() { stop(); }

    bool start(int port, int num_threads) {
        if (ctx_) return true;

        port_str_ = std::to_string(port);
        threads_str_ = std::to_string(num_threads);

        const char* options[] = {
            "listening_ports", port_str_.c_str(),
            "num_threads",     threads_str_.c_str(),
            "enable_keep_alive", "yes",
            nullptr
        };

        std::memset(&callbacks_, 0, sizeof(callbacks_));
        ctx_ = mg_start(&callbacks_, this, options);
        if (!ctx_) return false;
        installHandlers_();  // <-- ensure pre-registered routes are hooked up
        return true;
    }

    void stop() {
        if (!ctx_) return;
        mg_stop(ctx_);
        ctx_ = nullptr;

        {
            std::lock_guard<std::mutex> lk(mx_);
            http_routes_.clear();
            ws_routes_.clear();
            ws_sessions_.clear();
            route_data_.clear();
        }
    }

    bool running() const { return ctx_ != nullptr; }

    // ---- HTTP ----
    void registerEndpoint(const std::string& path, const RequestHandler& h) {
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_[path] = h;              // cache always
        if (!ctx_) return;                   // installed on start
        auto rd = std::unique_ptr<RouteData>(new RouteData());
        rd->impl = this; rd->path = path;
        mg_set_request_handler(ctx_, path.c_str(), &ServerImpl::httpHandler, rd.get());
        route_data_.push_back(std::move(rd));
    }

    void unregisterEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(mx_);
        http_routes_.erase(path);
        if (ctx_) {
            mg_set_request_handler(ctx_, path.c_str(), nullptr, nullptr);
            eraseRouteDataForPath_(path);
        }
    }

    // ---- WebSocket ----
    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& h) {
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_[path] = h;
        if (!ctx_) return;
        auto rd = std::unique_ptr<RouteData>(new RouteData());
        rd->impl = this; rd->path = path; rd->ws_handler = h;
        mg_set_websocket_handler(ctx_, path.c_str(),
            &ServerImpl::wsConnect, &ServerImpl::wsReady, &ServerImpl::wsData, &ServerImpl::wsClose,
            rd.get());
        route_data_.push_back(std::move(rd));
    }

    void unregisterWebSocketEndpoint(const std::string& path) {
        std::lock_guard<std::mutex> lk(mx_);
        ws_routes_.erase(path);
        ws_sessions_.erase(path);
        if (ctx_) {
            mg_set_websocket_handler(ctx_, path.c_str(), nullptr, nullptr, nullptr, nullptr, nullptr);
            eraseRouteDataForPath_(path);
        }
    }

    size_t broadcast(const std::string& path, const void* data, size_t n, bool binary) {
        std::lock_guard<std::mutex> lk(mx_);
        size_t ok = 0;
        auto it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) return 0;
        auto& set = it->second;
        for (auto iter = set.begin(); iter != set.end(); ) {
            const auto& sp = *iter;
            if (!sp || !sp->isOpen()) { iter = set.erase(iter); continue; }
            if (sp->send(data, n, binary)) ++ok;
            ++iter;
        }
        return ok;
    }

    void forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) {
        std::vector<std::shared_ptr<CivetWebSocketSession>> copy;
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
        return it == ws_sessions_.end() ? 0 : it->second.size();
    }

private:
    struct RouteData {
        ServerImpl* impl{nullptr};
        std::string path;
        WebSocketHandler ws_handler; // used only for WS paths
    };

    // ---------- HTTP handler ----------
    static int httpHandler(struct mg_connection* conn, void* cbdata) {
        RouteData* rd = static_cast<RouteData*>(cbdata);
        if (!rd || !rd->impl) return 0;

        ServerImpl* self = rd->impl;
        RequestHandler handler;
        {
            std::lock_guard<std::mutex> lk(self->mx_);
            auto it = self->http_routes_.find(rd->path);
            if (it == self->http_routes_.end()) return 0;
            handler = it->second;
        }

        CivetRequest req(conn);
        CivetResponse res(conn);
        try {
            handler(req, res);
        } catch (...) {
            const char* msg = "Internal Server Error";
            mg_printf(conn, "HTTP/1.1 500 Internal Server Error\r\n"
                            "Content-Type: text/plain\r\n"
                            "Connection: close\r\n"
                            "Content-Length: %zu\r\n\r\n%s", std::strlen(msg), msg);
        }
        return 1;
    }

    // ---------- WebSocket handlers ----------
    static int wsConnect(const struct mg_connection* conn, void* cbdata) {
        (void)conn;
        RouteData* rd = static_cast<RouteData*>(cbdata);
        if (!rd || !rd->impl) return 1; // reject on error
        // Return 0 to accept, non-zero to reject
        return 0;
    }

    static void wsReady(struct mg_connection* conn, void* cbdata) {
        RouteData* rd = static_cast<RouteData*>(cbdata);
        if (!rd || !rd->impl) return;

        auto session = std::make_shared<CivetWebSocketSession>(conn, rd->path);
        {
            std::lock_guard<std::mutex> lk(rd->impl->mx_);
            rd->impl->ws_sessions_[rd->path].insert(session);
        }
        mg_set_user_connection_data(conn, session.get());

        if (rd->ws_handler.onOpen) rd->ws_handler.onOpen(*session);
    }

    static int wsData(struct mg_connection* conn, int bits, char* data, size_t len, void* cbdata) {
        auto* rd = static_cast<RouteData*>(cbdata);
        if (!rd || !rd->impl) return 0;

        auto* sess = static_cast<CivetWebSocketSession*>(mg_get_user_connection_data(conn));
        if (!sess || !rd->ws_handler.onMessage) return 1;

        bool isBinary = ((bits & 0x0F) == MG_WEBSOCKET_OPCODE_BINARY);
        bool isText   = ((bits & 0x0F) == MG_WEBSOCKET_OPCODE_TEXT);
        if (isBinary || isText) {
            rd->ws_handler.onMessage(*sess, reinterpret_cast<const uint8_t*>(data), len, isBinary);
        }
        return 1;
    }

    static void wsClose(const struct mg_connection* conn, void* cbdata) {
        auto* rd = static_cast<RouteData*>(cbdata);
        if (!rd || !rd->impl) return;

        auto* raw = static_cast<CivetWebSocketSession*>(mg_get_user_connection_data(conn));
        if (raw) raw->_markClosed();

        if (rd->ws_handler.onClose && raw) {
            rd->ws_handler.onClose(*raw, static_cast<int>(WsCloseCode::Normal), std::string());
        }

        std::lock_guard<std::mutex> lk(rd->impl->mx_);
        auto it = rd->impl->ws_sessions_.find(rd->path);
        if (it != rd->impl->ws_sessions_.end()) {
            for (auto iter = it->second.begin(); iter != it->second.end(); ) {
                if (iter->get() == raw) iter = it->second.erase(iter);
                else ++iter;
            }
        }
    }
    void installHandlers_() {
        std::lock_guard<std::mutex> lk(mx_);
        route_data_.clear();
        // HTTP routes
        for (auto &kv : http_routes_) {
            auto rd = std::unique_ptr<RouteData>(new RouteData());
            rd->impl = this; rd->path = kv.first;
            mg_set_request_handler(ctx_, kv.first.c_str(), &ServerImpl::httpHandler, rd.get());
            route_data_.push_back(std::move(rd));
        }
        // WS routes
        for (auto &kv : ws_routes_) {
            auto rd = std::unique_ptr<RouteData>(new RouteData());
            rd->impl = this; rd->path = kv.first; rd->ws_handler = kv.second;
            mg_set_websocket_handler(ctx_, kv.first.c_str(),
                &ServerImpl::wsConnect, &ServerImpl::wsReady, &ServerImpl::wsData, &ServerImpl::wsClose,
                rd.get());
            route_data_.push_back(std::move(rd));
        }
    }

    void eraseRouteDataForPath_(const std::string& path) {
        route_data_.erase(std::remove_if(route_data_.begin(), route_data_.end(),
            [&](const std::unique_ptr<RouteData>& p){ return p && p->path == path; }),
            route_data_.end());
    }

    struct mg_context* ctx_{nullptr};
    struct mg_callbacks callbacks_{};

    // Route storage
    mutable std::mutex mx_;
    std::unordered_map<std::string, RequestHandler> http_routes_;
    std::unordered_map<std::string, WebSocketHandler> ws_routes_;

    // Active WS sessions per path (strong refs; erased on close)
    std::unordered_map<std::string, std::unordered_set<std::shared_ptr<CivetWebSocketSession>>> ws_sessions_;

    // Lifetime for cbdata
    std::vector<std::unique_ptr<RouteData>> route_data_;

    // Options retained to keep c_str pointers valid during mg_start (defensive)
    std::string port_str_;
    std::string threads_str_;
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

#endif // HAVE_STREAM_BACKEND_CIVETWEB
