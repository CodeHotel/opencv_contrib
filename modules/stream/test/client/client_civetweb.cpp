// client_civetweb.cpp
//
// Backend: CivetWeb (WS only; no TLS/WSS)
// Builds only for test targets.
// Requires civetweb built with USE_WEBSOCKET.

#if defined(HAVE_STREAM_HTTP_CIVETWEB) && defined(OCV_BUILD_TESTS)

#include "../client.hpp"
#include <civetweb.h>

#include <atomic>
#include <cassert>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <vector>

#ifndef MG_WEBSOCKET_OPCODE_TEXT
#define MG_WEBSOCKET_OPCODE_TEXT   0x1
#endif
#ifndef MG_WEBSOCKET_OPCODE_BINARY
#define MG_WEBSOCKET_OPCODE_BINARY 0x2
#endif
#ifndef MG_WEBSOCKET_OPCODE_CLOSE
#define MG_WEBSOCKET_OPCODE_CLOSE  0x8
#endif
#ifndef MG_WEBSOCKET_OPCODE_PING
#define MG_WEBSOCKET_OPCODE_PING   0x9
#endif
#ifndef MG_WEBSOCKET_OPCODE_PONG
#define MG_WEBSOCKET_OPCODE_PONG   0xA
#endif

namespace {
    using cv::utils::logging::LogLevel;
    static cv::utils::logging::LogTag kStreamLogTag(
        "cv.stream.client",
        LogLevel::LOG_LEVEL_VERBOSE
    );
    cv::utils::logging::LogTag* kTag = &kStreamLogTag;
}

namespace cv {
namespace stream {
namespace client {

// ==============================
// Client::Impl (CivetWeb)
// ==============================

class Client::Impl {
public:
    Impl() = default;
    ~Impl() { close(WsCloseCode::Normal, std::string()); }

    bool connect(const std::string& url,
                 const WebSocketHandler& callbacks,
                 const WebSocketClientOptions& opts);

    void close(WsCloseCode code, const std::string& reason);

    bool send(const void* data, size_t size, bool binary);

    bool isOpen() const { return open_.load(std::memory_order_acquire); }

    std::string remoteAddress() const {
        std::lock_guard<std::mutex> lk(mx_);
        return remote_;
    }

    void setWriteQueueLimit(size_t) {}
    size_t queuedBytes() const { return 0; }
    void setNoDelay(bool) {}
    void enableCompression(bool) {}

private:
    // CivetWeb client callbacks:
    static int data_cb(struct mg_connection* c, int flags, char* data, size_t len, void* user);
    static void close_cb(const struct mg_connection* c, void* user);

    // Very small ws:// URL parser.
    static bool parseWsUrl(const std::string& url, std::string& host, int& port, std::string& path) {
        host.clear(); path = "/"; port = 0;

        const std::string ws  = "ws://";
        size_t off = 0;
        if (url.compare(0, ws.size(), ws) == 0) {
            off = ws.size(); port = 80;
        } else {
            return false;
        }

        // host[:port][/path[?query]]
        const size_t slash = url.find('/', off);
        const std::string hostport = (slash == std::string::npos) ? url.substr(off) : url.substr(off, slash - off);
        if (hostport.empty()) return false;

        size_t col = hostport.rfind(':');
        if (col != std::string::npos && hostport.find(']') == std::string::npos) { // naive: ignore IPv6 literal with :]
            host = hostport.substr(0, col);
            try { port = std::stoi(hostport.substr(col + 1)); } catch (...) { return false; }
        } else {
            host = hostport;
        }

        if (slash != std::string::npos) {
            path = url.substr(slash);
            if (path.empty() || path[0] != '/') path = "/" + path;
        }
        return !host.empty() && port > 0 && port < 65536;
    }

    void onOpen() {
        open_.store(true, std::memory_order_release);
        WebSocketHandler cb;
        {
            std::lock_guard<std::mutex> lk(mx_);
            cb = cb_;
        }
        if (cb.onOpen) cb.onOpen();
    }

    void onClose(WsCloseCode code, const std::string& reason) {
        const bool wasOpen = open_.exchange(false, std::memory_order_acq_rel);
        if (!wasOpen) return;
        WebSocketHandler cb;
        {
            std::lock_guard<std::mutex> lk(mx_);
            cb = cb_;
        }
        if (cb.onClosed) cb.onClosed(code, reason);
    }

    void onMessage(int flags, const char* data, size_t len) {
        WebSocketHandler cb;
        {
            std::lock_guard<std::mutex> lk(mx_);
            cb = cb_;
        }
        const int opcode = (flags & 0x0F);
        if (opcode == MG_WEBSOCKET_OPCODE_PING || opcode == MG_WEBSOCKET_OPCODE_PONG) {
            // No ping/pong callbacks in the test handler; ignore.
            return;
        }
        if (cb.onMessage && (opcode == MG_WEBSOCKET_OPCODE_BINARY || opcode == MG_WEBSOCKET_OPCODE_TEXT)) {
            const bool isBinary = (opcode == MG_WEBSOCKET_OPCODE_BINARY);
            cb.onMessage(static_cast<const void*>(data), len, isBinary);
        }
    }

private:
    mutable std::mutex mx_;
    struct mg_connection* conn_ = nullptr;
    WebSocketHandler cb_;
    std::atomic<bool> open_{false};
    std::string remote_;
};

// CivetWeb client data callback
int Client::Impl::data_cb(struct mg_connection* c, int flags, char* data, size_t len, void* user) {
    (void)c;
    auto* self = static_cast<Client::Impl*>(user);
    if (!self) return 0;

    const int opcode = (flags & 0x0F);
    if (opcode == MG_WEBSOCKET_OPCODE_CLOSE) {
        self->onClose(WsCloseCode::Normal, std::string());
        return 1;
    }

    self->onMessage(flags, data, len);
    return 1; // keep connection
}

// CivetWeb client close callback
void Client::Impl::close_cb(const struct mg_connection* /*c*/, void* user) {
    auto* self = static_cast<Client::Impl*>(user);
    if (!self) return;
    self->onClose(WsCloseCode::Normal, std::string());
}

// Connect
bool Client::Impl::connect(const std::string& url,
                           const WebSocketHandler& callbacks,
                           const WebSocketClientOptions& opts)
{
    (void)opts; // CivetWeb client API does not expose custom headers/subprotocols here.

    std::string host, path;
    int port = 0;
    if (!parseWsUrl(url, host, port, path)) {
        if (callbacks.onError) callbacks.onError("Invalid ws:// URL");
        return false;
    }

    char ebuf[256] = {0};

    // CivetWeb performs the handshake synchronously; if it returns non-null, we're connected.
    mg_connection* c = mg_connect_websocket_client(
        host.c_str(),
        port,
        0, // NO TLS (ws:// only)
        ebuf,
        sizeof(ebuf),
        path.c_str(),
        nullptr,
        &Client::Impl::data_cb,
        &Client::Impl::close_cb,
        this);

    if (!c) {
        if (callbacks.onError) callbacks.onError(ebuf[0] ? std::string(ebuf) : std::string("connect failed"));
        return false;
    }

    {
        std::lock_guard<std::mutex> lk(mx_);
        conn_ = c;
        cb_ = callbacks;
        remote_.clear();
        remote_.reserve(host.size() + 8);
        remote_.append(host).append(":");
        char tmp[16]; std::snprintf(tmp, sizeof(tmp), "%d", port);
        remote_.append(tmp);
    }

    onOpen();
    return true;
}

// Close
void Client::Impl::close(WsCloseCode /*code*/, const std::string& /*reason*/) {
    mg_connection* toClose = nullptr;
    {
        std::lock_guard<std::mutex> lk(mx_);
        toClose = conn_;
        conn_ = nullptr;
    }
    if (toClose) {
        mg_close_connection(toClose);
    }
    onClose(WsCloseCode::Normal, std::string());
}

// Send frame
bool Client::Impl::send(const void* data, size_t size, bool binary) {
    std::lock_guard<std::mutex> lk(mx_);
    if (!conn_) return false;

    mg_lock_connection(conn_);
    const int opcode = binary ? MG_WEBSOCKET_OPCODE_BINARY : MG_WEBSOCKET_OPCODE_TEXT;
    int rc = mg_websocket_client_write(conn_, opcode,
                                       static_cast<const char*>(data),
                                       size);
    mg_unlock_connection(conn_);
    return rc >= 0;
}

// ==============================
// Client (public)
// ==============================

Client::Client() : pimpl(new Client::Impl) {}
Client::~Client() = default;

bool Client::connect(const std::string& url,
                     const WebSocketHandler& callbacks,
                     const WebSocketClientOptions& opts) {
    return pimpl->connect(url, callbacks, opts);
}

void Client::close(WsCloseCode code, const std::string& reason) {
    pimpl->close(code, reason);
}

bool Client::send(const void* data, size_t size, bool binary) {
    return pimpl->send(data, size, binary);
}

bool Client::isOpen() const { return pimpl->isOpen(); }

std::string Client::remoteAddress() const { return pimpl->remoteAddress(); }

void Client::setWriteQueueLimit(size_t b) { pimpl->setWriteQueueLimit(b); }
size_t Client::queuedBytes() const { return pimpl->queuedBytes(); }
void Client::setNoDelay(bool on) { pimpl->setNoDelay(on); }
void Client::enableCompression(bool on) { pimpl->enableCompression(on); }

Client::Client(Client&&) noexcept = default;
Client& Client::operator=(Client&&) noexcept = default;

std::unique_ptr<Client> createWebSocketClient() {
    return std::unique_ptr<Client>(new Client());
}

// ==============================
// Minimal HTTP GET (HTTP only)
// ==============================

namespace {

// Simple "http://" URL parser supporting arbitrary ports.
bool parseHttpUrl(const std::string& url, std::string& host, int& port, std::string& path) {
    host.clear(); path = "/"; port = 0;

    const std::string http = "http://";
    size_t off = 0;
    if (url.compare(0, http.size(), http) == 0) {
        off = http.size(); port = 80;
    } else {
        return false;
    }

    const size_t slash = url.find('/', off);
    const std::string hostport = (slash == std::string::npos) ? url.substr(off) : url.substr(off, slash - off);
    if (hostport.empty()) return false;

    size_t col = hostport.rfind(':');
    if (col != std::string::npos && hostport.find(']') == std::string::npos) {
        host = hostport.substr(0, col);
        try { port = std::stoi(hostport.substr(col + 1)); } catch (...) { return false; }
    } else {
        host = hostport;
    }

    if (slash != std::string::npos) {
        path = url.substr(slash);
        if (path.empty() || path[0] != '/') path = "/" + path;
    }
    return !host.empty() && port > 0 && port < 65536;
}

// Trim helper (left/right)
static inline std::string trim(const std::string& s) {
    size_t b = 0, e = s.size();
    while (b < e && (s[b] == ' ' || s[b] == '\t' || s[b] == '\r' || s[b] == '\n')) ++b;
    while (e > b && (s[e-1] == ' ' || s[e-1] == '\t' || s[e-1] == '\r' || s[e-1] == '\n')) --e;
    return s.substr(b, e - b);
}

} // namespace

HttpResponse httpGet(const std::string& url, const HttpRequestOptions& opts) {
    char ebuf[256] = {0};

    std::string host, path;
    int port = 0;
    if (!parseHttpUrl(url, host, port, path)) {
        return HttpResponse{};  // default status = -1
    }

    mg_connection* c = mg_connect_client(host.c_str(), port, 0, ebuf, sizeof(ebuf));
    if (!c) {
        return HttpResponse{};  // default status = -1
    }

    // Build request
    std::string req;
    req.reserve(256);
    req += "GET ";
    req += path.empty() ? "/" : path;
    req += " HTTP/1.0\r\nHost: ";
    req += host;
    req += "\r\nConnection: close\r\n";
    for (const auto& kv : opts.headers) {
        req += kv.first; req += ": "; req += kv.second; req += "\r\n";
    }
    req += "\r\n";

    mg_lock_connection(c);
    mg_printf(c, "%s", req.c_str());
    mg_unlock_connection(c);

    // Read full response (headers + body)
    std::string raw;
    raw.reserve(1024);
    char buf[8192];
    size_t maxBytes = opts.maxBodyBytes ? (opts.maxBodyBytes + 65536) : (10 * 1024 * 1024 + 65536); // room for headers
    for (;;) {
        int n = mg_read(c, buf, sizeof(buf));
        if (n <= 0) break;
        if (raw.size() + static_cast<size_t>(n) > maxBytes) {
            raw.append(buf, buf + (maxBytes - raw.size()));
            break;
        }
        raw.append(buf, buf + n);
    }

    mg_close_connection(c);

    // Parse status line and headers
    HttpResponse resp;
    resp.status = -1;

    const std::string sep = "\r\n\r\n";
    size_t hdr_end = raw.find(sep);
    if (hdr_end == std::string::npos) {
        return resp; // malformed
    }

    const std::string header_blob = raw.substr(0, hdr_end);
    resp.body = raw.substr(hdr_end + sep.size());

    size_t line_start = 0;
    size_t line_end = header_blob.find("\r\n", line_start);
    if (line_end == std::string::npos) return resp;

    // Status line: HTTP/1.x <code> ...
    const std::string status_line = header_blob.substr(line_start, line_end - line_start);
    size_t sp1 = status_line.find(' ');
    if (sp1 != std::string::npos) {
        size_t sp2 = status_line.find(' ', sp1 + 1);
        std::string code_str = (sp2 == std::string::npos)
                               ? status_line.substr(sp1 + 1)
                               : status_line.substr(sp1 + 1, sp2 - (sp1 + 1));
        try { resp.status = std::stoi(code_str); } catch (...) { resp.status = -1; }
    }

    // Headers
    line_start = line_end + 2;
    while (line_start < header_blob.size()) {
        line_end = header_blob.find("\r\n", line_start);
        const size_t len = (line_end == std::string::npos) ? (header_blob.size() - line_start) : (line_end - line_start);
        std::string line = header_blob.substr(line_start, len);
        if (!line.empty()) {
            size_t colon = line.find(':');
            if (colon != std::string::npos) {
                std::string k = trim(line.substr(0, colon));
                std::string v = trim(line.substr(colon + 1));
                resp.headers.emplace_back(std::move(k), std::move(v));
            }
        }
        if (line_end == std::string::npos) break;
        line_start = line_end + 2;
    }

    return resp;
}

} // namespace test
} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_BACKEND_CIVETWEB && OCV_BUILD_TESTS
