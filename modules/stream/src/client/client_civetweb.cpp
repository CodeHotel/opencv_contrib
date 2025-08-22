// client_civetweb.cpp
//
// Backend: CivetWeb
// Builds only for test targets.
// Requires civetweb built with USE_WEBSOCKET.
//
#if defined(HAVE_STREAM_BACKEND_CIVETWEB) && defined(OCV_BUILD_TESTS)

#include "opencv2/stream/client.hpp"
#include "opencv2/stream/server.hpp" // for WsCloseCode + WebSocketHandler
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

namespace cv {
namespace stream {

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

    // Very small ws:// / wss:// URL parser.
    static bool parseWsUrl(const std::string& url, bool& tls, std::string& host, int& port, std::string& path) {
        tls = false; host.clear(); path = "/"; port = 0;

        // scheme
        const std::string ws  = "ws://";
        const std::string wss = "wss://";
        size_t off = 0;
        if (url.compare(0, ws.size(), ws) == 0) {
            off = ws.size(); tls = false; port = 80;
        } else if (url.compare(0, wss.size(), wss) == 0) {
            off = wss.size(); tls = true; port = 443;
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
        if (cb.onOpen) {
            // Prepare a lightweight session adapter to pass to onOpen.
            struct OpenAdapter final : WebSocketSession {
                explicit OpenAdapter(Client::Impl* impl) : impl(impl) {}
                bool send(const void* d, size_t n, bool bin) override { return impl->send(d, n, bin); }
                void close(WsCloseCode c, const std::string& r) override { impl->close(c, r); }
                bool isOpen() const override { return impl->isOpen(); }
                std::string remoteAddress() const override { return impl->remoteAddress(); }
                Client::Impl* impl;
            } adapter(this);
            cb.onOpen(adapter);
        }
    }

    void onClose(int code, const std::string& reason) {
        bool wasOpen = open_.exchange(false, std::memory_order_acq_rel);
        WebSocketHandler cb;
        {
            std::lock_guard<std::mutex> lk(mx_);
            cb = cb_;
        }
        if (wasOpen && cb.onClose) {
            // Same small adapter as above.
            struct CloseAdapter final : WebSocketSession {
                explicit CloseAdapter(Client::Impl* impl) : impl(impl) {}
                bool send(const void* d, size_t n, bool bin) override { return impl->send(d, n, bin); }
                void close(WsCloseCode c, const std::string& r) override { impl->close(c, r); }
                bool isOpen() const override { return impl->isOpen(); }
                std::string remoteAddress() const override { return impl->remoteAddress(); }
                Client::Impl* impl;
            } adapter(this);
            cb.onClose(adapter, code, reason);
        }
    }

    void onMessage(int flags, const char* data, size_t len) {
        WebSocketHandler cb;
        {
            std::lock_guard<std::mutex> lk(mx_);
            cb = cb_;
        }
        const int opcode = (flags & 0x0F);
        if (opcode == MG_WEBSOCKET_OPCODE_PING) {
            if (cb.onPing) {
                // Adapter not strictly required, but keep consistent
                struct PingAdapter final : WebSocketSession {
                    explicit PingAdapter(Client::Impl* impl) : impl(impl) {}
                    bool send(const void* d, size_t n, bool bin) override { return impl->send(d, n, bin); }
                    void close(WsCloseCode c, const std::string& r) override { impl->close(c, r); }
                    bool isOpen() const override { return impl->isOpen(); }
                    std::string remoteAddress() const override { return impl->remoteAddress(); }
                    Client::Impl* impl;
                } adapter(this);
                cb.onPing(adapter, reinterpret_cast<const uint8_t*>(data), len);
            }
            return;
        }
        if (opcode == MG_WEBSOCKET_OPCODE_PONG) {
            if (cb.onPong) {
                struct PongAdapter final : WebSocketSession {
                    explicit PongAdapter(Client::Impl* impl) : impl(impl) {}
                    bool send(const void* d, size_t n, bool bin) override { return impl->send(d, n, bin); }
                    void close(WsCloseCode c, const std::string& r) override { impl->close(c, r); }
                    bool isOpen() const override { return impl->isOpen(); }
                    std::string remoteAddress() const override { return impl->remoteAddress(); }
                    Client::Impl* impl;
                } adapter(this);
                cb.onPong(adapter, reinterpret_cast<const uint8_t*>(data), len);
            }
            return;
        }
        if (cb.onMessage && (opcode == MG_WEBSOCKET_OPCODE_BINARY || opcode == MG_WEBSOCKET_OPCODE_TEXT)) {
            const bool isBinary = (opcode == MG_WEBSOCKET_OPCODE_BINARY);
            // Minimal adapter to satisfy signature
            struct MsgAdapter final : WebSocketSession {
                explicit MsgAdapter(Client::Impl* impl) : impl(impl) {}
                bool send(const void* d, size_t n, bool bin) override { return impl->send(d, n, bin); }
                void close(WsCloseCode c, const std::string& r) override { impl->close(c, r); }
                bool isOpen() const override { return impl->isOpen(); }
                std::string remoteAddress() const override { return impl->remoteAddress(); }
                Client::Impl* impl;
            } adapter(this);
            cb.onMessage(adapter, reinterpret_cast<const uint8_t*>(data), len, isBinary);
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
        self->onClose(static_cast<int>(WsCloseCode::Normal), std::string());
        return 1;
    }

    self->onMessage(flags, data, len);
    return 1; // keep connection
}

// CivetWeb client close callback
void Client::Impl::close_cb(const struct mg_connection* /*c*/, void* user) {
    auto* self = static_cast<Client::Impl*>(user);
    if (!self) return;
    self->onClose(static_cast<int>(WsCloseCode::Normal), std::string());
}

// Connect
bool Client::Impl::connect(const std::string& url,
                           const WebSocketHandler& callbacks,
                           const WebSocketClientOptions& opts)
{
    (void)opts; // CivetWeb client API does not expose custom headers/subprotocols here.

    bool tls = false;
    std::string host, path;
    int port = 0;
    if (!parseWsUrl(url, tls, host, port, path)) {
        return false;
    }

    char ebuf[256] = {0};

    // CivetWeb performs the handshake synchronously; if it returns non-null, we're connected.
    // origin = nullptr; data & close callbacks are invoked by CivetWeb's internal thread.
    mg_connection* c = mg_connect_websocket_client(
        host.c_str(),
        port,
        tls ? 1 : 0,
        ebuf,
        sizeof(ebuf),
        path.c_str(),
        nullptr,
        &Client::Impl::data_cb,
        &Client::Impl::close_cb,
        this);

    if (!c) {
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
    onClose(static_cast<int>(WsCloseCode::Normal), std::string());
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

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_BACKEND_CIVETWEB && OCV_BUILD_TESTS
