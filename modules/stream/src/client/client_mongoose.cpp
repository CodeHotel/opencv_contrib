#if defined(HAVE_STREAM_HTTP_MONGOOSE)

#include "opencv2/stream/client.hpp"
#include "mongoose.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <cstdio>

#if defined(_WIN32)
  #include <winsock2.h>
#else
  #include <sys/types.h>
  #include <sys/socket.h>
#endif

namespace cv {
namespace stream {

static inline void CLOG(const char* tag, const char* fmt, ...) {
    std::fprintf(stderr, "[CLIENT:%s] ", tag);
    va_list ap; va_start(ap, fmt);
    std::vfprintf(stderr, fmt, ap);
    va_end(ap);
    std::fprintf(stderr, "\n");
    std::fflush(stderr);
}

// ==============================
// Client::Impl
// ==============================

class Client::Impl {
public:
    Impl() = default;
    ~Impl() { close(WsCloseCode::Normal, std::string()); }

bool connect(const std::string& url,
             const WebSocketHandler& callbacks,
             const WebSocketClientOptions& opts)
{
    {   // short scope for lock
        std::lock_guard<std::mutex> g(state_mx_);
        if (running_) return isOpen_;
        handler_ = callbacks;
        opts_    = opts;
        url_     = url;
    }

    // Build extra headers (same as your original code)
    std::string extra_headers;
    for (const auto& kv : opts.headers) {
        extra_headers += kv.first; extra_headers += ": "; extra_headers += kv.second; extra_headers += "\r\n";
    }
    if (!opts.subprotocols.empty()) {
        extra_headers += "Sec-WebSocket-Protocol: ";
        for (size_t i = 0; i < opts.subprotocols.size(); ++i) {
            if (i) extra_headers += ", ";
            extra_headers += opts.subprotocols[i];
        }
        extra_headers += "\r\n";
    }
    if (opts.enableCompression) {
        extra_headers += "Sec-WebSocket-Extensions: permessage-deflate\r\n";
    }

    mg_mgr_init(&mgr_);
    mgr_.userdata = this;

    conn_ = mg_ws_connect(&mgr_, url_.c_str(), &Impl::ev_handler, this,
                          extra_headers.empty() ? nullptr : extra_headers.c_str());
    if (!conn_) {
        mg_mgr_free(&mgr_);
        return false;
    }

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(opts.connectTimeoutMs <= 0 ? 5000 : opts.connectTimeoutMs);

    while (std::chrono::steady_clock::now() < deadline) {
        mg_mgr_poll(&mgr_, 50);
        {
            std::lock_guard<std::mutex> lk(state_mx_);
            if (openNotified_ || failed_) break;
        }
    }

    bool connected_ok;
    {
        std::lock_guard<std::mutex> lk(state_mx_);
        connected_ok = isOpen_ && !failed_;
    }

    if (!connected_ok) {
        mg_mgr_free(&mgr_);
        conn_ = nullptr;
        running_ = false;
        return false;
    }

    running_ = true;
    poll_thread_ = std::thread([this] {
        while (running_) mg_mgr_poll(&mgr_, 50);
    });
    return true;
}


    void close(WsCloseCode code, const std::string& reason) {
        std::unique_lock<std::mutex> g(state_mx_);
        if (!running_) {
            if (conn_ || mgr_.conns) {
                mg_mgr_free(&mgr_);
                conn_ = nullptr;
            }
            return;
        }
        running_ = false;
        mg_connection* c = conn_;
        conn_ = nullptr;
        g.unlock();

        if (c) mg_close_conn(c);
        if (poll_thread_.joinable()) poll_thread_.join();

        mg_mgr_free(&mgr_);

        if (handler_.onClose) {
            DummySession ds(*this);
            handler_.onClose(ds, static_cast<int>(code), reason);
        }
        isOpen_ = false;
        CLOG("close", "Closed");
    }

    bool send(const void* data, size_t size, bool binary) {
        std::lock_guard<std::mutex> g(state_mx_);
        if (!isOpen_ || !conn_) return false;
        int op = binary ? WEBSOCKET_OP_BINARY : WEBSOCKET_OP_TEXT;
        mg_ws_send(conn_, static_cast<const char*>(data), size, op);
        return true;
    }

    bool isOpen() const { return isOpen_.load(std::memory_order_acquire); }
    std::string remoteAddress() const { return std::string(); }
    void setWriteQueueLimit(size_t) {}
    size_t queuedBytes() const { return 0; }
    void setNoDelay(bool) {}
    void enableCompression(bool) {}

private:
    class DummySession : public WebSocketSession {
    public:
        explicit DummySession(Impl& impl) : impl_(impl) {}
        bool send(const void*, size_t, bool) override { return false; }
        void close(WsCloseCode, const std::string&) override {}
        bool isOpen() const override { return impl_.isOpen(); }
        std::string remoteAddress() const override { return impl_.remoteAddress(); }
        void setWriteQueueLimit(size_t) override {}
        size_t queuedBytes() const override { return 0; }
        void setNoDelay(bool) override {}
        void enableCompression(bool) override {}
    private:
        Impl& impl_;
    };

    // Event handler
    static void ev_handler(mg_connection* c, int ev, void* ev_data) {
        auto* self = static_cast<Impl*>(c->mgr->userdata);
        if (!self) return;

        switch (ev) {
            case MG_EV_CONNECT: {
                CLOG("ev", "CONNECT conn=%p", (void*)c);
#if MG_ENABLE_MBEDTLS || MG_ENABLE_OPENSSL
                if (self->url_.rfind("wss://", 0) == 0 && self->opts_.verifyTLS) {
                    struct mg_tls_opts topts;
                    memset(&topts, 0, sizeof(topts));
                    if (!self->opts_.caCertPath.empty()) topts.ca = self->opts_.caCertPath.c_str();
                    mg_tls_init(c, &topts);
                }
#endif
                (void)ev_data;
                break;
            }

            case MG_EV_HTTP_MSG: {
                // This happens if the server answered with a plain HTTP response (e.g. 404) and not WS
                auto* hm = (mg_http_message*) ev_data;
                std::string uri(hm->uri.buf ? hm->uri.buf : "", hm->uri.len);
                std::string proto(hm->proto.buf ? hm->proto.buf : "", hm->proto.len);
                CLOG("ev", "HTTP_MSG proto='%s' uri='%s' (probably no WS upgrade) -> fail-fast",
                     proto.c_str(), uri.c_str());
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    if (!self->openNotified_) self->failed_ = true;
                }
                mg_close_conn(c);
                break;
            }

            case MG_EV_WS_OPEN: {
                CLOG("ev", "WS_OPEN conn=%p", (void*)c);
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    self->isOpen_ = true;
                    self->openNotified_ = true;
                }
                if (self->handler_.onOpen) {
                    CallbackSession sess(*self, c);
                    self->handler_.onOpen(sess);
                }
                break;
            }

            case MG_EV_WS_MSG: {
                auto* wm = static_cast<mg_ws_message*>(ev_data);
                bool binary = (wm->flags & WEBSOCKET_OP_BINARY) != 0;
                CLOG("ev", "WS_MSG len=%zu bin=%d", wm->data.len, (int)binary);
                if (self->handler_.onMessage) {
                    CallbackSession sess(*self, c);
                    self->handler_.onMessage(sess,
                                             reinterpret_cast<const uint8_t*>(wm->data.buf),
                                             wm->data.len, binary);
                }
                break;
            }

            case MG_EV_ERROR: {
                const char* msg = static_cast<const char*>(ev_data);
                CLOG("ev", "ERROR: %s", msg ? msg : "(null)");
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    if (!self->openNotified_) self->failed_ = true;
                }
                if (self->handler_.onError) {
                    CallbackSession sess(*self, c);
                    self->handler_.onError(sess, /*errorCode*/ -1, msg ? msg : "error");
                }
                break;
            }

            case MG_EV_CLOSE: {
                CLOG("ev", "CLOSE conn=%p", (void*)c);
                bool wasOpen = false;
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    wasOpen = self->isOpen_;
                    self->isOpen_ = false;
                    if (!self->openNotified_) self->failed_ = true; // fail-fast if never opened
                }
                if (wasOpen && self->handler_.onClose) {
                    CallbackSession sess(*self, c);
                    self->handler_.onClose(sess, (int) WsCloseCode::Normal, std::string());
                }
                break;
            }

            default:
                break;
        }
    }

    // Small session facade to satisfy WebSocketHandler’s expected interface
    class CallbackSession : public WebSocketSession {
    public:
        CallbackSession(Impl& impl, mg_connection* c) : impl_(impl), c_(c) {}
        bool send(const void* data, size_t size, bool binary) override {
            int op = binary ? WEBSOCKET_OP_BINARY : WEBSOCKET_OP_TEXT;
            mg_ws_send(c_, static_cast<const char*>(data), size, op);
            return true;
        }
        void close(WsCloseCode, const std::string&) override { mg_close_conn(c_); }
        bool isOpen() const override { return impl_.isOpen(); }
        std::string remoteAddress() const override { return std::string(); }
        void setWriteQueueLimit(size_t) override {}
        size_t queuedBytes() const override { return 0; }
        void setNoDelay(bool) override {}
        void enableCompression(bool) override {}
    private:
        Impl& impl_;
        mg_connection* c_;
    };

private:
    // Mongoose manager and connection
    mg_mgr mgr_{};
    mg_connection* conn_{nullptr};

    // Threading / state
    std::thread poll_thread_;
    mutable std::mutex state_mx_;
    std::atomic<bool> running_{false};
    std::atomic<bool> isOpen_{false};
    bool openNotified_{false};
    bool failed_{false};

    // Options and handler
    WebSocketClientOptions opts_;
    WebSocketHandler handler_;
    std::string url_;
};

// ==============================
// Public cv::stream::Client
// ==============================

Client::Client() : pimpl(new Impl) {}
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

void Client::setWriteQueueLimit(size_t bytes) { pimpl->setWriteQueueLimit(bytes); }
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

#endif // HAVE_STREAM_BACKEND_MONGOOSE
