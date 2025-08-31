#if defined(HAVE_STREAM_HTTP_MONGOOSE)

#include "../client.hpp"
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
#include <cstdarg>

#if defined(_WIN32)
  #include <winsock2.h>
#else
  #include <sys/types.h>
  #include <sys/socket.h>
#endif

#include "opencv2/core/utils/logger.hpp"

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
        {
            std::lock_guard<std::mutex> g(state_mx_);
            if (running_) return isOpen_;
            handler_ = callbacks;
            opts_    = opts;
            url_     = url;
        }

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

        const int ct_ms = opts.connectTimeoutMs <= 0 ? 5000 : opts.connectTimeoutMs;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ct_ms);

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

        if (handler_.onClosed) {
            handler_.onClosed(code, reason);
        }
        isOpen_ = false;
        CV_LOG_INFO(kTag, "Closed");
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
    static void ev_handler(mg_connection* c, int ev, void* ev_data) {
        // Prefer fn_data when available; fall back to mgr->userdata.
        auto* self = static_cast<Impl*>(c->fn_data ? c->fn_data : c->mgr->userdata);
        if (!self) return;

        switch (ev) {
            case MG_EV_CONNECT: {
                CV_LOG_INFO(kTag, "CONNECT conn=" << (void*)c);
                (void)ev_data;
                break;
            }

            case MG_EV_HTTP_MSG: {
                auto* hm = (mg_http_message*) ev_data;
                std::string uri(hm->uri.buf ? hm->uri.buf : "", hm->uri.len);
                std::string proto(hm->proto.buf ? hm->proto.buf : "", hm->proto.len);
                CV_LOG_INFO(kTag, "HTTP_MSG proto='" << proto << "' uri='" << uri << "' (no WS upgrade) -> fail-fast");
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    if (!self->openNotified_) self->failed_ = true;
                }
                mg_close_conn(c);
                break;
            }

            case MG_EV_WS_OPEN: {
                CV_LOG_INFO(kTag, "WS_OPEN conn=" << (void*)c);
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    self->isOpen_ = true;
                    self->openNotified_ = true;
                }
                if (self->handler_.onOpen) {
                    self->handler_.onOpen();
                }
                break;
            }

            case MG_EV_WS_MSG: {
                auto* wm = static_cast<mg_ws_message*>(ev_data);
                bool binary = (wm->flags & WEBSOCKET_OP_BINARY) != 0;
                CV_LOG_INFO(kTag, "WS_MSG len=" << wm->data.len << " bin=" << (int)binary);
                if (self->handler_.onMessage) {
                    self->handler_.onMessage(
                        reinterpret_cast<const uint8_t*>(wm->data.buf),
                        wm->data.len,
                        binary
                    );
                }
                break;
            }

            case MG_EV_ERROR: {
                const char* msg = static_cast<const char*>(ev_data);
                CV_LOG_INFO(kTag, "ERROR: " << (msg ? msg : "(null)"));
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    if (!self->openNotified_) self->failed_ = true;
                }
                if (self->handler_.onError) {
                    self->handler_.onError(msg ? std::string(msg) : std::string("error"));
                }
                break;
            }

            case MG_EV_CLOSE: {
                CV_LOG_INFO(kTag, "CLOSE conn=" << (void*)c);
                bool wasOpen = false;
                {
                    std::lock_guard<std::mutex> lk(self->state_mx_);
                    wasOpen = self->isOpen_;
                    self->isOpen_ = false;
                    if (!self->openNotified_) self->failed_ = true;
                }
                if (wasOpen && self->handler_.onClosed) {
                    self->handler_.onClosed(WsCloseCode::Normal, std::string());
                }
                break;
            }

            default:
                break;
        }
    }

private:
    mg_mgr mgr_{};
    mg_connection* conn_{nullptr};

    std::thread poll_thread_;
    mutable std::mutex state_mx_;
    std::atomic<bool> running_{false};
    std::atomic<bool> isOpen_{false};
    bool openNotified_{false};
    bool failed_{false};

    WebSocketClientOptions opts_;
    WebSocketHandler handler_;
    std::string url_;
};

// ==============================
// Public cv::stream::test::Client
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

// ==============================
// Minimal HTTP GET (HTTP only)
// ==============================

namespace {
struct HttpGetState {
    HttpResponse* out;
    bool done = false;
    bool failed = false;
    size_t max_body = 10 * 1024 * 1024;
    std::string request_path;
    std::string host_header;
    std::string extra_headers;
};

static void http_ev_handler(mg_connection* c, int ev, void* ev_data) {
    auto* st = static_cast<HttpGetState*>(c->mgr->userdata);
    if (!st) return;

    switch (ev) {
        case MG_EV_CONNECT: {
            std::ostringstream req;
            req << "GET " << (st->request_path.empty() ? "/" : st->request_path) << " HTTP/1.1\r\n";
            req << "Host: " << st->host_header << "\r\n";
            req << "Connection: close\r\n";
            req << "User-Agent: opencv-stream-test\r\n";
            if (!st->extra_headers.empty()) req << st->extra_headers;
            req << "\r\n";
            auto s = req.str();
            mg_send(c, s.data(), s.size());
            break;
        }
        case MG_EV_HTTP_MSG: {
            auto* hm = (mg_http_message*) ev_data;
            // If your Mongoose version lacks mg_http_status(hm), parse from hm->uri/headers as needed.
            #ifdef mg_http_status
            st->out->status = mg_http_status(hm);
            #else
            st->out->status = mg_http_status(hm);
            #endif
            for (int i = 0; i < MG_MAX_HTTP_HEADERS; ++i) {
                auto& h = hm->headers[i];
                if (!h.name.buf || h.name.len == 0) break;
                std::string name(h.name.buf, h.name.len);
                std::string val(h.value.buf ? h.value.buf : "", h.value.len);
                st->out->headers.emplace_back(std::move(name), std::move(val));
            }
            size_t n = hm->body.len;
            if (n > st->max_body) n = st->max_body;
            st->out->body.assign(hm->body.buf ? hm->body.buf : "", n);
            st->done = true;
            mg_close_conn(c);
            break;
        }
        case MG_EV_ERROR: {
            st->failed = true;
            break;
        }
        case MG_EV_CLOSE: {
            if (!st->done) st->failed = true;
            break;
        }
        default: break;
    }
}
} // namespace

HttpResponse httpGet(const std::string& url, const HttpRequestOptions& opts) {
    HttpResponse out;
    if (url.rfind("http://", 0) != 0) return out;

    std::string rest = url.substr(7);
    std::string hostport, path = "/";
    auto slash = rest.find('/');
    if (slash == std::string::npos) {
        hostport = rest;
    } else {
        hostport = rest.substr(0, slash);
        path = rest.substr(slash);
    }

    std::string extra_headers;
    for (const auto& kv : opts.headers) {
        extra_headers += kv.first;
        extra_headers += ": ";
        extra_headers += kv.second;
        extra_headers += "\r\n";
    }

    mg_mgr mgr;
    mg_mgr_init(&mgr);
    HttpGetState st;
    st.out = &out;
    st.max_body = opts.maxBodyBytes;
    st.request_path = path;
    st.host_header = hostport;
    st.extra_headers = extra_headers;
    mgr.userdata = &st;

    mg_connection* c = mg_http_connect(&mgr, url.c_str(), http_ev_handler, &st);
    if (!c) {
        mg_mgr_free(&mgr);
        return out;
    }

    const int timeout_ms = opts.timeoutMs <= 0 ? 5000 : opts.timeoutMs;
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);

    while (std::chrono::steady_clock::now() < deadline && !st.done && !st.failed) {
        mg_mgr_poll(&mgr, 50);
    }

    mg_mgr_free(&mgr);

    if (st.failed || !st.done) {
        out.status = -1;
        out.body.clear();
        out.headers.clear();
    }
    return out;
}

} // namespace test
} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_HTTP_MONGOOSE
