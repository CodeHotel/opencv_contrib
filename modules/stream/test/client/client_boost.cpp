#if defined(HAVE_STREAM_HTTP_BOOST) && defined(OCV_BUILD_TESTS)

#include "../client.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

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

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// -----------------------------------------------------------------------------
// Client::Impl
// -----------------------------------------------------------------------------

class Client::Impl {
public:
    Impl()
        : resolver_(ioc_)
        , ws_(ioc_)
        , strand_(ioc_.get_executor()) {}

    ~Impl() {
        if (isOpen()) {
            close(WsCloseCode::GoingAway, "Client destroyed");
        }
        if (io_thread_.joinable()) {
            ioc_.stop();
            io_thread_.join();
        }
    }

    bool connect(const std::string& url, const WebSocketHandler& callbacks, const WebSocketClientOptions& opts);
    void close(WsCloseCode code, const std::string& reason);
    bool send(const void* data, size_t size, bool binary);
    bool isOpen() const { return is_open_.load(std::memory_order_relaxed); }
    std::string remoteAddress() const { return remote_address_; }

    void setWriteQueueLimit(size_t bytes) { write_queue_limit_bytes_.store(bytes, std::memory_order_relaxed); }
    size_t queuedBytes() const { return queued_bytes_.load(std::memory_order_relaxed); }
    void setNoDelay(bool on) {
        options_.noDelay = on;
        beast::error_code ec;
        ws_.next_layer().set_option(tcp::no_delay(on), ec);
    }
    void enableCompression(bool on) { options_.enableCompression = on; }

private:
    struct QueuedMessage {
        std::shared_ptr<const std::vector<uint8_t>> buffer;
        bool is_binary;
    };

    bool parseWsUrl(const std::string& url, std::string& host, std::string& port, std::string& target);

    void on_resolve(beast::error_code ec, tcp::resolver::results_type results);
    void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type endpoint);
    void on_handshake(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void do_write();
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
    void on_close(beast::error_code ec);
    void fail_connection(beast::error_code ec, const char* where);

    net::io_context ioc_;
    tcp::resolver resolver_;
    websocket::stream<tcp::socket> ws_;
    beast::flat_buffer buffer_;
    net::strand<net::any_io_executor> strand_;

    std::string host_;
    std::string port_;
    std::string target_;
    std::string remote_address_;

    WebSocketHandler handler_;
    WebSocketClientOptions options_;
    std::thread io_thread_;
    std::vector<QueuedMessage> write_queue_;
    std::atomic<bool> is_open_{false};
    std::promise<bool> connect_promise_;

    std::atomic<size_t> write_queue_limit_bytes_{std::numeric_limits<size_t>::max()};
    std::atomic<size_t> queued_bytes_{0};

    std::atomic<WsCloseCode> last_close_code_{WsCloseCode::Normal};
    std::string last_close_reason_;
};
    namespace {
        // host[:port] -> host, port
        inline void split_host_port(const std::string& in, std::string& host, std::string& port) {
            const auto p = in.find(':');
            if (p == std::string::npos) { host = in; port.clear(); }
            else { host = in.substr(0, p); port = in.substr(p + 1); }
        }
    }

// -----------------------------------------------------------------------------
// Client::Impl — methods
// -----------------------------------------------------------------------------

bool Client::Impl::parseWsUrl(const std::string& url, std::string& host, std::string& port, std::string& target) {
    try {
        const auto pos = url.find("://");
        if (pos == std::string::npos) return false;
        const auto scheme = url.substr(0, pos);
        if (scheme != "ws") return false; // no TLS/WSS
        std::string rest = url.substr(pos + 3);
        auto slash = rest.find('/');
        target = (slash == std::string::npos) ? "/" : rest.substr(slash);
        const std::string authority = (slash == std::string::npos) ? rest : rest.substr(0, slash);
        split_host_port(authority, host, port);
        if (port.empty()) port = "80";
        return !host.empty() && !port.empty();
    } catch (...) {
        return false;
    }
}

bool Client::Impl::connect(const std::string& url, const WebSocketHandler& callbacks, const WebSocketClientOptions& opts) {
    CV_LOG_DEBUG(kTag, "connect(" << url << ")");
    handler_ = callbacks;
    options_ = opts;

    if (!parseWsUrl(url, host_, port_, target_)) {
        CV_LOG_DEBUG(kTag, "Invalid URL or non-ws scheme");
        return false;
    }

    connect_promise_ = std::promise<bool>();
    auto fut = connect_promise_.get_future();

    resolver_.async_resolve(host_, port_, beast::bind_front_handler(&Impl::on_resolve, this));
    io_thread_ = std::thread([this]() { ioc_.run(); });

    const auto status = fut.wait_for(std::chrono::milliseconds(options_.connectTimeoutMs));
    if (status == std::future_status::timeout) {
        CV_LOG_DEBUG(kTag, "Connection timed out");
        net::post(ioc_, [this]() {
            beast::error_code ec;
            resolver_.cancel();
            ws_.next_layer().cancel(ec);
        });
        return false;
    }

    bool ok = false;
    try { ok = fut.get(); } catch (...) { ok = false; }
    CV_LOG_DEBUG(kTag, "connect() result=" << (ok ? "success" : "failure"));
    return ok;
}

void Client::Impl::fail_connection(beast::error_code ec, const char* where) {
    CV_LOG_DEBUG(kTag, "fail_connection at " << where << ": " << ec.message());
    if (handler_.onError) handler_.onError(std::string(where) + ": " + ec.message());
    try { connect_promise_.set_value(false); } catch (const std::future_error&) {}
}

void Client::Impl::on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) { fail_connection(ec, "resolve"); return; }
    CV_LOG_DEBUG(kTag, "Resolved " << host_ << ":" << port_);
    net::async_connect(ws_.next_layer(), results, beast::bind_front_handler(&Impl::on_connect, this));
}

void Client::Impl::on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type endpoint) {
    if (ec) { fail_connection(ec, "connect"); return; }

    remote_address_ = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    CV_LOG_DEBUG(kTag, "Connected to " << remote_address_ << ", handshake...");

    // TCP_NODELAY
    beast::error_code ec2;
    ws_.next_layer().set_option(tcp::no_delay(options_.noDelay), ec2);

    // Optional permessage-deflate
    if (options_.enableCompression) {
        websocket::permessage_deflate pmd;
        pmd.client_enable = true;
        pmd.server_enable = true;
        ws_.set_option(pmd);
    }

    // Handshake headers and subprotocols
    if (!options_.headers.empty() || !options_.subprotocols.empty()) {
        ws_.set_option(websocket::stream_base::decorator(
            [this](websocket::request_type& req){
                for (const auto& kv : options_.headers) {
                    req.set(http::string_to_field(kv.first), kv.second);
                }
                if (!options_.subprotocols.empty()) {
                    std::string joined;
                    for (size_t i = 0; i < options_.subprotocols.size(); ++i) {
                        if (i) joined += ", ";
                        joined += options_.subprotocols[i];
                    }
                    req.set(http::field::sec_websocket_protocol, joined);
                }
            }
        ));
    }

    ws_.async_handshake(host_, target_, beast::bind_front_handler(&Impl::on_handshake, this));
}

void Client::Impl::on_handshake(beast::error_code ec) {
    if (ec) { fail_connection(ec, "handshake"); return; }
    CV_LOG_DEBUG(kTag, "Handshake OK");
    is_open_.store(true, std::memory_order_relaxed);
    try { connect_promise_.set_value(true); } catch (const std::future_error&) {}

    if (handler_.onOpen) handler_.onOpen();
    do_read();
}

void Client::Impl::do_read() {
    ws_.async_read(buffer_, beast::bind_front_handler(&Impl::on_read, this));
}

void Client::Impl::on_read(beast::error_code ec, std::size_t) {
    if (ec) {
        if (ec == websocket::error::closed) {
            // Peer-initiated close; surface code and reason if available.
            websocket::close_reason cr = ws_.reason();
            const auto code = static_cast<WsCloseCode>(static_cast<unsigned>(cr.code));
            if (handler_.onClosed) {
                handler_.onClosed(code, std::string(cr.reason.data(), cr.reason.size()));
            }
            is_open_.store(false, std::memory_order_relaxed);
        } else {
            CV_LOG_DEBUG(kTag, "read error: " << ec.message());
            if (handler_.onError) handler_.onError(std::string("read: ") + ec.message());
            is_open_.store(false, std::memory_order_relaxed);
        }
        return;
    }

    const void* ptr = buffer_.data().data();
    const size_t len = buffer_.size();
    const bool binary = !ws_.got_text();

    if (handler_.onMessage) handler_.onMessage(ptr, len, binary);

    buffer_.consume(buffer_.size());
    do_read();
}

void Client::Impl::close(WsCloseCode code, const std::string& reason) {
    CV_LOG_DEBUG(kTag, "close(" << static_cast<unsigned>(code) << ", \"" << reason << "\")");
    if (!is_open_.load(std::memory_order_relaxed)) return;

    last_close_code_.store(code, std::memory_order_relaxed);
    last_close_reason_ = reason;

    net::dispatch(strand_, [this]() {
        if (this->is_open_.exchange(false)) {
            websocket::close_reason cr;
            cr.code = static_cast<websocket::close_code>(this->last_close_code_.load());
            cr.reason = this->last_close_reason_;
            this->ws_.async_close(cr, beast::bind_front_handler(&Impl::on_close, this));
        }
    });
}

void Client::Impl::on_close(beast::error_code ec) {
    if (ec) {
        CV_LOG_DEBUG(kTag, "close error: " << ec.message());
        if (handler_.onError) handler_.onError(std::string("close: ") + ec.message());
        return;
    }
    if (handler_.onClosed) {
        handler_.onClosed(last_close_code_.load(), last_close_reason_);
    }
}

bool Client::Impl::send(const void* data, size_t size, bool binary) {
    if (!isOpen()) return false;

    const size_t cur = queued_bytes_.load(std::memory_order_relaxed);
    const size_t lim = write_queue_limit_bytes_.load(std::memory_order_relaxed);
    if (cur + size > lim) {
        CV_LOG_DEBUG(kTag, "backpressure: queued=" << cur << " + size=" << size << " > limit=" << lim);
        return false;
    }

    auto buffer = std::make_shared<std::vector<uint8_t>>(
        static_cast<const uint8_t*>(data),
        static_cast<const uint8_t*>(data) + size
    );
    queued_bytes_.fetch_add(size, std::memory_order_relaxed);

    net::post(strand_, [this, buffer, binary]() {
        this->write_queue_.push_back({ buffer, binary });
        if (this->write_queue_.size() > 1) return;
        this->do_write();
    });
    return true;
}

void Client::Impl::do_write() {
    if (write_queue_.empty()) return;
    ws_.binary(write_queue_.front().is_binary);
    ws_.async_write(net::buffer(*write_queue_.front().buffer),
                    beast::bind_front_handler(&Impl::on_write, this));
}

void Client::Impl::on_write(beast::error_code ec, std::size_t /*bytes_transferred*/) {
    if (!write_queue_.empty()) {
        queued_bytes_.fetch_sub(write_queue_.front().buffer->size(), std::memory_order_relaxed);
    }

    if (ec) {
        CV_LOG_DEBUG(kTag, "write error: " << ec.message());
        if (handler_.onError) handler_.onError(std::string("write: ") + ec.message());
        is_open_.store(false, std::memory_order_relaxed);
        return;
    }

    if (!write_queue_.empty()) {
        write_queue_.erase(write_queue_.begin());
    }
    if (!write_queue_.empty()) do_write();
}

// -----------------------------------------------------------------------------
// Client API (PIMPL forwarding)
// -----------------------------------------------------------------------------

Client::Client() : pimpl(nullptr) {}
Client::~Client() = default;

bool Client::connect(const std::string& url, const WebSocketHandler& callbacks, const WebSocketClientOptions& opts) {
    if (pimpl) return false;
    pimpl.reset(new Impl());
    if (pimpl->connect(url, callbacks, opts)) return true;
    pimpl.reset();
    return false;
}

void Client::close(WsCloseCode code, const std::string& reason) { if (pimpl) pimpl->close(code, reason); }
bool Client::send(const void* data, size_t size, bool binary) { return pimpl ? pimpl->send(data, size, binary) : false; }
bool Client::isOpen() const { return pimpl ? pimpl->isOpen() : false; }
std::string Client::remoteAddress() const { return pimpl ? pimpl->remoteAddress() : std::string(); }

Client::Client(Client&&) noexcept = default;
Client& Client::operator=(Client&&) noexcept = default;

void Client::setWriteQueueLimit(size_t bytes) { if (pimpl) pimpl->setWriteQueueLimit(bytes); }
size_t Client::queuedBytes() const { return pimpl ? pimpl->queuedBytes() : 0; }
void Client::setNoDelay(bool on) { if (pimpl) pimpl->setNoDelay(on); }
void Client::enableCompression(bool on) { if (pimpl) pimpl->enableCompression(on); }

std::unique_ptr<Client> createWebSocketClient() {
    struct ClientCtor : public Client { ClientCtor() : Client() {} };
    return std::unique_ptr<Client>(new ClientCtor());
}

// -----------------------------------------------------------------------------
// Minimal HTTP GET (HTTP only; no TLS)
// -----------------------------------------------------------------------------

static bool parseHttpUrl(const std::string& url, std::string& host, std::string& port, std::string& target) {
    try {
        const auto pos = url.find("://");
        if (pos == std::string::npos) return false;
        const auto scheme = url.substr(0, pos);
        if (scheme != "http") return false;
        std::string rest = url.substr(pos + 3);
        auto slash = rest.find('/');
        target = (slash == std::string::npos) ? "/" : rest.substr(slash);
        const std::string authority = (slash == std::string::npos) ? rest : rest.substr(0, slash);
        split_host_port(authority, host, port);
        if (port.empty()) port = "80";
        return !host.empty() && !port.empty();
    } catch (...) {
        return false;
    }
}

HttpResponse httpGet(const std::string& url, const HttpRequestOptions& opts) {
    CV_LOG_DEBUG(kTag, "HTTP GET " << url);

    HttpResponse out;
    std::string host, port, target;
    if (!parseHttpUrl(url, host, port, target)) {
        CV_LOG_DEBUG(kTag, "Invalid/unsupported URL (http:// only)");
        out.status = -1;
        return out;
    }

    beast::error_code ec;
    net::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);

    auto results = resolver.resolve(host, port, ec);
    if (ec) { out.status = -1; return out; }

    stream.expires_after(std::chrono::milliseconds(std::max(1, opts.timeoutMs)));
    stream.connect(results, ec);
    if (ec) { out.status = -1; return out; }

    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host);
    req.set(http::field::user_agent, std::string("cv-stream-test/") + BOOST_BEAST_VERSION_STRING);
    for (const auto& kv : opts.headers) req.set(http::string_to_field(kv.first), kv.second);

    http::write(stream, req, ec);
    if (ec) { out.status = -1; goto shutdown; }

    {
        beast::flat_buffer buffer;
        http::response_parser<http::string_body> parser;
        parser.body_limit(opts.maxBodyBytes);
        stream.expires_after(std::chrono::milliseconds(std::max(1, opts.timeoutMs)));
        http::read(stream, buffer, parser, ec);
        if (ec) { out.status = -1; goto shutdown; }
        auto res = parser.release();
        out.status = static_cast<int>(res.result_int());
        out.body = std::move(res.body());
        for (const auto& f : res.base()) {
            out.headers.emplace_back(std::string(f.name_string()), std::string(f.value()));
        }
    }

shutdown:
    {
        beast::error_code ec2;
        stream.socket().shutdown(tcp::socket::shutdown_both, ec2);
        (void)ec2;
    }
    return out;
}

} // namespace test
} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_HTTP_BOOST && OCV_BUILD_TESTS
