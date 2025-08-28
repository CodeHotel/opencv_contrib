#if defined(HAVE_STREAM_HTTP_BOOST) && defined(OCV_BUILD_TESTS)

#include "opencv2/stream/client.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/http.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sstream>

namespace cv {
namespace stream {

namespace beast = boost::beast;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// ==============================
// Client::Impl
// ==============================

class Client::Impl : public WebSocketSession {
public:
    Impl() : resolver_(ioc_), ws_(ioc_) {}

    ~Impl() override {
        if (isOpen()) {
            close(WsCloseCode::GoingAway, "Client object destroyed");
        }
        if (io_thread_.joinable()) {
            ioc_.stop();
            io_thread_.join();
        }
    }

    bool connect(const std::string& url, const WebSocketHandler& callbacks, const WebSocketClientOptions& opts);
    void close(WsCloseCode code, const std::string& reason) override;
    bool send(const void* data, size_t size, bool binary) override;
    bool isOpen() const override { return is_open_.load(); }
    std::string remoteAddress() const override { return remote_address_; }

private:
    bool parseUrl(const std::string& url, std::string& host, std::string& port, std::string& target);
    void on_resolve(beast::error_code ec, tcp::resolver::results_type results);
    void on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type endpoint);
    void on_handshake(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void do_write();
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
    void on_close(beast::error_code ec);
    void fail_connection(beast::error_code ec, const char* what);

    struct QueuedMessage {
        std::shared_ptr<const std::vector<uint8_t>> buffer;
        bool is_binary;
    };

    net::io_context ioc_;
    tcp::resolver resolver_;
    websocket::stream<tcp::socket> ws_;
    beast::flat_buffer buffer_;
    net::strand<net::any_io_executor> strand_{ioc_.get_executor()};
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
};

// ==============================
// Client::Impl Method Implementations
// ==============================

bool Client::Impl::parseUrl(const std::string& url, std::string& host, std::string& port, std::string& target) {
    try {
        std::string temp_url = url;
        std::string protocol;
        size_t protocol_end = temp_url.find("://");
        if (protocol_end == std::string::npos) return false;
        protocol = temp_url.substr(0, protocol_end);
        if (protocol != "ws" && protocol != "wss") return false;
        std::string rest = temp_url.substr(protocol_end + 3);
        size_t path_start = rest.find('/');
        target = (path_start == std::string::npos) ? "/" : rest.substr(path_start);
        host = (path_start == std::string::npos) ? rest : rest.substr(0, path_start);
        size_t port_start = host.find(':');
        if (port_start != std::string::npos) {
            port = host.substr(port_start + 1);
            host = host.substr(0, port_start);
        } else {
            port = (protocol == "wss") ? "443" : "80";
        }
        return !host.empty() && !port.empty();
    } catch (...) {
        return false;
    }
}

bool Client::Impl::connect(const std::string& url, const WebSocketHandler& callbacks, const WebSocketClientOptions& opts) {
    LOG_DEBUG("Client", "connect() called for URL: " << url);
    handler_ = callbacks;
    options_ = opts;
    if (!parseUrl(url, host_, port_, target_)) {
        LOG_DEBUG("Client", "URL parsing failed.");
        return false;
    }

    connect_promise_ = std::promise<bool>();
    auto connect_future = connect_promise_.get_future();

    LOG_DEBUG("Client", "Resolving " << host_ << ":" << port_);
    resolver_.async_resolve(host_, port_, beast::bind_front_handler(&Impl::on_resolve, this));
    io_thread_ = std::thread([this]() { ioc_.run(); });

    auto status = connect_future.wait_for(std::chrono::milliseconds(options_.connectTimeoutMs));
    if (status == std::future_status::timeout) {
        LOG_DEBUG("Client", "Connection timed out.");
        net::post(ioc_, [this](){ resolver_.cancel(); });
        return false;
    }

    bool result = connect_future.get();
    LOG_DEBUG("Client", "connect() finished with result: " << (result ? "success" : "failure"));
    return result;
}

void Client::Impl::fail_connection(beast::error_code ec, const char* what) {
    LOG_DEBUG("Client", "fail_connection() what: " << what << ", error: " << ec.message());
    if (handler_.onError) {
        handler_.onError(*this, ec.value(), what);
    }
    try { connect_promise_.set_value(false); } catch (const std::future_error&) {}
}

void Client::Impl::on_resolve(beast::error_code ec, tcp::resolver::results_type results) {
    if (ec) { fail_connection(ec, "resolve"); return; }
    LOG_DEBUG("Client", "on_resolve succeeded. Connecting...");
    net::async_connect(ws_.next_layer(), results, beast::bind_front_handler(&Impl::on_connect, this));
}

void Client::Impl::on_connect(beast::error_code ec, tcp::resolver::results_type::endpoint_type endpoint) {
    if (ec) { fail_connection(ec, "connect"); return; }
    remote_address_ = endpoint.address().to_string() + ":" + std::to_string(endpoint.port());
    LOG_DEBUG("Client", "on_connect succeeded to " << remote_address_ << ". Starting handshake...");
    ws_.next_layer().set_option(tcp::no_delay(options_.noDelay));
    ws_.async_handshake(host_, target_, beast::bind_front_handler(&Impl::on_handshake, this));
}

void Client::Impl::on_handshake(beast::error_code ec) {
    if (ec) { fail_connection(ec, "handshake"); return; }
    LOG_DEBUG("Client", "on_handshake succeeded.");
    is_open_ = true;
    if (handler_.onOpen) {
        handler_.onOpen(*this);
    }
    do_read();
    try { connect_promise_.set_value(true); } catch (const std::future_error&) {}
}

void Client::Impl::do_read() {
    ws_.async_read(buffer_, beast::bind_front_handler(&Impl::on_read, this));
}

void Client::Impl::on_read(beast::error_code ec, std::size_t) {
    if (ec) {
        LOG_DEBUG("Client", "on_read failed: " << ec.message());
        if (is_open_.exchange(false)) {
            if (handler_.onError) handler_.onError(*this, ec.value(), "read");
        }
        return;
    }
    LOG_DEBUG("Client", "on_read received " << buffer_.size() << " bytes.");
    if (handler_.onMessage) {
        handler_.onMessage(*this, static_cast<const uint8_t*>(buffer_.data().data()), buffer_.size(), ws_.binary());
    }
    buffer_.consume(buffer_.size());
    do_read();
}

void Client::Impl::close(WsCloseCode code, const std::string& reason) {
    LOG_DEBUG("Client", "close() called with code " << static_cast<int>(code));
    if (!is_open_.load()) return;
    net::dispatch(strand_, [this, code, reason]() {
        if (this->is_open_.exchange(false)) {
            this->ws_.async_close({static_cast<websocket::close_code>(code), reason},
                beast::bind_front_handler(&Impl::on_close, this));
        }
    });
}

void Client::Impl::on_close(beast::error_code ec) {
    LOG_DEBUG("Client", "on_close called. ec: " << ec.message());
    if (ec && handler_.onError) {
        handler_.onError(*this, ec.value(), "close");
    }
}

bool Client::Impl::send(const void* data, size_t size, bool binary) {
    if (!isOpen()) return false;
    auto buffer = std::make_shared<std::vector<uint8_t>>(static_cast<const uint8_t*>(data), static_cast<const uint8_t*>(data) + size);
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

void Client::Impl::on_write(beast::error_code ec, std::size_t) {
    if (ec) {
        LOG_DEBUG("Client", "on_write failed: " << ec.message());
        if (is_open_.exchange(false)) {
            if (handler_.onError) handler_.onError(*this, ec.value(), "write");
        }
        return;
    }
    write_queue_.erase(write_queue_.begin());
    if (!write_queue_.empty()) do_write();
}

// ==============================
// Client API (PIMPL Forwarding)
// ==============================

Client::Client() : pimpl(nullptr) {}
Client::~Client() = default;

bool Client::connect(const std::string& url, const WebSocketHandler& callbacks, const WebSocketClientOptions& opts) {
    if (pimpl) return false;
    pimpl = std::unique_ptr<Impl>(new Impl());
    if (pimpl->connect(url, callbacks, opts)) {
        return true;
    }
    pimpl.reset();
    return false;
}

void Client::close(WsCloseCode code, const std::string& reason) { if (pimpl) pimpl->close(code, reason); }
bool Client::send(const void* data, size_t size, bool binary) { return pimpl ? pimpl->send(data, size, binary) : false; }
bool Client::isOpen() const { return pimpl ? pimpl->isOpen() : false; }
std::string Client::remoteAddress() const { return pimpl ? pimpl->remoteAddress() : ""; }

Client::Client(Client&&) noexcept = default;
Client& Client::operator=(Client&&) noexcept = default;

void Client::setWriteQueueLimit(size_t) {}
size_t Client::queuedBytes() const { return 0; }
void Client::setNoDelay(bool on) { (void)on; }
void Client::enableCompression(bool on) { (void)on; }

std::unique_ptr<Client> createWebSocketClient() {
    struct ClientConstructor : public Client { ClientConstructor() : Client() {} };
    return std::unique_ptr<Client>(new ClientConstructor());
}

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_BACKEND_BOOST && OCV_BUILD_TESTS
