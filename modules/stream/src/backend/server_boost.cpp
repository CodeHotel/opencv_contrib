#if defined(HAVE_STREAM_BACKEND_BOOST)

#include "opencv2/stream/server.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/write.hpp>
#include <boost/config.hpp>
#include <boost/format.hpp>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <atomic>
#include <future>
#include <sstream>

namespace cv {
namespace stream {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// ==============================
// Forward Declarations & Helpers
// ==============================

class BoostRequest;
class BoostResponse;
class BoostWebSocketSession;
class HttpSession;

struct WeakPtrSessionHash {
    std::size_t operator()(const std::weak_ptr<BoostWebSocketSession>& wp) const {
        auto sp = wp.lock();
        return std::hash<decltype(sp.get())>()(sp.get());
    }
};

struct WeakPtrSessionEqual {
    bool operator()(const std::weak_ptr<BoostWebSocketSession>& a, const std::weak_ptr<BoostWebSocketSession>& b) const {
        return !a.owner_before(b) && !b.owner_before(a);
    }
};

// ==============================
// Request / Response
// ==============================

class BoostRequest final : public Request {
public:
    explicit BoostRequest(const http::request<http::string_body>& req) : req_(req) {}
    std::string getMethod() const override { return std::string(beast::http::to_string(req_.method())); }
    std::string getPath() const override { return std::string(req_.target()); }
    bool isWebSocketUpgrade() const override { return websocket::is_upgrade(req_); }
    const http::request<http::string_body>& raw() const { return req_; }
private:
    const http::request<http::string_body>& req_;
};

class BoostResponse final : public Response {
public:
    BoostResponse(std::shared_ptr<HttpSession> session, http::request<http::string_body>&& req);
    ~BoostResponse() override;
    void setStatusCode(int code) override { res_.result(static_cast<http::status>(code)); }
    void setHeader(const std::string& key, const std::string& value) override { res_.set(key, value); }
    bool write(const char* data, size_t size) override;
    bool acceptWebSocket(const WebSocketHandler& handler, const std::vector<std::string>& subprotocols) override;
private:
    friend class HttpSession;
    std::weak_ptr<HttpSession> session_;
    http::request<http::string_body> req_;
    http::response<http::empty_body> res_;
    std::stringstream body_buffer_;
    bool response_sent_{false};
};

// ==============================
// WebSocket Session
// ==============================

class BoostWebSocketSession final : public WebSocketSession, public std::enable_shared_from_this<BoostWebSocketSession>
{
public:
    BoostWebSocketSession(beast::tcp_stream&& stream, std::string path)
        : ws_(std::move(stream)), path_(std::move(path)), strand_(ws_.get_executor()) {}
    ~BoostWebSocketSession() override = default;
    void run(http::request<http::string_body> req, WebSocketHandler handler);
    bool send(const void* data, size_t size, bool binary) override;
    void close(WsCloseCode code, const std::string& reason) override;
    bool isOpen() const override { return is_open_.load(); }
    std::string remoteAddress() const override { return remote_address_; }
    const std::string& path() const { return path_; }
private:
    void on_accept(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void do_write();
    void on_write(beast::error_code ec, std::size_t bytes_transferred);
    void on_close(beast::error_code ec);
    struct QueuedMessage {
        std::shared_ptr<const std::vector<uint8_t>> buffer;
        bool is_binary;
    };
    websocket::stream<beast::tcp_stream> ws_;
    std::string path_;
    WebSocketHandler handler_;
    beast::flat_buffer buffer_;
    net::strand<net::any_io_executor> strand_;
    std::vector<QueuedMessage> queue_;
    std::atomic<bool> is_open_{false};
    std::string remote_address_;
};

// ==============================
// HTTP Session
// ==============================

class HttpSession : public std::enable_shared_from_this<HttpSession> {
public:
    HttpSession(tcp::socket&& socket, Server::ServerImpl* server_impl)
        : stream_(std::move(socket)), server_impl_(server_impl) {}
    void run() { net::dispatch(stream_.get_executor(), beast::bind_front_handler(&HttpSession::do_read, shared_from_this())); }
    beast::tcp_stream& stream() { return stream_; }
    Server::ServerImpl* server_impl() { return server_impl_; }
private:
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes_transferred);
    void handle_request();
    beast::tcp_stream stream_;
    beast::flat_buffer buffer_;
    Server::ServerImpl* server_impl_;
    http::request<http::string_body> req_;
};

// ==============================
// Server::ServerImpl
// ==============================

class Server::ServerImpl {
public:
    ServerImpl() : ioc_(), acceptor_(ioc_) {}
    ~ServerImpl() { stop(); }
    bool start(int port, int num_threads);
    void stop();
    bool isRunning() const { return running_.load(); }
    void registerEndpoint(const std::string& path, const RequestHandler& handler);
    void unregisterEndpoint(const std::string& path);
    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler);
    void unregisterWebSocketEndpoint(const std::string& path);
    size_t broadcast(const std::string& path, const void* data, size_t size, bool binary);
    void forEachWebSocket(const std::string& path, const WebSocketVisitor& fn);
    size_t numWebSocketClients(const std::string& path) const;
    RequestHandler findHttpHandler(const std::string& path);
    // FIX: Change return type to indicate success/failure of lookup.
    std::pair<WebSocketHandler, bool> findWebSocketHandler(const std::string& path);
    void addWebSocketSession(const std::shared_ptr<BoostWebSocketSession>& session);
    void removeWebSocketSession(const std::shared_ptr<BoostWebSocketSession>& session);
private:
    void do_accept();
    void on_accept(beast::error_code ec, tcp::socket socket);
    net::io_context ioc_;
    tcp::acceptor acceptor_;
    std::vector<std::thread> threads_;
    std::atomic<bool> running_{false};
    mutable std::mutex http_mutex_;
    std::unordered_map<std::string, RequestHandler> http_routes_;
    mutable std::mutex ws_handler_mutex_;
    std::unordered_map<std::string, WebSocketHandler> ws_handlers_;
    using SessionSet = std::unordered_set<std::weak_ptr<BoostWebSocketSession>, WeakPtrSessionHash, WeakPtrSessionEqual>;
    mutable std::mutex ws_mutex_;
    std::unordered_map<std::string, SessionSet> ws_sessions_;
};

// ==============================
// Method Implementations
// ==============================

BoostResponse::BoostResponse(std::shared_ptr<HttpSession> session, http::request<http::string_body>&& req)
    : session_(session), req_(std::move(req)) {}

BoostResponse::~BoostResponse() {
    if (response_sent_) return;
    auto session = session_.lock();
    if (!session) return;
    http::response<http::string_body> final_res;
    final_res.result(res_.result());
    for(auto const& field : res_) {
        final_res.set(field.name(), field.value());
    }
    final_res.version(req_.version());
    final_res.keep_alive(req_.keep_alive());
    final_res.body() = body_buffer_.str();
    final_res.prepare_payload();
    final_res.set(http::field::server, "OpenCV-Stream-Server/Boost");
    try {
        http::write(session->stream(), final_res);
    } catch(...) {}
}

bool BoostResponse::write(const char* data, size_t size) {
    if (size > 0) {
        body_buffer_.write(data, size);
    }
    return true;
}

bool BoostResponse::acceptWebSocket(const WebSocketHandler& handler, const std::vector<std::string>&) {
    auto http_session = session_.lock();
    if (!http_session) return false;
    response_sent_ = true;
    auto ws_session = std::make_shared<BoostWebSocketSession>(
        std::move(http_session->stream()),
        std::string(req_.target()));
    http_session->server_impl()->addWebSocketSession(ws_session);
    ws_session->run(std::move(req_), handler);
    return true;
}

void BoostWebSocketSession::run(http::request<http::string_body> req, WebSocketHandler handler) {
    handler_ = std::move(handler);
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
    ws_.set_option(websocket::stream_base::decorator(
        [](websocket::response_type& res) { res.set(http::field::server, "OpenCV-Stream-Server/Boost"); }));
    ws_.async_accept(req, beast::bind_front_handler(&BoostWebSocketSession::on_accept, shared_from_this()));
}

bool BoostWebSocketSession::send(const void* data, size_t size, bool binary) {
    if (!isOpen()) return false;
    auto buffer = std::make_shared<std::vector<uint8_t>>(static_cast<const uint8_t*>(data), static_cast<const uint8_t*>(data) + size);
    net::post(strand_, [self = shared_from_this(), buffer, binary]() {
        self->queue_.push_back({ buffer, binary });
        if (self->queue_.size() > 1) return;
        self->do_write();
    });
    return true;
}

void BoostWebSocketSession::close(WsCloseCode code, const std::string& reason) {
    net::dispatch(strand_, [self = shared_from_this(), code, reason]() {
        if (self->is_open_.exchange(false)) {
            self->ws_.async_close({static_cast<websocket::close_code>(code), reason},
                beast::bind_front_handler(&BoostWebSocketSession::on_close, self));
        }
    });
}

void BoostWebSocketSession::on_accept(beast::error_code ec) {
    if (ec) { if (handler_.onError) handler_.onError(*this, ec.value(), "accept"); return; }
    try {
        auto ep = ws_.next_layer().socket().remote_endpoint();
        remote_address_ = ep.address().to_string() + ":" + std::to_string(ep.port());
    } catch(...) {}
    is_open_ = true;
    if (handler_.onOpen) handler_.onOpen(*this);
    do_read();
}

void BoostWebSocketSession::do_read() {
    ws_.async_read(buffer_, beast::bind_front_handler(&BoostWebSocketSession::on_read, shared_from_this()));
}

void BoostWebSocketSession::on_read(beast::error_code ec, std::size_t) {
    if (ec == websocket::error::closed || ec == http::error::end_of_stream) {
        if (is_open_.exchange(false) && handler_.onClose) {
           handler_.onClose(*this, static_cast<int>(WsCloseCode::Normal), "client closed");
        }
        return;
    }
    if (ec) {
        if (is_open_.exchange(false)) {
            if (handler_.onError) handler_.onError(*this, ec.value(), "read");
            if (handler_.onClose) handler_.onClose(*this, static_cast<int>(WsCloseCode::AbnormalClosure), ec.message());
        }
        return;
    }
    if (handler_.onMessage) {
        handler_.onMessage(*this, static_cast<const uint8_t*>(buffer_.data().data()), buffer_.size(), ws_.binary());
    }
    buffer_.consume(buffer_.size());
    do_read();
}

void BoostWebSocketSession::do_write() {
    if (queue_.empty()) return;
    ws_.binary(queue_.front().is_binary);
    ws_.async_write(net::buffer(*queue_.front().buffer), beast::bind_front_handler(&BoostWebSocketSession::on_write, shared_from_this()));
}

void BoostWebSocketSession::on_write(beast::error_code ec, std::size_t) {
    if (ec) {
        if (is_open_.exchange(false) && handler_.onError) handler_.onError(*this, ec.value(), "write");
        return;
    }
    queue_.erase(queue_.begin());
    if (!queue_.empty()) do_write();
}

void BoostWebSocketSession::on_close(beast::error_code ec) {
    if (ec && handler_.onError) handler_.onError(*this, ec.value(), "close");
}

void HttpSession::do_read() {
    req_ = {};
    stream_.expires_after(std::chrono::seconds(30));
    http::async_read(stream_, buffer_, req_, beast::bind_front_handler(&HttpSession::on_read, shared_from_this()));
}

void HttpSession::on_read(beast::error_code ec, std::size_t) {
    if (ec == http::error::end_of_stream) { stream_.socket().shutdown(tcp::socket::shutdown_send, ec); return; }
    if (ec) return;
    handle_request();
}

void HttpSession::handle_request() {
    if (websocket::is_upgrade(req_)) {
        // FIX: Use the new findWebSocketHandler that returns a pair.
        auto handler_pair = server_impl_->findWebSocketHandler(std::string(req_.target()));
        // FIX: Check the boolean flag, not the optional onOpen callback.
        if (handler_pair.second) {
            auto response_ptr = std::make_shared<BoostResponse>(shared_from_this(), std::move(req_));
            response_ptr->acceptWebSocket(handler_pair.first, {});
        } else {
            http::response<http::string_body> res{http::status::not_found, req_.version()};
            res.keep_alive(false);
            res.body() = "WebSocket endpoint not found.";
            res.prepare_payload();
            http::write(stream_, res);
            beast::error_code ec;
            stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
        }
        return;
    }

    bool const keep_alive = req_.keep_alive();
    BoostRequest breq(req_);
    std::string path(breq.getPath());
    auto response_ptr = std::make_shared<BoostResponse>(shared_from_this(), std::move(req_));
    RequestHandler http_handler = server_impl_->findHttpHandler(path);
    if (http_handler) {
        try {
            http_handler(breq, *response_ptr);
        } catch (...) {
            response_ptr->response_sent_ = true;
            http::response<http::string_body> res{http::status::internal_server_error, 11};
            res.set(http::field::server, "OpenCV-Stream-Server/Boost");
            res.set(http::field::content_type, "text/plain");
            res.keep_alive(keep_alive);
            res.body() = "Internal Server Error in request handler.";
            res.prepare_payload();
            http::write(stream_, res);
        }
    } else {
        response_ptr->setStatusCode(404);
        response_ptr->setHeader("Content-Type", "text/plain");
        const std::string body = "Resource not found.";
        response_ptr->write(body.c_str(), body.length());
    }

    if (keep_alive) {
        do_read();
    } else {
        beast::error_code ec;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
    }
}

bool Server::ServerImpl::start(int port, int num_threads) {
    if (running_.load()) return true;
    auto setup_promise = std::make_shared<std::promise<bool>>();
    auto setup_future = setup_promise->get_future();
    net::post(ioc_, [this, port, setup_promise]() {
        beast::error_code ec;
        auto const address = net::ip::make_address("0.0.0.0");
        auto endpoint = tcp::endpoint{address, static_cast<unsigned short>(port)};
        acceptor_.open(endpoint.protocol(), ec);
        if(ec) { setup_promise->set_value(false); return; }
        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        if(ec) { setup_promise->set_value(false); return; }
        acceptor_.bind(endpoint, ec);
        if(ec) { setup_promise->set_value(false); return; }
        acceptor_.listen(net::socket_base::max_listen_connections, ec);
        if(ec) { setup_promise->set_value(false); return; }
        do_accept();
        setup_promise->set_value(true);
    });
    threads_.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        threads_.emplace_back([this] { ioc_.run(); });
    }
    bool success = setup_future.get();
    if (success) {
        running_ = true;
    } else {
        ioc_.stop();
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
        if(ioc_.stopped()) ioc_.restart();
    }
    return success;
}

void Server::ServerImpl::stop() {
    if (!running_.exchange(false)) return;
    net::post(ioc_, [this]() {
        acceptor_.close();
        std::lock_guard<std::mutex> lock(ws_mutex_);
        for(auto const& pair : ws_sessions_) {
            for(const auto& s_weak : pair.second) {
                if (auto s = s_weak.lock()) {
                    s->close(WsCloseCode::GoingAway, "Server shutdown");
                }
            }
        }
        ws_sessions_.clear();
    });
    if (!ioc_.stopped()) {
        ioc_.stop();
    }
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
    if(ioc_.stopped()) ioc_.restart();
}

void Server::ServerImpl::registerEndpoint(const std::string& path, const RequestHandler& handler) { std::lock_guard<std::mutex> lock(http_mutex_); http_routes_[path] = handler; }
void Server::ServerImpl::unregisterEndpoint(const std::string& path) { std::lock_guard<std::mutex> lock(http_mutex_); http_routes_.erase(path); }
void Server::ServerImpl::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) { std::lock_guard<std::mutex> lock(ws_handler_mutex_); ws_handlers_[path] = handler; }
void Server::ServerImpl::unregisterWebSocketEndpoint(const std::string& path) { std::lock_guard<std::mutex> lock(ws_handler_mutex_); ws_handlers_.erase(path); }

size_t Server::ServerImpl::broadcast(const std::string& path, const void* data, size_t size, bool binary) {
    std::lock_guard<std::mutex> lock(ws_mutex_);
    auto it = ws_sessions_.find(path);
    if (it == ws_sessions_.end()) return 0;
    size_t count = 0;
    auto& sessions = it->second;
    for (auto iter = sessions.begin(); iter != sessions.end(); ) {
        if (auto session = iter->lock()) {
            session->send(data, size, binary);
            ++count;
            ++iter;
        } else { iter = sessions.erase(iter); }
    }
    return count;
}

void Server::ServerImpl::forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) {
    std::vector<std::shared_ptr<BoostWebSocketSession>> sessions_copy;
    {
        std::lock_guard<std::mutex> lock(ws_mutex_);
        auto it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) return;
        sessions_copy.reserve(it->second.size());
        for (const auto& weak_s : it->second) {
            if (auto shared_s = weak_s.lock()) { sessions_copy.push_back(shared_s); }
        }
    }
    for (const auto& s : sessions_copy) fn(*s);
}

size_t Server::ServerImpl::numWebSocketClients(const std::string& path) const {
    std::lock_guard<std::mutex> lock(ws_mutex_);
    auto it = ws_sessions_.find(path);
    return it == ws_sessions_.end() ? 0 : it->second.size();
}

RequestHandler Server::ServerImpl::findHttpHandler(const std::string& path) { std::lock_guard<std::mutex> lock(http_mutex_); auto it = http_routes_.find(path); return (it != http_routes_.end()) ? it->second : nullptr; }

// FIX: Implement the new return type.
std::pair<WebSocketHandler, bool> Server::ServerImpl::findWebSocketHandler(const std::string& path) {
    std::lock_guard<std::mutex> lock(ws_handler_mutex_);
    auto it = ws_handlers_.find(path);
    if (it != ws_handlers_.end()) {
        return {it->second, true};
    }
    return {WebSocketHandler{}, false};
}

void Server::ServerImpl::addWebSocketSession(const std::shared_ptr<BoostWebSocketSession>& session) { std::lock_guard<std::mutex> lock(ws_mutex_); ws_sessions_[session->path()].insert(session); }
void Server::ServerImpl::removeWebSocketSession(const std::shared_ptr<BoostWebSocketSession>& session) {
    std::lock_guard<std::mutex> lock(ws_mutex_);
    auto it = ws_sessions_.find(session->path());
    if (it != ws_sessions_.end()) {
        it->second.erase(session);
        if (it->second.empty()) { ws_sessions_.erase(it); }
    }
}

void Server::ServerImpl::do_accept() {
    acceptor_.async_accept(net::make_strand(ioc_), beast::bind_front_handler(&ServerImpl::on_accept, this));
}

void Server::ServerImpl::on_accept(beast::error_code ec, tcp::socket socket) {
    if (!ec) {
        std::make_shared<HttpSession>(std::move(socket), this)->run();
    }
    if (acceptor_.is_open()) {
        do_accept();
    }
}

Server::Server() : pimpl(new ServerImpl) {}
Server::~Server() = default;
bool Server::start(int port, int num_threads) { return pimpl->start(port, num_threads); }
void Server::stop() { pimpl->stop(); }
bool Server::isRunning() const { return pimpl->isRunning(); }
void Server::registerEndpoint(const std::string& path, const RequestHandler& handler) { pimpl->registerEndpoint(path, handler); }
void Server::unregisterEndpoint(const std::string& path) { pimpl->unregisterEndpoint(path); }
void Server::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) { pimpl->registerWebSocketEndpoint(path, handler); }
void Server::unregisterWebSocketEndpoint(const std::string& path) { pimpl->unregisterWebSocketEndpoint(path); }
size_t Server::broadcast(const std::string& path, const void* data, size_t size, bool binary) { return pimpl->broadcast(path, data, size, binary); }
void Server::forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) { pimpl->forEachWebSocket(path, fn); }
size_t Server::numWebSocketClients(const std::string& path) const { return pimpl->numWebSocketClients(path); }
Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;
std::unique_ptr<Server> createServer() { return std::unique_ptr<Server>(new Server()); }

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_BACKEND_BOOST
