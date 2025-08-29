#if defined(HAVE_STREAM_HTTP_BOOST)

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
#include <chrono>
#include <iomanip>

namespace cv {
namespace stream {

namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// ==============================
// Super-Verbose Logger (thread-safe)
// ==============================
namespace detail {
    static std::mutex& log_mutex() {
        static std::mutex m;
        return m;
    }
    static std::atomic<bool>& log_enabled() {
        static std::atomic<bool> e{true};
        return e;
    }
    inline std::string now_timestamp() {
        using namespace std::chrono;
        auto tp = system_clock::now();
        auto tt = system_clock::to_time_t(tp);
        auto ms = duration_cast<milliseconds>(tp.time_since_epoch()) % 1000;
        std::tm tm{};
#if defined(_WIN32)
        localtime_s(&tm, &tt);
#else
        localtime_r(&tt, &tm);
#endif
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S") << '.' << std::setw(3) << std::setfill('0') << ms.count();
        return oss.str();
    }
    inline void vlog(const char* func, const std::string& msg) {
        if (!log_enabled().load(std::memory_order_relaxed)) return;
        std::lock_guard<std::mutex> lk(log_mutex());
        std::cerr << now_timestamp()
                  << " [tid " << std::this_thread::get_id() << "] "
                  << func << " | " << msg << std::endl;
    }
}

#define VLOG(MSG_STREAM) do { std::ostringstream _vlog_ss__; _vlog_ss__ << MSG_STREAM; ::cv::stream::detail::vlog(__FUNCTION__, _vlog_ss__.str()); } while(0)

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
    explicit BoostRequest(const http::request<http::string_body>& req) : req_(req) {
        VLOG("BoostRequest::ctor created; method=" << beast::http::to_string(req_.method())
             << " target=" << req_.target() << " version=" << req_.version()
             << " keep_alive=" << req_.keep_alive());
    }
    std::string getMethod() const override {
        auto m = std::string(beast::http::to_string(req_.method()));
        VLOG("getMethod -> " << m);
        return m;
    }
    std::string getPath() const override {
        auto p = std::string(req_.target());
        VLOG("getPath -> " << p);
        return p;
    }
    bool isWebSocketUpgrade() const override {
        bool up = websocket::is_upgrade(req_);
        VLOG("isWebSocketUpgrade -> " << (up ? "true" : "false"));
        return up;
    }
    const http::request<http::string_body>& raw() const { return req_; }
private:
    const http::request<http::string_body>& req_;
};

class BoostResponse final : public Response {
public:
    BoostResponse(std::shared_ptr<HttpSession> session, http::request<http::string_body>&& req);
    ~BoostResponse() override;
    void setStatusCode(int code) override {
        VLOG("setStatusCode(" << code << ")");
        res_.result(static_cast<http::status>(code));
    }
    void setHeader(const std::string& key, const std::string& value) override {
        VLOG("setHeader key=" << key << " value=" << value);
        res_.set(key, value);
    }
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
        : ws_(std::move(stream)), path_(std::move(path)), strand_(ws_.get_executor()) {
        VLOG("BoostWebSocketSession::ctor path=" << path_);
    }
    ~BoostWebSocketSession() override {
        VLOG("~BoostWebSocketSession dtor path=" << path_ << " is_open=" << (is_open_.load() ? "true" : "false"));
    }
    void run(http::request<http::string_body> req, WebSocketHandler handler);
    bool send(const void* data, size_t size, bool binary) override;
    void close(WsCloseCode code, const std::string& reason) override;
    bool isOpen() const override { bool v = is_open_.load(); VLOG("isOpen -> " << (v ? "true" : "false")); return v; }
    std::string remoteAddress() const override { VLOG("remoteAddress -> " << remote_address_); return remote_address_; }
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
        : stream_(std::move(socket)), server_impl_(server_impl) {
        VLOG("HttpSession::ctor created; server_impl=" << (void*)server_impl_);
    }
    void run() {
        VLOG("HttpSession::run dispatch do_read");
        net::dispatch(stream_.get_executor(), beast::bind_front_handler(&HttpSession::do_read, shared_from_this()));
    }
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
    ServerImpl() : ioc_(), acceptor_(ioc_) {
        VLOG("ServerImpl::ctor");
    }
    ~ServerImpl() {
        VLOG("ServerImpl::dtor");
        stop();
    }
    bool start(int port, int num_threads);
    void stop();
    bool isRunning() const { bool r = running_.load(); VLOG("isRunning -> " << (r ? "true" : "false")); return r; }
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
    : session_(session), req_(std::move(req)) {
    VLOG("BoostResponse::ctor; keep_alive=" << req_.keep_alive() << " version=" << req_.version());
}

BoostResponse::~BoostResponse() {
    VLOG("~BoostResponse dtor; response_sent_=" << (response_sent_ ? "true" : "false"));
    if (response_sent_) return;
    auto session = session_.lock();
    if (!session) { VLOG("session expired; no write"); return; }
    http::response<http::string_body> final_res;
    final_res.result(res_.result());
    for(auto const& field : res_) {
        final_res.set(field.name(), field.value());
        VLOG("copy header -> " << field.name_string() << ": " << field.value());
    }
    final_res.version(req_.version());
    final_res.keep_alive(req_.keep_alive());
    final_res.body() = body_buffer_.str();
    VLOG("final body size=" << final_res.body().size());
    final_res.prepare_payload();
    final_res.set(http::field::server, "OpenCV-Stream-Server/Boost");
    try {
        VLOG("http::write response begin");
        http::write(session->stream(), final_res);
        VLOG("http::write response end");
    } catch(const std::exception& e) {
        VLOG("http::write threw exception: " << e.what());
    } catch(...) {
        VLOG("http::write threw unknown exception");
    }
}

bool BoostResponse::write(const char* data, size_t size) {
    VLOG("write called size=" << size);
    if (size > 0) {
        body_buffer_.write(data, size);
        VLOG("buffered body size now=" << body_buffer_.tellp());
    }
    return true;
}

bool BoostResponse::acceptWebSocket(const WebSocketHandler& handler, const std::vector<std::string>&) {
    VLOG("acceptWebSocket called");
    auto http_session = session_.lock();
    if (!http_session) { VLOG("acceptWebSocket failed: session expired"); return false; }
    response_sent_ = true;
    auto ws_session = std::make_shared<BoostWebSocketSession>(
        std::move(http_session->stream()),
        std::string(req_.target()));
    VLOG("created BoostWebSocketSession for path=" << std::string(req_.target()));
    http_session->server_impl()->addWebSocketSession(ws_session);
    ws_session->run(std::move(req_), handler);
    return true;
}

void BoostWebSocketSession::run(http::request<http::string_body> req, WebSocketHandler handler) {
    VLOG("WebSocket run; setting options; path=" << path_);
    handler_ = std::move(handler);
    ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
    ws_.set_option(websocket::stream_base::decorator(
        [](websocket::response_type& res) { res.set(http::field::server, "OpenCV-Stream-Server/Boost"); }));
    VLOG("async_accept begin (upgrade)");
    ws_.async_accept(req, beast::bind_front_handler(&BoostWebSocketSession::on_accept, shared_from_this()));
}

bool BoostWebSocketSession::send(const void* data, size_t size, bool binary) {
    VLOG("send called size=" << size << " binary=" << (binary ? "true" : "false") << " isOpen=" << (isOpen() ? "true":"false"));
    if (!isOpen()) return false;
    auto buffer = std::make_shared<std::vector<uint8_t>>(static_cast<const uint8_t*>(data), static_cast<const uint8_t*>(data) + size);
    net::post(strand_, [self = shared_from_this(), buffer, binary]() {
        VLOG("post: enqueue message size=" << buffer->size() << " binary=" << (binary ? "true" : "false") << " queue_size_before=" << self->queue_.size());
        self->queue_.push_back({ buffer, binary });
        if (self->queue_.size() > 1) {
            VLOG("writer already active; return");
            return;
        }
        self->do_write();
    });
    return true;
}

void BoostWebSocketSession::close(WsCloseCode code, const std::string& reason) {
    VLOG("close called code=" << static_cast<int>(code) << " reason=" << reason);
    net::dispatch(strand_, [self = shared_from_this(), code, reason]() {
        VLOG("dispatch close; current is_open=" << (self->is_open_.load() ? "true" : "false"));
        if (self->is_open_.exchange(false)) {
            VLOG("async_close begin");
            self->ws_.async_close({static_cast<websocket::close_code>(code), reason},
                beast::bind_front_handler(&BoostWebSocketSession::on_close, self));
        } else {
            VLOG("close skipped; already closed");
        }
    });
}

void BoostWebSocketSession::on_accept(beast::error_code ec) {
    VLOG("on_accept ec=" << ec.value() << " msg=" << ec.message());
    if (ec) { if (handler_.onError) handler_.onError(*this, ec.value(), "accept"); return; }
    try {
        auto ep = ws_.next_layer().socket().remote_endpoint();
        remote_address_ = ep.address().to_string() + ":" + std::to_string(ep.port());
        VLOG("remote endpoint=" << remote_address_);
    } catch(const std::exception& e) {
        VLOG("remote endpoint exception: " << e.what());
    } catch(...) {
        VLOG("remote endpoint unknown exception");
    }
    is_open_ = true;
    VLOG("WebSocket OPEN path=" << path_);
    if (handler_.onOpen) handler_.onOpen(*this);
    do_read();
}

void BoostWebSocketSession::do_read() {
    VLOG("do_read begin; buffer size currently=" << buffer_.size());
    ws_.async_read(buffer_, beast::bind_front_handler(&BoostWebSocketSession::on_read, shared_from_this()));
}


    inline std::string render_ws_payload(boost::beast::flat_buffer& buf, bool is_binary) {
    // Get a contiguous copy regardless of the underlying buffer sequence
    std::string payload = boost::beast::buffers_to_string(buf.data());

    if (!is_binary) {
        std::string preview;
        preview.reserve(payload.size());
        for (unsigned char ch : payload) {
            preview.push_back((ch >= 32 && ch != 127) ? static_cast<char>(ch) : '.');
        }
        if (preview.size() > 1024) { preview.resize(1024); preview += "…"; }
        std::ostringstream oss;
        oss << "TEXT(" << payload.size() << "B): \"" << preview << '"';
        return oss.str();
    } else {
        constexpr std::size_t kMax = 256;
        std::ostringstream oss;
        oss << std::hex << std::setfill('0');
        oss << "BIN(" << std::dec << payload.size() << "B) [first "
            << std::min(payload.size(), kMax) << " bytes]: ";
        std::size_t limit = std::min(payload.size(), kMax);
        for (std::size_t i = 0; i < limit; ++i) {
            unsigned v = static_cast<unsigned>(static_cast<unsigned char>(payload[i]));
            oss << std::setw(2) << std::hex << v << (i + 1 == limit ? "" : " ");
        }
        if (payload.size() > kMax) oss << " …";
        return oss.str();
    }
}

    void BoostWebSocketSession::on_read(beast::error_code ec, std::size_t bytes_transferred) {
    VLOG("on_read ec=" << ec.value() << " msg=" << ec.message() << " bytes=" << bytes_transferred);
    if (ec == websocket::error::closed || ec == http::error::end_of_stream) {
        VLOG("peer closed or end_of_stream; firing onClose if needed");
        if (is_open_.exchange(false) && handler_.onClose) {
            handler_.onClose(*this, static_cast<int>(WsCloseCode::Normal), "client closed");
        }
        return;
    }
    if (ec) {
        VLOG("read error; signaling onError/onClose");
        if (is_open_.exchange(false)) {
            if (handler_.onError) handler_.onError(*this, ec.value(), "read");
            if (handler_.onClose) handler_.onClose(*this, static_cast<int>(WsCloseCode::AbnormalClosure), ec.message());
        }
        return;
    }

    // --- NEW: pretty-print the inbound client message ---
    bool is_bin = ws_.binary();
    VLOG("message received; " << render_ws_payload(buffer_, is_bin));

    // Deliver to user callback
    // Deliver to user callback
    if (handler_.onMessage) {
        std::string tmp = boost::beast::buffers_to_string(buffer_.data());
        handler_.onMessage(*this,
                           reinterpret_cast<const uint8_t*>(tmp.data()),
                           tmp.size(),
                           ws_.binary());
    }


    // Clear and read next
    buffer_.consume(buffer_.size());
    VLOG("buffer consumed; scheduling next read");
    do_read();
}

void BoostWebSocketSession::do_write() {
    if (queue_.empty()) { VLOG("do_write: queue empty; return"); return; }
    VLOG("do_write: sending front size=" << queue_.front().buffer->size() << " binary=" << (queue_.front().is_binary ? "true":"false") << " queue_size=" << queue_.size());
    ws_.binary(queue_.front().is_binary);
    ws_.async_write(net::buffer(*queue_.front().buffer), beast::bind_front_handler(&BoostWebSocketSession::on_write, shared_from_this()));
}

void BoostWebSocketSession::on_write(beast::error_code ec, std::size_t bytes_transferred) {
    VLOG("on_write ec=" << ec.value() << " msg=" << ec.message() << " bytes=" << bytes_transferred);
    if (ec) {
        VLOG("write error; signaling onError if open");
        if (is_open_.exchange(false) && handler_.onError) handler_.onError(*this, ec.value(), "write");
        return;
    }
    if (!queue_.empty()) {
        VLOG("erase sent message; queue_size_before=" << queue_.size());
        queue_.erase(queue_.begin());
        VLOG("queue_size_after=" << queue_.size());
    } else {
        VLOG("queue unexpectedly empty after write");
    }
    if (!queue_.empty()) {
        VLOG("more to write; recurse do_write");
        do_write();
    } else {
        VLOG("write queue drained");
    }
}

void BoostWebSocketSession::on_close(beast::error_code ec) {
    VLOG("on_close ec=" << ec.value() << " msg=" << ec.message());
    if (ec && handler_.onError) handler_.onError(*this, ec.value(), "close");
}

void HttpSession::do_read() {
    VLOG("HttpSession::do_read reset request and set timeout");
    req_ = {};
    stream_.expires_after(std::chrono::seconds(30));
    VLOG("async_read begin");
    http::async_read(stream_, buffer_, req_, beast::bind_front_handler(&HttpSession::on_read, shared_from_this()));
}

void HttpSession::on_read(beast::error_code ec, std::size_t bytes_transferred) {
    VLOG("on_read ec=" << ec.value() << " msg=" << ec.message() << " bytes=" << bytes_transferred);
    if (ec == http::error::end_of_stream) {
        VLOG("end_of_stream; shutdown send");
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
        return;
    }
    if (ec) { VLOG("read error; returning"); return; }
    VLOG("read ok; handle_request");
    handle_request();
}

void HttpSession::handle_request() {
    VLOG("handle_request; target=" << req_.target() << " method=" << beast::http::to_string(req_.method()) << " keep_alive=" << (req_.keep_alive() ? "true":"false"));
    if (websocket::is_upgrade(req_)) {
        VLOG("WebSocket upgrade detected; find handler");
        // FIX: Use the new findWebSocketHandler that returns a pair.
        auto handler_pair = server_impl_->findWebSocketHandler(std::string(req_.target()));
        VLOG("ws handler found? " << (handler_pair.second ? "yes":"no"));
        // FIX: Check the boolean flag, not the optional onOpen callback.
        if (handler_pair.second) {
            auto response_ptr = std::make_shared<BoostResponse>(shared_from_this(), std::move(req_));
            VLOG("accepting WebSocket");
            response_ptr->acceptWebSocket(handler_pair.first, {});
        } else {
            VLOG("no ws handler; sending 404 for websocket endpoint");
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
    VLOG("HTTP request; keep_alive=" << (keep_alive ? "true" : "false"));
    BoostRequest breq(req_);
    std::string path(breq.getPath());
    VLOG("path=" << path);
    auto response_ptr = std::make_shared<BoostResponse>(shared_from_this(), std::move(req_));
    RequestHandler http_handler = server_impl_->findHttpHandler(path);
    VLOG("http_handler found? " << (http_handler ? "yes":"no"));
    if (http_handler) {
        try {
            VLOG("invoking http_handler");
            http_handler(breq, *response_ptr);
            VLOG("http_handler returned");
        } catch (const std::exception& e) {
            VLOG("handler exception: " << e.what());
            response_ptr->response_sent_ = true;
            http::response<http::string_body> res{http::status::internal_server_error, 11};
            res.set(http::field::server, "OpenCV-Stream-Server/Boost");
            res.set(http::field::content_type, "text/plain");
            res.keep_alive(keep_alive);
            res.body() = "Internal Server Error in request handler.";
            res.prepare_payload();
            http::write(stream_, res);
        } catch (...) {
            VLOG("handler unknown exception");
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
        VLOG("no http handler; emit 404 via deferred destructor");
        response_ptr->setStatusCode(404);
        response_ptr->setHeader("Content-Type", "text/plain");
        const std::string body = "Resource not found.";
        response_ptr->write(body.c_str(), body.length());
    }

    if (keep_alive) {
        VLOG("keep_alive true; scheduling next read");
        do_read();
    } else {
        VLOG("keep_alive false; shutdown send");
        beast::error_code ec;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ec);
    }
}

bool Server::ServerImpl::start(int port, int num_threads) {
    VLOG("start called port=" << port << " threads=" << num_threads << " running=" << (running_.load() ? "true":"false"));
    if (running_.load()) { VLOG("already running; return true"); return true; }
    auto setup_promise = std::make_shared<std::promise<bool>>();
    auto setup_future = setup_promise->get_future();
    net::post(ioc_, [this, port, setup_promise]() {
        VLOG("setup lambda posted to io_context");
        beast::error_code ec;
        auto const address = net::ip::make_address("0.0.0.0");
        auto endpoint = tcp::endpoint{address, static_cast<unsigned short>(port)};
        acceptor_.open(endpoint.protocol(), ec);
        VLOG("acceptor_.open ec=" << ec.value() << " msg=" << ec.message());
        if(ec) { setup_promise->set_value(false); return; }
        acceptor_.set_option(net::socket_base::reuse_address(true), ec);
        VLOG("acceptor_.set_option(reuse_address) ec=" << ec.value() << " msg=" << ec.message());
        if(ec) { setup_promise->set_value(false); return; }
        acceptor_.bind(endpoint, ec);
        VLOG("acceptor_.bind ec=" << ec.value() << " msg=" << ec.message());
        if(ec) { setup_promise->set_value(false); return; }
        acceptor_.listen(net::socket_base::max_listen_connections, ec);
        VLOG("acceptor_.listen ec=" << ec.value() << " msg=" << ec.message());
        if(ec) { setup_promise->set_value(false); return; }
        do_accept();
        VLOG("do_accept scheduled; signaling setup success");
        setup_promise->set_value(true);
    });
    threads_.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i) {
        VLOG("spawning thread #" << i);
        threads_.emplace_back([this, i] {
            VLOG("io_context.run start on worker #" << i);
            ioc_.run();
            VLOG("io_context.run end on worker #" << i);
        });
    }
    VLOG("waiting for setup_future");
    bool success = setup_future.get();
    VLOG("setup_future -> " << (success ? "true" : "false"));
    if (success) {
        running_ = true;
        VLOG("running_ set to true");
    } else {
        VLOG("setup failed; stopping ioc and joining threads");
        ioc_.stop();
        for (auto& t : threads_) {
            if (t.joinable()) t.join();
        }
        threads_.clear();
        if(ioc_.stopped()) {
            VLOG("ioc was stopped; restarting");
            ioc_.restart();
        }
    }
    return success;
}

void Server::ServerImpl::stop() {
    VLOG("stop called; running=" << (running_.load() ? "true":"false"));
    if (!running_.exchange(false)) { VLOG("not running; return"); return; }
    net::post(ioc_, [this]() {
        VLOG("stop lambda: closing acceptor and closing all websockets");
        acceptor_.close();
        std::lock_guard<std::mutex> lock(ws_mutex_);
        for(auto const& pair : ws_sessions_) {
            VLOG("closing session group path=" << pair.first << " count=" << pair.second.size());
            for(const auto& s_weak : pair.second) {
                if (auto s = s_weak.lock()) {
                    VLOG("closing a websocket session path=" << s->path());
                    s->close(WsCloseCode::GoingAway, "Server shutdown");
                } else {
                    VLOG("encountered expired websocket weak_ptr");
                }
            }
        }
        ws_sessions_.clear();
    });
    if (!ioc_.stopped()) {
        VLOG("stopping io_context");
        ioc_.stop();
    }
    for (auto& t : threads_) {
        if (t.joinable()) {
            VLOG("joining a worker thread");
            t.join();
        }
    }
    threads_.clear();
    if(ioc_.stopped()) {
        VLOG("io_context stopped; restarting");
        ioc_.restart();
    }
}

void Server::ServerImpl::registerEndpoint(const std::string& path, const RequestHandler& handler) {
    VLOG("registerEndpoint path=" << path);
    std::lock_guard<std::mutex> lock(http_mutex_); http_routes_[path] = handler;
}
void Server::ServerImpl::unregisterEndpoint(const std::string& path) {
    VLOG("unregisterEndpoint path=" << path);
    std::lock_guard<std::mutex> lock(http_mutex_); http_routes_.erase(path);
}
void Server::ServerImpl::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) {
    VLOG("registerWebSocketEndpoint path=" << path);
    std::lock_guard<std::mutex> lock(ws_handler_mutex_); ws_handlers_[path] = handler;
}
void Server::ServerImpl::unregisterWebSocketEndpoint(const std::string& path) {
    VLOG("unregisterWebSocketEndpoint path=" << path);
    std::lock_guard<std::mutex> lock(ws_handler_mutex_); ws_handlers_.erase(path);
}

size_t Server::ServerImpl::broadcast(const std::string& path, const void* data, size_t size, bool binary) {
    VLOG("broadcast path=" << path << " size=" << size << " binary=" << (binary ? "true":"false"));
    std::lock_guard<std::mutex> lock(ws_mutex_);
    auto it = ws_sessions_.find(path);
    if (it == ws_sessions_.end()) { VLOG("no sessions for path"); return 0; }
    size_t count = 0;
    auto& sessions = it->second;
    for (auto iter = sessions.begin(); iter != sessions.end(); ) {
        if (auto session = iter->lock()) {
            VLOG("sending to a session; remaining=" << (sessions.size() - count));
            session->send(data, size, binary);
            ++count;
            ++iter;
        } else {
            VLOG("expired session found; erasing");
            iter = sessions.erase(iter);
        }
    }
    VLOG("broadcast complete; delivered=" << count);
    return count;
}

void Server::ServerImpl::forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) {
    VLOG("forEachWebSocket path=" << path);
    std::vector<std::shared_ptr<BoostWebSocketSession>> sessions_copy;
    {
        std::lock_guard<std::mutex> lock(ws_mutex_);
        auto it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) { VLOG("no sessions for path"); return; }
        sessions_copy.reserve(it->second.size());
        for (const auto& weak_s : it->second) {
            if (auto shared_s = weak_s.lock()) { sessions_copy.push_back(shared_s); }
        }
    }
    VLOG("invoking visitor on " << sessions_copy.size() << " sessions");
    for (const auto& s : sessions_copy) fn(*s);
}

size_t Server::ServerImpl::numWebSocketClients(const std::string& path) const {
    VLOG("numWebSocketClients path=" << path);
    std::lock_guard<std::mutex> lock(ws_mutex_);
    auto it = ws_sessions_.find(path);
    size_t n = it == ws_sessions_.end() ? 0 : it->second.size();
    VLOG("numWebSocketClients -> " << n);
    return n;
}

RequestHandler Server::ServerImpl::findHttpHandler(const std::string& path) {
    VLOG("findHttpHandler path=" << path);
    std::lock_guard<std::mutex> lock(http_mutex_);
    auto it = http_routes_.find(path);
    bool found = (it != http_routes_.end());
    VLOG("findHttpHandler found=" << (found ? "true":"false"));
    return found ? it->second : nullptr;
}

// FIX: Implement the new return type.
std::pair<WebSocketHandler, bool> Server::ServerImpl::findWebSocketHandler(const std::string& path) {
    VLOG("findWebSocketHandler path=" << path);
    std::lock_guard<std::mutex> lock(ws_handler_mutex_);
    auto it = ws_handlers_.find(path);
    bool found = (it != ws_handlers_.end());
    VLOG("findWebSocketHandler found=" << (found ? "true":"false"));
    if (found) {
        return {it->second, true};
    }
    return {WebSocketHandler{}, false};
}

void Server::ServerImpl::addWebSocketSession(const std::shared_ptr<BoostWebSocketSession>& session) {
    VLOG("addWebSocketSession path=" << session->path());
    std::lock_guard<std::mutex> lock(ws_mutex_);
    ws_sessions_[session->path()].insert(session);
}
void Server::ServerImpl::removeWebSocketSession(const std::shared_ptr<BoostWebSocketSession>& session) {
    VLOG("removeWebSocketSession path=" << session->path());
    std::lock_guard<std::mutex> lock(ws_mutex_);
    auto it = ws_sessions_.find(session->path());
    if (it != ws_sessions_.end()) {
        it->second.erase(session);
        VLOG("erased from set; remaining=" << it->second.size());
        if (it->second.empty()) {
            VLOG("session set empty; erasing path group");
            ws_sessions_.erase(it);
        }
    } else {
        VLOG("no session set for path");
    }
}

void Server::ServerImpl::do_accept() {
    VLOG("do_accept scheduling async_accept");
    acceptor_.async_accept(net::make_strand(ioc_), beast::bind_front_handler(&ServerImpl::on_accept, this));
}

void Server::ServerImpl::on_accept(beast::error_code ec, tcp::socket socket) {
    VLOG("on_accept ec=" << ec.value() << " msg=" << ec.message());
    if (!ec) {
        VLOG("creating HttpSession for new connection");
        std::make_shared<HttpSession>(std::move(socket), this)->run();
    } else {
        VLOG("accept error; not creating session");
    }
    if (acceptor_.is_open()) {
        VLOG("acceptor open; scheduling next accept");
        do_accept();
    } else {
        VLOG("acceptor closed; not accepting further");
    }
}

Server::Server() : pimpl(new ServerImpl) { VLOG("Server::ctor"); }
Server::~Server() = default;
bool Server::start(int port, int num_threads) { VLOG("Server::start"); return pimpl->start(port, num_threads); }
void Server::stop() { VLOG("Server::stop"); pimpl->stop(); }
bool Server::isRunning() const { VLOG("Server::isRunning"); return pimpl->isRunning(); }
void Server::registerEndpoint(const std::string& path, const RequestHandler& handler) { VLOG("Server::registerEndpoint path=" << path); pimpl->registerEndpoint(path, handler); }
void Server::unregisterEndpoint(const std::string& path) { VLOG("Server::unregisterEndpoint path=" << path); pimpl->unregisterEndpoint(path); }
void Server::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) { VLOG("Server::registerWebSocketEndpoint path=" << path); pimpl->registerWebSocketEndpoint(path, handler); }
void Server::unregisterWebSocketEndpoint(const std::string& path) { VLOG("Server::unregisterWebSocketEndpoint path=" << path); pimpl->unregisterWebSocketEndpoint(path); }
size_t Server::broadcast(const std::string& path, const void* data, size_t size, bool binary) { VLOG("Server::broadcast path=" << path << " size=" << size); return pimpl->broadcast(path, data, size, binary); }
void Server::forEachWebSocket(const std::string& path, const WebSocketVisitor& fn) { VLOG("Server::forEachWebSocket path=" << path); pimpl->forEachWebSocket(path, fn); }
size_t Server::numWebSocketClients(const std::string& path) const { VLOG("Server::numWebSocketClients path=" << path); return pimpl->numWebSocketClients(path); }
Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;
std::unique_ptr<Server> createServer() { VLOG("createServer"); return std::unique_ptr<Server>(new Server()); }

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_BACKEND_BOOST
