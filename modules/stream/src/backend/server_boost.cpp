#if defined(HAVE_STREAM_HTTP_BOOST)

#include "opencv2/stream/server.hpp"

#include <opencv2/core/utils/logger.hpp>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <iomanip>

namespace cv {
namespace stream {

namespace beast = boost::beast;
namespace http  = beast::http;
namespace websocket = beast::websocket;
namespace net   = boost::asio;
using tcp = boost::asio::ip::tcp;


    namespace {
        using cv::utils::logging::LogLevel;
        static cv::utils::logging::LogTag kStreamLogTag(
            "cv.stream.server",
            LogLevel::LOG_LEVEL_VERBOSE
        );
        cv::utils::logging::LogTag* kTag = &kStreamLogTag;
    }



// =====================================================================================
// Helpers
// =====================================================================================

template <typename T> static inline std::string to_string(const T& v) { std::ostringstream o; o << v; return o.str(); }

// Weak-pointer set helpers for session bookkeeping
template <class T>
struct WeakPtrHash {
    std::size_t operator()(const std::weak_ptr<T>& wp) const {
        if (auto sp = wp.lock()) return std::hash<const void*>()(sp.get());
        // Avoid collisions for expired entries; returning 0 is okay since we also use equality.
        return 0u;
    }
};
template <class T>
struct WeakPtrEqual {
    bool operator()(const std::weak_ptr<T>& a, const std::weak_ptr<T>& b) const {
        return !a.owner_before(b) && !b.owner_before(a);
    }
};

// Short preview for logging payloads (INFO/DEBUG)
static inline std::string preview_payload(const beast::flat_buffer& buf, bool binary) {
    std::string s = beast::buffers_to_string(buf.data());
    if (!binary) {
        std::string out; out.reserve(s.size());
        for (unsigned char ch : s) out.push_back((ch >= 32 && ch != 127) ? char(ch) : '.');
        if (out.size() > 1024) { out.resize(1024); out += "…"; }
        return std::string("TEXT(") + to_string(s.size()) + "B): \"" + out + "\"";
    }
    const std::size_t kMax = 256;
    std::ostringstream oss; oss << "BIN(" << s.size() << "B) [first " << std::min(s.size(), kMax) << " bytes]: ";
    oss << std::hex;
    const std::size_t lim = std::min(s.size(), kMax);
    for (std::size_t i = 0; i < lim; ++i) {
        unsigned v = static_cast<unsigned>(static_cast<unsigned char>(s[i]));
        if (i) oss << ' ';
        oss << std::setw(2) << std::setfill('0') << v;
    }
    if (s.size() > kMax) oss << " …";
    return oss.str();
}

// =====================================================================================
// Forward decls
// =====================================================================================

class BoostRequest;
class BoostResponse;
class BoostWebSocketSession;
class HttpSession;

class Server::ServerImpl {
public:
    ServerImpl();
    ~ServerImpl();

    bool start(int port, int num_threads);
    void stop();
    bool isRunning() const { return running_.load(); }

    void setConfig(const ServerConfig& cfg) {
        std::lock_guard<std::mutex> lk(cfg_mtx_);
        config_ = cfg;
    }
    ServerConfig getConfig() const {
        std::lock_guard<std::mutex> lk(cfg_mtx_);
        return config_;
    }

    void registerEndpoint(const std::string& path, const RequestHandler& handler);
    void unregisterEndpoint(const std::string& path);

    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler);
    void unregisterWebSocketEndpoint(const std::string& path);

    size_t broadcast(const std::string& path, const void* data, size_t size, bool binary);
    void forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn);
    size_t numWebSocketClients(const std::string& path) const;

    RequestHandler findHttpHandler(const std::string& path);
    std::pair<WebSocketHandler, bool> findWebSocketHandler(const std::string& path);

    void addWebSocketSession(const std::string& path, const std::shared_ptr<BoostWebSocketSession>& s);
    void removeWebSocketSession(const std::string& path, const std::shared_ptr<BoostWebSocketSession>& s);

    net::io_context& ioc() { return ioc_; }

private:
    void do_accept();
    void on_accept(beast::error_code ec, tcp::socket socket);

private:
    mutable std::mutex         cfg_mtx_;
    ServerConfig               config_;

    net::io_context            ioc_;
    tcp::acceptor              acceptor_;
    std::vector<std::thread>   threads_;
    std::atomic<bool>          running_;

    mutable std::mutex                         http_mtx_;
    std::unordered_map<std::string, RequestHandler> http_routes_;

    mutable std::mutex                         ws_h_mtx_;
    std::unordered_map<std::string, WebSocketHandler> ws_handlers_;

    using SessionSet = std::unordered_set<std::weak_ptr<BoostWebSocketSession>, WeakPtrHash<BoostWebSocketSession>, WeakPtrEqual<BoostWebSocketSession>>;
    mutable std::mutex                         ws_mtx_;
    std::unordered_map<std::string, SessionSet> ws_sessions_;
};

// =====================================================================================
// Request/Response wrappers
// =====================================================================================

    class BoostRequest final : public Request {
    public:
        explicit BoostRequest(const http::request<http::string_body>& req)
            : method_(std::string(beast::http::to_string(req.method()))),
              path_(std::string(req.target())),
              is_ws_(websocket::is_upgrade(req))
        {}

        std::string getMethod() const CV_OVERRIDE { return method_; }
        std::string getPath()   const CV_OVERRIDE { return path_; }
        bool isWebSocketUpgrade() const CV_OVERRIDE { return is_ws_; }

        // optional: expose snapshots if you need them elsewhere
        const std::string& methodSnapshot() const { return method_; }
        const std::string& pathSnapshot()   const { return path_; }
        bool wsUpgradeSnapshot() const { return is_ws_; }

    private:
        std::string method_;
        std::string path_;
        bool        is_ws_;
    };

class BoostResponse final : public Response {
public:
    BoostResponse(std::shared_ptr<HttpSession> session,
                  http::request<http::string_body>&& req)
        : session_(session), req_(std::move(req)) {
        res_.version(req_.version());
        res_.keep_alive(req_.keep_alive());
        res_.result(http::status::ok);
        res_.set(http::field::server, "OpenCV-Stream-Server/Boost");
    }

    ~BoostResponse() CV_OVERRIDE;

    void setStatusCode(int code) CV_OVERRIDE { res_.result(static_cast<http::status>(code)); }
    void setHeader(const std::string& key, const std::string& value) CV_OVERRIDE { res_.set(key, value); }

    bool write(const char* data, size_t size) CV_OVERRIDE {
        if (size) body_.write(data, static_cast<std::streamsize>(size));
        return true;
    }

    bool acceptWebSocket(const WebSocketHandler& handler,
                         const std::vector<std::string>& /*subprotocols*/) CV_OVERRIDE;

private:
    friend class HttpSession;

    std::weak_ptr<HttpSession>        session_;
    http::request<http::string_body>  req_;
    http::response<http::string_body> res_;
    std::ostringstream                body_;
    bool                              sent_ = false;
};

// =====================================================================================
// WebSocket session
// =====================================================================================

class BoostWebSocketSession
    : public WebSocketSession
    , public std::enable_shared_from_this<BoostWebSocketSession> {
public:
    BoostWebSocketSession(Server::ServerImpl* impl,
                          beast::tcp_stream&& stream,
                          std::string path)
        : impl_(impl)
        , ws_(std::move(stream))
        , path_(std::move(path))
        , strand_(ws_.get_executor())
        , challenge_timer_(ws_.get_executor())
        , ack_timer_(ws_.get_executor())
        , rng_(static_cast<unsigned>(std::random_device()()))
    {}

    ~BoostWebSocketSession() CV_OVERRIDE {}

    // WebSocketSession API
    bool send(const void* data, size_t size, bool binary) CV_OVERRIDE;
    void close(WsCloseCode code, const std::string& reason) CV_OVERRIDE;
    bool isOpen() const CV_OVERRIDE { return open_.load(std::memory_order_acquire); }
    std::string remoteAddress() const CV_OVERRIDE { return remote_; }

    void run(http::request<http::string_body> req, WebSocketHandler handler);

    // Accessors
    const std::string& path() const { return path_; }

private:
    struct OutMsg { std::shared_ptr<std::vector<uint8_t> > buf; bool bin; };

    void on_accept(beast::error_code ec);
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes);
    void do_write();
    void on_write(beast::error_code ec, std::size_t bytes);
    void on_close(beast::error_code ec);

    // Keepalive/challenge
    void schedule_next_challenge();
    void issue_challenge();
    std::string make_token(std::size_t n);
    void on_ack_timeout(const beast::error_code& ec);

    // Control frames
    void install_control_callbacks();

private:
    Server::ServerImpl*                       impl_;
    websocket::stream<beast::tcp_stream>      ws_;
    std::string                               path_;
    WebSocketHandler                          handler_;
    beast::flat_buffer                        inbuf_;
    net::strand<net::any_io_executor>         strand_;
    std::vector<OutMsg>                       outq_;
    std::atomic<bool>                         open_{false};
    std::string                               remote_;

    // Keepalive
    net::steady_timer                         challenge_timer_;
    net::steady_timer                         ack_timer_;
    std::string                               expected_ack_; // "ACK <token>"
    std::mt19937                              rng_;
};

// =====================================================================================
// HTTP session – handles HTTP and WS upgrade per connection
// =====================================================================================

class HttpSession : public std::enable_shared_from_this<HttpSession> {
public:
    HttpSession(tcp::socket&& socket, Server::ServerImpl* impl)
        : stream_(std::move(socket)), impl_(impl) {}

    void run() {
        net::dispatch(stream_.get_executor(),
                      beast::bind_front_handler(&HttpSession::do_read, shared_from_this()));
    }

    beast::tcp_stream& stream() { return stream_; }
    Server::ServerImpl* server_impl() { return impl_; }

private:
    void do_read();
    void on_read(beast::error_code ec, std::size_t bytes);
    void handle_request();

private:
    beast::tcp_stream                 stream_;
    beast::flat_buffer                buf_;
    Server::ServerImpl*               impl_;
    http::request<http::string_body>  req_;
};

// =====================================================================================
// BoostResponse
// =====================================================================================

BoostResponse::~BoostResponse() {
    if (sent_) return;
    if (auto sess = session_.lock()) {
        res_.body() = body_.str();
        res_.prepare_payload();
        beast::error_code ec;
        http::write(sess->stream(), res_, ec);
        if (ec) CV_LOG_WARNING(kTag, "HTTP write error: " << ec.message());
    }
}

bool BoostResponse::acceptWebSocket(const WebSocketHandler& handler,
                                    const std::vector<std::string>& /*subprotocols*/) {
    auto http_session = session_.lock();
    if (!http_session) return false;

    // Mark as sent; ownership of TCP stream is transferred to WS session
    sent_ = true;

    std::shared_ptr<BoostWebSocketSession> ws_session =
        std::make_shared<BoostWebSocketSession>(
            http_session->server_impl(),
            std::move(http_session->stream()),
            std::string(req_.target()));

    http_session->server_impl()->addWebSocketSession(ws_session->path(), ws_session);
    ws_session->run(std::move(req_), handler);
    return true;
}

// =====================================================================================
// BoostWebSocketSession
// =====================================================================================

void BoostWebSocketSession::run(http::request<http::string_body> req, WebSocketHandler handler) {
    handler_ = handler;

    // Disable framework-level timeouts so our policy governs liveness exclusively.
    beast::get_lowest_layer(ws_).expires_never();
    // Do NOT call websocket::stream_base::timeout::suggested(...)

    // Decorate upgrade response.
    ws_.set_option(websocket::stream_base::decorator(
        [](websocket::response_type& res) { res.set(http::field::server, "OpenCV-Stream-Server/Boost"); }
    ));

    install_control_callbacks();

    ws_.async_accept(std::move(req),
        beast::bind_front_handler(&BoostWebSocketSession::on_accept, shared_from_this()));
}

void BoostWebSocketSession::install_control_callbacks() {
    ws_.control_callback(
        [self = shared_from_this()](websocket::frame_type kind, boost::beast::string_view payload) {
            if (kind == websocket::frame_type::ping) {
                if (self->handler_.onPing) {
                    const uint8_t* p = reinterpret_cast<const uint8_t*>(payload.data());
                    self->handler_.onPing(*self, p, payload.size());
                }
                CV_LOG_VERBOSE(kTag, 2, "WS ping from " << self->remote_);
            } else if (kind == websocket::frame_type::pong) {
                if (self->handler_.onPong) {
                    const uint8_t* p = reinterpret_cast<const uint8_t*>(payload.data());
                    self->handler_.onPong(*self, p, payload.size());
                }
                CV_LOG_VERBOSE(kTag, 2, "WS pong from " << self->remote_);
            }
        }
    );
}

bool BoostWebSocketSession::send(const void* data, size_t size, bool binary) {
    if (!isOpen()) return false;
    auto buf = std::make_shared<std::vector<uint8_t> >(
        static_cast<const uint8_t*>(data), static_cast<const uint8_t*>(data) + size);
    net::post(strand_, [self = shared_from_this(), buf, binary]() {
        self->outq_.push_back({ buf, binary });
        if (self->outq_.size() == 1) self->do_write();
    });
    return true;
}

void BoostWebSocketSession::close(WsCloseCode code, const std::string& reason) {
    net::dispatch(strand_, [self = shared_from_this(), code, reason]() {
        if (!self->open_.exchange(false)) return;
        beast::error_code ig;
        self->ack_timer_.cancel(ig);
        self->challenge_timer_.cancel(ig);
        self->ws_.async_close({ static_cast<websocket::close_code>(static_cast<int>(code)), reason },
            beast::bind_front_handler(&BoostWebSocketSession::on_close, self));
    });
}

void BoostWebSocketSession::on_accept(beast::error_code ec) {
    if (ec) {
        if (handler_.onError) handler_.onError(*this, ec.value(), "accept");
        return;
    }

    try {
        tcp::endpoint ep = ws_.next_layer().socket().remote_endpoint();
        remote_ = ep.address().to_string() + ":" + to_string(ep.port());
    } catch (...) { remote_.clear(); }

    open_.store(true, std::memory_order_release);
    CV_LOG_INFO(kTag, "WS OPEN path=" << path_ << " remote=" << remote_);
    if (handler_.onOpen) handler_.onOpen(*this);

    // Start keepalive if enabled.
    ServerConfig cfg = impl_->getConfig();
    if (cfg.ws.enabled) schedule_next_challenge();

    do_read();
}

void BoostWebSocketSession::do_read() {
    ws_.async_read(inbuf_,
        beast::bind_front_handler(&BoostWebSocketSession::on_read, shared_from_this()));
}

void BoostWebSocketSession::on_read(beast::error_code ec, std::size_t bytes) {
    if (ec == websocket::error::closed || ec == http::error::end_of_stream) {
        if (open_.exchange(false) && handler_.onClose)
            handler_.onClose(*this, static_cast<int>(WsCloseCode::Normal), "client closed");
        impl_->removeWebSocketSession(path_, shared_from_this());
        return;
    }
    if (ec) {
        if (open_.exchange(false)) {
            if (handler_.onError) handler_.onError(*this, ec.value(), "read");
            if (handler_.onClose) handler_.onClose(*this, static_cast<int>(WsCloseCode::AbnormalClosure), ec.message());
        }
        impl_->removeWebSocketSession(path_, shared_from_this());
        return;
    }

    const bool is_bin = ws_.binary();
    CV_LOG_VERBOSE(kTag, 3, "WS recv " << bytes << "B " << (is_bin ? "BIN" : "TEXT")
                     << " from " << remote_ << " | " << preview_payload(inbuf_, is_bin));

    // Keepalive ACK check (text frames)
    if (!is_bin) {
        std::string payload = beast::buffers_to_string(inbuf_.data());
        if (!expected_ack_.empty() && payload == expected_ack_) {
            CV_LOG_DEBUG(kTag, "Keepalive ACK ok from " << remote_);
            expected_ack_.clear();
            beast::error_code ignore;
            ack_timer_.cancel(ignore);
        }
    }

    // Forward to app handler
    if (handler_.onMessage) {
        std::string tmp = beast::buffers_to_string(inbuf_.data());
        handler_.onMessage(*this,
                           reinterpret_cast<const uint8_t*>(tmp.data()),
                           tmp.size(),
                           is_bin);
    }

    inbuf_.consume(inbuf_.size());
    do_read();
}

void BoostWebSocketSession::do_write() {
    if (outq_.empty()) return;
    ws_.binary(outq_.front().bin);
    ws_.async_write(net::buffer(*outq_.front().buf),
        beast::bind_front_handler(&BoostWebSocketSession::on_write, shared_from_this()));
}

void BoostWebSocketSession::on_write(beast::error_code ec, std::size_t /*bytes*/) {
    if (ec) {
        if (open_.exchange(false) && handler_.onError) handler_.onError(*this, ec.value(), "write");
        impl_->removeWebSocketSession(path_, shared_from_this());
        return;
    }
    if (!outq_.empty()) outq_.erase(outq_.begin());
    if (!outq_.empty()) do_write();
}

void BoostWebSocketSession::on_close(beast::error_code ec) {
    if (ec && handler_.onError) handler_.onError(*this, ec.value(), "close");
    impl_->removeWebSocketSession(path_, shared_from_this());
    CV_LOG_INFO(kTag, "WS CLOSED path=" << path_ << (remote_.empty() ? "" : (" remote=" + remote_)));
}

// ---- Keepalive (challenge / ACK) --------------------------------------------

void BoostWebSocketSession::schedule_next_challenge() {
    ServerConfig cfg = impl_->getConfig();
    if (!cfg.ws.enabled) return;
    if (cfg.ws.challengeInterval.count() <= 0) return; // periodic disabled

    challenge_timer_.expires_after(cfg.ws.challengeInterval);
    challenge_timer_.async_wait(
        net::bind_executor(strand_, [self = shared_from_this()](const beast::error_code& ec) {
            if (ec || !self->open_.load()) return;
            self->issue_challenge();
        })
    );
}

std::string BoostWebSocketSession::make_token(std::size_t n) {
    static const char alphabet[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::uniform_int_distribution<int> dist(0, static_cast<int>(sizeof(alphabet) - 2));
    std::string s; s.reserve(n);
    for (std::size_t i = 0; i < n; ++i) s.push_back(alphabet[static_cast<std::size_t>(dist(rng_))]);
    return s;
}

void BoostWebSocketSession::issue_challenge() {
    ServerConfig cfg = impl_->getConfig();
    if (!cfg.ws.enabled) return;

    const std::string token = make_token(cfg.ws.challengeSize);
    expected_ack_ = cfg.ws.ackPrefix + token;

    // Send token as TEXT; client must answer "ACK <token>"
    const bool ok = send(token.data(), token.size(), /*binary*/false);
    CV_LOG_DEBUG(kTag, "Keepalive challenge -> " << remote_ << " token=\"" << token << "\" (" << (ok ? "sent" : "drop") << ")");

    // Arm ACK timeout
    ack_timer_.expires_after(cfg.ws.clientTimeout);
    ack_timer_.async_wait(
        net::bind_executor(strand_, [self = shared_from_this()](const beast::error_code& ec) {
            self->on_ack_timeout(ec);
        })
    );

    // Schedule next periodic challenge
    schedule_next_challenge();
}

void BoostWebSocketSession::on_ack_timeout(const beast::error_code& ec) {
    if (ec || !open_.load()) return; // cancelled or already closed
    if (!expected_ack_.empty()) {
        CV_LOG_WARNING(kTag, "Keepalive timeout" << (remote_.empty() ? "" : (" from " + remote_)) << "; closing");
        close(WsCloseCode::PolicyViolation, "keepalive timeout");
    }
}

// =====================================================================================
// HttpSession
// =====================================================================================

void HttpSession::do_read() {
    // Short HTTP read deadline; timeouts are disabled once upgraded to WS.
    stream_.expires_after(std::chrono::seconds(30));
    req_ = {};
    http::async_read(stream_, buf_, req_,
                     beast::bind_front_handler(&HttpSession::on_read, shared_from_this()));
}

void HttpSession::on_read(beast::error_code ec, std::size_t /*bytes*/) {
    if (ec == http::error::end_of_stream) {
        beast::error_code ignore;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ignore);
        return;
    }
    if (ec) {
        CV_LOG_WARNING(kTag, "HTTP read error: " << ec.message());
        return;
    }
    handle_request();
}

void HttpSession::handle_request() {
    const bool keep_alive = req_.keep_alive();
    BoostRequest breq(req_);
    const std::string path = breq.getPath();

    // --- WebSocket upgrade path ---
    if (websocket::is_upgrade(req_)) {
        auto ws = impl_->findWebSocketHandler(path);
        if (!ws.second) {
            // Unknown WS endpoint: respond with a normal HTTP 404 (no upgrade)
            ServerConfig cfg = impl_->getConfig();
            http::response<http::string_body> res{ http::status::not_found, req_.version() };
            res.keep_alive(false);
            res.set(http::field::server, "OpenCV-Stream-Server/Boost");
            res.set(http::field::content_type, cfg.notFound.contentType);
            for (size_t i = 0; i < cfg.defaultNotFoundHeaders.size(); ++i)
                res.set(cfg.defaultNotFoundHeaders[i].first, cfg.defaultNotFoundHeaders[i].second);
            res.body() = cfg.notFound.body;
            res.prepare_payload();
            beast::error_code ec;
            http::write(stream_, res, ec);
            beast::error_code ignore;
            stream_.socket().shutdown(tcp::socket::shutdown_send, ignore);
            return;
        }

        // Hand off the connection to a WebSocket session (this moves the TCP stream)
        std::shared_ptr<BoostResponse> resp(new BoostResponse(shared_from_this(), std::move(req_)));
        (void)resp->acceptWebSocket(ws.first, {});
        return;
    }

    // --- Plain HTTP path ---
    {
        // Response scoped: ensures destructor writes the reply before we possibly close the socket
        std::shared_ptr<BoostResponse> resp(new BoostResponse(shared_from_this(), std::move(req_)));

        RequestHandler http_h = impl_->findHttpHandler(path);
        if (http_h) {
            try {
                http_h(breq, *resp);
            } catch (const std::exception& e) {
                CV_LOG_ERROR(kTag, "HTTP handler exception: " << e.what());
                resp->setStatusCode(500);
                resp->setHeader("Content-Type", "text/plain; charset=utf-8");
                const std::string msg = "Internal Server Error in request handler.";
                resp->write(msg.data(), msg.size());
            } catch (...) {
                CV_LOG_ERROR(kTag, "HTTP handler unknown exception");
                resp->setStatusCode(500);
                resp->setHeader("Content-Type", "text/plain; charset=utf-8");
                const std::string msg = "Internal Server Error in request handler.";
                resp->write(msg.data(), msg.size());
            }
        } else {
            // Uniform 404 page
            ServerConfig cfg = impl_->getConfig();
            resp->setStatusCode(404);
            resp->setHeader("Content-Type", cfg.notFound.contentType);
            for (size_t i = 0; i < cfg.defaultNotFoundHeaders.size(); ++i)
                resp->setHeader(cfg.defaultNotFoundHeaders[i].first, cfg.defaultNotFoundHeaders[i].second);
            resp->write(cfg.notFound.body.data(), cfg.notFound.body.size());
        }
    } // resp dtor writes the response here

    if (keep_alive) {
        do_read();
    } else {
        beast::error_code ignore;
        stream_.socket().shutdown(tcp::socket::shutdown_send, ignore);
    }
}


// =====================================================================================
// Server::ServerImpl
// =====================================================================================

Server::ServerImpl::ServerImpl()
    : config_()
    , ioc_()
    , acceptor_(ioc_)
    , running_(false) {
    CV_LOG_INFO(kTag, "ServerImpl created");
}

Server::ServerImpl::~ServerImpl() {
    stop();
}

bool Server::ServerImpl::start(int port, int num_threads) {
    if (running_.load()) return true;

    beast::error_code ec;
    auto const address = net::ip::make_address("0.0.0.0");
    tcp::endpoint ep(address, static_cast<unsigned short>(port));

    acceptor_.open(ep.protocol(), ec); if (ec) { CV_LOG_ERROR(kTag, "acceptor open: " << ec.message()); return false; }
    acceptor_.set_option(net::socket_base::reuse_address(true), ec); if (ec) { CV_LOG_ERROR(kTag, "reuse_address: " << ec.message()); return false; }
    acceptor_.bind(ep, ec); if (ec) { CV_LOG_ERROR(kTag, "bind: " << ec.message()); return false; }
    acceptor_.listen(net::socket_base::max_listen_connections, ec); if (ec) { CV_LOG_ERROR(kTag, "listen: " << ec.message()); return false; }

    do_accept();

    threads_.reserve(std::max(1, num_threads));
    for (int i = 0; i < std::max(1, num_threads); ++i) {
        threads_.push_back(std::thread([this, i]() {
            CV_LOG_INFO(kTag, "Worker #" << i << " run()");
            ioc_.run();
            CV_LOG_INFO(kTag, "Worker #" << i << " stopped");
        }));
    }
    running_.store(true);
    CV_LOG_INFO(kTag, "Server started on *:" << port << " threads=" << num_threads);
    return true;
}

void Server::ServerImpl::stop() {
    if (!running_.exchange(false)) return;

    // Close listener & attempt to close all WS sessions
    net::post(ioc_, [this]() {
        beast::error_code ec;
        acceptor_.close(ec);

        std::lock_guard<std::mutex> lk(ws_mtx_);
        for (std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.begin();
             it != ws_sessions_.end(); ++it) {
            for (SessionSet::const_iterator sit = it->second.begin(); sit != it->second.end(); ++sit) {
                if (std::shared_ptr<BoostWebSocketSession> s = sit->lock()) {
                    s->close(WsCloseCode::GoingAway, "Server shutdown");
                }
            }
        }
        ws_sessions_.clear();
    });

    ioc_.stop();
    for (size_t i = 0; i < threads_.size(); ++i) {
        if (threads_[i].joinable()) threads_[i].join();
    }
    threads_.clear();
    ioc_.restart();
    CV_LOG_INFO(kTag, "Server stopped");
}

void Server::ServerImpl::registerEndpoint(const std::string& path, const RequestHandler& handler) {
    std::lock_guard<std::mutex> lk(http_mtx_); http_routes_[path] = handler;
}
void Server::ServerImpl::unregisterEndpoint(const std::string& path) {
    std::lock_guard<std::mutex> lk(http_mtx_); http_routes_.erase(path);
}
void Server::ServerImpl::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) {
    std::lock_guard<std::mutex> lk(ws_h_mtx_); ws_handlers_[path] = handler;
}
void Server::ServerImpl::unregisterWebSocketEndpoint(const std::string& path) {
    std::lock_guard<std::mutex> lk(ws_h_mtx_); ws_handlers_.erase(path);
}

size_t Server::ServerImpl::broadcast(const std::string& path, const void* data, size_t size, bool binary) {
    std::vector<std::shared_ptr<BoostWebSocketSession> > copy;
    {
        std::lock_guard<std::mutex> lk(ws_mtx_);
        std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) return 0;
        copy.reserve(it->second.size());
        for (SessionSet::const_iterator s = it->second.begin(); s != it->second.end(); ++s) {
            if (std::shared_ptr<BoostWebSocketSession> sp = s->lock()) copy.push_back(sp);
        }
    }
    size_t count = 0;
    for (size_t i = 0; i < copy.size(); ++i) {
        count += copy[i]->send(data, size, binary) ? 1 : 0;
    }
    return count;
}

void Server::ServerImpl::forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) {
    std::vector<std::shared_ptr<BoostWebSocketSession> > copy;
    {
        std::lock_guard<std::mutex> lk(ws_mtx_);
        std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.find(path);
        if (it == ws_sessions_.end()) return;
        copy.reserve(it->second.size());
        for (SessionSet::const_iterator s = it->second.begin(); s != it->second.end(); ++s) {
            if (std::shared_ptr<BoostWebSocketSession> sp = s->lock()) copy.push_back(sp);
        }
    }
    for (size_t i = 0; i < copy.size(); ++i) fn(*copy[i]);
}

size_t Server::ServerImpl::numWebSocketClients(const std::string& path) const {
    std::lock_guard<std::mutex> lk(ws_mtx_);
    std::unordered_map<std::string, SessionSet>::const_iterator it = ws_sessions_.find(path);
    return (it == ws_sessions_.end()) ? 0u : it->second.size();
}

RequestHandler Server::ServerImpl::findHttpHandler(const std::string& path) {
    std::lock_guard<std::mutex> lk(http_mtx_);
    std::unordered_map<std::string, RequestHandler>::iterator it = http_routes_.find(path);
    return (it == http_routes_.end()) ? RequestHandler() : it->second;
}

std::pair<WebSocketHandler, bool> Server::ServerImpl::findWebSocketHandler(const std::string& path) {
    std::lock_guard<std::mutex> lk(ws_h_mtx_);
    std::unordered_map<std::string, WebSocketHandler>::iterator it = ws_handlers_.find(path);
    return (it == ws_handlers_.end()) ? std::make_pair(WebSocketHandler(), false) : std::make_pair(it->second, true);
}

void Server::ServerImpl::addWebSocketSession(const std::string& path, const std::shared_ptr<BoostWebSocketSession>& s) {
    std::lock_guard<std::mutex> lk(ws_mtx_);
    ws_sessions_[path].insert(std::weak_ptr<BoostWebSocketSession>(s));
}
void Server::ServerImpl::removeWebSocketSession(const std::string& path, const std::shared_ptr<BoostWebSocketSession>& s) {
    std::lock_guard<std::mutex> lk(ws_mtx_);
    std::unordered_map<std::string, SessionSet>::iterator it = ws_sessions_.find(path);
    if (it == ws_sessions_.end()) return;
    it->second.erase(std::weak_ptr<BoostWebSocketSession>(s));
    if (it->second.empty()) ws_sessions_.erase(it);
}

void Server::ServerImpl::do_accept() {
    acceptor_.async_accept(net::make_strand(ioc_),
        beast::bind_front_handler(&Server::ServerImpl::on_accept, this));
}

void Server::ServerImpl::on_accept(beast::error_code ec, tcp::socket socket) {
    if (!ec) {
        std::make_shared<HttpSession>(std::move(socket), this)->run();
    } else {
        CV_LOG_WARNING(kTag, "accept error: " << ec.message());
    }
    if (acceptor_.is_open()) do_accept();
}

// =====================================================================================
// Server (public API)
// =====================================================================================

Server::Server() : pimpl(new ServerImpl) {}
Server::~Server() {}
bool Server::start(int port, int num_threads) { return pimpl->start(port, num_threads); }
void Server::stop() { pimpl->stop(); }
bool Server::isRunning() const { return pimpl->isRunning(); }

void Server::setConfig(const ServerConfig& cfg) { pimpl->setConfig(cfg); }
const ServerConfig& Server::getConfig() const {
    // Snapshot under lock (impl) and return a stable reference to a static.
    static ServerConfig snap;
    snap = pimpl->getConfig();
    return snap;
}

void Server::registerEndpoint(const std::string& path, const RequestHandler& handler) { pimpl->registerEndpoint(path, handler); }
void Server::unregisterEndpoint(const std::string& path) { pimpl->unregisterEndpoint(path); }
void Server::registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler) { pimpl->registerWebSocketEndpoint(path, handler); }
void Server::unregisterWebSocketEndpoint(const std::string& path) { pimpl->unregisterWebSocketEndpoint(path); }
size_t Server::broadcast(const std::string& path, const void* data, size_t size, bool binary) { return pimpl->broadcast(path, data, size, binary); }
void Server::forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn) { pimpl->forEachWebSocket(path, fn); }
size_t Server::numWebSocketClients(const std::string& path) const { return pimpl->numWebSocketClients(path); }

Server::Server(Server&&) noexcept = default;
Server& Server::operator=(Server&&) noexcept = default;

std::unique_ptr<Server> createServer() {
    return std::unique_ptr<Server>(new Server());
}
std::unique_ptr<Server> createServer(const ServerConfig& cfg) {
    auto s = createServer();   // uses the friended zero-arg factory
    s->setConfig(cfg);
    return s;
}

} // namespace stream
} // namespace cv

#endif // HAVE_STREAM_HTTP_BOOST
