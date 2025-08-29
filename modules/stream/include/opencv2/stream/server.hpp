#pragma once
#ifndef OPENCV_STREAM_SERVER_HPP
#define OPENCV_STREAM_SERVER_HPP

#include <opencv2/core/cvdef.h>
#include <opencv2/core/utils/logger.hpp>

#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>
#include <chrono>

namespace cv {
namespace stream {

// Forward decls
class Request;
class Response;
class HttpSession;
class WebSocketSession;
struct WebSocketHandler;

using RequestHandler = std::function<void(const Request& request, Response& response)>;

// ----------------------
// HTTP
// ----------------------

class CV_EXPORTS_W Request {
public:
    virtual ~Request() = default;
    virtual std::string getMethod() const = 0;
    virtual std::string getPath() const = 0;

    // WebSocket upgrade discovery
    virtual bool isWebSocketUpgrade() const { return false; } // default false if backend doesn't override
};

class CV_EXPORTS_W Response {
public:
    virtual ~Response() = default;

    // Basic status & headers
    virtual void setStatusCode(int code) = 0;                         // e.g. 200, 404
    virtual void setHeader(const std::string& key,
                           const std::string& value) = 0;
    virtual bool write(const char* data, size_t size) = 0;            // return false on transport error

    // Optional HTTP->WS upgrade path (handler takes over the connection if accepted).
    // Returns true if the upgrade succeeded and control has been transferred to the WS handler.
    virtual bool acceptWebSocket(const WebSocketHandler& handler,
                                 const std::vector<std::string>& subprotocols = {}) {
        (void)handler; (void)subprotocols; return false;
    }
};

// ----------------------
// WebSocket
// ----------------------

enum class WsCloseCode : int {
    Normal                 = 1000,
    GoingAway              = 1001,
    ProtocolError          = 1002,
    UnsupportedData        = 1003,
    NoStatusRcvd           = 1005, // reserved
    AbnormalClosure        = 1006, // reserved
    InvalidFramePayload    = 1007,
    PolicyViolation        = 1008,
    MessageTooBig          = 1009,
    MandatoryExt           = 1010,
    InternalError          = 1011,
    TLSHandshake           = 1015  // reserved
};

class CV_EXPORTS_W WebSocketSession {
public:
    virtual ~WebSocketSession() = default;

    // Non-blocking enqueue; returns false if dropped/failed due to backpressure.
    virtual bool send(const void* data, size_t size, bool binary = true) = 0;

    // Close the session.
    virtual void close(WsCloseCode code = WsCloseCode::Normal,
                       const std::string& reason = std::string()) = 0;

    // Session state / info.
    virtual bool isOpen() const = 0;
    virtual std::string remoteAddress() const { return std::string(); }

    // Backpressure controls (optional; no-ops if unsupported).
    virtual void setWriteQueueLimit(size_t bytes) { (void)bytes; }
    virtual size_t queuedBytes() const { return 0; }

    // Transport knobs (optional; no-ops if unsupported).
    virtual void setNoDelay(bool on) { (void)on; }
    virtual void enableCompression(bool on) { (void)on; }
};

// Application-level callbacks. All are optional.
struct WebSocketHandler {
    std::function<void(WebSocketSession& s)> onOpen;
    std::function<void(WebSocketSession& s, const uint8_t* data, size_t n, bool binary)> onMessage;
    std::function<void(WebSocketSession& s, int code, const std::string& reason)> onClose;
    std::function<void(WebSocketSession& s, int errorCode, const char* where)> onError;
    std::function<void(WebSocketSession& s, const uint8_t* data, size_t n)> onPing;
    std::function<void(WebSocketSession& s, const uint8_t* data, size_t n)> onPong;
};

// ----------------------
// Unified behavior & configuration
// ----------------------

// Default Not Found payload (customizable via ServerConfig)
struct CV_EXPORTS_W NotFoundPage {
    // MIME type and body used when no endpoint matches; backends must use this instead of crashing.
    std::string contentType = "text/plain; charset=utf-8";
    std::string body = "404 Not Found\n";
};

// WebSocket keepalive / timeout policy.
// Server periodically sends a random ASCII challenge; client must echo back: "ACK <challenge>".
// If no valid ACK arrives within `clientTimeout`, the server closes the connection (PolicyViolation).
struct CV_EXPORTS_W WsKeepalivePolicy {
    bool        enabled = true;
    // How often to challenge (0 => disable periodic challenges but still honor clientTimeout on demand).
    std::chrono::milliseconds challengeInterval{15000};      // e.g., 15s
    std::chrono::milliseconds clientTimeout{45000};          // e.g., 45s
    // Number of random characters appended after "ACK " to form the required client reply.
    std::size_t challengeSize = 12;
    // Prefix the client must send back before the challenge token.
    std::string ackPrefix = "ACK ";
    // If true, disables the underlying framework’s built-in idle/heartbeat timeouts so only this policy applies.
    bool disableFrameworkIdleTimeout = true;
};

// Global server configuration. May be set before or after start(); backends should snapshot at start().
struct CV_EXPORTS_W ServerConfig {
    NotFoundPage     notFound;
    WsKeepalivePolicy ws;
    // Server-wide default headers for 404 responses (e.g., security headers). Empty = none.
    std::vector<std::pair<std::string, std::string>> defaultNotFoundHeaders{};
};

// ----------------------
// Server
// ----------------------

class CV_EXPORTS_W Server {
public:
    ~Server();

    // Start / Stop
    bool start(int port, int num_threads = 1); // honors current config()
    void stop();
    bool isRunning() const;

    // Options (set any time; effective for new connections; backends may also apply live)
    void setConfig(const ServerConfig& cfg);
    const ServerConfig& getConfig() const;

    // HTTP endpoints
    void registerEndpoint(const std::string& path, const RequestHandler& handler);
    void unregisterEndpoint(const std::string& path);

    // WebSocket endpoints
    void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler);
    void unregisterWebSocketEndpoint(const std::string& path);

    // Utilities for WS fanout / inspection (optional no-ops on backends that don't support them)
    size_t broadcast(const std::string& path, const void* data, size_t size, bool binary = true);
    void forEachWebSocket(const std::string& path, const std::function<void(WebSocketSession&)>& fn);
    size_t numWebSocketClients(const std::string& path) const;

    // Fallbacks / unhandled behavior:
    // If no HTTP endpoint matches, the backend must:
    //   - set 404, apply config().defaultNotFoundHeaders, Content-Type = config().notFound.contentType
    //   - write config().notFound.body
    // and return without throwing/aborting.
    //
    // For WebSockets: if a WS path is not registered, the server must respond with HTTP 404 (not upgrade).

    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) noexcept;
    Server& operator=(Server&&) noexcept;

private:
    class ServerImpl;
    std::unique_ptr<ServerImpl> pimpl;

    Server();

    friend std::unique_ptr<Server> createServer();
    friend class HttpSession;       // HTTP internals
    friend class WebSocketSession;  // WS internals (per-backend)
#if defined(HAVE_STREAM_HTTP_BOOST)
    friend class BoostWebSocketSession;
#endif
#if defined(HAVE_STREAM_HTTP_MONGOOSE)
    friend class MongooseWebSocketSession;
#endif
};

// Factory (default config)
CV_EXPORTS_W std::unique_ptr<Server> createServer();

// Optional convenience: factory with explicit config at creation.
CV_EXPORTS_W std::unique_ptr<Server> createServer(const ServerConfig& cfg);

// ----------------------
// Notes for backend implementations (summary)
// ----------------------
//
// 1) Logging
//    - Use OpenCV logging macros, e.g.:
//        CV_LOG_INFO(("stream.server"), "Server starting on port " << port);
//        CV_LOG_WARNING(("stream.server"), "No endpoint for " << path);
//        CV_LOG_ERROR(("stream.server"), "Backend error: " << ec);
//    - Do NOT use any custom logging macros in this API.
//
// 2) Uniform 404
//    - If no HTTP endpoint matches, respond with:
//        setStatusCode(404)
//        setHeader("Content-Type", config.notFound.contentType)
//        for (auto& kv : config.defaultNotFoundHeaders) setHeader(kv.first, kv.second);
//        write(config.notFound.body.data(), config.notFound.body.size());
//    - Never throw or crash for unknown endpoints.
//
// 3) WS Keepalive/Timeout
//    - If WsKeepalivePolicy.enabled:
//       * On open (or on timer every `challengeInterval`), generate a random ASCII token of length `challengeSize`.
//       * Send it to client as a text frame payload, e.g. the token itself (or prefixed; choice is backend-local).
//         (Recommendation: send just the token; the required client reply is "ACK <token>").
//       * Consider the connection "alive" if you receive a text frame with payload exactly: `ackPrefix + token`
//         before `clientTimeout` elapses since the challenge was sent.
//       * If timeout fires first, close with WsCloseCode::PolicyViolation and a short reason like "keepalive timeout".
//    - When disableFrameworkIdleTimeout=true, disable any library/default per-connection idle timeouts,
//      so only this policy governs liveness.
//    - Backends must ignore arbitrary incoming client traffic for liveness unless it matches the ACK pattern.
//      (Your app-level onMessage still fires for all other messages.)
//
// 4) Safety
//    - Never abort/terminate the process due to malformed client input. Return errors or close that connection.
//    - It's acceptable for user-provided handlers to throw/abort—this is outside transport safety guarantees.
//
// ----------------------

} // namespace stream
} // namespace cv

#endif // OPENCV_STREAM_SERVER_HPP
