#ifndef OPENCV_STREAM_SERVER_HPP
#define OPENCV_STREAM_SERVER_HPP

#include "opencv2/core/cvdef.h"
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <cstddef>
#include <cstdint>

namespace cv {
    namespace stream {

        // Forward decls
        class Request;
        class Response;
        class HttpSession;
        class WebSocketSession;
        struct WebSocketHandler;

        using RequestHandler = std::function<void(const Request& request, Response& response)>;

        // --- HTTP ---

        class CV_EXPORTS_W Request
        {
        public:
            virtual ~Request() = default;
            virtual std::string getMethod() const = 0;
            virtual std::string getPath() const = 0;

            // WebSocket upgrade discovery
            virtual bool isWebSocketUpgrade() const { return false; } // default false if backend doesn't override
        };

        class CV_EXPORTS_W Response
        {
        public:
            virtual ~Response() = default;
            virtual void setStatusCode(int code) = 0;
            virtual void setHeader(const std::string& key, const std::string& value) = 0;
            virtual bool write(const char* data, size_t size) = 0;

            // Optional HTTP->WS upgrade path (handler takes over the connection if accepted).
            // Returns true if the upgrade succeeded and control has been transferred to the WS handler.
            virtual bool acceptWebSocket(const WebSocketHandler& handler,
                                         const std::vector<std::string>& subprotocols = {}) { (void)handler; (void)subprotocols; return false; }
        };

        // --- WebSocket ---

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

        class CV_EXPORTS_W WebSocketSession
        {
        public:
            virtual ~WebSocketSession() = default;

            // Non-blocking enqueue; returns false if dropped/failed due to backpressure.
            virtual bool send(const void* data, size_t size, bool binary = true) = 0;

            // Close the session.
            virtual void close(WsCloseCode code = WsCloseCode::Normal, const std::string& reason = std::string()) = 0;

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

        struct WebSocketHandler
        {
            // All callbacks are optional.
            std::function<void(WebSocketSession& s)> onOpen;
            std::function<void(WebSocketSession& s, const uint8_t* data, size_t n, bool binary)> onMessage;
            std::function<void(WebSocketSession& s, int code, const std::string& reason)> onClose;
            std::function<void(WebSocketSession& s, int errorCode, const char* where)> onError;
            std::function<void(WebSocketSession& s, const uint8_t* data, size_t n)> onPing;
            std::function<void(WebSocketSession& s, const uint8_t* data, size_t n)> onPong;
        };

        using WebSocketVisitor = std::function<void(WebSocketSession&)>;

        // --- Server ---

        class CV_EXPORTS_W Server
        {
        public:
            ~Server();
            bool start(int port, int num_threads = 1);
            void stop();
            bool isRunning() const;

            // HTTP endpoints
            void registerEndpoint(const std::string& path, const RequestHandler& handler);
            void unregisterEndpoint(const std::string& path);

            // WebSocket endpoints
            void registerWebSocketEndpoint(const std::string& path, const WebSocketHandler& handler);
            void unregisterWebSocketEndpoint(const std::string& path);

            // Utilities for WS fanout / inspection (optional no-ops on backends that don't support them)
            size_t broadcast(const std::string& path, const void* data, size_t size, bool binary = true);
            void forEachWebSocket(const std::string& path, const WebSocketVisitor& fn);
            size_t numWebSocketClients(const std::string& path) const;

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
        };

        CV_EXPORTS_W std::unique_ptr<Server> createServer();

    } // namespace stream
} // namespace cv

// --- Debug Logging ---
#ifdef DEBUG
#define LOG_DEBUG(source, msg) do { \
std::ostringstream os; \
os << "[" << (source) << ":" << std::this_thread::get_id() << "] " << msg << std::endl; \
std::cout << os.str(); \
} while (0)
#else
#define LOG_DEBUG(source, msg)
#endif

#endif // OPENCV_STREAM_SERVER_HPP
