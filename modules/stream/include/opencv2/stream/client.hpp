#ifndef OPENCV_STREAM_CLIENT_HPP
#define OPENCV_STREAM_CLIENT_HPP

// Build this header only for test builds.
#ifdef OCV_BUILD_TESTS

#include "opencv2/core/cvdef.h"
#include "opencv2/stream/server.hpp"  // reuse WsCloseCode + WebSocketHandler types

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace cv {
namespace stream {

    // Options for WebSocket client connections used in tests.
    struct CV_EXPORTS_W WebSocketClientOptions {
        // Connection timeout in milliseconds (implementation should block up to this when connect() is called).
        int connectTimeoutMs = 5000;

        // Optional extra HTTP headers for the handshake.
        std::vector<std::pair<std::string, std::string>> headers;

        // Optional list of subprotocols to request.
        std::vector<std::string> subprotocols;

        // Transport knobs (implementations may ignore if unsupported).
        bool enableCompression = false; // permessage-deflate, etc.
        bool noDelay = true;            // TCP_NODELAY

        // TLS options (used when url starts with wss://). Implementations may ignore if unsupported.
        bool verifyTLS = true;
        std::string caCertPath;         // path to CA bundle / directory (backend-dependent)
    };

    // A thin, test-oriented WebSocket client.
    // Backend is selected at build time in the corresponding .cpp via HAVE_STREAM_BACKEND_*.
    class CV_EXPORTS_W Client
    {
    public:
        ~Client();

        // Establish a connection to ws:// or wss:// URL.
        // Returns true if the connection is established by the time the function returns,
        // false if it fails or times out (see WebSocketClientOptions::connectTimeoutMs).
        bool connect(const std::string& url,
                     const WebSocketHandler& callbacks,
                     const WebSocketClientOptions& opts = WebSocketClientOptions());

        // Close the connection.
        void close(WsCloseCode code = WsCloseCode::Normal, const std::string& reason = std::string());

        // Send a data frame. Returns false on failure/backpressure.
        bool send(const void* data, size_t size, bool binary = true);

        // Convenience overloads.
        bool send(const std::string& text) { return send(text.data(), text.size(), /*binary=*/false); }
        bool send(const std::vector<uint8_t>& bytes, bool binary = true) {
            return bytes.empty() ? true : send(bytes.data(), bytes.size(), binary);
        }

        // State / info.
        bool isOpen() const;
        std::string remoteAddress() const; // ip:port if available, else empty

        // Optional controls (no-ops if unsupported by the backend).
        void setWriteQueueLimit(size_t bytes);
        size_t queuedBytes() const;
        void setNoDelay(bool on);
        void enableCompression(bool on);

        Client(const Client&) = delete;
        Client& operator=(const Client&) = delete;
        Client(Client&&) noexcept;
        Client& operator=(Client&&) noexcept;

    private:
        class Impl;
        std::unique_ptr<Impl> pimpl;

        Client();

        friend std::unique_ptr<Client> createWebSocketClient();
    };

    // Factory that returns a client bound to the selected backend (CIVETWEB, BOOST, or MONGOOSE).
    CV_EXPORTS_W std::unique_ptr<Client> createWebSocketClient();

} // namespace stream
} // namespace cv


// --- Debug Logging ---
#ifdef DEBUG
// FIX: Removed parentheses around `msg` to allow stream chaining.
#define LOG_DEBUG(source, msg) do { \
std::ostringstream os; \
os << "[" << (source) << ":" << std::this_thread::get_id() << "] " << msg << std::endl; \
std::cout << os.str(); \
} while (0)
#else
#define LOG_DEBUG(source, msg)
#endif

#endif // OCV_BUILD_TESTS
#endif // OPENCV_STREAM_CLIENT_HPP
