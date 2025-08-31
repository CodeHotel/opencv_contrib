#ifndef OPENCV_STREAM_CLIENT_HPP
#define OPENCV_STREAM_CLIENT_HPP

#ifdef OCV_BUILD_TESTS

#include "opencv2/core/cvdef.h"
#include "opencv2/core/utils/logger.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace cv {
namespace stream {
namespace client {

    // WebSocket close codes used by the test client.
    enum class WsCloseCode : uint16_t {
        Normal              = 1000,
        GoingAway           = 1001,
        ProtocolError       = 1002,
        UnsupportedData     = 1003,
        NoStatus            = 1005,
        AbnormalClosure     = 1006,
        InvalidPayload      = 1007,
        PolicyViolation     = 1008,
        MessageTooBig       = 1009,
        MandatoryExtension  = 1010,
        InternalError       = 1011,
        ServiceRestart      = 1012,
        TryAgainLater       = 1013,
        TLSHandshake        = 1015
    };

    // Callback bundle for client-side events.
    struct CV_EXPORTS_W WebSocketHandler {
        std::function<void()> onOpen;
        std::function<void(const void* data, size_t size, bool binary)> onMessage;
        std::function<void(WsCloseCode code, const std::string& reason)> onClosed;
        std::function<void(const std::string& what)> onError;
    };

    // Options for client connections (WS only; no TLS/WSS).
    struct CV_EXPORTS_W WebSocketClientOptions {
        int connectTimeoutMs = 5000;
        std::vector<std::pair<std::string, std::string>> headers;
        std::vector<std::string> subprotocols;
        bool enableCompression = false;
        bool noDelay = true;
    };

    // Thin, test-oriented WebSocket client (backend selected at build time).
    class CV_EXPORTS_W Client
    {
    public:
        ~Client();

        bool connect(const std::string& url,
                     const WebSocketHandler& callbacks,
                     const WebSocketClientOptions& opts = WebSocketClientOptions());

        void close(WsCloseCode code = WsCloseCode::Normal, const std::string& reason = std::string());

        bool send(const void* data, size_t size, bool binary = true);
        bool send(const std::string& text) { return send(text.data(), text.size(), /*binary=*/false); }
        bool send(const std::vector<uint8_t>& bytes, bool binary = true) {
            return bytes.empty() ? true : send(bytes.data(), bytes.size(), binary);
        }

        bool isOpen() const;
        std::string remoteAddress() const;

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

    // Factory bound to the selected backend (e.g., CIVETWEB, BOOST, MONGOOSE).
    CV_EXPORTS_W std::unique_ptr<Client> createWebSocketClient();


    // ------------------------------------------------------------------------
    // Minimal HTTP GET (HTTP only; no HTTPS/TLS) for local CI tests
    // ------------------------------------------------------------------------

    struct CV_EXPORTS_W HttpRequestOptions {
        int timeoutMs = 5000;
        std::vector<std::pair<std::string, std::string>> headers;
        size_t maxBodyBytes = 10 * 1024 * 1024; // 10 MiB
    };

    struct CV_EXPORTS_W HttpResponse {
        int status = -1;  // -1 on transport/parse error
        std::vector<std::pair<std::string, std::string>> headers;
        std::string body;
    };

    CV_EXPORTS_W HttpResponse httpGet(const std::string& url,
                                      const HttpRequestOptions& opts = HttpRequestOptions());

} // namespace test
} // namespace stream
} // namespace cv

#endif // OCV_BUILD_TESTS
#endif // OPENCV_STREAM_CLIENT_HPP
