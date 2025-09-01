#ifndef OPENCV_STREAM_CLIENT_HPP
#define OPENCV_STREAM_CLIENT_HPP

#ifdef OCV_BUILD_TESTS

#include "opencv2/core/cvdef.h"

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

struct CV_EXPORTS WebSocketHandler {
    std::function<void()> onOpen;
    std::function<void(const void* data, size_t size, bool binary)> onMessage;
    std::function<void(WsCloseCode code, const std::string& reason)> onClosed;
    std::function<void(const std::string& what)> onError;
};

struct CV_EXPORTS WebSocketClientOptions {
    int connectTimeoutMs = 5000;
    std::vector<std::pair<std::string, std::string>> headers;
    std::vector<std::string> subprotocols;
    bool enableCompression = false;
    bool noDelay = true;
};

class CV_EXPORTS Client {
public:
    ~Client() noexcept;

    [[nodiscard]] bool connect(const std::string& url,
                               const WebSocketHandler& callbacks,
                               const WebSocketClientOptions& opts = WebSocketClientOptions());

    void close(WsCloseCode code = WsCloseCode::Normal, const std::string& reason = std::string()) noexcept;

    [[nodiscard]] bool send(const void* data, size_t size, bool binary = true);
    bool send(const std::string& text) { return send(text.data(), text.size(), /*binary=*/false); }
    bool send(const std::vector<uint8_t>& bytes, bool binary = true) {
        return bytes.empty() ? true : send(bytes.data(), bytes.size(), binary);
    }

    [[nodiscard]] bool isOpen() const noexcept;
    std::string remoteAddress() const;

    void setWriteQueueLimit(size_t bytes) noexcept;
    [[nodiscard]] size_t queuedBytes() const noexcept;
    void setNoDelay(bool on) noexcept;
    void enableCompression(bool on) noexcept;

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    Client(Client&&) noexcept;
    Client& operator=(Client&&) noexcept;

private:
    class Impl;
    std::unique_ptr<Impl> pimpl;

    Client();

    friend std::unique_ptr<Client> createClient();
    friend std::unique_ptr<Client> createWebSocketClient();
};

CV_EXPORTS std::unique_ptr<Client> createClient();
CV_EXPORTS std::unique_ptr<Client> createWebSocketClient();

struct CV_EXPORTS HttpRequestOptions {
    int timeoutMs = 5000;
    std::vector<std::pair<std::string, std::string>> headers;
    size_t maxBodyBytes = 10 * 1024 * 1024;
};

struct CV_EXPORTS HttpResponse {
    int status = -1;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
};

[[nodiscard]] CV_EXPORTS HttpResponse httpGet(const std::string& url,
                                              const HttpRequestOptions& opts = HttpRequestOptions());

} // namespace client
} // namespace stream
} // namespace cv

#endif // OCV_BUILD_TESTS
#endif // OPENCV_STREAM_CLIENT_HPP
