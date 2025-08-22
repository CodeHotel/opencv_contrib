// test/websocket_test.cpp
#ifdef OCV_BUILD_TESTS

#include "opencv2/stream/server.hpp"
#include "opencv2/stream/client.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <cctype>   // std::isspace
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

static std::mutex g_log_mx; // serialize output across threads

static inline bool parse_int(const uint8_t* data, size_t n, long long& out) {
    // Trim spaces
    size_t i = 0, j = n;
    while (i < j && std::isspace(static_cast<unsigned char>(data[i]))) ++i;
    while (j > i && std::isspace(static_cast<unsigned char>(data[j - 1]))) --j;
    if (i >= j) return false;

    bool neg = false;
    if (data[i] == '+' || data[i] == '-') { neg = (data[i] == '-'); ++i; }
    if (i >= j) return false;

    long long v = 0;
    for (; i < j; ++i) {
        unsigned char c = data[i];
        if (c < '0' || c > '9') return false;
        v = v * 10 + (c - '0');
    }
    out = neg ? -v : v;
    return true;
}

int main() {
    // -------------------------
    // 1) Start server + WS route
    // -------------------------
    auto server = cv::stream::createServer();
    if (!server) {
        std::cerr << "[FAIL] createServer() returned null\n";
        return 1;
    }

    cv::stream::WebSocketHandler wsHandler;
    wsHandler.onMessage = [](cv::stream::WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        const std::string payload(reinterpret_cast<const char*>(data), n);
        {
            std::lock_guard<std::mutex> lk(g_log_mx);
            std::cout << "[Server] Message received from client " << s.remoteAddress()
                      << " : " << payload << std::endl;
        }

        long long v = 0;
        if (!parse_int(data, n, v)) {
            const std::string err = "bad";
            {
                std::lock_guard<std::mutex> lk(g_log_mx);
                std::cout << "[Server] Reply to " << s.remoteAddress()
                          << " : " << err << std::endl;
            }
            s.send(err.data(), err.size(), /*binary=*/false);
            return;
        }

        const std::string out = std::to_string(v + 1);
        {
            std::lock_guard<std::mutex> lk(g_log_mx);
            std::cout << "[Server] Reply to " << s.remoteAddress()
                      << " : " << out << std::endl;
        }
        s.send(out.data(), out.size(), /*binary=*/false);
    };

    const std::string path = "/ws";
    server->registerWebSocketEndpoint(path, wsHandler);

    // Try a small range of ports to reduce flakiness on shared runners.
    int port = 19080;
    bool started = false;
    for (int attempt = 0; attempt < 20 && !started; ++attempt) {
        if (server->start(port, 2)) {
            started = true;
            break;
        }
        ++port;
    }
    if (!started) {
        std::cerr << "[FAIL] Server could not start on ports 19080..19100\n";
        return 1;
    }
    {
        std::lock_guard<std::mutex> lk(g_log_mx);
        std::cout << "[INFO] Server listening on " << port << std::endl;
    }

    // -------------------------
    // 2) Client job: connect & verify echo (+1)
    // -------------------------
    auto client = cv::stream::createWebSocketClient();
    if (!client) {
        std::cerr << "[FAIL] createWebSocketClient() returned null\n";
        server->stop();
        return 1;
    }

    std::mutex mx;
    std::condition_variable cv_;
    std::deque<std::string> inbox;
    std::atomic<bool> open{false};
    std::atomic<bool> done{false};
    std::atomic<bool> ok{true};

    cv::stream::WebSocketHandler clientCb;
    clientCb.onOpen = [&](cv::stream::WebSocketSession& s) {
        open.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lk(g_log_mx);
        std::cout << "[Client] Connected to " << s.remoteAddress() << std::endl;
    };
    clientCb.onMessage = [&](cv::stream::WebSocketSession& s, const uint8_t* data, size_t n, bool /*binary*/) {
        const std::string payload(reinterpret_cast<const char*>(data), n);
        {
            std::lock_guard<std::mutex> lk(g_log_mx);
            std::cout << "[Client] Message received from server " << s.remoteAddress()
                      << " : " << payload << std::endl;
        }
        {
            std::lock_guard<std::mutex> lk(mx);
            inbox.push_back(payload);
        }
        cv_.notify_all();
    };
    clientCb.onClose = [&](cv::stream::WebSocketSession&, int /*code*/, const std::string& /*reason*/) {
        done.store(true, std::memory_order_release);
        cv_.notify_all();
    };

    const std::string url = std::string("ws://127.0.0.1:") + std::to_string(port) + path;
    cv::stream::WebSocketClientOptions opts;
    if (!client->connect(url, clientCb, opts)) {
        std::cerr << "[FAIL] Client failed to connect to " << url << "\n";
        server->stop();
        return 1;
    }

    // Wait for open (with timeout)
    {
        auto t0 = std::chrono::steady_clock::now();
        while (!open.load(std::memory_order_acquire) &&
               std::chrono::steady_clock::now() - t0 < std::chrono::seconds(2)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    if (!open.load(std::memory_order_acquire)) {
        std::cerr << "[FAIL] Client did not reach OPEN state\n";
        client->close();
        server->stop();
        return 1;
    }

    // Send 0..10, expect 1..11 (as text)
    for (int i = 0; i <= 10; ++i) {
        const std::string msg = std::to_string(i);
        {
            std::lock_guard<std::mutex> lk(g_log_mx);
            std::cout << "[Client] Message sent to server " << url << " : " << msg << std::endl;
        }
        if (!client->send(msg)) {
            std::cerr << "[FAIL] send(\"" << msg << "\") failed\n";
            ok.store(false);
            break;
        }

        // Wait for one reply
        std::unique_lock<std::mutex> lk(mx);
        bool got = cv_.wait_for(lk, std::chrono::seconds(2), [&]{ return !inbox.empty(); });
        if (!got) {
            std::cerr << "[FAIL] timed out waiting for reply to " << msg << "\n";
            ok.store(false);
            break;
        }
        std::string reply = std::move(inbox.front());
        inbox.pop_front();
        lk.unlock();

        const std::string expected = std::to_string(i + 1);
        if (reply != expected) {
            std::cerr << "[FAIL] for " << msg << " expected " << expected << " but got " << reply << "\n";
            ok.store(false);
            break;
        }
    }

    if (ok.load()) {
        std::cout << "[PASS] WebSocket round-trip 0..10 -> 1..11 verified\n";
        std::cout << "Shutting down server and client..\n";
        return 0;
    }

    // Cleanup
    client->close();
    server->stop();

    std::cerr << "[FAIL] WebSocket test failed\n";
    return 1;
}

#endif // OCV_BUILD_TESTS
