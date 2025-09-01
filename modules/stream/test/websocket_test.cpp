// test/websocket_test.cpp
#if defined(OCV_BUILD_TESTS)
#include "opencv2/stream/server.hpp"
#include "client.hpp"

#include <opencv2/core/utils/logger.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <cctype>
#include <deque>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <sstream>
#include <utility>
#include <array>

using namespace std::chrono;
namespace cst = cv::stream::client;

// ----------------- logging setup -----------------
namespace {
    using cv::utils::logging::LogLevel;
    static cv::utils::logging::LogTag kStreamLogTag(
        "cv.stream.test",
        LogLevel::LOG_LEVEL_VERBOSE
    );
    cv::utils::logging::LogTag* kTag = &kStreamLogTag;
}

// ----------------- helpers -----------------

static inline bool parse_int(const uint8_t* data, size_t n, long long& out) {
    size_t i = 0, j = n;
    while (i < j && std::isspace(static_cast<unsigned char>(data[i]))) ++i;
    while (j > i && std::isspace(static_cast<unsigned char>(data[j - 1]))) --j;
    if (i >= j) {
        CV_LOG_DEBUG(kTag, "parse_int: empty/whitespace-only payload (n=" << n << ")");
        return false;
    }

    bool neg = false;
    if (data[i] == '+' || data[i] == '-') { neg = (data[i] == '-'); ++i; }
    if (i >= j) {
        CV_LOG_DEBUG(kTag, "parse_int: sign without digits");
        return false;
    }

    long long v = 0;
    for (; i < j; ++i) {
        unsigned char c = data[i];
        if (c < '0' || c > '9') {
            CV_LOG_DEBUG(kTag, "parse_int: non-digit at offset " << (i));
            return false;
        }
        v = v * 10 + (c - '0');
    }
    out = neg ? -v : v;
    CV_LOG_DEBUG(kTag, "parse_int: parsed value=" << out);
    return true;
}

static bool try_start_server(cv::stream::Server& server, int base_port, int attempts, int& chosen_port, int num_threads = 2) {
    int port = base_port;
    for (int attempt = 0; attempt < attempts; ++attempt, ++port) {
        CV_LOG_DEBUG(kTag, "Server start attempt " << attempt+1 << "/" << attempts
                           << " on port " << port << " threads=" << num_threads);
        if (server.start(port, num_threads)) {
            chosen_port = port;
            CV_LOG_INFO(kTag, "Server started on port " << port << " with " << num_threads << " thread(s)");
            return true;
        }
        CV_LOG_WARNING(kTag, "Server failed to bind port " << port << ", trying next...");
    }
    CV_LOG_ERROR(kTag, "Server could not start on any port in [" << base_port << ", " << (base_port + attempts - 1) << "]");
    return false;
}

struct AnchorCtx {
    std::mutex mx;
    std::condition_variable cv;
    std::deque<std::string> inbox;
    std::atomic<bool> open{false};
    std::atomic<bool> closed{false};
    std::atomic<bool> errorClosed{false};
    std::atomic<int>  closeCode{-1};
    std::string closeReason;
};

static bool wait_server_stops(cv::stream::Server& server, int timeout_seconds) {
    auto t0 = std::chrono::steady_clock::now();
    CV_LOG_DEBUG(kTag, "Waiting for server to stop (timeout=" << timeout_seconds << "s) ...");
    while (server.isRunning() &&
           std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now() - t0
           ).count() < timeout_seconds) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    bool stopped = !server.isRunning();
    CV_LOG_INFO(kTag, "Server stop wait result: " << (stopped ? "stopped" : "still running"));
    return stopped;
}


// --------------- Phase 1 + 2: HTTP -----------------

static bool phase_http_server_and_test(
    std::unique_ptr<cv::stream::Server>& server,
    int& port_out,
    int threads,
    std::string& path_ws,
    std::array<std::pair<std::string,std::string>,3>& http_routes_and_bodies,
    cv::stream::ServerConfig cfg_out = cv::stream::ServerConfig()
) {
    CV_LOG_INFO(kTag, "[Phase 1/2] Creating HTTP/WS server");
    server = cv::stream::createServer();
    if (!server) {
        CV_LOG_ERROR(kTag, "[FAIL] createServer() returned null");
        return false;
    }

    CV_LOG_DEBUG(kTag, "Applying initial ServerConfig and registering HTTP endpoints...");
    server->setConfig(cfg_out);

    for (const auto& kv : http_routes_and_bodies) {
        const std::string path = kv.first;
        const std::string body = kv.second;
        CV_LOG_DEBUG(kTag, "Register HTTP endpoint path=\"" << path << "\" body.size=" << body.size());
        server->registerEndpoint(path, [body](const cv::stream::Request& req, cv::stream::Response& res){
            CV_LOG_DEBUG(kTag, "HTTP handler: method=" << req.getMethod()
                                 << " path=" << req.getPath());
            res.setStatusCode(200);
            res.setHeader("Content-Type", "text/html; charset=utf-8");
            res.write(body.data(), body.size());
        });
    }

    path_ws = "/ws";
    CV_LOG_DEBUG(kTag, "Register WebSocket echo endpoint at \"" << path_ws << "\"");
    cv::stream::WebSocketHandler wsEcho;
    wsEcho.onMessage = [](cv::stream::WebSocketSession& s, const uint8_t* data, size_t n, bool binary) {
        CV_LOG_DEBUG(kTag, "WS onMessage: binary=" << (binary?1:0) << " size=" << n);
        long long v = 0;
        if (!parse_int(data, n, v)) {
            const std::string err = "bad";
            CV_LOG_WARNING(kTag, "WS echo: parse failed, sending error reply: \"" << err << "\"");
            s.send(err.data(), err.size(), /*binary=*/false);
            return;
        }
        const std::string out = std::to_string(v + 1);
        CV_LOG_DEBUG(kTag, "WS echo: parsed=" << v << " replying=\"" << out << "\"");
        s.send(out.data(), out.size(), /*binary=*/false);
    };
    server->registerWebSocketEndpoint(path_ws, wsEcho);

    int chosen = 0;
    if (!try_start_server(*server, 19080, 40, chosen, threads)) {
        CV_LOG_ERROR(kTag, "[FAIL] Server could not start on ports 19080..19119");
        return false;
    }
    port_out = chosen;
    CV_LOG_INFO(kTag, "HTTP/WS Server listening on " << chosen << " threads=" << threads);

    cst::HttpRequestOptions httpOpts;
    httpOpts.timeoutMs = 4000;

    CV_LOG_DEBUG(kTag, "Verifying registered HTTP endpoints (" << http_routes_and_bodies.size() << " routes) ...");
    for (const auto& kv : http_routes_and_bodies) {
        const std::string url = std::string("http://127.0.0.1:") + std::to_string(port_out) + kv.first;
        auto resp = cst::httpGet(url, httpOpts);
        CV_LOG_DEBUG(kTag, "HTTP GET " << url << " -> status=" << resp.status << " body.size=" << resp.body.size());
        if (resp.status != 200 || resp.body != kv.second) {
            CV_LOG_ERROR(kTag, "[FAIL] HTTP GET " << url << " expected 200 & exact body; got code=" << resp.status
                             << " body.size=" << resp.body.size());
            return false;
        }
    }

    const std::string unknownUrl = std::string("http://127.0.0.1:") + std::to_string(port_out) + "/__no_such_endpoint__";
    auto nf = cst::httpGet(unknownUrl, httpOpts);
    CV_LOG_DEBUG(kTag, "HTTP GET (404 expected) " << unknownUrl << " -> status=" << nf.status << " body=\"" << nf.body << "\"");
    if (nf.status != 404) {
        CV_LOG_ERROR(kTag, "[FAIL] Unknown endpoint did not return 404 (got " << nf.status << ")");
        return false;
    }
    if (nf.body != "404 Not Found\n") {
        CV_LOG_ERROR(kTag, "[FAIL] Unknown endpoint body mismatch. Expected '404 Not Found\\n' got '" << nf.body << "'");
        return false;
    }

    {
        const std::string url = std::string("http://127.0.0.1:") + std::to_string(port_out) + http_routes_and_bodies[0].first;
        auto resp = cst::httpGet(url, httpOpts);
        CV_LOG_DEBUG(kTag, "HTTP GET post-404 healthcheck " << url << " -> " << resp.status);
        if (resp.status != 200 || resp.body != http_routes_and_bodies[0].second) {
            CV_LOG_ERROR(kTag, "[FAIL] Post-404 health check failed");
            return false;
        }
    }

    CV_LOG_INFO(kTag, "[PASS] Phase 2: HTTP endpoints + 404 default page verified");
    return true;
}

// --------------- Phase 3: WS incremental (keep anchor open) -----------------

static bool phase_ws_incremental_anchor(
    int port,
    const std::string& path_ws,
    std::unique_ptr<cst::Client>& anchorClient,
    std::shared_ptr<AnchorCtx>& anchorCtx
) {
    CV_LOG_INFO(kTag, "[Phase 3] Starting WS incremental (+1) with persistent anchor");
    anchorClient = cst::createWebSocketClient();
    if (!anchorClient) { CV_LOG_ERROR(kTag, "[FAIL] anchor client null"); return false; }

    anchorCtx = std::make_shared<AnchorCtx>();

    cst::WebSocketHandler cbs;
    cbs.onOpen = [ctx=anchorCtx](){
        ctx->open.store(true, std::memory_order_release);
        CV_LOG_INFO(kTag, "[Anchor] Connected");
    };
    cbs.onMessage = [ctx=anchorCtx](const void* d, size_t n, bool){
        {
            std::lock_guard<std::mutex> lk(ctx->mx);
            ctx->inbox.emplace_back(reinterpret_cast<const char*>(d), n);
        }
        CV_LOG_DEBUG(kTag, "[Anchor] Received message size=" << n);
        ctx->cv.notify_all();
    };
    cbs.onClosed = [ctx=anchorCtx](cst::WsCloseCode code, const std::string& reason){
        ctx->closeCode = static_cast<int>(code);
        ctx->closeReason = reason;
        ctx->closed.store(true, std::memory_order_release);
        CV_LOG_WARNING(kTag, "[Anchor] Closed: code=" << static_cast<int>(code) << " reason=\"" << reason << "\"");
        ctx->cv.notify_all();
    };
    cbs.onError = [ctx=anchorCtx, &anchorClient](const std::string& err){
        CV_LOG_WARNING(kTag, "[Anchor] Error: " << err);
        if (!anchorClient->isOpen()) { ctx->errorClosed.store(true, std::memory_order_release); ctx->cv.notify_all(); }
    };

    cst::WebSocketClientOptions opts;
    const std::string url = std::string("ws://127.0.0.1:") + std::to_string(port) + path_ws;
    CV_LOG_DEBUG(kTag, "Connecting anchor to " << url);
    if (!anchorClient->connect(url, cbs, opts)) {
        CV_LOG_ERROR(kTag, "[FAIL] anchor connect failed"); return false;
    }

    {
        auto t0 = steady_clock::now();
        while (!anchorCtx->open.load() && steady_clock::now() - t0 < std::chrono::seconds(2))
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!anchorCtx->open.load()) { CV_LOG_ERROR(kTag, "[FAIL] anchor not open"); return false; }
    }

    for (int i=0;i<=10;i++){
        const std::string msg = std::to_string(i);
        if (!anchorClient->send(msg)) { CV_LOG_ERROR(kTag, "[FAIL] anchor send (msg=" << msg << ")"); return false; }
        CV_LOG_DEBUG(kTag, "[Anchor] Sent \"" << msg << "\"; awaiting reply...");
        std::unique_lock<std::mutex> lk(anchorCtx->mx);
        bool got = anchorCtx->cv.wait_for(lk, std::chrono::seconds(2), [&]{ return !anchorCtx->inbox.empty(); });
        if (!got) { CV_LOG_ERROR(kTag, "[FAIL] anchor wait reply (msg=" << msg << ")"); return false; }
        std::string reply = std::move(anchorCtx->inbox.front()); anchorCtx->inbox.pop_front();
        lk.unlock();
        std::string expected = std::to_string(i+1);
        CV_LOG_DEBUG(kTag, "[Anchor] Reply=\"" << reply << "\" expected=\"" << expected << "\"");
        if (reply != expected) {
            CV_LOG_ERROR(kTag, "[FAIL] anchor reply mismatch for " << msg << " got " << reply << " expected " << expected);
            return false;
        }
    }

    CV_LOG_INFO(kTag, "[PASS] Phase 3: WS incremental (+1) verified (anchor kept open)");
    return true;
}

// --------------- Phase 4: session stress + Phase 4.5 shutdown -----------------

static bool phase_session_stress_and_shutdown(
    cv::stream::Server& server,
    int port,
    const std::string& path_ws,
    int threads,
    cst::Client& anchorClient,
    std::shared_ptr<AnchorCtx> anchorCtx
) {
    CV_LOG_INFO(kTag, "[Phase 4] Spawning WS swarm to stress sessions and verify anchor integrity");
    const int target = 3 * threads;
    std::vector<std::unique_ptr<cst::Client>> swarm;
    swarm.reserve(target);

    std::atomic<int> opens{0};

    // Callbacks must not capture stack objects by reference that can outlive this function.
    auto make_cb = [&](std::shared_ptr<bool> opened){
        cst::WebSocketHandler h;
        h.onOpen = [opened, &opens](){
            *opened = true;
            int o = opens.fetch_add(1) + 1;
            CV_LOG_DEBUG(kTag, "[Swarm] onOpen (opens=" << o << ")");
        };
        h.onClosed = [](cst::WsCloseCode code, const std::string& reason){
            CV_LOG_DEBUG(kTag, "[Swarm] onClosed code=" << static_cast<int>(code) << " reason=\"" << reason << "\"");
        };
        h.onError  = [](const std::string& err){
            CV_LOG_DEBUG(kTag, "[Swarm] onError: " << err);
        };
        return h;
    };

    const std::string url = std::string("ws://127.0.0.1:") + std::to_string(port) + path_ws;
    CV_LOG_DEBUG(kTag, "Swarm connecting to " << url << " count=" << target);

    for (int i=0; i<target; ++i){
        auto c = cst::createWebSocketClient();
        if (!c) { CV_LOG_WARNING(kTag, "[Swarm] createWebSocketClient() returned null"); continue; }
        auto opened = std::make_shared<bool>(false);
        auto cb = make_cb(opened);
        cst::WebSocketClientOptions opts;
        opts.connectTimeoutMs = 750;
        (void)c->connect(url, cb, opts);
        swarm.emplace_back(std::move(c));
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));

    if (!server.isRunning()) {
        CV_LOG_ERROR(kTag, "[FAIL] server not running after swarm connect attempts");
        return false;
    }

    {
        const std::string msg = "41";
        CV_LOG_DEBUG(kTag, "Anchor integrity check: send \"" << msg << "\"");
        if (!anchorClient.send(msg)) { CV_LOG_ERROR(kTag, "[FAIL] anchor send after swarm"); return false; }
        std::unique_lock<std::mutex> lk(anchorCtx->mx);
        bool got = anchorCtx->cv.wait_for(lk, std::chrono::seconds(2), [&]{ return !anchorCtx->inbox.empty(); });
        if (!got) { CV_LOG_ERROR(kTag, "[FAIL] anchor did not receive after swarm"); return false; }
        std::string reply = std::move(anchorCtx->inbox.front()); anchorCtx->inbox.pop_front();
        CV_LOG_DEBUG(kTag, "Anchor post-swarm reply=\"" << reply << "\"");
        if (reply != "42") { CV_LOG_ERROR(kTag, "[FAIL] anchor reply after swarm expected 42 got " << reply); return false; }
    }

    CV_LOG_INFO(kTag, "[PASS] Phase 4: session stress (3x threads=" << threads << ") with anchor integrity");

    CV_LOG_DEBUG(kTag, "Closing anchor and swarm clients...");
    anchorClient.close();
    for (auto& c : swarm) if (c) c->close();
    swarm.clear(); // ensure destruction after threads joined

    CV_LOG_DEBUG(kTag, "Stopping server...");
    server.stop();
    bool stopped = wait_server_stops(server, 10);
    if (!stopped) {
        CV_LOG_ERROR(kTag, "[FAIL] server did not stop within 10s");
        return false;
    }
    CV_LOG_INFO(kTag, "[PASS] Phase 4.5: server/client shutdown within timeout");
    return true;
}


// --------------- Phase 5: KA timeout + Phase 5.5 shutdown -----------------

static bool phase_keepalive_timeout_and_shutdown() {
    CV_LOG_INFO(kTag, "[Phase 5] Keepalive (KA) timeout test: server init");
    auto server = cv::stream::createServer();
    if (!server) { CV_LOG_ERROR(kTag, "[FAIL] createServer() null (KA)"); return false; }

    cv::stream::ServerConfig cfg = server->getConfig();
    cfg.ws.enabled = true;
    cfg.ws.challengeInterval = std::chrono::milliseconds(2000);
    cfg.ws.clientTimeout     = std::chrono::milliseconds(2000);
    cfg.ws.challengeSize     = 8;
    cfg.ws.ackPrefix         = "ACK ";
    cfg.ws.disableFrameworkIdleTimeout = true;
    server->setConfig(cfg);

    const std::string path = "/ka";
    // Match requested logging style example:
    const std::string path_ = path;
    const bool ka_enabled_ = cfg.ws.enabled;
    CV_LOG_DEBUG(kTag, "WS KA init path=" << path_ << " enabled=" << (ka_enabled_ ? 1 : 0));

    cv::stream::WebSocketHandler wsHandler;
    wsHandler.onOpen = [](cv::stream::WebSocketSession& s){
        CV_LOG_DEBUG(kTag, "[KA-Server] session open from " << s.remoteAddress());
    };
    wsHandler.onClose = [](cv::stream::WebSocketSession& s, int code, const std::string& reason){
        CV_LOG_DEBUG(kTag, "[KA-Server] session closed (" << code << "): " << reason
                                 << " from " << s.remoteAddress());
    };
    server->registerWebSocketEndpoint(path, wsHandler);

    int port = 0;
    if (!try_start_server(*server, 19110, 40, port, /*threads=*/1)) {
        CV_LOG_ERROR(kTag, "[FAIL] KA server failed to start");
        return false;
    }
    CV_LOG_INFO(kTag, "KA server listening on " << port);

    auto client = cst::createWebSocketClient();
    if (!client) { CV_LOG_ERROR(kTag, "[FAIL] KA client null"); server->stop(); return false; }

    std::atomic<int>  challenges{0};
    std::atomic<bool> done{false};
    std::atomic<bool> graceful{false};
    std::atomic<int>  close_code{-1};
    std::string close_reason;
    std::atomic<long long> first_miss_ns{-1};

    std::mutex mx;
    std::condition_variable cv;

    cst::WebSocketHandler cbs;
    cbs.onOpen = [&](){
        CV_LOG_INFO(kTag, "[Client-KA] connected to " << client->remoteAddress());
    };
    cbs.onMessage = [&](const void* dd, size_t n, bool){
        const uint8_t* d = static_cast<const uint8_t*>(dd);
        std::string token(reinterpret_cast<const char*>(d), n);
        while (!token.empty() && std::isspace(static_cast<unsigned char>(token.front()))) token.erase(token.begin());
        while (!token.empty() && std::isspace(static_cast<unsigned char>(token.back()))) token.pop_back();

        int seen = ++challenges;
        CV_LOG_DEBUG(kTag, "[Client-KA] challenge #" << seen << " token=\"" << token << "\"");
        if (seen <= 3) {
            const std::string ack = "ACK " + token;
            (void)client->send(ack);
            CV_LOG_DEBUG(kTag, "[Client-KA] sent \"" << ack << "\"");
        } else if (seen == 4 && first_miss_ns.load() < 0) {
            first_miss_ns.store(duration_cast<nanoseconds>(steady_clock::now().time_since_epoch()).count());
            CV_LOG_DEBUG(kTag, "[Client-KA] intentionally NOT acking challenge #" << seen << " to trigger timeout");
        }
        //std::lock_guard<std::mutex> lk(mx); cv.notify_all();
        cv.notify_one(); //no mutex needed
    };
    cbs.onClosed = [&](cst::WsCloseCode code, const std::string& reason){
        close_code = static_cast<int>(code); close_reason = reason; done.store(true);
        CV_LOG_INFO(kTag, "[Client-KA] closed by server code=" << close_code.load() << " reason=\"" << close_reason << "\"");
        //std::lock_guard<std::mutex> lk(mx); cv.notify_all();
        cv.notify_all(); //no mutex needed
    };
    cbs.onError = [&](const std::string& err){
        CV_LOG_WARNING(kTag, "[Client-KA] error: " << err);
        //if (!client->isOpen()) { graceful.store(true); std::lock_guard<std::mutex> lk(mx); cv.notify_all(); }
        if (!client->isOpen()) { graceful.store(true, std::memory_order_release); cv.notify_all(); }
    };

    cst::WebSocketClientOptions opts;
    const std::string url = std::string("ws://127.0.0.1:") + std::to_string(port) + path;
    CV_LOG_DEBUG(kTag, "[Client-KA] connecting to " << url);
    if (!client->connect(url, cbs, opts)) {
        CV_LOG_ERROR(kTag, "[FAIL] KA client connect failed"); server->stop(); return false;
    }

    {
        std::unique_lock<std::mutex> lk(mx);
        bool got3 = cv.wait_for(lk, std::chrono::seconds(10), [&]{ return challenges.load() >= 3; });
        CV_LOG_DEBUG(kTag, "[Client-KA] wait >=3 challenges -> " << (got3 ? "ok" : "timeout"));
        if (!got3) { CV_LOG_ERROR(kTag, "[FAIL] KA: <3 challenges"); client->close(); server->stop(); return false; }
    }

    {
        std::unique_lock<std::mutex> lk(mx);
        bool closed = cv.wait_for(lk, std::chrono::seconds(6), [&]{ return done.load() || graceful.load(); });
        CV_LOG_DEBUG(kTag, "[Client-KA] wait for closure -> " << (closed ? "closed" : "still open"));
        if (!closed) { CV_LOG_ERROR(kTag, "[FAIL] KA: server did not close after missed ACK"); client->close(); server->stop(); return false; }
    }

    const int expected = static_cast<int>(cst::WsCloseCode::PolicyViolation);
    if (done.load() && close_code.load() != expected) {
        CV_LOG_ERROR(kTag, "[FAIL] KA: expected 1008 but got " << close_code.load() << " reason=\"" << close_reason << "\"");
        client->close(); server->stop(); return false;
    } else if (!done.load()) {
        CV_LOG_WARNING(kTag, "[WARN] KA: backend signaled closure via read error (no code)");
    }

    CV_LOG_DEBUG(kTag, "Closing KA client and stopping KA server...");
    client->close();
    server->stop();
    if (!wait_server_stops(*server, 10)) {
        CV_LOG_ERROR(kTag, "[FAIL] KA server did not stop within 10s");
        return false;
    }

    CV_LOG_INFO(kTag, "[PASS] Phase 5 & 5.5: KA timeout then clean shutdown");
    return true;
}

// -------------------------------- main --------------------------------

int main() {
    CV_LOG_INFO(kTag, "===== websocket_test starting =====");
    std::unique_ptr<cv::stream::Server> server;
    int port = 0;
    std::string path_ws;
    std::array<std::pair<std::string,std::string>,3> http_routes_and_bodies {{
        {"/",     "<!doctype html><html><body>root ok</body></html>\n"},
        {"/a",    "<!doctype html><html><body>a ok</body></html>\n"},
        {"/page", "<!doctype html><html><body>page ok</body></html>\n"}
    }};
    const int threads = 3;

    if (!phase_http_server_and_test(server, port, threads, path_ws, http_routes_and_bodies)) return 1;

    std::unique_ptr<cst::Client> anchorClient;
    std::shared_ptr<AnchorCtx> anchorCtx;
    if (!phase_ws_incremental_anchor(port, path_ws, anchorClient, anchorCtx)) {
        CV_LOG_WARNING(kTag, "Phase 3 failed; stopping server if running...");
        server->stop();
        wait_server_stops(*server, 10);
        return 1;
    }

    if (!phase_session_stress_and_shutdown(*server, port, path_ws, threads, *anchorClient, anchorCtx)) {
        CV_LOG_ERROR(kTag, "Phase 4 failed");
        return 1;
    }

    if (!phase_keepalive_timeout_and_shutdown()) {
        CV_LOG_ERROR(kTag, "Phase 5 failed");
        return 1;
    }

    CV_LOG_INFO(kTag, "[PASS] All phases completed");
    CV_LOG_INFO(kTag, "===== websocket_test finished =====");
    return 0;
}
#endif
