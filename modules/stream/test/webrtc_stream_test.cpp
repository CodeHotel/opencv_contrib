#if defined(OCV_BUILD_TESTS) && defined(HAVE_STREAM_WEBRTC_GSTREAMER) && defined(HAVE_STREAM_COMPRESSION)

#include <opencv2/core.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/videoio.hpp>
#include <opencv2/imgproc.hpp>
#include <opencv2/stream/stream_helper.hpp>
#include <opencv2/stream/encoder.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <set>

using namespace cv;
using namespace cv::stream;

struct EncodedItem {
    std::vector<uint8_t> au;
    bool key = false;
    int64_t pts = -1; // ns
};

static bool is_h264_idr(const std::vector<uint8_t>& au) {
    const uint8_t* p = au.data();
    const uint8_t* const end = p + au.size();
    auto next_start = [&](const uint8_t* q) -> const uint8_t* {
        for (; q + 3 < end; ++q) {
            if (q[0]==0 && q[1]==0 && (q[2]==1 || (q[2]==0 && q+4<end && q[3]==1)))
                return q + (q[2]==1 ? 3 : 4);
        }
        return nullptr;
    };
    const uint8_t* qcur = p;
    while ((qcur = next_start(qcur))) {
        if (qcur >= end) break;
        uint8_t nal_type = qcur[0] & 0x1F;
        if (nal_type == 5) return true; // IDR
        const uint8_t* r = qcur + 1;
        while (r + 3 < end && !(r[0]==0 && r[1]==0 && (r[2]==1 || (r[2]==0 && r+4<end && r[3]==1)))) ++r;
        qcur = r;
    }
    return false;
}

static bool parseInt(const std::string& s, int& out) {
    if (s.empty()) return false;
    char* endp = nullptr;
    long v = std::strtol(s.c_str(), &endp, 10);
    if (*endp != '\0') return false;
    out = static_cast<int>(v);
    return true;
}

int main(int argc, char** argv) {
    // [device] [fps] [width] [height]
    std::string device = "/dev/video0";
    int desiredFps = -1;
    int desiredW = -1, desiredH = -1;

    if (argc >= 2) device = argv[1];
    if (argc >= 3) desiredFps = std::max(1, std::atoi(argv[2]));
    if (argc >= 5) { desiredW = std::max(1, std::atoi(argv[3])); desiredH = std::max(1, std::atoi(argv[4])); }

    // 1) Open V4L2 camera
    cv::VideoCapture cap;
    int index = -1;
    if (parseInt(device, index)) {
        if (!cap.open(index, cv::CAP_V4L2)) {
            std::cerr << "Failed to open V4L2 camera by index: " << index << "\n";
            return 1;
        }
    } else {
        if (!cap.open(device, cv::CAP_V4L2)) {
            std::cerr << "Failed to open V4L2 camera: " << device << "\n";
            return 1;
        }
    }

    if (desiredW > 0 && desiredH > 0) {
        cap.set(cv::CAP_PROP_FRAME_WIDTH,  desiredW);
        cap.set(cv::CAP_PROP_FRAME_HEIGHT, desiredH);
    }
    if (desiredFps > 0) {
        cap.set(cv::CAP_PROP_FPS, desiredFps);
    }
    // Prefer MJPEG if available (best-effort)
    cap.set(cv::CAP_PROP_FOURCC, cv::VideoWriter::fourcc('M','J','P','G'));
    cap.set(cv::CAP_PROP_BUFFERSIZE, 1);


    // Probe first frame
    cv::Mat probe;
    if (!cap.read(probe) || probe.empty()) {
        std::cerr << "Failed to capture initial frame from camera.\n";
        return 2;
    }
    // Normalize to CV_8UC3 (BGR) if needed
    if (probe.type() == CV_8UC4) {
        cv::cvtColor(probe, probe, cv::COLOR_BGRA2BGR);
    } else if (probe.type() == CV_8UC1) {
        cv::cvtColor(probe, probe, cv::COLOR_GRAY2BGR);
    }

    const int W = probe.cols;
    const int H = probe.rows;

    int fps = 0;
    double capFps = cap.get(cv::CAP_PROP_FPS);
    if (capFps > 0.5 && capFps < 500.0) fps = static_cast<int>(std::lround(capFps));
    if (desiredFps > 0) fps = desiredFps;
    if (fps <= 0) fps = 30;

    const int64_t frameDurNs = (int64_t)1000000000LL / fps;

    // 2) H.264 encoder (Annex-B), prefer NVENC
    EncoderParams ep;
    ep.width     = W;
    ep.height    = H;
    ep.framerate = fps;
    ep.bitrate   = 5000000;   // 5 Mbps  (C++11-safe)
    ep.gopSize   = std::max(1, fps/2);       // ~1s keyint
    {
        auto avail = Encoder::getAvailableEncoders();
        if (avail.count("h264_nvenc"))      ep.codecName = "h264_nvenc";
        else if (avail.count("libx264"))    ep.codecName = "libx264";
        else                                 ep.codecName = "auto";
    }

    Encoder enc;
    if (!enc.open(ep)) {
        std::cerr << "Failed to open FFmpeg encoder (H.264)\n";
        return 3;
    }

    // 3) Leaky-latest queue between producer and EncodedSource
    static const size_t MAX_Q = 2;  // keep the last 1–2 AUs to stay near-live
    std::deque<EncodedItem> q;
    std::mutex qmtx;

    std::atomic<bool> run{true};

    std::thread producer([&]() {
        int64_t nextPts = 0;
        cv::Mat frame = probe;

        for (;;) {
            if (!frame.empty()) {
                // Ensure CV_8UC3 and expected size
                if (frame.type() == CV_8UC4) {
                    cv::cvtColor(frame, frame, cv::COLOR_BGRA2BGR);
                } else if (frame.type() == CV_8UC1) {
                    cv::cvtColor(frame, frame, cv::COLOR_GRAY2BGR);
                }
                if (frame.cols != W || frame.rows != H) {
                    cv::Mat resized;
                    cv::resize(frame, resized, cv::Size(W, H), 0, 0, cv::INTER_LINEAR);
                    frame = resized;
                }

                enc.push(frame);

                std::vector<uint8_t> au;
                while (enc.pullAsRtp(au)) {
                    EncodedItem it;
                    it.au  = std::move(au);
                    it.key = is_h264_idr(it.au);
                    it.pts = nextPts;
                    {
                        std::lock_guard<std::mutex> lk(qmtx);
                        while (q.size() >= MAX_Q) q.pop_front();   // drop oldest to prevent latency build-up
                        q.emplace_back(std::move(it));             // keep only the most recent frames
                    }

                    au.clear();
                    nextPts += frameDurNs;
                }
            }

            // Grab next frame
            if (!cap.read(frame) || frame.empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
        }
    });

    // 4) EncodedSource (Annex-B for H.264)
    EncodedSource encodedSource = [&](std::vector<uint8_t>& au, bool& key, int64_t& ptsNs) -> bool {
        std::lock_guard<std::mutex> lk(qmtx);
        if (q.empty())
            return false;
        EncodedItem it = std::move(q.front());
        q.pop_front();
        au = std::move(it.au);
        key = it.key;
        ptsNs = it.pts;
        return true;
    };

    // 5) Start Stream server
    std::unique_ptr<StreamHelper> srv = createStreamHelper();
    if (!srv) {
        std::cerr << "Failed to create Stream\n";
        run = false; producer.join();
        return 4;
    }
    const bool secure = false;
    if (!srv->start("0.0.0.0", 25565, secure, /*threads*/2)) {
        std::cerr << "Failed to start server on 0.0.0.0:25565\n";
        run = false; producer.join();
        return 5;
    }

    // 6) WebRTC options
    WebRtcOptions wopt;
    wopt.ingest.framerate = fps;
    wopt.ingest.useProvidedTimestamps = true;

    wopt.webrtc.iceServers.push_back({ "stun:stun.l.google.com:19302", "", "" });
    wopt.webrtc.trickleIce = true;

    wopt.webrtc.preferredCodecs.clear();
    wopt.webrtc.preferredCodecs.push_back(VideoCodec::H264);

    wopt.webrtc.initialBitrate = 5000000;
    wopt.webrtc.minBitrate     = 500000;
    wopt.webrtc.maxBitrate     = 15000000;

    wopt.webrtc.mtu = 1200;
    wopt.webrtc.lowLatency = true;
    wopt.webrtc.enableDataChannel = false;

    wopt.autostartOffer = true;
    wopt.title = "WebRTC (FFmpeg pre-encoded H.264) — Camera " + device + " " + std::to_string(W) + "x" + std::to_string(H) + " @ " + std::to_string(fps) + "fps";

    // 7) Endpoint
    EndpointHandle h = srv->addWebRtc("/webrtc", encodedSource, wopt);
    if (!h.valid()) {
        std::cerr << "Failed to add /webrtc endpoint\n";
        run = false; producer.join();
        return 6;
    }

    // 8) Embedded page
    WebviewOptions view;
    view.videoClient = webpage::VideoClient::WebRTC;
    view.title = "OpenCV Stream — WebRTC (FFmpeg pre-encoded H.264)";
    srv->mountEmbedded("/", "/webrtc", view);

    std::cout << "Serving WebRTC (FFmpeg pre-encoded H.264) on http://127.0.0.1:25565/\n";
    std::cout << "Camera: " << device << " | " << W << "x" << H << " @ " << fps << "fps\n";
    std::cout << "Tip: FFmpeg may use NVENC if available via h264_nvenc.\n";

    for (;;)
        std::this_thread::sleep_for(std::chrono::hours(24));

    run = false;
    producer.join();
    return 0;
}

#else

int main(int, char**) {
    std::cerr << "This test requires OCV_BUILD_TESTS, HAVE_STREAM_WEBRTC_GSTREAMER, and HAVE_STREAM_COMPRESSION.\n";
    return 0;
}

#endif
