#if defined(OCV_BUILD_TESTS) && defined(HAVE_STREAM_WEBRTC_GSTREAMER)

#include <opencv2/core.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/stream/stream.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>

using namespace cv;
using namespace cv::stream;

static void collectFiles(const std::string& dir, std::vector<std::string>& out) {
    static const char* exts[] = { "png","bmp","jpg","jpeg","tif","tiff","webp" };
    std::set<std::string> uniq;
    for (const char* e : exts) {
        std::vector<std::string> tmp;
        std::string pat = dir;
        if (!pat.empty() && pat.back() != '/' && pat.back() != '\\') pat += "/";
        pat += "*.";
        pat += e;
        cv::glob(pat, tmp, false);
        for (auto& s : tmp) uniq.insert(s);
    }
    out.assign(uniq.begin(), uniq.end());
    std::sort(out.begin(), out.end());
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << (argv[0] ? argv[0] : "test_webrtc_nvenc") << " <imagepath>\n";
        return 1;
    }
    const std::string dir = argv[1];

    // 1) gather + load images (BGR)
    std::vector<std::string> files;
    collectFiles(dir, files);
    if (files.empty()) {
        std::cerr << "No images found in: " << dir << "\n";
        return 2;
    }

    std::vector<cv::Mat> frames;
    frames.reserve(files.size());
    for (const auto& f : files) {
        cv::Mat img = cv::imread(f, cv::IMREAD_COLOR);
        if (img.empty()) {
            std::cerr << "Skip unreadable: " << f << "\n";
            continue;
        }
        frames.emplace_back(std::move(img));
    }
    if (frames.empty()) {
        std::cerr << "All images failed to load.\n";
        return 3;
    }

    const int W = frames[0].cols;
    const int H = frames[0].rows;
    // keep only CV_8UC3 frames that match the first image size
    frames.erase(std::remove_if(frames.begin(), frames.end(),
                 [W,H](const cv::Mat& m){ return m.cols!=W || m.rows!=H || m.type()!=CV_8UC3; }),
                 frames.end());
    if (frames.empty()) {
        std::cerr << "No images with matching size/type (need CV_8UC3 " << W << "x" << H << ").\n";
        return 4;
    }

    // 2) build a 20 fps round-robin FrameSource with monotonic PTS
    const int fps = 20;
    const int64_t frameDurNs = (int64_t)1000000000LL / fps;
    std::atomic<size_t> idx{0};
    std::atomic<int64_t> pts{0};

    FrameSource source = [&, frameDurNs](cv::Mat& out, int64_t& ptsNs) -> bool {
        size_t i = idx.fetch_add(1);
        i %= frames.size();
        out = frames[i];
        ptsNs = pts.fetch_add(frameDurNs);
        return true;
    };

    // 3) start Stream server
    std::unique_ptr<Stream> srv = createStream();
    if (!srv) {
        std::cerr << "Failed to create Stream\n";
        return 5;
    }
    const bool secure = false; // lab/jupyter mode; UI enabled
    if (!srv->start("0.0.0.0", 25565, secure, /*threads*/2)) {
        std::cerr << "Failed to start server on 0.0.0.0:25565\n";
        return 6;
    }

    // 4) configure WebRTC to prefer H.264 (NVENC via nvh264enc if present)
    WebRtcOptions wopt;
    // Ingest raw BGR; pipeline encodes internally
    wopt.ingest.mode      = IngestMode::AutoEncodeRawBGR;
    wopt.ingest.width     = W;
    wopt.ingest.height    = H;
    wopt.ingest.framerate = fps;
    wopt.ingest.useProvidedTimestamps = true; // we provide PTS

    // ICE/STUN (works on most networks; adjust as needed)
    wopt.webrtc.iceServers.push_back({ "stun:stun.l.google.com:19302", "", "" });
    wopt.webrtc.trickleIce = true;

    // Prefer H.264 only, so our encoder selector chooses NVENC first.
    wopt.webrtc.preferredCodecs.clear();
    wopt.webrtc.preferredCodecs.push_back(VideoCodec::H264);

    // Bitrate hints (bps)
    wopt.webrtc.initialBitrate = 5000000; // 5 Mbps
    wopt.webrtc.minBitrate     = 500000;
    wopt.webrtc.maxBitrate     = 15000000;

    // Transport tuning
    wopt.webrtc.mtu = 1200;
    wopt.webrtc.lowLatency = true;

    // Data channel optional (off here)
    wopt.webrtc.enableDataChannel = false;

    // Have server generate an offer on client connect
    wopt.autostartOffer = true;

    wopt.title = "WebRTC (NVENC preferred) — " + std::to_string(W) + "x" + std::to_string(H) + " @ " + std::to_string(fps) + "fps";

    // 5) add WebRTC endpoint that ingests raw frames (internal encoder uses NVENC if available)
    EndpointHandle h = srv->addWebRtcRaw("/webrtc", source, wopt);
    if (!h.valid()) {
        std::cerr << "Failed to add /webrtc endpoint\n";
        return 7;
    }

    // 6) mount a tiny page at "/" that uses the WebRTC JS client
    WebviewOptions view;
    view.videoClient = webpage::VideoClient::WebRTC;
    view.title = "OpenCV Stream — WebRTC (NVENC preferred)";
    srv->mountEmbedded("/", "/webrtc", view);

    std::cout << "Serving WebRTC (NVENC preferred) on http://127.0.0.1:25565/\n";
    std::cout << "Folder: " << dir << " | Frames: " << frames.size() << " | " << W << "x" << H << " @ " << fps << "fps\n";
    std::cout << "Tip: ensure the GStreamer NVENC plugin (nvh264enc) is installed to actually use NVENC.\n";

    // 7) block forever
    for (;;)
        std::this_thread::sleep_for(std::chrono::hours(24));
    return 0;
}

#else

int main(int, char**) {
    std::cerr << "This test requires OCV_BUILD_TESTS and HAVE_STREAM_WEBRTC_GSTREAMER.\n";
    return 0;
}

#endif
