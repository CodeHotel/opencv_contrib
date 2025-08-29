#if defined(OCV_BUILD_TESTS) && defined(HAVE_STREAM_WEBRTC_GSTREAMER) && defined(HAVE_STREAM_COMPRESSION)

#include <opencv2/core.hpp>
#include <opencv2/imgcodecs.hpp>
#include <opencv2/stream/stream.hpp>
#include <opencv2/stream/encoder.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <iostream>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>

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

struct EncodedItem {
    std::vector<uint8_t> au;
    bool key = false;
    int64_t pts = -1; // ns
};

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: " << (argv[0] ? argv[0] : "test_webrtc_ffmpeg") << " <imagepath>\n";
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

    // 2) build a 20 fps producer that encodes frames to H.264 (Annex-B AUs)
    const int fps = 20;
    const int64_t frameDurNs = (int64_t)1000000000LL / fps;

    // Choose an encoder (prefer NVENC if available)
    EncoderParams ep;
    ep.width     = W;
    ep.height    = H;
    ep.framerate = fps;
    ep.bitrate   = 5000000;       // 5 Mbps
    ep.gopSize   = fps;             // ~1s keyint
    {
        auto avail = Encoder::getAvailableEncoders();
        if (avail.count("h264_nvenc"))      ep.codecName = "h264_nvenc";
        else if (avail.count("libx264"))    ep.codecName = "libx264";
        else                                 ep.codecName = "auto"; // let it pick a valid H.264 encoder
    }

    Encoder enc;
    if (!enc.open(ep)) {
        std::cerr << "Failed to open FFmpeg encoder (H.264)\n";
        return 5;
    }

    // Simple queue between producer and EncodedSource
    std::deque<EncodedItem> q;
    std::mutex qmtx;

    std::atomic<bool> run{true};
    std::atomic<size_t> idx{0};
    std::atomic<int64_t> pts{0};

    auto is_h264_idr = [](const std::vector<uint8_t>& au) -> bool {
        // Cheap Annex-B scan for IDR (nal_unit_type == 5)
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
            // advance to next start code
            const uint8_t* r = qcur + 1;
            while (r + 3 < end && !(r[0]==0 && r[1]==0 && (r[2]==1 || (r[2]==0 && r+4<end && r[3]==1)))) ++r;
            qcur = r;
        }
        return false;
    };

    std::thread producer([&]() {
        int64_t nextPts = 0;
        while (run) {
            // push next frame
            size_t i = idx.fetch_add(1);
            i %= frames.size();
            enc.push(frames[i]);

            // drain encoded AUs and enqueue
            std::vector<uint8_t> au;
            while (enc.pullAsRtp(au)) {
                EncodedItem it;
                it.au = std::move(au);
                it.key = is_h264_idr(it.au);
                it.pts = nextPts;
                {
                    std::lock_guard<std::mutex> lk(qmtx);
                    q.emplace_back(std::move(it));
                }
                au.clear();
                nextPts += frameDurNs;
            }

            // pace ~fps
            std::this_thread::sleep_for(std::chrono::nanoseconds(frameDurNs));
        }
    });

    // EncodedSource: pop one AU when available (Annex-B for H.264)
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

    // 3) start Stream server
    std::unique_ptr<Stream> srv = createStream();
    if (!srv) {
        std::cerr << "Failed to create Stream\n";
        run = false; producer.join();
        return 6;
    }
    const bool secure = false; // lab/jupyter mode; UI enabled
    if (!srv->start("0.0.0.0", 25565, secure, /*threads*/2)) {
        std::cerr << "Failed to start server on 0.0.0.0:25565\n";
        run = false; producer.join();
        return 7;
    }

    // 4) configure WebRTC to prefer H.264; ingest is pre-encoded elementary stream
    WebRtcOptions wopt;

    wopt.ingest.framerate = fps;
    wopt.ingest.useProvidedTimestamps = true; // we provide PTS

    // ICE/STUN (works on most networks; adjust as needed)
    wopt.webrtc.iceServers.push_back({ "stun:stun.l.google.com:19302", "", "" });
    wopt.webrtc.trickleIce = true;

    wopt.webrtc.preferredCodecs.clear();
    wopt.webrtc.preferredCodecs.push_back(VideoCodec::H264); // we are producing H.264

    // Bitrate hints (bps)
    wopt.webrtc.initialBitrate = 5000000;
    wopt.webrtc.minBitrate     =   500000;
    wopt.webrtc.maxBitrate     = 15000000;

    // Transport tuning
    wopt.webrtc.mtu = 1200;
    wopt.webrtc.lowLatency = true;

    // Data channel optional (off here)
    wopt.webrtc.enableDataChannel = false;

    // Have server generate an offer on client connect
    wopt.autostartOffer = true;

    wopt.title = "WebRTC (FFmpeg pre-encoded H.264) — " + std::to_string(W) + "x" + std::to_string(H) + " @ " + std::to_string(fps) + "fps";

    // 5) add WebRTC endpoint with pre-encoded source
    EndpointHandle h = srv->addWebRtc("/webrtc", encodedSource, wopt);
    if (!h.valid()) {
        std::cerr << "Failed to add /webrtc endpoint\n";
        run = false; producer.join();
        return 8;
    }

    // 6) mount a tiny page at "/" that uses the WebRTC JS client
    WebviewOptions view;
    view.videoClient = webpage::VideoClient::WebRTC;
    view.title = "OpenCV Stream — WebRTC (FFmpeg pre-encoded H.264)";
    srv->mountEmbedded("/", "/webrtc", view);

    std::cout << "Serving WebRTC (FFmpeg pre-encoded H.264) on http://127.0.0.1:25565/\n";
    std::cout << "Folder: " << dir << " | Frames: " << frames.size() << " | " << W << "x" << H << " @ " << fps << "fps\n";
    std::cout << "Tip: FFmpeg may use NVENC if available via h264_nvenc.\n";

    // 7) block forever
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
