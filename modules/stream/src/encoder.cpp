#if defined(HAVE_STREAM_COMPRESSION)
#include "opencv2/core.hpp"
#include "opencv2/stream/encoder.hpp"

#include <string>
#include <vector>
#include <map>
#include <memory>
#include <cstdint>
#include <queue>
#include <chrono>
#include <stdexcept>
#include <mutex>
#include <tuple>
#include <filesystem>
#include <numeric>

extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
#include <libavutil/timestamp.h>
#include <libavutil/opt.h>
#include <libavutil/hwcontext.h>
}

// Global FFmpeg initialization flag
static std::once_flag ffmpeg_init_flag;

static void initialize_ffmpeg() {
    av_log_set_level(AV_LOG_WARNING); // Set logging to avoid verbosity
}

namespace cv {
namespace stream {

EncoderParams::EncoderParams() = default;
RecordingParams::RecordingParams() = default;

// Internal implementation class
class Encoder::EncoderImpl {
public:
    EncoderImpl() = default;
    ~EncoderImpl();

    bool open(const EncoderParams& params);
    void release();
    bool isOpened() const;
    bool push(const cv::Mat& frame);

    // Egress
    bool pullAsRtp(std::vector<uint8_t>& packet);
    bool getFmp4InitializationSegment(std::vector<uint8_t>& initSegment);
    bool pullAsFmp4(std::vector<uint8_t>& fragment);

    // Recording
    bool configureRecording(const RecordingParams& params);
    bool startRecording();
    void stopRecording();
    bool isRecording() const;
    bool splitSegment();

    static std::map<std::string, bool> getAvailableEncoders();

private:
    // Core FFmpeg objects
    AVCodec* codec = nullptr;
    AVCodecContext* codec_ctx = nullptr;
    SwsContext* sws_ctx = nullptr;
    AVFrame* frame = nullptr;
    AVPacket* packet = nullptr;

    // Recording objects
    AVFormatContext* record_fmt_ctx = nullptr;
    AVIOContext* record_avio_ctx = nullptr;
    RecordingParams current_recording_params;
    int record_stream_idx = -1;
    long long last_segment_pts = -1;
    int segment_seq = 0;
    std::string current_record_path;

    // State
    EncoderParams current_params;
    bool m_is_opened = false;
    bool m_is_recording = false;
    mutable std::mutex mtx;

    // Streaming egress
    std::queue<AVPacket*> encoded_packets;

    // Helper functions
    bool setup_codec_context();
    bool setup_recording_context(const RecordingParams& params);
    bool write_record_header();
    bool write_record_trailer();
    std::string generate_record_path(int sequence_number = -1);
};

// -- EncoderImpl Implementation ------------------------------------------------

Encoder::EncoderImpl::~EncoderImpl() {
    release();
}

bool Encoder::EncoderImpl::open(const EncoderParams& params) {
    std::lock_guard<std::mutex> lock(mtx);
    if (m_is_opened) {
        release();
    }

    std::call_once(ffmpeg_init_flag, initialize_ffmpeg);

    current_params = params;

    // Find the encoder
    if (params.codecName.empty() || params.codecName == "auto") {
        // Find best available codec in order: h264, vp8, av1
        std::vector<std::string> prioritized_list = {
            "h264_nvenc", "h264_qsv", "h264_videotoolbox", "libx264",
            "libvpx-vp8", "libvpx",
            "av1_nvenc", "av1_qsv", "libsvtav1", "libaom-av1"
        };
        for (const auto& name : prioritized_list) {
            codec = avcodec_find_encoder_by_name(name.c_str());
            if (codec) {
                current_params.codecName = name;
                break;
            }
        }
    } else {
        codec = avcodec_find_encoder_by_name(params.codecName.c_str());
    }

    if (!codec) {
        // Could not find codec
        return false;
    }

    if (!setup_codec_context()) {
        release();
        return false;
    }

    // Allocate frame and packet
    frame = av_frame_alloc();
    if (!frame) {
        release();
        return false;
    }
    frame->format = codec_ctx->pix_fmt;
    frame->width = codec_ctx->width;
    frame->height = codec_ctx->height;
    if (av_frame_get_buffer(frame, 32) < 0) {
        release();
        return false;
    }

    packet = av_packet_alloc();
    if (!packet) {
        release();
        return false;
    }

    m_is_opened = true;
    return true;
}

bool Encoder::EncoderImpl::setup_codec_context() {
    codec_ctx = avcodec_alloc_context3(codec);
    if (!codec_ctx) {
        return false;
    }

    codec_ctx->bit_rate = current_params.bitrate > 0 ? current_params.bitrate : 2000000;
    codec_ctx->width = current_params.width;
    codec_ctx->height = current_params.height;
    codec_ctx->time_base = {1, current_params.framerate > 0 ? current_params.framerate : 30};
    codec_ctx->gop_size = current_params.gopSize > 0 ? current_params.gopSize : 12;
    codec_ctx->pix_fmt = AV_PIX_FMT_YUV420P; // Most common for H.264/VP8

    // Open the codec
    if (avcodec_open2(codec_ctx, codec, nullptr) < 0) {
        return false;
    }

    // Initialize swscaler context
    sws_ctx = sws_getContext(
        current_params.width, current_params.height, AV_PIX_FMT_BGR24,
        codec_ctx->width, codec_ctx->height, codec_ctx->pix_fmt,
        SWS_BICUBIC, nullptr, nullptr, nullptr);
    if (!sws_ctx) {
        return false;
    }

    return true;
}

void Encoder::EncoderImpl::release() {
    std::lock_guard<std::mutex> lock(mtx);
    if (m_is_recording) {
        stopRecording();
    }
    if (codec_ctx) {
        avcodec_free_context(&codec_ctx);
        codec_ctx = nullptr;
    }
    if (sws_ctx) {
        sws_freeContext(sws_ctx);
        sws_ctx = nullptr;
    }
    if (frame) {
        av_frame_free(&frame);
        frame = nullptr;
    }
    if (packet) {
        av_packet_free(&packet);
        packet = nullptr;
    }
    while (!encoded_packets.empty()) {
        av_packet_free(&encoded_packets.front());
        encoded_packets.pop();
    }
    m_is_opened = false;
}

bool Encoder::EncoderImpl::isOpened() const {
    // 뮤텍스가 const이므로, lock_guard를 사용할 수 없습니다.
    // 대신, 간단히 상태 변수를 반환합니다.
    return m_is_opened;
}

bool Encoder::EncoderImpl::push(const cv::Mat& frame_mat) {
    std::lock_guard<std::mutex> lock(mtx);
    if (!m_is_opened) {
        return false;
    }

    // Convert BGR Mat to YUV420p AVFrame
    const uint8_t* in_data[1] = {frame_mat.data};
    int in_linesize[1] = {static_cast<int>(frame_mat.cols * frame_mat.elemSize())};
    sws_scale(sws_ctx, in_data, in_linesize, 0, frame_mat.rows, frame->data, frame->linesize);

    frame->pts = av_rescale_q(std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()).count(),
        {1, 1000000}, codec_ctx->time_base);

    int ret = avcodec_send_frame(codec_ctx, frame);
    if (ret < 0) {
        return false;
    }

    while (ret >= 0) {
        ret = avcodec_receive_packet(codec_ctx, packet);
        if (ret == AVERROR(EAGAIN) || ret == AVERROR_EOF) {
            break;
        } else if (ret < 0) {
            return false;
        }

        // Add to streaming queue
        encoded_packets.push(av_packet_clone(packet));

        // If recording, write to file
        if (m_is_recording) {
            if (record_fmt_ctx->oformat->flags & AVFMT_GLOBALHEADER) {
                packet->stream_index = record_stream_idx;
                av_packet_rescale_ts(packet, codec_ctx->time_base, record_fmt_ctx->streams[record_stream_idx]->time_base);
                av_interleaved_write_frame(record_fmt_ctx, packet);
            }
        }
        av_packet_unref(packet);
    }

    return true;
}

bool Encoder::EncoderImpl::pullAsRtp(std::vector<uint8_t>& packet_vec) {
    std::lock_guard<std::mutex> lock(mtx);
    if (!m_is_opened || encoded_packets.empty()) {
        return false;
    }

    AVPacket* p = encoded_packets.front();
    encoded_packets.pop();

    packet_vec.assign(p->data, p->data + p->size);
    av_packet_free(&p);

    return true;
}

// Fmp4 methods are stubs for future implementation
bool Encoder::EncoderImpl::getFmp4InitializationSegment(std::vector<uint8_t>& initSegment) {
    return false;
}

bool Encoder::EncoderImpl::pullAsFmp4(std::vector<uint8_t>& fragment) {
    return false;
}

bool Encoder::EncoderImpl::configureRecording(const RecordingParams& params) {
    std::lock_guard<std::mutex> lock(mtx);
    if (m_is_recording) {
        stopRecording();
    }
    current_recording_params = params;
    return true;
}

bool Encoder::EncoderImpl::startRecording() {
    std::lock_guard<std::mutex> lock(mtx);
    if (m_is_recording) {
        return true;
    }
    if (!m_is_opened) {
        return false;
    }

    if (!setup_recording_context(current_recording_params)) {
        return false;
    }

    m_is_recording = write_record_header();
    return m_is_recording;
}

void Encoder::EncoderImpl::stopRecording() {
    std::lock_guard<std::mutex> lock(mtx);
    if (!m_is_recording) {
        return;
    }
    write_record_trailer();
    m_is_recording = false;
    if (record_avio_ctx) {
        av_write_trailer(record_fmt_ctx);
        avio_closep(&record_avio_ctx);
        record_avio_ctx = nullptr;
    }
    if (record_fmt_ctx) {
        avformat_free_context(record_fmt_ctx);
        record_fmt_ctx = nullptr;
    }
    record_stream_idx = -1;
}

bool Encoder::EncoderImpl::isRecording() const {
    // 뮤텍스가 const이므로, lock_guard를 사용할 수 없습니다.
    // 대신, 간단히 상태 변수를 반환합니다.
    return m_is_recording;
}

bool Encoder::EncoderImpl::splitSegment() {
    std::lock_guard<std::mutex> lock(mtx);
    if (!m_is_recording) {
        return false;
    }

    write_record_trailer();

    // Check if maxSegments is exceeded and prune
    if (current_recording_params.maxSegments > 0) {
        // Simple file-based pruning (not implemented here, requires file system access)
    }

    segment_seq++;

    return write_record_header();
}

bool Encoder::EncoderImpl::setup_recording_context(const RecordingParams& params) {
    std::string filename = generate_record_path(segment_seq);
    int ret = avio_open(&record_avio_ctx, filename.c_str(), AVIO_FLAG_WRITE);
    if (ret < 0) {
        return false;
    }

    ret = avformat_alloc_output_context2(&record_fmt_ctx, nullptr, nullptr, filename.c_str());
    if (ret < 0) {
        avio_closep(&record_avio_ctx);
        return false;
    }
    record_fmt_ctx->pb = record_avio_ctx;

    AVStream* stream = avformat_new_stream(record_fmt_ctx, nullptr);
    if (!stream) {
        avio_closep(&record_avio_ctx);
        avformat_free_context(record_fmt_ctx);
        return false;
    }
    record_stream_idx = stream->index;
    avcodec_parameters_from_context(stream->codecpar, codec_ctx);

    if (record_fmt_ctx->oformat->flags & AVFMT_GLOBALHEADER) {
        codec_ctx->flags |= AV_CODEC_FLAG_GLOBAL_HEADER;
    }

    return true;
}

bool Encoder::EncoderImpl::write_record_header() {
    if (!record_fmt_ctx) return false;
    int ret = avformat_write_header(record_fmt_ctx, nullptr);
    if (ret < 0) {
        return false;
    }
    return true;
}

bool Encoder::EncoderImpl::write_record_trailer() {
    if (!record_fmt_ctx) return false;
    int ret = av_write_trailer(record_fmt_ctx);
    return ret >= 0;
}

std::string Encoder::EncoderImpl::generate_record_path(int sequence_number) {
    std::string path = current_recording_params.destination;
    // Simple placeholder replacement for {seq}, {utc}, {local}
    // TODO: Implement proper string replacement
    if (path.find("{seq}") != std::string::npos && sequence_number >= 0) {
        path.replace(path.find("{seq}"), 5, std::to_string(sequence_number));
    }
    return path;
}

std::map<std::string, bool> Encoder::EncoderImpl::getAvailableEncoders() {
    std::map<std::string, bool> encoders;
    void* i = nullptr;
    const AVCodec* c;
    while ((c = av_codec_iterate(&i))) {
        if (av_codec_is_encoder(c)) {
            bool is_hw = c->capabilities & AV_CODEC_CAP_DR1; // Simple heuristic, might need a better check
            encoders[c->name] = is_hw;
        }
    }
    return encoders;
}

// -- Public Encoder Class Wrapper Implementation --------------------------------

Encoder::Encoder() : pimpl(new EncoderImpl()) {}

Encoder::~Encoder() = default;

bool Encoder::open(const EncoderParams& params) {
    return pimpl->open(params);
}

void Encoder::release() {
    pimpl->release();
}

bool Encoder::isOpened() const {
    return pimpl->isOpened();
}

bool Encoder::push(const cv::Mat& frame) {
    return pimpl->push(frame);
}

bool Encoder::pullAsRtp(std::vector<uint8_t>& packet) {
    return pimpl->pullAsRtp(packet);
}

bool Encoder::getFmp4InitializationSegment(std::vector<uint8_t>& initSegment) {
    return pimpl->getFmp4InitializationSegment(initSegment);
}

bool Encoder::pullAsFmp4(std::vector<uint8_t>& fragment) {
    return pimpl->pullAsFmp4(fragment);
}

bool Encoder::configureRecording(const RecordingParams& params) {
    return pimpl->configureRecording(params);
}

bool Encoder::startRecording() {
    return pimpl->startRecording();
}

void Encoder::stopRecording() {
    pimpl->stopRecording();
}

bool Encoder::isRecording() const {
    return pimpl->isRecording();
}

bool Encoder::splitSegment() {
    return pimpl->splitSegment();
}

std::map<std::string, bool> Encoder::getAvailableEncoders() {
    return EncoderImpl::getAvailableEncoders();
}

Encoder::Encoder(Encoder&&) noexcept = default;
Encoder& Encoder::operator=(Encoder&&) noexcept = default;

} // namespace stream
} // namespace cv
#endif