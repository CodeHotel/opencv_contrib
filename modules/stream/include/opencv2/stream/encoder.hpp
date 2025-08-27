#ifndef OPENCV_ENCODER_HPP
#define OPENCV_ENCODER_HPP

#include <opencv2/core.hpp>
#include <string>
#include <vector>
#include <map>

// FFmpeg headers are C-style, so they need to be wrapped in extern "C"
// when included in a C++ source file.
extern "C" {
#include <libavcodec/avcodec.h>
#include <libswscale/swscale.h>
#include <libavutil/frame.h>
#include <libavutil/imgutils.h>
}

namespace cv {
namespace stream {

/** @brief Parameters for configuring a video encoder. */
struct CV_EXPORTS_W EncoderParams
{
    CV_WRAP EncoderParams();

    CV_PROP_RW int width;
    CV_PROP_RW int height;
    CV_PROP_RW int bitrate; // in bits per second
    CV_PROP_RW int framerate;
    CV_PROP_RW int gopSize; // Group of Pictures (keyframe interval)

    /** @brief Name of the encoder to use (e.g., "libx264", "h264_nvenc").
     *
     * Set to "auto" to allow the system to select the best available hardware
     * encoder, falling back to software if none are found.
     */
    CV_PROP_RW std::string codecName;
};

/**
 * @brief A generic video encoder class.
 *
 * This class provides an interface for encoding cv::Mat frames into a
 * compressed video stream. It is designed to be independent of the transport
 * mechanism (e.g., WebRTC, WebSockets). The underlying implementation uses FFmpeg.
 */
class CV_EXPORTS_W Encoder
{
public:
    CV_WRAP Encoder();
    ~Encoder();

    /** @brief Initializes the encoder with the specified parameters.
     *
     * @param params An EncoderParams struct containing the configuration.
     * @return true if initialization is successful, false otherwise.
     */
    CV_WRAP bool open(const EncoderParams& params);

    /** @brief Closes the encoder and releases all associated resources. */
    CV_WRAP void release();

    /** @brief Checks if the encoder has been successfully initialized.
     *
     * @return true if the encoder is open and ready to receive frames.
     */
    CV_WRAP bool isOpened() const;

    /** @brief Submits a single frame to the encoder.
     *
     * This is a non-blocking call. The frame is added to an internal queue
     * for encoding on a separate thread.
     *
     * @param frame The cv::Mat frame to be encoded. The frame must have the
     * same dimensions as specified in the EncoderParams.
     * @return true if the frame was successfully submitted, false if the
     * encoder's input queue is full.
     */
    CV_WRAP bool push(const cv::Mat& frame);

    /** @brief Retrieves one encoded video packet suitable for RTP/WebRTC.
     *
     * This non-blocking function pulls a raw, compressed packet (e.g., an H.264
     * NAL unit) from the encoder's output queue.
     *
     * @param packet A vector that will be filled with the encoded data.
     * @return true if a packet was retrieved, false if the output queue is empty.
     */
    bool pullAsRtp(std::vector<uint8_t>& packet);

    /** @brief Retrieves the fMP4 initialization segment.
     *
     * This segment (the 'moov' atom) is required by the browser's Media Source
     * Extensions (MSE) to set up the video player. It should be sent to the
     * client once at the beginning of a stream.
     *
     * @param initSegment A vector that will be filled with the initialization data.
     * @return true if the segment was successfully generated and retrieved.
     */
    bool getFmp4InitializationSegment(std::vector<uint8_t>& initSegment);

    /** @brief Retrieves one encoded fMP4 media fragment.
     *
     * This non-blocking function pulls a complete fMP4 fragment (a 'moof' and
     * 'mdat' atom pair) from the output queue, suitable for sending over
     * WebSockets for use with MSE.
     *
     * @param fragment A vector that will be filled with the fragment data.
     * @return true if a fragment was retrieved, false if the output queue is empty.
     */
    bool pullAsFmp4(std::vector<uint8_t>& fragment);

    /** @brief Probes the system for available encoders.
     *
     * @return A map where the key is the encoder name (e.g., "h264_nvenc")
     * and the value is true if it is a hardware-accelerated encoder.
     */
    CV_WRAP static std::map<std::string, bool> getAvailableEncoders();

    // The class is non-copyable but movable for proper resource management.
    Encoder(const Encoder&) = delete;
    Encoder& operator=(const Encoder&) = delete;
    Encoder(Encoder&&) noexcept;
    Encoder& operator=(Encoder&&) noexcept;

private:
    class EncoderImpl;
    std::unique_ptr<EncoderImpl> pimpl;
};

} // namespace stream
} // namespace cv

#endif //OPENCV_ENCODER_HPP
