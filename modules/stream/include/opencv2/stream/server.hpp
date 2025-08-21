#ifndef OPENCV_STREAM_SERVER_HPP
#define OPENCV_STREAM_SERVER_HPP

#include "opencv2/core/cvdef.h" // For CV_EXPORTS_W and CV_WRAP
#include <functional>            // For std::function
#include <memory>                // For std::unique_ptr
#include <string>                // For std::string
#include <vector>                // For std::vector
#include <utility>               // For std::pair

/**
  @defgroup stream Streaming Server Module
*/

namespace cv {
namespace stream {

//! @addtogroup stream
//! @{

// Forward declarations for the abstract request and response interfaces.
class Request;
class Response;

/** @brief Defines the generic signature for a function that handles an HTTP request.
 *
 * An object of this type is registered with the Server to handle incoming requests
 * for a specific URL endpoint. The function is provided with request details and
 * a response object to formulate and send the reply.
 *
 * @param request An object providing read-only access to the incoming HTTP request details.
 * @param response An object used to construct and send the HTTP response to the client.
 */
using RequestHandler = std::function<void(const Request& request, Response& response)>;

/** @brief An abstract interface representing an incoming HTTP request.
 *
 * This class provides a backend-agnostic way to inspect the properties of a
 * client's request, such as its method, URL path, and headers.
 * Concrete implementations are provided by the chosen server backend.
 */
class CV_EXPORTS_W Request
{
public:
    virtual ~Request() = default;

    /** @brief Returns the HTTP method of the request (e.g., "GET", "POST"). */
    virtual std::string getMethod() const = 0;

    /** @brief Returns the path component of the request URL. */
    virtual std::string getPath() const = 0;
};

/** @brief An abstract interface for constructing and sending an HTTP response.
 *
 * This class provides a backend-agnostic way to send data back to a client.
 * It is designed to support both single responses and continuous data streams,
 * making it suitable for a variety of HTTP-based protocols.
 */
class CV_EXPORTS_W Response
{
public:
    virtual ~Response() = default;

    /** @brief Sets the HTTP status code for the response.
     *
     * @param code The standard HTTP status code (e.g., 200 for OK, 404 for Not Found).
     */
    virtual void setStatusCode(int code) = 0;

    /** @brief Sets an HTTP header for the response.
     *
     * If the header already exists, its value is typically overwritten.
     *
     * @param key The name of the HTTP header (e.g., "Content-Type").
     * @param value The value of the header.
     */
    virtual void setHeader(const std::string& key, const std::string& value) = 0;

    /** @brief Writes a block of data to the response body.
     *
     * This method can be called multiple times to send data in chunks, which is
     * essential for streaming. The underlying implementation must handle the
     * connection state.
     *
     * @param data A pointer to the buffer containing the data to send.
     * @param size The number of bytes to send from the buffer.
     * @return true if the write was successful, false if the connection is closed or an error occurred.
     */
    virtual bool write(const char* data, size_t size) = 0;
};

/** @brief A generic, backend-agnostic web server.
 *
 * This class provides the core functionality for running an HTTP server. It is
 * responsible for managing network connections and routing incoming requests
 * to user-registered handlers. It has no knowledge of application-specific logic.
 */
class CV_EXPORTS_W Server
{
public:
    /** @brief Destructor. Ensures the server is stopped and resources are released. */
    ~Server();

    /** @brief Starts the server and begins listening for connections on the specified port.
     *
     * @param port The TCP port on which the server will listen.
     * @param num_threads The number of worker threads to handle client requests.
     * @return true if the server started successfully, false on failure (e.g., port in use).
     */
    CV_WRAP bool start(int port, int num_threads = 1);

    /** @brief Stops the server, closes all connections, and releases network resources. */
    CV_WRAP void stop();

    /** @brief Queries the current state of the server.
     *
     * @return true if the server is currently running and listening for connections.
     */
    CV_WRAP bool isRunning() const;

    /** @brief Registers a handler function for a specific URL path.
     *
     * When a request is received for the given path, the provided handler will be
     * invoked. If a handler is already registered for this path, it will be replaced.
     *
     * @param path The URL path to associate with the handler (e.g., "/status").
     * @param handler The function to execute for requests to this path.
     */
    void registerEndpoint(const std::string& path, const RequestHandler& handler);

    /** @brief Removes a handler for a specific URL path.
     *
     * @param path The URL path of the handler to remove.
     */
    void unregisterEndpoint(const std::string& path);

    // --- Lifecycle Management ---
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;
    Server(Server&&) noexcept;
    Server& operator=(Server&&) noexcept;

private:
    class ServerImpl;
    std::unique_ptr<ServerImpl> pimpl;

    // The constructor is private to enforce creation via the factory function.
    Server();

    friend std::unique_ptr<Server> createServer();
};

/** @brief Factory function to create a server instance.
 *
 * This function instantiates a Server with a backend implementation chosen at
 * compile time via CMake flags (e.g., HAVE_STREAM_BACKEND_ASIO).
 *
 * @return A unique pointer to a Server instance, or nullptr if no backend is available.
 */
CV_EXPORTS_W std::unique_ptr<Server> createServer();

//! @}
} // namespace stream
} // namespace cv

#endif // OPENCV_STREAM_SERVER_HPP