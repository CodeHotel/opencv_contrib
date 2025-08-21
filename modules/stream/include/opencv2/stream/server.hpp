#ifndef OPENCV_STREAM_SERVER_HPP
#define OPENCV_STREAM_SERVER_HPP

#include "opencv2/core/cvdef.h"
#include <functional>
#include <memory>
#include <string>
#include <vector>
#include <utility>

namespace cv {
    namespace stream {

        class Request;
        class Response;
        class HttpSession;

        using RequestHandler = std::function<void(const Request& request, Response& response)>;

        class CV_EXPORTS_W Request
        {
        public:
            virtual ~Request() = default;
            virtual std::string getMethod() const = 0;
            virtual std::string getPath() const = 0;
        };

        class CV_EXPORTS_W Response
        {
        public:
            virtual ~Response() = default;
            virtual void setStatusCode(int code) = 0;
            virtual void setHeader(const std::string& key, const std::string& value) = 0;
            virtual bool write(const char* data, size_t size) = 0;
        };

        class CV_EXPORTS_W Server
        {
        public:
            ~Server();
            bool start(int port, int num_threads = 1);
            void stop();
            bool isRunning() const;
            void registerEndpoint(const std::string& path, const RequestHandler& handler);
            void unregisterEndpoint(const std::string& path);

            Server(const Server&) = delete;
            Server& operator=(const Server&) = delete;
            Server(Server&&) noexcept;
            Server& operator=(Server&&) noexcept;

        private:
            class ServerImpl;
            std::unique_ptr<ServerImpl> pimpl;

            Server();

            friend std::unique_ptr<Server> createServer();
            friend class HttpSession; // <-- FIX: Grant HttpSession access to private members
        };

        CV_EXPORTS_W std::unique_ptr<Server> createServer();

    } // namespace stream
} // namespace cv

#endif // OPENCV_STREAM_SERVER_HPP