#include "opencv2/stream/server.hpp"
#include <iostream>
#include <thread>
#include <chrono>

int main() {
    // 1. Create a server instance using the factory function.
    auto server = cv::stream::createServer();
    if (!server) {
        std::cerr << "Failed to create server. No backend available?" << std::endl;
        return -1;
    }

    // 2. Define the handler for incoming requests.
    // This lambda function will be executed for every client that connects to the endpoint.
    cv::stream::RequestHandler handler = [](const cv::stream::Request&, cv::stream::Response& response) {
        // The HTML content to be sent.
        const std::string html_content = "<h1>Hello World!</h1>";

        // Set the HTTP status code to 200 (OK).
        response.setStatusCode(200);

        // Set the appropriate header for HTML content.
        response.setHeader("Content-Type", "text/html");

        // Write the HTML string to the response body.
        response.write(html_content.c_str(), html_content.length());
    };

    // 3. Register the handler for the root path "/".
    server->registerEndpoint("/", handler);

    // 4. Start the server on port 8080 with 2 worker threads.
    if (server->start(8080, 2)) {
        std::cout << "Server started successfully!" << std::endl;
        std::cout << "Open your web browser and navigate to http://localhost:8080" << std::endl;

        // 5. Keep the main thread alive indefinitely.
        // The server runs in background threads.
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }

    } else {
        std::cerr << "Failed to start server. Is port 8080 already in use?" << std::endl;
        return -1;
    }

    // The server will be stopped automatically when the 'server' unique_ptr goes out of scope.
    return 0;
}