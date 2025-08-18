#include <iostream>

// libmicrohttpd
#include <microhttpd.h>

// FFmpeg
extern "C" {
#include <libavcodec/avcodec.h>
#include <libavformat/avformat.h>
}
static enum MHD_Result answer_to_connection(void *cls,
                                struct MHD_Connection *connection,
                                const char *url,
                                const char *method,
                                const char *version,
                                const char *upload_data,
                                size_t *upload_data_size,
                                void **con_cls) {
    const char *page = "<html><body><h1>Hello from libmicrohttpd!</h1></body></html>";
    struct MHD_Response *response;
    enum MHD_Result ret;

    response = MHD_create_response_from_buffer(strlen(page),
                                               (void*)page,
                                               MHD_RESPMEM_PERSISTENT);
    ret = MHD_queue_response(connection, MHD_HTTP_OK, response);
    MHD_destroy_response(response);

    return ret;
}

int main() {
    struct MHD_Daemon *daemon;
    daemon = MHD_start_daemon(MHD_USE_INTERNAL_POLLING_THREAD,
                              8888,                // 포트
                              NULL, NULL,          // 접속 허용 콜백
                              &answer_to_connection, NULL, // 요청 처리 콜백
                              MHD_OPTION_END);
    if (nullptr == daemon) {
        std::cerr << "Failed to start HTTP server\n";
        return 1;
    }
    std::cout << "HTTP server running on http://localhost:8888\n";

    unsigned version = avcodec_version();
    std::cout << "FFmpeg avcodec version: "
              << AV_VERSION_MAJOR(version) << "."
              << AV_VERSION_MINOR(version) << "."
              << AV_VERSION_MICRO(version) << std::endl;

    std::cout << "Press Enter to stop the server..." << std::endl;
    getchar();

    MHD_stop_daemon(daemon);
    return 0;
}