#pragma once

#include <ucp/api/ucp.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

namespace ucxq {

struct Message {
    std::vector<uint8_t> data;
};

class FanInQueue {
public:
    // role: "server" or "client"; server binds and listens; client connects
    FanInQueue(const std::string& role, const std::string& ip, int tcp_port);
    ~FanInQueue();

    // Server: start listening; Client: connect N endpoints (one per sender thread)
    void start(size_t num_endpoints);

    // Client: send message on a specific endpoint index (0..num_endpoints-1)
    void send(size_t ep_index, const void* buf, size_t len, uint64_t tag = 0xABCDEF);

    // Server-only: create N local endpoints to self (UCX self/shm transports)
    void create_local_endpoints(size_t count);

    // Server: blocking dequeue; returns false on shutdown
    bool dequeue(Message& out);

    // Shutdown and cleanup
    void stop();

private:
    void progressThread();
    void acceptThread();

    // TCP OOB helpers
    int tcp_listen_fd_ = -1;
    int tcp_port_ = 0;
    std::string ip_;
    std::string role_;

    // UCX context/worker
    ucp_context_h context_ = nullptr;
    ucp_worker_h worker_ = nullptr;

    std::vector<ucp_ep_h> eps_;

    std::thread progress_thr_;
    std::thread accept_thr_;
    std::atomic<bool> running_{false};

    std::mutex q_mu_;
    std::queue<Message> q_;
};

} // namespace ucxq


