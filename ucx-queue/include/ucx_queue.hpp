#pragma once

#include <ucp/api/ucp.h>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <condition_variable>
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

    // Server-only: create N local endpoints to self (UCX self/shm transports).
    // Returns the base index in `eps_` where these endpoints were appended contiguously.
    size_t create_local_endpoints(size_t count);

    // Introspection
    size_t endpoint_count() const;

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
    std::atomic<size_t> ep_count_{0};
    std::mutex eps_mu_;

    std::thread progress_thr_;
    std::thread accept_thr_;
    std::atomic<bool> running_{false};

    std::mutex q_mu_;
    std::queue<Message> q_;
    std::condition_variable q_cv_;
};

} // namespace ucxq


