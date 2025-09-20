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

// Single-producer single-consumer ring buffer.
//Capacity must be power of two, because we use bitwise AND to calculate the next index.
template <typename T>
class SpscRing {
public:
    explicit SpscRing(size_t capacity_pow2)
        : capacity_mask_(capacity_pow2 - 1),
          buffer_(capacity_pow2),
          head_(0),
          tail_(0) {}

    bool try_enqueue(T&& item) {
        const size_t head = head_.load(std::memory_order_relaxed);
        const size_t next = (head + 1) & capacity_mask_;
        if (next == tail_.load(std::memory_order_acquire)) {
            return false; // full
        }
        buffer_[head] = std::move(item);
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool try_dequeue(T& out) {
        const size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false; // empty
        }
        out = std::move(buffer_[tail]);
        tail_.store((tail + 1) & capacity_mask_, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return tail_.load(std::memory_order_acquire) == head_.load(std::memory_order_acquire);
    }

private:
    size_t capacity_mask_;
    std::vector<T> buffer_;
    std::atomic<size_t> head_;
    std::atomic<size_t> tail_;
};

class FanInQueueReceiver {
public:
    FanInQueueReceiver(const std::string& ip, int tcp_port);
    ~FanInQueueReceiver();

    // Start listening and accepting remote endpoints
    void start();

    // Blocking dequeue; returns false on shutdown
    bool dequeue(Message& out);

    // Introspection
    size_t endpoint_count() const;

    // Shutdown and cleanup
    void stop();

private:
    void progressThread();
    void acceptThread();
    static void onRecvCb(void* request, ucs_status_t status, const ucp_tag_recv_info_t* info, void* user_data);

    // TCP OOB helpers
    int tcp_listen_fd_ = -1;
    int tcp_port_ = 0;
    std::string ip_;

    // UCX context/worker
    ucp_context_h context_ = nullptr;
    ucp_worker_h worker_ = nullptr;

    std::vector<ucp_ep_h> eps_;
    std::atomic<size_t> ep_count_{0};
    std::mutex eps_mu_;

    std::thread progress_thr_;
    std::thread accept_thr_;
    std::atomic<bool> running_{false};

    // Lock-free SPSC queue (producer: progress thread; consumer: user thread)
    SpscRing<Message> q_{1024};
};

class FanInQueueSender {
public:
    FanInQueueSender(const std::string& ip, int tcp_port);
    ~FanInQueueSender();

    // Connect a single endpoint to the receiver via TCP OOB
    void start();

    // Send message on this sender's endpoint
    void send(const void* buf, size_t len, uint64_t tag = 0xABCDEF);

    size_t endpoint_count() const;

    void stop();

private:
    // TCP OOB
    int tcp_port_ = 0;
    std::string ip_;

    // UCX
    ucp_context_h context_ = nullptr;
    ucp_worker_h worker_ = nullptr;
    ucp_ep_h ep_ = nullptr;
};

} // namespace ucxq


