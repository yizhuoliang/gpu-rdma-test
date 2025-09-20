#include "ucx_queue.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <chrono>

namespace ucxq {

struct RecvCtx {
    FanInQueueReceiver* self;
    std::vector<uint8_t>* buf;
};

static void wait_req(ucp_worker_h w, void* req) {
    if (req == nullptr) return;
    if (!UCS_PTR_IS_PTR(req)) {
        ucs_status_t st = (ucs_status_t)UCS_PTR_STATUS(req);
        if (st != UCS_OK) {
            std::cerr << "UCX err: " << ucs_status_string(st) << std::endl;
            std::abort();
        }
        return;
    }
    while (ucp_request_check_status(req) == UCS_INPROGRESS) {
        ucp_worker_progress(w);
    }
    ucp_request_free(req);
}

FanInQueueReceiver::FanInQueueReceiver(const std::string& ip, int tcp_port)
    : tcp_port_(tcp_port), ip_(ip) {
    ucp_config_t* cfg{};
    if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) {
        throw std::runtime_error("ucp_config_read failed");
    }
    ucp_params_t p{}; p.field_mask = UCP_PARAM_FIELD_FEATURES; p.features = UCP_FEATURE_TAG;
    if (ucp_init(&p, cfg, &context_) != UCS_OK) {
        ucp_config_release(cfg);
        throw std::runtime_error("ucp_init failed");
    }
    ucp_config_release(cfg);

    ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_MULTI;
    if (ucp_worker_create(context_, &wp, &worker_) != UCS_OK) {
        throw std::runtime_error("ucp_worker_create failed");
    }
}

FanInQueueReceiver::~FanInQueueReceiver() {
    stop();
}

void FanInQueueReceiver::start() {
    running_.store(true);
    // Start progress thread
    progress_thr_ = std::thread(&FanInQueueReceiver::progressThread, this);

    // Setup TCP listener for address exchange
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    int yes = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(tcp_port_);
    inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr);
    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) != 0) throw std::runtime_error("bind failed");
    if (listen(fd, 128) != 0) throw std::runtime_error("listen failed");
    tcp_listen_fd_ = fd;
    // accept endpoints in a separate thread
    accept_thr_ = std::thread(&FanInQueueReceiver::acceptThread, this);
}

void FanInQueueReceiver::acceptThread() {
    while (running_.load()) {
        int cfd = accept(tcp_listen_fd_, nullptr, nullptr);
        if (cfd < 0) break;
        // receive peer address
        uint32_t peer_l{}; if (recv(cfd, &peer_l, sizeof(peer_l), MSG_WAITALL) != (ssize_t)sizeof(peer_l)) { close(cfd); continue; }
        peer_l = ntohl(peer_l); std::vector<uint8_t> peer(peer_l);
        if (recv(cfd, peer.data(), peer.size(), MSG_WAITALL) != (ssize_t)peer.size()) { close(cfd); continue; }
        // send our address
        ucp_address_t* my_addr{}; size_t my_len{}; if (ucp_worker_get_address(worker_, &my_addr, &my_len) != UCS_OK) { close(cfd); continue; }
        uint32_t l = htonl((uint32_t)my_len);
        ::send(cfd, &l, sizeof(l), 0);
        ::send(cfd, my_addr, my_len, 0);
        ucp_worker_release_address(worker_, my_addr);
        // create endpoint
        ucp_ep_params_t ep{}; ep.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; ep.address = (ucp_address_t*)peer.data();
        ucp_ep_h eph{}; if (ucp_ep_create(worker_, &ep, &eph) != UCS_OK) { close(cfd); continue; }
        {
            std::lock_guard<std::mutex> lg(eps_mu_);
            eps_.push_back(eph);
        }
        ep_count_.fetch_add(1, std::memory_order_relaxed);
        close(cfd);
    }
}

void FanInQueueReceiver::progressThread() {
    const uint64_t TAG = 0xABCDEF;
    while (running_.load()) {
        // Post receives for any available messages (bounded per iteration)
        int posted = 0;
        for (; posted < 64 && running_.load(); ++posted) {
            ucp_tag_recv_info_t info{};
            ucp_tag_message_h msg = ucp_tag_probe_nb(worker_, TAG, (uint64_t)-1, 1, &info);
            if (msg == nullptr) break;

            // Allocate buffer and context for completion callback
            auto* buf = new std::vector<uint8_t>(info.length);
            auto* ctx = new RecvCtx{this, buf};

            ucp_request_param_t prm{};
            prm.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA;
            prm.user_data = ctx;
            prm.cb.recv = &FanInQueueReceiver::onRecvCb;

            void* req = ucp_tag_msg_recv_nbx(worker_, buf->data(), buf->size(), msg, &prm);
            if (UCS_PTR_IS_ERR(req)) {
                delete buf;
                delete ctx;
                continue;
            }
            if (!UCS_PTR_IS_PTR(req)) {
                // Completed immediately; enqueue here (callback will not be called)
                ucs_status_t st = (ucs_status_t)UCS_PTR_STATUS(req);
                if (st == UCS_OK) {
                    Message m{.data = std::move(*buf)};
                    // Busy retry a few times if full to avoid dropping
                    for (int i = 0; i < 64; ++i) {
                        if (q_.try_enqueue(std::move(m))) { break; }
                        std::this_thread::yield();
                    }
                }
                delete buf;
                delete ctx;
            }
        }

        // Drive completions
        if (ucp_worker_progress(worker_) == 0) {
            std::this_thread::yield();
        }
    }
}

void FanInQueueReceiver::onRecvCb(void* request, ucs_status_t status, const ucp_tag_recv_info_t* info, void* user_data) {
    auto* ctx = static_cast<RecvCtx*>(user_data);
    FanInQueueReceiver* self = ctx->self;
    std::vector<uint8_t>* buf = ctx->buf;

    if (status == UCS_OK) {
        Message m{.data = std::move(*buf)};
        // Enqueue with minimal contention; spin briefly if full
        for (int i = 0; i < 64; ++i) {
            if (self->q_.try_enqueue(std::move(m))) { break; }
            std::this_thread::yield();
        }
    }

    delete buf;
    delete ctx;
    if (UCS_PTR_IS_PTR(request)) {
        ucp_request_free(request);
    }
}

// Sender implementation
FanInQueueSender::FanInQueueSender(const std::string& ip, int tcp_port)
    : tcp_port_(tcp_port), ip_(ip) {
    ucp_config_t* cfg{};
    if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) {
        throw std::runtime_error("ucp_config_read failed");
    }
    ucp_params_t p{}; p.field_mask = UCP_PARAM_FIELD_FEATURES; p.features = UCP_FEATURE_TAG;
    if (ucp_init(&p, cfg, &context_) != UCS_OK) {
        ucp_config_release(cfg);
        throw std::runtime_error("ucp_init failed");
    }
    ucp_config_release(cfg);

    ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_MULTI;
    if (ucp_worker_create(context_, &wp, &worker_) != UCS_OK) {
        throw std::runtime_error("ucp_worker_create failed");
    }
}

FanInQueueSender::~FanInQueueSender() { stop(); }

void FanInQueueSender::start() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(tcp_port_);
    inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr);
    int attempts = 0; const int kMaxAttempts = 200;
    while (connect(fd, (sockaddr*)&addr, sizeof(addr)) != 0) {
        if (++attempts >= kMaxAttempts) { close(fd); throw std::runtime_error("connect failed"); }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ucp_address_t* my_addr{}; size_t my_len{};
    if (ucp_worker_get_address(worker_, &my_addr, &my_len) != UCS_OK) throw std::runtime_error("get_address failed");
    uint32_t l = htonl((uint32_t)my_len);
    if (::send(fd, &l, sizeof(l), 0) != (ssize_t)sizeof(l)) throw std::runtime_error("send len failed");
    if (::send(fd, my_addr, my_len, 0) != (ssize_t)my_len) throw std::runtime_error("send addr failed");
    ucp_worker_release_address(worker_, my_addr);

    uint32_t peer_l{}; if (recv(fd, &peer_l, sizeof(peer_l), MSG_WAITALL) != (ssize_t)sizeof(peer_l)) throw std::runtime_error("recv len failed");
    peer_l = ntohl(peer_l); std::vector<uint8_t> peer(peer_l);
    if (recv(fd, peer.data(), peer.size(), MSG_WAITALL) != (ssize_t)peer.size()) throw std::runtime_error("recv addr failed");

    ucp_ep_params_t ep{}; ep.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; ep.address = (ucp_address_t*)peer.data();
    if (ucp_ep_create(worker_, &ep, &ep_) != UCS_OK) throw std::runtime_error("ep create failed");
    close(fd);
}

void FanInQueueSender::send(const void* buf, size_t len, uint64_t tag) {
    if (ep_ == nullptr) return;
    ucp_request_param_t prm{}; prm.op_attr_mask = 0;
    void* req = ucp_tag_send_nbx(ep_, buf, len, tag, &prm);
    wait_req(worker_, req);
}

bool FanInQueueReceiver::dequeue(Message& out) {
    // Single consumer: spin/yield until an item arrives or shutdown
    while (running_.load()) {
        if (q_.try_dequeue(out)) return true;
        std::this_thread::yield();
    }
    // After shutdown, drain any remaining
    return q_.try_dequeue(out);
}

void FanInQueueReceiver::stop() {
    if (!running_.exchange(false)) return;
    // Close listener first to unblock accept()
    if (tcp_listen_fd_ >= 0) { close(tcp_listen_fd_); tcp_listen_fd_ = -1; }
    if (accept_thr_.joinable()) accept_thr_.join();
    if (progress_thr_.joinable()) progress_thr_.join();
    // Close UCX endpoints gracefully/forcibly without hanging
    std::vector<void*> close_reqs;
    {
        std::lock_guard<std::mutex> lg(eps_mu_);
        close_reqs.reserve(eps_.size());
        for (auto ep : eps_) {
            ucp_request_param_t prm{};
            prm.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
            prm.flags = UCP_EP_CLOSE_FLAG_FORCE; // avoid handshake stall if peer not progressing
            void* req = ucp_ep_close_nbx(ep, &prm);
            if (UCS_PTR_IS_PTR(req)) {
                close_reqs.push_back(req);
            }
        }
        eps_.clear();
    }
    // Progress endpoint close requests
    for (void* req : close_reqs) {
        while (ucp_request_check_status(req) == UCS_INPROGRESS) {
            ucp_worker_progress(worker_);
        }
        ucp_request_free(req);
    }
    if (worker_) { ucp_worker_destroy(worker_); worker_ = nullptr; }
    if (context_) { ucp_cleanup(context_); context_ = nullptr; }
    // listener already closed above
}

size_t FanInQueueReceiver::endpoint_count() const { return ep_count_.load(std::memory_order_relaxed); }

void FanInQueueSender::stop() {
    if (ep_ != nullptr) {
        ucp_request_param_t prm{};
        prm.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS;
        prm.flags = UCP_EP_CLOSE_FLAG_FORCE;
        void* req = ucp_ep_close_nbx(ep_, &prm);
        if (UCS_PTR_IS_PTR(req)) {
            while (ucp_request_check_status(req) == UCS_INPROGRESS) {
                ucp_worker_progress(worker_);
            }
            ucp_request_free(req);
        }
        ep_ = nullptr;
    }
    if (worker_) { ucp_worker_destroy(worker_); worker_ = nullptr; }
    if (context_) { ucp_cleanup(context_); context_ = nullptr; }
}

size_t FanInQueueSender::endpoint_count() const { return ep_ != nullptr ? 1 : 0; }

} // namespace ucxq


