#include "ucx_queue.hpp"

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

namespace ucxq {

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

FanInQueue::FanInQueue(const std::string& role, const std::string& ip, int tcp_port)
    : tcp_port_(tcp_port), ip_(ip), role_(role) {
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

FanInQueue::~FanInQueue() {
    stop();
}

void FanInQueue::start(size_t num_endpoints) {
    running_.store(true);

    // Start progress thread
    progress_thr_ = std::thread(&FanInQueue::progressThread, this);

    // Setup TCP listener or connect for address exchange
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket failed");
    if (role_ == "server") {
        int yes = 1; setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(tcp_port_);
        inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr);
        if (bind(fd, (sockaddr*)&addr, sizeof(addr)) != 0) throw std::runtime_error("bind failed");
        if (listen(fd, 128) != 0) throw std::runtime_error("listen failed");
        tcp_listen_fd_ = fd;
        // accept endpoints in a separate thread
        accept_thr_ = std::thread(&FanInQueue::acceptThread, this);
    } else {
        // client: create N endpoints by connecting repeatedly
        for (size_t i = 0; i < num_endpoints; ++i) {
            int cfd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (cfd < 0) throw std::runtime_error("socket failed");
            sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(tcp_port_);
            inet_pton(AF_INET, ip_.c_str(), &addr.sin_addr);
            if (connect(cfd, (sockaddr*)&addr, sizeof(addr)) != 0) throw std::runtime_error("connect failed");

            // exchange worker addresses
            ucp_address_t* my_addr{}; size_t my_len{};
            if (ucp_worker_get_address(worker_, &my_addr, &my_len) != UCS_OK) throw std::runtime_error("get_address failed");
            uint32_t l = htonl((uint32_t)my_len);
            if (send(cfd, &l, sizeof(l), 0) != (ssize_t)sizeof(l)) throw std::runtime_error("send len failed");
            if (send(cfd, my_addr, my_len, 0) != (ssize_t)my_len) throw std::runtime_error("send addr failed");
            ucp_worker_release_address(worker_, my_addr);

            uint32_t peer_l{}; if (recv(cfd, &peer_l, sizeof(peer_l), MSG_WAITALL) != (ssize_t)sizeof(peer_l)) throw std::runtime_error("recv len failed");
            peer_l = ntohl(peer_l); std::vector<uint8_t> peer(peer_l);
            if (recv(cfd, peer.data(), peer.size(), MSG_WAITALL) != (ssize_t)peer.size()) throw std::runtime_error("recv addr failed");

            // create endpoint
            ucp_ep_params_t ep{}; ep.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; ep.address = (ucp_address_t*)peer.data();
            ucp_ep_h eph{}; if (ucp_ep_create(worker_, &ep, &eph) != UCS_OK) throw std::runtime_error("ep create failed");
            eps_.push_back(eph);
            close(cfd);
        }
        close(fd);
    }
}

void FanInQueue::acceptThread() {
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
        send(cfd, &l, sizeof(l), 0);
        send(cfd, my_addr, my_len, 0);
        ucp_worker_release_address(worker_, my_addr);
        // create endpoint
        ucp_ep_params_t ep{}; ep.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; ep.address = (ucp_address_t*)peer.data();
        ucp_ep_h eph{}; if (ucp_ep_create(worker_, &ep, &eph) != UCS_OK) { close(cfd); continue; }
        eps_.push_back(eph);
        close(cfd);
    }
}

void FanInQueue::progressThread() {
    const uint64_t TAG = 0xABCDEF;
    while (running_.load()) {
        // poll for any message using tag API with any source
        const size_t MAX = 2 * 1024 * 1024; // up to 2MB
        std::vector<uint8_t> buf(MAX);
        ucp_request_param_t prm{}; prm.op_attr_mask = 0;
        void* req = ucp_tag_recv_nbx(worker_, buf.data(), buf.size(), TAG, (uint64_t)-1, &prm);
        if (UCS_PTR_IS_ERR(req)) {
            // progress and retry
            ucp_worker_progress(worker_);
            continue;
        }
        wait_req(worker_, req);
        // Note: recv_nbx fills the buffer fully; higher-level protocols should prefix size.
        // For simplicity assume fixed-size messages are used.
        Message m; m.data = std::move(buf);
        {
            std::lock_guard<std::mutex> lg(q_mu_);
            q_.push(std::move(m));
        }
    }
}

void FanInQueue::send(size_t ep_index, const void* buf, size_t len, uint64_t tag) {
    if (ep_index >= eps_.size()) return;
    ucp_request_param_t prm{}; prm.op_attr_mask = 0;
    void* req = ucp_tag_send_nbx(eps_[ep_index], buf, len, tag, &prm);
    wait_req(worker_, req);
}

void FanInQueue::create_local_endpoints(size_t count) {
    // Create UCX endpoints to self using the worker address; UCX will use self/shm transports
    ucp_address_t* my_addr{}; size_t my_len{};
    if (ucp_worker_get_address(worker_, &my_addr, &my_len) != UCS_OK) throw std::runtime_error("get_address failed");
    for (size_t i = 0; i < count; ++i) {
        ucp_ep_params_t ep{}; ep.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; ep.address = my_addr;
        ucp_ep_h eph{}; if (ucp_ep_create(worker_, &ep, &eph) != UCS_OK) { ucp_worker_release_address(worker_, my_addr); throw std::runtime_error("ep create failed"); }
        eps_.push_back(eph);
    }
    ucp_worker_release_address(worker_, my_addr);
}

bool FanInQueue::dequeue(Message& out) {
    for (;;) {
        {
            std::lock_guard<std::mutex> lg(q_mu_);
            if (!q_.empty()) { out = std::move(q_.front()); q_.pop(); return true; }
        }
        if (!running_.load()) return false;
        // progress while waiting
        ucp_worker_progress(worker_);
        std::this_thread::yield();
    }
}

void FanInQueue::stop() {
    if (!running_.exchange(false)) return;
    if (accept_thr_.joinable()) accept_thr_.join();
    if (progress_thr_.joinable()) progress_thr_.join();
    for (auto ep : eps_) ucp_ep_destroy(ep);
    eps_.clear();
    if (worker_) { ucp_worker_destroy(worker_); worker_ = nullptr; }
    if (context_) { ucp_cleanup(context_); context_ = nullptr; }
    if (tcp_listen_fd_ >= 0) { close(tcp_listen_fd_); tcp_listen_fd_ = -1; }
}

} // namespace ucxq


