#include "../include/ucx_queue.hpp"
#include <zmq.hpp>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <cstring>
#include <atomic>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

using clock_type = std::chrono::high_resolution_clock;

static void run_zmq_fanin(bool isServer, const char* ip, int port, size_t num_local_senders, size_t num_remote_senders, size_t msg_bytes, int rounds) {
    zmq::context_t ctx(1);
    std::string endpoint = std::string("tcp://") + ip + ":" + std::to_string(port);
    if (isServer) {
        zmq::socket_t pull(ctx, ZMQ_PULL);
        pull.bind(endpoint);
        // local senders
        std::vector<std::thread> local;
        for (size_t i = 0; i < num_local_senders; ++i) {
            local.emplace_back([&, i]{
                zmq::socket_t push(ctx, ZMQ_PUSH);
                push.connect(endpoint);
                std::vector<uint8_t> payload(msg_bytes, 0x5A);
                for (int r = 0; r < rounds; ++r) {
                    push.send(zmq::buffer(payload), zmq::send_flags::none);
                }
            });
        }
        // receive
        size_t expected = (num_local_senders + num_remote_senders) * (size_t)rounds;
        auto t0 = clock_type::now();
        size_t got = 0; zmq::message_t msg;
        while (got < expected) { pull.recv(msg, zmq::recv_flags::none); ++got; }
        auto t1 = clock_type::now();
        std::cout << "ZMQ fan-in msg_bytes=" << msg_bytes << " total_msgs=" << expected << " total_usec=" << std::chrono::duration<double, std::micro>(t1 - t0).count() << std::endl;
        for (auto& th : local) th.join();
    } else {
        // client: remote senders
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i]{
                zmq::socket_t push(ctx, ZMQ_PUSH);
                push.connect(endpoint);
                std::vector<uint8_t> payload(msg_bytes, 0x5A);
                for (int r = 0; r < rounds; ++r) {
                    push.send(zmq::buffer(payload), zmq::send_flags::none);
                }
            });
        }
        for (auto& th : remote) th.join();
    }
}

static void run_zmq_fanin_all(bool isServer, const char* ip, int port, size_t num_local_senders, size_t num_remote_senders, const std::vector<size_t>& sizes, int rounds) {
    zmq::context_t ctx(1);
    std::string endpoint = std::string("tcp://") + ip + ":" + std::to_string(port);
    // control channel (port+1) to synchronize rounds across server/client
    int ctrl_fd = -1;
    int ctrl_port = port + 1;
    if (isServer) {
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        int yes = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        bind(lfd, (sockaddr*)&addr, sizeof(addr)); listen(lfd, 1); ctrl_fd = accept(lfd, nullptr, nullptr); close(lfd);
    } else {
        ctrl_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        while (connect(ctrl_fd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }
    }
    if (isServer) {
        zmq::socket_t pull(ctx, ZMQ_PULL);
        pull.bind(endpoint);
        // persistent local senders
        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::atomic<uint64_t> round_id{0};
        std::vector<std::thread> local;
        for (size_t i = 0; i < num_local_senders; ++i) {
            local.emplace_back([&, i]{
                zmq::socket_t push(ctx, ZMQ_PUSH);
                push.connect(endpoint);
                uint64_t seen = round_id.load();
                while (!done.load()) {
                    while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                    seen = round_id.load();
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x5A);
                    for (int r = 0; r < rounds; ++r) {
                        push.send(zmq::buffer(payload), zmq::send_flags::none);
                    }
                }
            });
        }
        for (size_t si = 0; si < sizes.size(); ++si) {
            size_index.store(si);
            uint32_t token = (uint32_t)si; ::send(ctrl_fd, &token, sizeof(token), 0);
            round_id.fetch_add(1, std::memory_order_acq_rel);
            size_t expected = (num_local_senders + num_remote_senders) * (size_t)rounds;
            auto t0 = clock_type::now();
            size_t got = 0; zmq::message_t msg;
            while (got < expected) { pull.recv(msg, zmq::recv_flags::none); ++got; }
            auto t1 = clock_type::now();
            std::cout << "ZMQ fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << std::chrono::duration<double, std::micro>(t1 - t0).count() << std::endl;
        }
        done.store(true);
        for (auto& th : local) th.join();
        if (ctrl_fd >= 0) close(ctrl_fd);
    } else {
        // persistent remote senders
        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::atomic<uint64_t> round_id{0};
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i]{
                zmq::socket_t push(ctx, ZMQ_PUSH);
                push.connect(endpoint);
                uint64_t seen = round_id.load();
                while (!done.load()) {
                    while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                    seen = round_id.load();
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x5A);
                    for (int r = 0; r < rounds; ++r) {
                        push.send(zmq::buffer(payload), zmq::send_flags::none);
                    }
                }
            });
        }
        for (size_t si = 0; si < sizes.size(); ++si) {
            uint32_t token{}; if (recv(ctrl_fd, &token, sizeof(token), MSG_WAITALL) != (ssize_t)sizeof(token)) break;
            size_index.store(token);
            round_id.fetch_add(1, std::memory_order_acq_rel);
        }
        done.store(true);
        for (auto& th : remote) th.join();
        if (ctrl_fd >= 0) close(ctrl_fd);
    }
}

static void run_ucx_fanin_all(bool isServer, const char* ip, int port, size_t num_local_senders, size_t num_remote_senders, const std::vector<size_t>& sizes, int rounds) {
    using namespace ucxq;
    if (isServer) {
        FanInQueueReceiver q(ip, port);
        q.start();

        // ========== Local fan-in measurement (only local threads) ==========
        {
            std::atomic<size_t> size_index{0};
            std::atomic<bool> done{false};
            std::atomic<uint64_t> round_id{0};
            std::vector<std::thread> local;
            std::atomic<bool> start_local{false};
            std::atomic<size_t> started_local{0};
            for (size_t i = 0; i < num_local_senders; ++i) {
                local.emplace_back([&, i]{
                    FanInQueueSender sender(ip, port);
                    sender.start();
                    while (!start_local.load()) { std::this_thread::yield(); }
                    uint64_t seen = round_id.load();
                    started_local.fetch_add(1, std::memory_order_acq_rel);
                    while (!done.load()) {
                        while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                        seen = round_id.load();
                        size_t idx = size_index.load();
                        if (idx >= sizes.size()) break;
                        size_t msg_bytes = sizes[idx];
                        std::vector<uint8_t> payload(msg_bytes, 0x6B);
                        for (int r = 0; r < rounds; ++r) {
                            sender.send(payload.data(), payload.size());
                        }
                    }
                    sender.stop();
                });
            }
            // Start local run
            start_local.store(true, std::memory_order_release);
            while (started_local.load(std::memory_order_acquire) < num_local_senders) { std::this_thread::yield(); }
            for (size_t si = 0; si < sizes.size(); ++si) {
                size_index.store(si);
                size_t expected = (num_local_senders) * (size_t)rounds;
                auto t0 = clock_type::now();
                round_id.fetch_add(1, std::memory_order_acq_rel);
                size_t got = 0; Message m;
                while (got < expected) { if (q.dequeue(m)) ++got; }
                auto t1 = clock_type::now();
                std::cout << "UCX local fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << std::chrono::duration<double, std::micro>(t1 - t0).count() << std::endl;
            }
            done.store(true);
            for (auto& th : local) th.join();
        }

        // ========== Remote fan-in measurement (only remote threads) ==========
        {
            // TCP control channel for UCX control address exchange
            int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
            int yes = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
            int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
            bind(lfd, (sockaddr*)&addr, sizeof(addr)); listen(lfd, 1);
            int ctrl_fd = accept(lfd, nullptr, nullptr); close(lfd);

            // UCX control setup
            auto ctrl_wait = [](ucp_worker_h w, void* req) {
                if (req == nullptr) return;
                if (!UCS_PTR_IS_PTR(req)) {
                    ucs_status_t st = (ucs_status_t)UCS_PTR_STATUS(req);
                    if (st != UCS_OK) std::abort();
                    return;
                }
                while (ucp_request_check_status(req) == UCS_INPROGRESS) { ucp_worker_progress(w); }
                ucp_request_free(req);
            };
            ucp_config_t* cfg{}; if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { std::cerr << "ucp_config_read failed" << std::endl; return; }
            ucp_params_t pp{}; pp.field_mask = UCP_PARAM_FIELD_FEATURES; pp.features = UCP_FEATURE_TAG;
            ucp_context_h cctx{}; if (ucp_init(&pp, cfg, &cctx) != UCS_OK) { std::cerr << "ucp_init failed" << std::endl; ucp_config_release(cfg); return; }
            ucp_config_release(cfg);
            ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_SINGLE;
            ucp_worker_h cworker{}; if (ucp_worker_create(cctx, &wp, &cworker) != UCS_OK) { std::cerr << "ucp_worker_create failed" << std::endl; ucp_cleanup(cctx); return; }
            ucp_address_t* saddr{}; size_t saddr_len{}; if (ucp_worker_get_address(cworker, &saddr, &saddr_len) != UCS_OK) { std::cerr << "get addr failed" << std::endl; ucp_worker_destroy(cworker); ucp_cleanup(cctx); return; }
            uint32_t sl = htonl((uint32_t)saddr_len); ::send(ctrl_fd, &sl, sizeof(sl), 0); ::send(ctrl_fd, saddr, saddr_len, 0);
            uint32_t rl{}; if (recv(ctrl_fd, &rl, sizeof(rl), MSG_WAITALL) != (ssize_t)sizeof(rl)) { std::cerr << "recv len failed" << std::endl; }
            rl = ntohl(rl); std::vector<uint8_t> raddr(rl); if (recv(ctrl_fd, raddr.data(), raddr.size(), MSG_WAITALL) != (ssize_t)raddr.size()) { std::cerr << "recv addr failed" << std::endl; }
            ucp_worker_release_address(cworker, saddr);
            ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = (ucp_address_t*)raddr.data();
            ucp_ep_h cep{}; if (ucp_ep_create(cworker, &epp, &cep) != UCS_OK) { std::cerr << "ep create failed" << std::endl; }
            ucp_request_param_t fparam{}; fparam.op_attr_mask = 0; ctrl_wait(cworker, ucp_ep_flush_nbx(cep, &fparam));

            const uint64_t CTRL_TAG = 0xC0DEC0DEULL;

            for (size_t si = 0; si < sizes.size(); ++si) {
                uint32_t token = (uint32_t)si;
                size_t expected = (num_remote_senders) * (size_t)rounds;
                auto t0 = clock_type::now();
                ucp_request_param_t sp{}; sp.op_attr_mask = 0;
                ctrl_wait(cworker, ucp_tag_send_nbx(cep, &token, sizeof(token), CTRL_TAG, &sp));
                size_t got = 0; Message m;
                while (got < expected) { if (q.dequeue(m)) ++got; }
                auto t1 = clock_type::now();
                std::cout << "UCX remote fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << std::chrono::duration<double, std::micro>(t1 - t0).count() << std::endl;
            }

            // send termination token 0xFFFFFFFF
            {
                uint32_t term = 0xFFFFFFFFu; ucp_request_param_t sp{}; sp.op_attr_mask = 0; ctrl_wait(cworker, ucp_tag_send_nbx(cep, &term, sizeof(term), CTRL_TAG, &sp));
            }
            ucp_ep_destroy(cep); ucp_worker_destroy(cworker); ucp_cleanup(cctx); close(ctrl_fd);
        }

        q.stop();
    } else {
        // ========== Remote client: set up UCX control and remote senders only ==========
        // TCP control channel to exchange UCX control addresses
        int ctrl_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        while (connect(ctrl_fd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

        // UCX control setup
        auto ctrl_wait = [](ucp_worker_h w, void* req) {
            if (req == nullptr) return;
            if (!UCS_PTR_IS_PTR(req)) { ucs_status_t st = (ucs_status_t)UCS_PTR_STATUS(req); if (st != UCS_OK) std::abort(); return; }
            while (ucp_request_check_status(req) == UCS_INPROGRESS) { ucp_worker_progress(w); }
            ucp_request_free(req);
        };
        ucp_config_t* cfg{}; if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { std::cerr << "ucp_config_read failed" << std::endl; return; }
        ucp_params_t pp{}; pp.field_mask = UCP_PARAM_FIELD_FEATURES; pp.features = UCP_FEATURE_TAG;
        ucp_context_h cctx{}; if (ucp_init(&pp, cfg, &cctx) != UCS_OK) { std::cerr << "ucp_init failed" << std::endl; ucp_config_release(cfg); return; }
        ucp_config_release(cfg);
        ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_SINGLE;
        ucp_worker_h cworker{}; if (ucp_worker_create(cctx, &wp, &cworker) != UCS_OK) { std::cerr << "ucp_worker_create failed" << std::endl; ucp_cleanup(cctx); return; }
        // Receive server addr, send client addr back
        uint32_t sl{}; if (recv(ctrl_fd, &sl, sizeof(sl), MSG_WAITALL) != (ssize_t)sizeof(sl)) { std::cerr << "recv len failed" << std::endl; }
        sl = ntohl(sl); std::vector<uint8_t> saddr(sl); if (recv(ctrl_fd, saddr.data(), saddr.size(), MSG_WAITALL) != (ssize_t)saddr.size()) { std::cerr << "recv addr failed" << std::endl; }
        ucp_address_t* caddr{}; size_t caddr_len{}; if (ucp_worker_get_address(cworker, &caddr, &caddr_len) != UCS_OK) { std::cerr << "get addr failed" << std::endl; }
        uint32_t cl = htonl((uint32_t)caddr_len); ::send(ctrl_fd, &cl, sizeof(cl), 0); ::send(ctrl_fd, caddr, caddr_len, 0);
        ucp_worker_release_address(cworker, caddr);
        // Create ep to server
        ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = (ucp_address_t*)saddr.data();
        ucp_ep_h cep{}; if (ucp_ep_create(cworker, &epp, &cep) != UCS_OK) { std::cerr << "ep create failed" << std::endl; }
        ucp_request_param_t fparam{}; fparam.op_attr_mask = 0; ctrl_wait(cworker, ucp_ep_flush_nbx(cep, &fparam));

        const uint64_t CTRL_TAG = 0xC0DEC0DEULL;

        // Remote sender threads
        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::atomic<uint64_t> round_id{0};
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i]{
                FanInQueueSender sender(ip, port);
                sender.start();
                uint64_t seen = round_id.load();
                while (!done.load()) {
                    while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                    seen = round_id.load();
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x6B);
                    for (int r = 0; r < rounds; ++r) {
                        sender.send(payload.data(), payload.size());
                    }
                }
                sender.stop();
            });
        }

        // Control loop: receive tokens from server via UCX and start rounds
        for (;;) {
            uint32_t token = 0;
            ucp_request_param_t rp{}; rp.op_attr_mask = 0;
            void* rreq = ucp_tag_recv_nbx(cworker, &token, sizeof(token), CTRL_TAG, (uint64_t)-1, &rp);
            // Wait
            if (UCS_PTR_IS_PTR(rreq)) { while (ucp_request_check_status(rreq) == UCS_INPROGRESS) { ucp_worker_progress(cworker); } ucp_request_free(rreq); }
            if (token == 0xFFFFFFFFu) break;
            size_index.store((size_t)token);
            round_id.fetch_add(1, std::memory_order_acq_rel);
        }
        done.store(true);
        for (auto& th : remote) th.join();
        ucp_ep_destroy(cep); ucp_worker_destroy(cworker); ucp_cleanup(cctx); close(ctrl_fd);
    }
}

int main(int argc, char** argv) {
    if (argc < 3) { std::cerr << "Usage: ucx_queue_test server|client zmq|ucx" << std::endl; return 1; }
    bool isServer = std::string(argv[1]) == "server";
    std::string mode = argv[2];
    const char* ip = "10.10.2.1"; // server IP
    int port = 61000;
    size_t local_threads = 16;
    size_t remote_threads = 16;
    const std::vector<size_t> sizes = {4096, 8192, 65536, 131072, 1048576};
    int rounds = 20;

    if (mode == "zmq") {
        run_zmq_fanin_all(isServer, ip, port, local_threads, remote_threads, sizes, rounds);
    } else if (mode == "ucx") {
        run_ucx_fanin_all(isServer, ip, port, local_threads, remote_threads, sizes, rounds);
    } else {
        std::cerr << "mode must be zmq or ucx" << std::endl; return 1;
    }
    return 0;
}


