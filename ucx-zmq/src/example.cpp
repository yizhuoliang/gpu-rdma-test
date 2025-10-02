#include "ucxq.hpp"
#include <zmq.hpp>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <cstring>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fstream>

using clock_type = std::chrono::high_resolution_clock;

// Helper to append one CSV row: size_bytes,pattern,round,latency_usec
static inline void csv_write_row(std::ofstream& csv, size_t size_bytes, const char* pattern, int round, double latency_usec) {
    csv << size_bytes << "," << pattern << "," << round << "," << latency_usec << "\n";
    csv.flush();
}

static void run_ucxq_fanin_all(bool isServer, const char* ip, int port, size_t num_local_senders, size_t num_remote_senders, const std::vector<size_t>& sizes, int rounds, std::ofstream* csv, int repeats, int warmup) {
    using namespace ucxq;
    context_t ctx(1);
    std::string endpoint = std::string("tcp://") + ip + ":" + std::to_string(port);
    const uint64_t CTRL_TAG = 0xC0DEC0DEULL;

    if (isServer) {
        socket_t pull(ctx, socket_type::pull);
        pull.bind(endpoint);

        // ========== Local fan-in measurement (only local UCXQ threads) ==========
        {
            std::atomic<size_t> size_index{0};
            std::atomic<bool> done{false};
            std::atomic<uint64_t> round_id{0};
            std::vector<std::thread> local;
            std::atomic<bool> start_local{false};
            std::atomic<size_t> started_local{0};
            for (size_t i = 0; i < num_local_senders; ++i) {
                local.emplace_back([&, i] {
                    socket_t push(ctx, socket_type::push);
                    push.connect(endpoint);
                    while (!start_local.load()) { std::this_thread::yield(); }
                    uint64_t seen = round_id.load();
                    started_local.fetch_add(1, std::memory_order_acq_rel);
                    while (!done.load()) {
                        while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                        seen = round_id.load();
                        size_t idx = size_index.load();
                        if (idx >= sizes.size()) break;
                        size_t msg_bytes = sizes[idx];
                        std::vector<uint8_t> payload(msg_bytes, 0x7C);
                        for (int r = 0; r < rounds; ++r) {
                            push.send(buffer(payload.data(), payload.size()));
                        }
                    }
                });
            }
            start_local.store(true, std::memory_order_release);
            while (started_local.load(std::memory_order_acquire) < num_local_senders) { std::this_thread::yield(); }
            message_t msg;
            for (size_t si = 0; si < sizes.size(); ++si) {
                for (int rep = 1; rep <= repeats + warmup; ++rep) {
                    size_index.store(si);
                    size_t expected = num_local_senders * static_cast<size_t>(rounds);
                    auto t0 = clock_type::now();
                    round_id.fetch_add(1, std::memory_order_acq_rel);
                    size_t got = 0;
                    while (got < expected) {
                        auto res = pull.recv(msg, recv_flags::none);
                        if (res && *res > 0) {
                            ++got;
                        }
                    }
                    auto t1 = clock_type::now();
                    double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();
                    std::cout << "UCXQ local fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << usec << std::endl;
                    if (csv && rep > warmup) csv_write_row(*csv, sizes[si], "UCXQ_local", rep - warmup, usec);
                }
            }
            done.store(true);
            for (auto& th : local) th.join();
        }

        // ========== Remote fan-in measurement ==========
        {
            ucp_context_h cctx{};
            ucp_worker_h cworker{};
            ucp_ep_h cep{};
            int ctrl_fd = -1;
            auto ctrl_wait = [](ucp_worker_h w, void* req) {
                if (req == nullptr) return;
                if (!UCS_PTR_IS_PTR(req)) {
                    ucs_status_t st = (ucs_status_t)UCS_PTR_STATUS(req);
                    if (st != UCS_OK) std::abort();
                    return;
                }
                while (ucp_request_check_status(req) == UCS_INPROGRESS) {
                    ucp_worker_progress(w);
                }
                ucp_request_free(req);
            };

            int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (lfd < 0) { std::cerr << "socket() failed" << std::endl; return; }
            int yes = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
            int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
            if (bind(lfd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::cerr << "bind() failed" << std::endl; close(lfd); return; }
            if (listen(lfd, 1) != 0) { std::cerr << "listen() failed" << std::endl; close(lfd); return; }
            ctrl_fd = accept(lfd, nullptr, nullptr);
            close(lfd);
            if (ctrl_fd < 0) { std::cerr << "accept() failed" << std::endl; return; }

            ucp_config_t* cfg{};
            if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) {
                std::cerr << "ucp_config_read failed" << std::endl;
                close(ctrl_fd);
                return;
            }
            ucp_params_t pp{}; pp.field_mask = UCP_PARAM_FIELD_FEATURES; pp.features = UCP_FEATURE_TAG;
            if (ucp_init(&pp, cfg, &cctx) != UCS_OK) {
                std::cerr << "ucp_init failed" << std::endl;
                ucp_config_release(cfg);
                close(ctrl_fd);
                return;
            }
            ucp_config_release(cfg);
            ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_SINGLE;
            if (ucp_worker_create(cctx, &wp, &cworker) != UCS_OK) {
                std::cerr << "ucp_worker_create failed" << std::endl;
                ucp_cleanup(cctx);
                close(ctrl_fd);
                return;
            }
            ucp_address_t* saddr{}; size_t saddr_len{};
            if (ucp_worker_get_address(cworker, &saddr, &saddr_len) != UCS_OK) {
                std::cerr << "get addr failed" << std::endl;
                ucp_worker_destroy(cworker);
                ucp_cleanup(cctx);
                close(ctrl_fd);
                return;
            }
            uint32_t sl = htonl(static_cast<uint32_t>(saddr_len));
            if (::send(ctrl_fd, &sl, sizeof(sl), 0) != (ssize_t)sizeof(sl)) { std::cerr << "send len failed" << std::endl; }
            if (::send(ctrl_fd, saddr, saddr_len, 0) != (ssize_t)saddr_len) { std::cerr << "send addr failed" << std::endl; }
            ucp_worker_release_address(cworker, saddr);
            uint32_t rl{};
            if (recv(ctrl_fd, &rl, sizeof(rl), MSG_WAITALL) != (ssize_t)sizeof(rl)) { std::cerr << "recv len failed" << std::endl; }
            rl = ntohl(rl);
            std::vector<uint8_t> raddr(rl);
            if (recv(ctrl_fd, raddr.data(), raddr.size(), MSG_WAITALL) != (ssize_t)raddr.size()) { std::cerr << "recv addr failed" << std::endl; }
            ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = reinterpret_cast<ucp_address_t*>(raddr.data());
            if (ucp_ep_create(cworker, &epp, &cep) != UCS_OK) { std::cerr << "ep create failed" << std::endl; }
            ucp_request_param_t fparam{}; fparam.op_attr_mask = 0;
            ctrl_wait(cworker, ucp_ep_flush_nbx(cep, &fparam));

            std::atomic<size_t> size_index{0};
            std::atomic<bool> done{false};
            std::atomic<uint64_t> round_id{0};
            std::atomic<bool> start_remote{false};
            std::atomic<size_t> started_remote{0};
            std::vector<std::thread> remote;
            for (size_t i = 0; i < num_remote_senders; ++i) {
                remote.emplace_back([&, i] {
                    socket_t push(ctx, socket_type::push);
                    push.connect(endpoint);
                    while (!start_remote.load()) { std::this_thread::yield(); }
                    uint64_t seen = round_id.load();
                    started_remote.fetch_add(1, std::memory_order_acq_rel);
                    while (!done.load()) {
                        while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                        seen = round_id.load();
                        size_t idx = size_index.load();
                        if (idx >= sizes.size()) break;
                        size_t msg_bytes = sizes[idx];
                        std::vector<uint8_t> payload(msg_bytes, 0x7D);
                        for (int r = 0; r < rounds; ++r) {
                            push.send(buffer(payload.data(), payload.size()));
                        }
                    }
                });
            }

            start_remote.store(true, std::memory_order_release);
            while (started_remote.load(std::memory_order_acquire) < num_remote_senders) { std::this_thread::yield(); }
            {
                uint32_t ack = 0xA5A5A5A5u;
                ucp_request_param_t sp{}; sp.op_attr_mask = 0;
                ctrl_wait(cworker, ucp_tag_send_nbx(cep, &ack, sizeof(ack), CTRL_TAG, &sp));
            }

            message_t msg;
            for (size_t si = 0; si < sizes.size(); ++si) {
                for (int rep = 1; rep <= repeats + warmup; ++rep) {
                    uint32_t token = static_cast<uint32_t>(si);
                    size_t expected = num_remote_senders * static_cast<size_t>(rounds);
                    auto t0 = clock_type::now();
                    ucp_request_param_t sp{}; sp.op_attr_mask = 0;
                    ctrl_wait(cworker, ucp_tag_send_nbx(cep, &token, sizeof(token), CTRL_TAG, &sp));
                    size_index.store(si);
                    round_id.fetch_add(1, std::memory_order_acq_rel);
                    size_t got = 0;
                    while (got < expected) {
                        auto res = pull.recv(msg, recv_flags::none);
                        if (res && *res > 0) {
                            ++got;
                        }
                    }
                    auto t1 = clock_type::now();
                    double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();
                    std::cout << "UCXQ remote fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << usec << std::endl;
                    if (csv && rep > warmup) csv_write_row(*csv, sizes[si], "UCXQ_remote", rep - warmup, usec);
                }
            }

            {
                uint32_t term = 0xFFFFFFFFu;
                ucp_request_param_t sp{}; sp.op_attr_mask = 0;
                ctrl_wait(cworker, ucp_tag_send_nbx(cep, &term, sizeof(term), CTRL_TAG, &sp));
            }
            done.store(true);
            for (auto& th : remote) th.join();
            ucp_ep_destroy(cep);
            ucp_worker_destroy(cworker);
            ucp_cleanup(cctx);
            close(ctrl_fd);
        }
    } else {
        // Remote client: establish UCX control channel first
        int ctrl_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (ctrl_fd < 0) { std::cerr << "socket() failed" << std::endl; return; }
        int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        while (connect(ctrl_fd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

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
        ucp_config_t* cfg{};
        if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { std::cerr << "ucp_config_read failed" << std::endl; close(ctrl_fd); return; }
        ucp_params_t pp{}; pp.field_mask = UCP_PARAM_FIELD_FEATURES; pp.features = UCP_FEATURE_TAG;
        ucp_context_h cctx{};
        if (ucp_init(&pp, cfg, &cctx) != UCS_OK) { std::cerr << "ucp_init failed" << std::endl; ucp_config_release(cfg); close(ctrl_fd); return; }
        ucp_config_release(cfg);
        ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_SINGLE;
        ucp_worker_h cworker{};
        if (ucp_worker_create(cctx, &wp, &cworker) != UCS_OK) { std::cerr << "ucp_worker_create failed" << std::endl; ucp_cleanup(cctx); close(ctrl_fd); return; }
        uint32_t sl{};
        if (recv(ctrl_fd, &sl, sizeof(sl), MSG_WAITALL) != (ssize_t)sizeof(sl)) { std::cerr << "recv len failed" << std::endl; }
        sl = ntohl(sl);
        std::vector<uint8_t> saddr(sl);
        if (recv(ctrl_fd, saddr.data(), saddr.size(), MSG_WAITALL) != (ssize_t)saddr.size()) { std::cerr << "recv addr failed" << std::endl; }
        ucp_address_t* caddr{}; size_t caddr_len{};
        if (ucp_worker_get_address(cworker, &caddr, &caddr_len) != UCS_OK) { std::cerr << "get addr failed" << std::endl; }
        uint32_t cl = htonl(static_cast<uint32_t>(caddr_len));
        ::send(ctrl_fd, &cl, sizeof(cl), 0);
        ::send(ctrl_fd, caddr, caddr_len, 0);
        ucp_worker_release_address(cworker, caddr);
        ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = reinterpret_cast<ucp_address_t*>(saddr.data());
        ucp_ep_h cep{};
        if (ucp_ep_create(cworker, &epp, &cep) != UCS_OK) { std::cerr << "ep create failed" << std::endl; }
        ucp_request_param_t fparam{}; fparam.op_attr_mask = 0;
        ctrl_wait(cworker, ucp_ep_flush_nbx(cep, &fparam));

        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::atomic<uint64_t> round_id{0};
        std::atomic<bool> start_remote{false};
        std::atomic<size_t> started_remote{0};
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i] {
                socket_t push(ctx, socket_type::push);
                push.connect(endpoint);
                while (!start_remote.load()) { std::this_thread::yield(); }
                uint64_t seen = round_id.load();
                started_remote.fetch_add(1, std::memory_order_acq_rel);
                while (!done.load()) {
                    while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                    seen = round_id.load();
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x7E);
                    for (int r = 0; r < rounds; ++r) {
                        push.send(buffer(payload.data(), payload.size()));
                    }
                }
            });
        }

        start_remote.store(true, std::memory_order_release);
        while (started_remote.load(std::memory_order_acquire) < num_remote_senders) { std::this_thread::yield(); }
        {
            uint32_t ack = 0;
            ucp_request_param_t rp{}; rp.op_attr_mask = 0;
            void* rreq = ucp_tag_recv_nbx(cworker, &ack, sizeof(ack), CTRL_TAG, static_cast<uint64_t>(-1), &rp);
            if (UCS_PTR_IS_PTR(rreq)) {
                while (ucp_request_check_status(rreq) == UCS_INPROGRESS) { ucp_worker_progress(cworker); }
                ucp_request_free(rreq);
            }
        }

        for (;;) {
            uint32_t token = 0;
            ucp_request_param_t rp{}; rp.op_attr_mask = 0;
            void* rreq = ucp_tag_recv_nbx(cworker, &token, sizeof(token), CTRL_TAG, static_cast<uint64_t>(-1), &rp);
            if (UCS_PTR_IS_PTR(rreq)) {
                while (ucp_request_check_status(rreq) == UCS_INPROGRESS) { ucp_worker_progress(cworker); }
                ucp_request_free(rreq);
            }
            if (token == 0xFFFFFFFFu) break;
            size_index.store(static_cast<size_t>(token));
            round_id.fetch_add(1, std::memory_order_acq_rel);
        }
        done.store(true);
        for (auto& th : remote) th.join();
        ucp_ep_destroy(cep);
        ucp_worker_destroy(cworker);
        ucp_cleanup(cctx);
        close(ctrl_fd);
    }
}

static void run_zmq_fanin_all(bool isServer, const char* ip, int port, size_t num_local_senders, size_t num_remote_senders, const std::vector<size_t>& sizes, int rounds, std::ofstream* csv, int repeats, int warmup) {
    zmq::context_t ctx(1);
    std::string endpoint = std::string("tcp://") + ip + ":" + std::to_string(port);
    const uint64_t CTRL_TAG = 0xC0DEC0DEULL;
    if (isServer) {
        zmq::socket_t pull(ctx, ZMQ_PULL);
        pull.bind(endpoint);

        // ========== Local fan-in measurement (only local ZMQ threads) ==========
        {
            std::atomic<size_t> size_index{0};
            std::atomic<bool> done{false};
            std::atomic<uint64_t> round_id{0};
            std::vector<std::thread> local;
            std::atomic<bool> start_local{false};
            std::atomic<size_t> started_local{0};
            for (size_t i = 0; i < num_local_senders; ++i) {
                local.emplace_back([&, i] {
                    zmq::socket_t push(ctx, ZMQ_PUSH);
                    push.connect(endpoint);
                    while (!start_local.load()) { std::this_thread::yield(); }
                    uint64_t seen = round_id.load();
                    started_local.fetch_add(1, std::memory_order_acq_rel);
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
            start_local.store(true, std::memory_order_release);
            while (started_local.load(std::memory_order_acquire) < num_local_senders) { std::this_thread::yield(); }
            zmq::message_t msg;
            for (size_t si = 0; si < sizes.size(); ++si) {
                for (int rep = 1; rep <= repeats + warmup; ++rep) {
                    size_index.store(si);
                    size_t expected = num_local_senders * static_cast<size_t>(rounds);
                    auto t0 = clock_type::now();
                    round_id.fetch_add(1, std::memory_order_acq_rel);
                    size_t got = 0;
                    while (got < expected) {
                        auto res = pull.recv(msg, zmq::recv_flags::none);
                        if (res && *res > 0) {
                            ++got;
                        }
                    }
                    auto t1 = clock_type::now();
                    double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();
                    std::cout << "ZMQ local fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << usec << std::endl;
                    if (csv && rep > warmup) csv_write_row(*csv, sizes[si], "ZMQ_local", rep - warmup, usec);
                }
            }
            done.store(true);
            for (auto& th : local) th.join();
        }

        // ========== Remote fan-in measurement ==========
        {
            ucp_context_h cctx{}; ucp_worker_h cworker{}; ucp_ep_h cep{}; int ctrl_fd = -1;
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
            int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
            if (lfd < 0) { std::cerr << "socket() failed" << std::endl; return; }
            int yes = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
            int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
            if (bind(lfd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::cerr << "bind() failed" << std::endl; close(lfd); return; }
            if (listen(lfd, 1) != 0) { std::cerr << "listen() failed" << std::endl; close(lfd); return; }
            ctrl_fd = accept(lfd, nullptr, nullptr);
            close(lfd);
            if (ctrl_fd < 0) { std::cerr << "accept() failed" << std::endl; return; }

            ucp_config_t* cfg{};
            if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { std::cerr << "ucp_config_read failed" << std::endl; close(ctrl_fd); return; }
            ucp_params_t pp{}; pp.field_mask = UCP_PARAM_FIELD_FEATURES; pp.features = UCP_FEATURE_TAG;
            if (ucp_init(&pp, cfg, &cctx) != UCS_OK) { std::cerr << "ucp_init failed" << std::endl; ucp_config_release(cfg); close(ctrl_fd); return; }
            ucp_config_release(cfg);
            ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_SINGLE;
            if (ucp_worker_create(cctx, &wp, &cworker) != UCS_OK) { std::cerr << "ucp_worker_create failed" << std::endl; ucp_cleanup(cctx); close(ctrl_fd); return; }
            ucp_address_t* saddr{}; size_t saddr_len{};
            if (ucp_worker_get_address(cworker, &saddr, &saddr_len) != UCS_OK) { std::cerr << "get addr failed" << std::endl; ucp_worker_destroy(cworker); ucp_cleanup(cctx); close(ctrl_fd); return; }
            uint32_t sl = htonl(static_cast<uint32_t>(saddr_len));
            if (::send(ctrl_fd, &sl, sizeof(sl), 0) != (ssize_t)sizeof(sl)) { std::cerr << "send len failed" << std::endl; }
            if (::send(ctrl_fd, saddr, saddr_len, 0) != (ssize_t)saddr_len) { std::cerr << "send addr failed" << std::endl; }
            ucp_worker_release_address(cworker, saddr);
            uint32_t rl{};
            if (recv(ctrl_fd, &rl, sizeof(rl), MSG_WAITALL) != (ssize_t)sizeof(rl)) { std::cerr << "recv len failed" << std::endl; }
            rl = ntohl(rl);
            std::vector<uint8_t> raddr(rl);
            if (recv(ctrl_fd, raddr.data(), raddr.size(), MSG_WAITALL) != (ssize_t)raddr.size()) { std::cerr << "recv addr failed" << std::endl; }
            ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = reinterpret_cast<ucp_address_t*>(raddr.data());
            if (ucp_ep_create(cworker, &epp, &cep) != UCS_OK) { std::cerr << "ep create failed" << std::endl; }
            ucp_request_param_t fparam{}; fparam.op_attr_mask = 0; ctrl_wait(cworker, ucp_ep_flush_nbx(cep, &fparam));

            std::atomic<size_t> size_index{0};
            std::atomic<bool> done{false};
            std::atomic<uint64_t> round_id{0};
            std::atomic<bool> start_remote{false};
            std::atomic<size_t> started_remote{0};
            std::vector<std::thread> remote;
            for (size_t i = 0; i < num_remote_senders; ++i) {
                remote.emplace_back([&, i] {
                    zmq::socket_t push(ctx, ZMQ_PUSH);
                    push.connect(endpoint);
                    while (!start_remote.load()) { std::this_thread::yield(); }
                    uint64_t seen = round_id.load();
                    started_remote.fetch_add(1, std::memory_order_acq_rel);
                    while (!done.load()) {
                        while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                        seen = round_id.load();
                        size_t idx = size_index.load();
                        if (idx >= sizes.size()) break;
                        size_t msg_bytes = sizes[idx];
                        std::vector<uint8_t> payload(msg_bytes, 0x5B);
                        for (int r = 0; r < rounds; ++r) {
                            push.send(zmq::buffer(payload), zmq::send_flags::none);
                        }
                    }
                });
            }
            start_remote.store(true, std::memory_order_release);
            while (started_remote.load(std::memory_order_acquire) < num_remote_senders) { std::this_thread::yield(); }
            { uint32_t ack = 0xA5A5A5A5u; ucp_request_param_t sp{}; sp.op_attr_mask = 0; ctrl_wait(cworker, ucp_tag_send_nbx(cep, &ack, sizeof(ack), CTRL_TAG, &sp)); }
            zmq::message_t msg;
            for (size_t si = 0; si < sizes.size(); ++si) {
                for (int rep = 1; rep <= repeats + warmup; ++rep) {
                    uint32_t token = static_cast<uint32_t>(si);
                    size_t expected = num_remote_senders * static_cast<size_t>(rounds);
                    auto t0 = clock_type::now();
                    ucp_request_param_t sp{}; sp.op_attr_mask = 0; ctrl_wait(cworker, ucp_tag_send_nbx(cep, &token, sizeof(token), CTRL_TAG, &sp));
                    size_index.store(si);
                    round_id.fetch_add(1, std::memory_order_acq_rel);
                    size_t got = 0;
                    while (got < expected) {
                        auto res = pull.recv(msg, zmq::recv_flags::none);
                        if (res && *res > 0) {
                            ++got;
                        }
                    }
                    auto t1 = clock_type::now();
                    double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();
                    std::cout << "ZMQ remote fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << usec << std::endl;
                    if (csv && rep > warmup) csv_write_row(*csv, sizes[si], "ZMQ_remote", rep - warmup, usec);
                }
            }
            { uint32_t term = 0xFFFFFFFFu; ucp_request_param_t sp{}; sp.op_attr_mask = 0; ctrl_wait(cworker, ucp_tag_send_nbx(cep, &term, sizeof(term), CTRL_TAG, &sp)); }
            done.store(true);
            for (auto& th : remote) th.join();
            ucp_ep_destroy(cep); ucp_worker_destroy(cworker); ucp_cleanup(cctx); close(ctrl_fd);
        }
    } else {
        int ctrl_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (ctrl_fd < 0) { std::cerr << "socket() failed" << std::endl; return; }
        int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        while (connect(ctrl_fd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

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
        ucp_config_t* cfg{}; if (ucp_config_read(nullptr, nullptr, &cfg) != UCS_OK) { std::cerr << "ucp_config_read failed" << std::endl; close(ctrl_fd); return; }
        ucp_params_t pp{}; pp.field_mask = UCP_PARAM_FIELD_FEATURES; pp.features = UCP_FEATURE_TAG;
        ucp_context_h cctx{}; if (ucp_init(&pp, cfg, &cctx) != UCS_OK) { std::cerr << "ucp_init failed" << std::endl; ucp_config_release(cfg); close(ctrl_fd); return; }
        ucp_config_release(cfg);
        ucp_worker_params_t wp{}; wp.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wp.thread_mode = UCS_THREAD_MODE_SINGLE;
        ucp_worker_h cworker{}; if (ucp_worker_create(cctx, &wp, &cworker) != UCS_OK) { std::cerr << "ucp_worker_create failed" << std::endl; ucp_cleanup(cctx); close(ctrl_fd); return; }
        uint32_t sl{}; if (recv(ctrl_fd, &sl, sizeof(sl), MSG_WAITALL) != (ssize_t)sizeof(sl)) { std::cerr << "recv len failed" << std::endl; }
        sl = ntohl(sl); std::vector<uint8_t> saddr(sl); if (recv(ctrl_fd, saddr.data(), saddr.size(), MSG_WAITALL) != (ssize_t)saddr.size()) { std::cerr << "recv addr failed" << std::endl; }
        ucp_address_t* caddr{}; size_t caddr_len{}; if (ucp_worker_get_address(cworker, &caddr, &caddr_len) != UCS_OK) { std::cerr << "get addr failed" << std::endl; }
        uint32_t cl = htonl(static_cast<uint32_t>(caddr_len)); ::send(ctrl_fd, &cl, sizeof(cl), 0); ::send(ctrl_fd, caddr, caddr_len, 0);
        ucp_worker_release_address(cworker, caddr);
        ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = reinterpret_cast<ucp_address_t*>(saddr.data());
        ucp_ep_h cep{}; if (ucp_ep_create(cworker, &epp, &cep) != UCS_OK) { std::cerr << "ep create failed" << std::endl; }
        ucp_request_param_t fparam{}; fparam.op_attr_mask = 0; ctrl_wait(cworker, ucp_ep_flush_nbx(cep, &fparam));

        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::atomic<uint64_t> round_id{0};
        std::atomic<bool> start_remote{false};
        std::atomic<size_t> started_remote{0};
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i] {
                zmq::socket_t push(ctx, ZMQ_PUSH);
                push.connect(endpoint);
                while (!start_remote.load()) { std::this_thread::yield(); }
                uint64_t seen = round_id.load();
                started_remote.fetch_add(1, std::memory_order_acq_rel);
                while (!done.load()) {
                    while (round_id.load() == seen && !done.load()) std::this_thread::yield();
                    seen = round_id.load();
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x5C);
                    for (int r = 0; r < rounds; ++r) {
                        push.send(zmq::buffer(payload), zmq::send_flags::none);
                    }
                }
            });
        }
        start_remote.store(true, std::memory_order_release);
        while (started_remote.load(std::memory_order_acquire) < num_remote_senders) { std::this_thread::yield(); }
        {
            uint32_t ack = 0;
            ucp_request_param_t rp{}; rp.op_attr_mask = 0;
            void* rreq = ucp_tag_recv_nbx(cworker, &ack, sizeof(ack), CTRL_TAG, static_cast<uint64_t>(-1), &rp);
            if (UCS_PTR_IS_PTR(rreq)) {
                while (ucp_request_check_status(rreq) == UCS_INPROGRESS) { ucp_worker_progress(cworker); }
                ucp_request_free(rreq);
            }
        }
        for (;;) {
            uint32_t token = 0;
            ucp_request_param_t rp{}; rp.op_attr_mask = 0;
            void* rreq = ucp_tag_recv_nbx(cworker, &token, sizeof(token), CTRL_TAG, static_cast<uint64_t>(-1), &rp);
            if (UCS_PTR_IS_PTR(rreq)) {
                while (ucp_request_check_status(rreq) == UCS_INPROGRESS) { ucp_worker_progress(cworker); }
                ucp_request_free(rreq);
            }
            if (token == 0xFFFFFFFFu) break;
            size_index.store(static_cast<size_t>(token));
            round_id.fetch_add(1, std::memory_order_acq_rel);
        }
        done.store(true);
        for (auto& th : remote) th.join();
        ucp_ep_destroy(cep); ucp_worker_destroy(cworker); ucp_cleanup(cctx); close(ctrl_fd);
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: ucxq_example server|client" << std::endl;
        return 1;
    }
    bool isServer = std::string(argv[1]) == "server";
    const char* ip = "10.10.2.1";
    int port = 61000;
    size_t local_threads = 16;
    size_t remote_threads = 16;
    const std::vector<size_t> sizes = {4096, 8192, 65536, 131072, 1048576};
    int rounds = 20;
    int repeats = 20;
    int warmup = 5;

    std::ofstream csv;
    if (isServer) {
        const char* csv_path = "results_ucx_zmq.csv";
        csv.open(csv_path, std::ios::out | std::ios::trunc);
        if (!csv.is_open()) {
            std::cerr << "Failed to open CSV for write: " << csv_path << std::endl;
        } else {
            csv << "size_bytes,pattern,round,latency_usec\n";
        }
    }

    run_ucxq_fanin_all(isServer, ip, port, local_threads, remote_threads, sizes, rounds, isServer ? &csv : nullptr, repeats, warmup);
    run_zmq_fanin_all(isServer, ip, port, local_threads, remote_threads, sizes, rounds, isServer ? &csv : nullptr, repeats, warmup);

    if (csv.is_open()) csv.close();
    return 0;
}
