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
        // control channel on port+1
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        int yes = 1; setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        bind(lfd, (sockaddr*)&addr, sizeof(addr)); listen(lfd, 1);
        printf("Listening on port %d\n", ctrl_port);
        int ctrl_fd = accept(lfd, nullptr, nullptr); close(lfd);
        printf("Accepted control channel on port %d\n", ctrl_port);

        // Persistent local sender threads iterate over sizes
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
                printf("Local sender %zu started\n", i);
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
                    printf("Local sender %zu finished round %zu\n", i, seen);
                }
                sender.stop();
            });
        }

        printf("Local sender threads started\n");
        // Unleash local senders and ensure all captured initial round_id before first round
        start_local.store(true, std::memory_order_release);
        while (started_local.load(std::memory_order_acquire) < num_local_senders) { std::this_thread::yield(); }
        
        for (size_t si = 0; si < sizes.size(); ++si) {
            size_index.store(si);
            uint32_t token = (uint32_t)si; ::send(ctrl_fd, &token, sizeof(token), 0);
            round_id.fetch_add(1, std::memory_order_acq_rel);
            size_t expected = (num_local_senders + num_remote_senders) * (size_t)rounds;
            auto t0 = clock_type::now();
            size_t got = 0; Message m;
            while (got < expected) { if (q.dequeue(m)) ++got; }
            auto t1 = clock_type::now();
            std::cout << "UCX fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << std::chrono::duration<double, std::micro>(t1 - t0).count() << std::endl;
        }
        done.store(true);
        for (auto& th : local) th.join();
        if (ctrl_fd >= 0) close(ctrl_fd);
        q.stop();
    } else {
        // remote: each thread owns its own sender
        int ctrl_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        int ctrl_port = port + 1; sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(ctrl_port); inet_pton(AF_INET, ip, &addr.sin_addr);
        while (connect(ctrl_fd, (sockaddr*)&addr, sizeof(addr)) != 0) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

        // Persistent remote sender threads iterate over sizes
        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::atomic<uint64_t> round_id{0};
        std::atomic<bool> start_remote{false};
        std::atomic<size_t> started_remote{0};
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i]{
                FanInQueueSender sender(ip, port);
                sender.start();
                printf("Remote sender %zu started\n", i);
                while (!start_remote.load()) { std::this_thread::yield(); }
                uint64_t seen = round_id.load();
                started_remote.fetch_add(1, std::memory_order_acq_rel);
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

        // Ensure all remote sender threads captured initial round_id before processing first token
        start_remote.store(true, std::memory_order_release);
        while (started_remote.load(std::memory_order_acquire) < num_remote_senders) { std::this_thread::yield(); }

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


