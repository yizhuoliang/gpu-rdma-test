#include "../include/ucx_queue.hpp"
#include <zmq.hpp>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>
#include <cstring>
#include <atomic>

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

static void run_ucx_fanin_all(bool isServer, const char* ip, int port, size_t num_local_senders, size_t num_remote_senders, const std::vector<size_t>& sizes, int rounds) {
    using namespace ucxq;
    if (isServer) {
        FanInQueue q("server", ip, port);
        q.start(num_local_senders + num_remote_senders);
        size_t local_base = q.create_local_endpoints(num_local_senders);
        while (q.endpoint_count() < (num_local_senders + num_remote_senders)) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

        // Persistent local sender threads iterate over sizes
        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::vector<std::thread> local;
        for (size_t i = 0; i < num_local_senders; ++i) {
            local.emplace_back([&, i]{
                while (!done.load()) {
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x6B);
                    for (int r = 0; r < rounds; ++r) {
                        q.send(local_base + i, payload.data(), payload.size());
                    }
                    // Wait for next size
                    while (size_index.load() == idx && !done.load()) std::this_thread::yield();
                }
            });
        }

        for (size_t si = 0; si < sizes.size(); ++si) {
            size_index.store(si);
            size_t expected = (num_local_senders + num_remote_senders) * (size_t)rounds;
            auto t0 = clock_type::now();
            size_t got = 0; Message m;
            while (got < expected) { if (q.dequeue(m)) ++got; }
            auto t1 = clock_type::now();
            std::cout << "UCX fan-in msg_bytes=" << sizes[si] << " total_msgs=" << expected << " total_usec=" << std::chrono::duration<double, std::micro>(t1 - t0).count() << std::endl;
        }
        done.store(true);
        for (auto& th : local) th.join();
        q.stop();
    } else {
        FanInQueue q("client", ip, port);
        q.start(num_remote_senders);
        while (q.endpoint_count() < num_remote_senders) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }

        // Persistent remote sender threads iterate over sizes
        std::atomic<size_t> size_index{0};
        std::atomic<bool> done{false};
        std::vector<std::thread> remote;
        for (size_t i = 0; i < num_remote_senders; ++i) {
            remote.emplace_back([&, i]{
                while (!done.load()) {
                    size_t idx = size_index.load();
                    if (idx >= sizes.size()) break;
                    size_t msg_bytes = sizes[idx];
                    std::vector<uint8_t> payload(msg_bytes, 0x6B);
                    for (int r = 0; r < rounds; ++r) {
                        q.send(i, payload.data(), payload.size());
                    }
                    // Wait for next size
                    while (size_index.load() == idx && !done.load()) std::this_thread::yield();
                }
            });
        }

        for (size_t si = 0; si < sizes.size(); ++si) {
            size_index.store(si);
            // Client just sends; give server time to drain
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        done.store(true);
        for (auto& th : remote) th.join();
        q.stop();
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
    const std::vector<size_t> sizes = {1024, 8192, 65536, 131072, 1048576};
    int rounds = 20;

    if (mode == "zmq") {
        for (size_t sz : sizes) {
            run_zmq_fanin(isServer, ip, port, local_threads, remote_threads, sz, rounds);
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    } else if (mode == "ucx") {
        run_ucx_fanin_all(isServer, ip, port, local_threads, remote_threads, sizes, rounds);
    } else {
        std::cerr << "mode must be zmq or ucx" << std::endl; return 1;
    }
    return 0;
}


