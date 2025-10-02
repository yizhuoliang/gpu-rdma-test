#include "ucxq.hpp"
#include <zmq.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <atomic>
#include <sys/socket.h>
#include <unistd.h>

using clock_type = std::chrono::high_resolution_clock;

// Helper to append one CSV row: size_bytes,pattern,round,latency_usec
static inline void csv_write_row(std::ofstream& csv, size_t size_bytes, const char* pattern, int round, double latency_usec) {
    csv << size_bytes << "," << pattern << "," << round << "," << latency_usec << "\n";
    csv.flush();
}

namespace {

constexpr uint32_t kControlAck = 0xA5A5A5A5u;
constexpr uint32_t kControlTerminate = 0xFFFFFFFFu;

bool send_u32(int fd, uint32_t value) {
    uint32_t be = htonl(value);
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(&be);
    size_t sent = 0;
    while (sent < sizeof(be)) {
        ssize_t rc = ::send(fd, ptr + sent, sizeof(be) - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

bool recv_u32(int fd, uint32_t& value) {
    uint32_t be = 0;
    uint8_t* ptr = reinterpret_cast<uint8_t*>(&be);
    size_t received = 0;
    while (received < sizeof(be)) {
        ssize_t rc = ::recv(fd, ptr + received, sizeof(be) - received, 0);
        if (rc <= 0) {
            return false;
        }
        received += static_cast<size_t>(rc);
    }
    value = ntohl(be);
    return true;
}

int open_control_listener(const char* ip, int port) {
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (lfd < 0) {
        std::cerr << "socket() failed: " << std::strerror(errno) << std::endl;
        return -1;
    }
    int yes = 1;
    setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
#ifdef SO_REUSEPORT
    setsockopt(lfd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes));
#endif
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        std::cerr << "inet_pton failed for " << ip << std::endl;
        close(lfd);
        return -1;
    }
    if (bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::cerr << "bind() failed: " << std::strerror(errno) << std::endl;
        close(lfd);
        return -1;
    }
    if (listen(lfd, 1) != 0) {
        std::cerr << "listen() failed: " << std::strerror(errno) << std::endl;
        close(lfd);
        return -1;
    }
    int cfd = ::accept(lfd, nullptr, nullptr);
    if (cfd < 0) {
        std::cerr << "accept() failed: " << std::strerror(errno) << std::endl;
    }
    close(lfd);
    return cfd;
}

int open_control_client(const char* ip, int port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "socket() failed: " << std::strerror(errno) << std::endl;
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        std::cerr << "inet_pton failed for " << ip << std::endl;
        close(fd);
        return -1;
    }
    int attempts = 0;
    constexpr int kMaxAttempts = 200;
    while (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        if (++attempts >= kMaxAttempts) {
            std::cerr << "connect() failed after retries: " << std::strerror(errno) << std::endl;
            close(fd);
            return -1;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return fd;
}

// Transport traits for UCXQ
struct UcxTransport {
    using Context = ucxq::context_t;
    using PushSocket = ucxq::socket_t;
    using PullSocket = ucxq::socket_t;
    using Message = ucxq::message_t;

    static Context create_context() { return Context(1); }
    static PullSocket make_pull(Context& ctx) { return PullSocket(ctx, ucxq::socket_type::pull); }
    static PushSocket make_push(Context& ctx) { return PushSocket(ctx, ucxq::socket_type::push); }
    static void bind(PullSocket& sock, const std::string& endpoint) { sock.bind(endpoint); }
    static void connect(PushSocket& sock, const std::string& endpoint) { sock.connect(endpoint); }
    static void send(PushSocket& sock, const uint8_t* data, size_t len) { sock.send(ucxq::buffer(data, len)); }
    static size_t recv(PullSocket& sock, Message& msg) {
        auto res = sock.recv(msg, ucxq::recv_flags::none);
        return res ? *res : 0;
    }
    static constexpr const char* prefix() { return "UCXQ"; }
    static uint8_t local_fill() { return 0x7C; }
    static uint8_t remote_fill() { return 0x7D; }
};

// Transport traits for ZeroMQ (optional baseline)
struct ZmqTransport {
    using Context = zmq::context_t;
    using PushSocket = zmq::socket_t;
    using PullSocket = zmq::socket_t;
    using Message = zmq::message_t;

    static Context create_context() { return Context(1); }
    static PullSocket make_pull(Context& ctx) { return PullSocket(ctx, zmq::socket_type::pull); }
    static PushSocket make_push(Context& ctx) { return PushSocket(ctx, zmq::socket_type::push); }
    static void bind(PullSocket& sock, const std::string& endpoint) { sock.bind(endpoint); }
    static void connect(PushSocket& sock, const std::string& endpoint) { sock.connect(endpoint); }
    static void send(PushSocket& sock, const uint8_t* data, size_t len) { sock.send(zmq::buffer(data, len), zmq::send_flags::none); }
    static size_t recv(PullSocket& sock, Message& msg) {
        auto res = sock.recv(msg, zmq::recv_flags::none);
        return res ? msg.size() : 0;
    }
    static constexpr const char* prefix() { return "ZMQ"; }
    static uint8_t local_fill() { return 0x5A; }
    static uint8_t remote_fill() { return 0x5B; }
};

template <typename Transport>
void run_local_fanin(typename Transport::Context& ctx,
                     typename Transport::PullSocket& pull,
                     const std::string& endpoint,
                     size_t num_local_senders,
                     const std::vector<size_t>& sizes,
                     int rounds,
                     int repeats,
                     int warmup,
                     std::ofstream* csv) {
    if (num_local_senders == 0) {
        return;
    }

    std::atomic<size_t> size_index{0};
    std::atomic<bool> done{false};
    std::atomic<uint64_t> round_id{0};
    std::atomic<bool> start_local{false};
    std::atomic<size_t> started_local{0};
    std::vector<std::thread> workers;
    workers.reserve(num_local_senders);

    for (size_t i = 0; i < num_local_senders; ++i) {
        workers.emplace_back([&, endpoint] {
            auto push = Transport::make_push(ctx);
            Transport::connect(push, endpoint);
            while (!start_local.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            uint64_t seen = round_id.load(std::memory_order_acquire);
            started_local.fetch_add(1, std::memory_order_acq_rel);
            while (!done.load(std::memory_order_acquire)) {
                while (round_id.load(std::memory_order_acquire) == seen && !done.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                if (done.load(std::memory_order_acquire)) {
                    break;
                }
                seen = round_id.load(std::memory_order_acquire);
                size_t idx = size_index.load(std::memory_order_acquire);
                if (idx >= sizes.size()) {
                    break;
                }
                size_t msg_bytes = sizes[idx];
                std::vector<uint8_t> payload(msg_bytes, Transport::local_fill());
                for (int r = 0; r < rounds; ++r) {
                    Transport::send(push, payload.data(), payload.size());
                }
            }
        });
    }

    start_local.store(true, std::memory_order_release);
    while (started_local.load(std::memory_order_acquire) < num_local_senders) {
        std::this_thread::yield();
    }

    typename Transport::Message msg;
    std::string label = std::string(Transport::prefix()) + "_local";
    for (size_t si = 0; si < sizes.size(); ++si) {
        for (int rep = 1; rep <= repeats + warmup; ++rep) {
            size_index.store(si, std::memory_order_release);
            round_id.fetch_add(1, std::memory_order_acq_rel);
            size_t expected = num_local_senders * static_cast<size_t>(rounds);
            auto t0 = clock_type::now();
            size_t got = 0;
            while (got < expected) {
                size_t bytes = Transport::recv(pull, msg);
                if (bytes > 0) {
                    ++got;
                }
            }
            auto t1 = clock_type::now();
            double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();
            std::cout << Transport::prefix() << " local fan-in msg_bytes=" << sizes[si]
                      << " total_msgs=" << expected << " total_usec=" << usec << std::endl;
            if (csv && rep > warmup) {
                csv_write_row(*csv, sizes[si], label.c_str(), rep - warmup, usec);
            }
        }
    }
    done.store(true, std::memory_order_release);
    for (auto& th : workers) {
        th.join();
    }
}

template <typename Transport>
void run_remote_server(typename Transport::PullSocket& pull,
                       int ctrl_fd,
                       size_t num_remote_senders,
                       const std::vector<size_t>& sizes,
                       int rounds,
                       int repeats,
                       int warmup,
                       std::ofstream* csv) {
    if (num_remote_senders == 0) {
        return;
    }

    uint32_t ack = 0;
    if (!recv_u32(ctrl_fd, ack) || ack != kControlAck) {
        std::cerr << "Failed to receive remote ready ack" << std::endl;
        return;
    }

    typename Transport::Message msg;
    std::string label = std::string(Transport::prefix()) + "_remote";
    for (size_t si = 0; si < sizes.size(); ++si) {
        for (int rep = 1; rep <= repeats + warmup; ++rep) {
            if (!send_u32(ctrl_fd, static_cast<uint32_t>(si))) {
                std::cerr << "Failed to send control token" << std::endl;
                return;
            }
            size_t expected = num_remote_senders * static_cast<size_t>(rounds);
            auto t0 = clock_type::now();
            size_t got = 0;
            while (got < expected) {
                size_t bytes = Transport::recv(pull, msg);
                if (bytes > 0) {
                    ++got;
                }
            }
            auto t1 = clock_type::now();
            double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();
            std::cout << Transport::prefix() << " remote fan-in msg_bytes=" << sizes[si]
                      << " total_msgs=" << expected << " total_usec=" << usec << std::endl;
            if (csv && rep > warmup) {
                csv_write_row(*csv, sizes[si], label.c_str(), rep - warmup, usec);
            }
        }
    }

    send_u32(ctrl_fd, kControlTerminate);
}

template <typename Transport>
void run_remote_client(typename Transport::Context& ctx,
                       const std::string& endpoint,
                       int ctrl_fd,
                       size_t num_remote_senders,
                       const std::vector<size_t>& sizes,
                       int rounds) {
    if (num_remote_senders == 0) {
        return;
    }

    std::atomic<size_t> size_index{0};
    std::atomic<bool> done{false};
    std::atomic<uint64_t> round_id{0};
    std::atomic<bool> start_remote{false};
    std::atomic<size_t> started_remote{0};

    std::vector<std::thread> senders;
    senders.reserve(num_remote_senders);
    for (size_t i = 0; i < num_remote_senders; ++i) {
        senders.emplace_back([&, endpoint] {
            auto push = Transport::make_push(ctx);
            Transport::connect(push, endpoint);
            while (!start_remote.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            uint64_t seen = round_id.load(std::memory_order_acquire);
            started_remote.fetch_add(1, std::memory_order_acq_rel);
            std::vector<uint8_t> payload;
            while (!done.load(std::memory_order_acquire)) {
                while (round_id.load(std::memory_order_acquire) == seen && !done.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                if (done.load(std::memory_order_acquire)) {
                    break;
                }
                seen = round_id.load(std::memory_order_acquire);
                size_t idx = size_index.load(std::memory_order_acquire);
                if (idx >= sizes.size()) {
                    break;
                }
                size_t msg_bytes = sizes[idx];
                payload.assign(msg_bytes, Transport::remote_fill());
                for (int r = 0; r < rounds; ++r) {
                    Transport::send(push, payload.data(), payload.size());
                }
            }
        });
    }

    start_remote.store(true, std::memory_order_release);
    while (started_remote.load(std::memory_order_acquire) < num_remote_senders) {
        std::this_thread::yield();
    }

    send_u32(ctrl_fd, kControlAck);

    for (;;) {
        uint32_t token = 0;
        if (!recv_u32(ctrl_fd, token)) {
            done.store(true, std::memory_order_release);
            break;
        }
        if (token == kControlTerminate) {
            done.store(true, std::memory_order_release);
            break;
        }
        if (token >= sizes.size()) {
            std::cerr << "Received invalid control token " << token << std::endl;
            done.store(true, std::memory_order_release);
            break;
        }
        size_index.store(static_cast<size_t>(token), std::memory_order_release);
        round_id.fetch_add(1, std::memory_order_acq_rel);
    }

    for (auto& th : senders) {
        th.join();
    }
}

template <typename Transport>
void run_fanin_all(bool isServer,
                   const char* ip,
                   int port,
                   size_t num_local_senders,
                   size_t num_remote_senders,
                   const std::vector<size_t>& sizes,
                   int rounds,
                   std::ofstream* csv,
                   int repeats,
                   int warmup) {
    using Context = typename Transport::Context;
    Context ctx = Transport::create_context();
    std::string endpoint = std::string("tcp://") + ip + ":" + std::to_string(port);

    if (isServer) {
        auto pull = Transport::make_pull(ctx);
        Transport::bind(pull, endpoint);
        run_local_fanin<Transport>(ctx, pull, endpoint, num_local_senders, sizes, rounds, repeats, warmup, csv);
        if (num_remote_senders > 0) {
            int ctrl_fd = open_control_listener(ip, port + 1);
            if (ctrl_fd >= 0) {
                run_remote_server<Transport>(pull, ctrl_fd, num_remote_senders, sizes, rounds, repeats, warmup, csv);
                close(ctrl_fd);
            }
        }
    } else {
        if (num_remote_senders > 0) {
            int ctrl_fd = open_control_client(ip, port + 1);
            if (ctrl_fd >= 0) {
                run_remote_client<Transport>(ctx, endpoint, ctrl_fd, num_remote_senders, sizes, rounds);
                close(ctrl_fd);
            }
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: ucxq_example server|client" << std::endl;
        return 1;
    }
    bool isServer = std::string(argv[1]) == "server";
    if (!isServer && std::string(argv[1]) != "client") {
        std::cerr << "Usage: ucxq_example server|client" << std::endl;
        return 1;
    }

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

    run_fanin_all<UcxTransport>(isServer, ip, port, local_threads, remote_threads, sizes, rounds, isServer ? &csv : nullptr, repeats, warmup);
    // Uncomment to compare against ZeroMQ baseline using the same harness.
    // run_fanin_all<ZmqTransport>(isServer, ip, port + 100, local_threads, remote_threads, sizes, rounds, isServer ? &csv : nullptr, repeats, warmup);

    if (csv.is_open()) {
        csv.close();
    }
    std::cout << "Benchmarks completed." << std::endl;
    return 0;
}
