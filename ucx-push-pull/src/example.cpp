#include "ucxq.hpp"
#include <zmq.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fstream>
#include <iostream>
#include <iomanip>
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

enum class Semantics { SenderBind_ReceiverConnect, SenderConnect_ReceiverBind };

static inline const char* semantics_str(Semantics s) {
    return s == Semantics::SenderBind_ReceiverConnect ? "sb_rc" : "sc_rb";
}

constexpr uint32_t kControlAck = 0xA5A5A5A5u;
constexpr uint32_t kControlRegister = 0xC0DEC0DEu;
constexpr uint32_t kControlTerminate = 0xFFFFFFFFu;
constexpr int kSenderPortOffset = 2;

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

bool send_bytes(int fd, const void* data, size_t len) {
    const uint8_t* ptr = static_cast<const uint8_t*>(data);
    size_t sent = 0;
    while (sent < len) {
        ssize_t rc = ::send(fd, ptr + sent, len - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<size_t>(rc);
    }
    return true;
}

bool recv_bytes(int fd, void* data, size_t len) {
    uint8_t* ptr = static_cast<uint8_t*>(data);
    size_t received = 0;
    while (received < len) {
        ssize_t rc = ::recv(fd, ptr + received, len - received, 0);
        if (rc <= 0) {
            return false;
        }
        received += static_cast<size_t>(rc);
    }
    return true;
}

std::string make_endpoint(const std::string& ip, int port) {
    return std::string("tcp://") + ip + ":" + std::to_string(port);
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
    // Bind on all interfaces to avoid NIC/IP mismatches
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
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
    constexpr int kMaxAttempts = 5000; // allow ample time for server to reach control listener
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

// Start control listener immediately (non-blocking accept). Returns listening fd.
int start_control_listener_any(int port) {
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
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
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
    std::cout << "Control listening on 0.0.0.0:" << port << std::endl;
    return lfd;
}

int accept_control(int lfd) {
    int cfd = ::accept(lfd, nullptr, nullptr);
    if (cfd < 0) {
        std::cerr << "accept() failed: " << std::strerror(errno) << std::endl;
    }
    close(lfd);
    return cfd;
}

// Transport traits for UCXQ
struct UcxTransport {
    struct Context {};
    using PushSocket = ucxq::socket_t;
    using PullSocket = ucxq::socket_t;
    using Message = ucxq::message_t;

    static Context create_context() { return Context{}; }
    static PullSocket make_pull(Context& /*ctx*/) { return PullSocket(ucxq::socket_type::pull); }
    static PushSocket make_push(Context& /*ctx*/) { return PushSocket(ucxq::socket_type::push); }
    static void receiver_connect(PullSocket& sock, const std::string& endpoint) { sock.connect(endpoint); }
    static void receiver_bind(PullSocket& sock, const std::string& endpoint) { sock.bind(endpoint); }
    static void sender_bind(PushSocket& sock, const std::string& endpoint) { sock.bind(endpoint); }
    static void sender_connect(PushSocket& sock, const std::string& endpoint) { sock.connect(endpoint); }
    static void send(PushSocket& sock, const uint8_t* data, size_t len) { sock.send(ucxq::buffer(data, len)); }
    static size_t recv(PullSocket& sock, Message& msg) {
        auto res = sock.recv(msg);
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
    static void receiver_connect(PullSocket& sock, const std::string& endpoint) { sock.connect(endpoint); }
    static void sender_bind(PushSocket& sock, const std::string& endpoint) { sock.bind(endpoint); }
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
                     const std::string& bind_ip,
                     int base_port,
                     size_t num_local_senders,
                     const std::vector<size_t>& sizes,
                     int rounds,
                     int repeats,
                     int warmup,
                     std::ofstream* csv,
                     Semantics semantics) {
    if (num_local_senders == 0) {
        return;
    }

    const int first_sender_port = base_port + kSenderPortOffset;
    std::vector<std::string> sender_endpoints;
    std::string receiver_endpoint;
    if (semantics == Semantics::SenderBind_ReceiverConnect) {
        sender_endpoints.reserve(num_local_senders);
        for (size_t i = 0; i < num_local_senders; ++i) {
            int listen_port = first_sender_port + static_cast<int>(i);
            sender_endpoints.push_back(make_endpoint(bind_ip, listen_port));
        }
    } else {
        // Receiver binds one endpoint; all local senders connect.
        receiver_endpoint = make_endpoint(bind_ip, base_port);
    }

    std::atomic<size_t> size_index{0};
    std::atomic<bool> done{false};
    std::atomic<uint64_t> round_id{0};
    std::atomic<bool> start_local{false};
    std::atomic<size_t> started_local{0};
    std::vector<std::thread> workers;
    workers.reserve(num_local_senders);

    for (size_t i = 0; i < num_local_senders; ++i) {
        std::string ep = (semantics == Semantics::SenderBind_ReceiverConnect) ? sender_endpoints[i] : receiver_endpoint;
        workers.emplace_back([&, ep] {
            auto push = Transport::make_push(ctx);
            if (semantics == Semantics::SenderBind_ReceiverConnect) {
                Transport::sender_bind(push, ep);
            } else {
                Transport::sender_connect(push, ep);
            }
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

    if (semantics == Semantics::SenderBind_ReceiverConnect) {
        for (const auto& ep : sender_endpoints) {
            Transport::receiver_connect(pull, ep);
        }
    } else {
        Transport::receiver_bind(pull, receiver_endpoint);
    }

    start_local.store(true, std::memory_order_release);
    while (started_local.load(std::memory_order_acquire) < num_local_senders) {
        std::this_thread::yield();
    }

    typename Transport::Message msg;
    std::string label = std::string(Transport::prefix()) + "_local_" + semantics_str(semantics);
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
                       std::ofstream* csv,
                       Semantics semantics,
                       const std::string& server_ip,
                       int base_port) {
    if (num_remote_senders == 0) {
        return;
    }

    size_t active_remote_senders = num_remote_senders;
    if (semantics == Semantics::SenderBind_ReceiverConnect) {
        uint32_t token = 0;
        if (!recv_u32(ctrl_fd, token)) {
            std::cerr << "Failed to receive control preamble" << std::endl;
            return;
        }
        std::vector<std::string> remote_endpoints;
        uint32_t ack = 0;
        if (token == kControlRegister) {
            uint32_t count = 0;
            if (!recv_u32(ctrl_fd, count)) {
                std::cerr << "Failed to receive remote sender count" << std::endl;
                return;
            }
            if (count != num_remote_senders) {
                std::cerr << "Remote sender count mismatch: expected " << num_remote_senders
                          << " but got " << count << std::endl;
            }
            remote_endpoints.resize(count);
            if (!remote_endpoints.empty()) {
                active_remote_senders = remote_endpoints.size();
            }
        for (uint32_t i = 0; i < count; ++i) {
            uint32_t len = 0;
            if (!recv_u32(ctrl_fd, len)) {
                std::cerr << "Failed to receive endpoint length" << std::endl;
                return;
            }
            std::string endpoint(len, '\0');
            if (!recv_bytes(ctrl_fd, endpoint.data(), len)) {
                std::cerr << "Failed to receive endpoint string" << std::endl;
                return;
            }
            remote_endpoints[i] = std::move(endpoint);
            std::cout << "[server] received endpoint: " << endpoint << std::endl;
        }
        for (const auto& ep : remote_endpoints) {
            std::cout << "[server] connecting to: " << ep << std::endl;
            Transport::receiver_connect(pull, ep);
        }
            if (!recv_u32(ctrl_fd, ack)) {
                std::cerr << "Failed to receive remote ready ack" << std::endl;
                return;
            }
        } else {
            ack = token;
        }
        if (ack != kControlAck) {
            std::cerr << "Unexpected control token " << std::hex << ack << std::dec << std::endl;
            return;
        }
    } else {
        // Receiver bind, sender connect: server tells client the endpoint to connect to.
        const std::string ep = make_endpoint(server_ip, base_port);
        Transport::receiver_bind(pull, ep);
        if (!send_u32(ctrl_fd, kControlRegister)) {
            std::cerr << "Failed to send control register" << std::endl;
            return;
        }
        uint32_t len = static_cast<uint32_t>(ep.size());
        if (!send_u32(ctrl_fd, len) || !send_bytes(ctrl_fd, ep.data(), ep.size())) {
            std::cerr << "Failed to send receiver endpoint" << std::endl;
            return;
        }
        uint32_t ack = 0;
        if (!recv_u32(ctrl_fd, ack) || ack != kControlAck) {
            std::cerr << "Failed to receive ack for receiver endpoint" << std::endl;
            return;
        }
    }

    typename Transport::Message msg;
    std::string label = std::string(Transport::prefix()) + "_remote_" + semantics_str(semantics);
    for (size_t si = 0; si < sizes.size(); ++si) {
        for (int rep = 1; rep <= repeats + warmup; ++rep) {
            if (!send_u32(ctrl_fd, static_cast<uint32_t>(si))) {
                std::cerr << "Failed to send control token" << std::endl;
                return;
            }
            size_t expected = active_remote_senders * static_cast<size_t>(rounds);
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
                       const std::string& listen_ip,
                       int base_port,
                       int ctrl_fd,
                       size_t num_remote_senders,
                       const std::vector<size_t>& sizes,
                       int rounds,
                       Semantics semantics) {
    if (num_remote_senders == 0) {
        return;
    }

    std::atomic<size_t> size_index{0};
    std::atomic<bool> done{false};
    std::atomic<uint64_t> round_id{0};
    std::atomic<bool> start_remote{false};
    std::atomic<size_t> started_remote{0};

    std::vector<std::thread> senders;

    if (semantics == Semantics::SenderBind_ReceiverConnect) {
        const int first_sender_port = base_port + kSenderPortOffset;
        std::vector<std::string> sender_endpoints;
        sender_endpoints.reserve(num_remote_senders);
        for (size_t i = 0; i < num_remote_senders; ++i) {
            int listen_port = first_sender_port + static_cast<int>(i);
            sender_endpoints.push_back(make_endpoint(listen_ip, listen_port));
        }

        senders.reserve(num_remote_senders);
        for (size_t i = 0; i < num_remote_senders; ++i) {
            std::string sender_endpoint = sender_endpoints[i];
            senders.emplace_back([&, sender_endpoint] {
                auto push = Transport::make_push(ctx);
                Transport::sender_bind(push, sender_endpoint);
                started_remote.fetch_add(1, std::memory_order_acq_rel);
                while (!start_remote.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                uint64_t seen = round_id.load(std::memory_order_acquire);
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

        // Wait for all senders to bind before sending endpoints to server
        while (started_remote.load(std::memory_order_acquire) < num_remote_senders) {
            std::this_thread::yield();
        }

        if (!send_u32(ctrl_fd, kControlRegister)) {
            std::cerr << "Failed to send control register opcode" << std::endl;
            done.store(true, std::memory_order_release);
        } else if (!send_u32(ctrl_fd, static_cast<uint32_t>(num_remote_senders))) {
            std::cerr << "Failed to send remote sender count" << std::endl;
            done.store(true, std::memory_order_release);
        } else {
        for (const auto& ep : sender_endpoints) {
            std::cout << "[client] sending endpoint: " << ep << std::endl;
            uint32_t len = static_cast<uint32_t>(ep.size());
            if (!send_u32(ctrl_fd, len) || !send_bytes(ctrl_fd, ep.data(), ep.size())) {
                std::cerr << "Failed to send remote endpoint" << std::endl;
                done.store(true, std::memory_order_release);
                break;
            }
        }
        }

        if (done.load(std::memory_order_acquire)) {
            for (auto& th : senders) {
                th.join();
            }
            return;
        }

        // Signal senders to start sending
        start_remote.store(true, std::memory_order_release);
        send_u32(ctrl_fd, kControlAck);
    } else {
        // Wait for server to send receiver endpoint, then connect senders.
        uint32_t token = 0;
        if (!recv_u32(ctrl_fd, token) || token != kControlRegister) {
            std::cerr << "Failed to receive control register from server" << std::endl;
            return;
        }
        uint32_t len = 0;
        if (!recv_u32(ctrl_fd, len)) {
            std::cerr << "Failed to receive receiver endpoint length" << std::endl;
            return;
        }
        std::string receiver_ep(len, '\0');
        if (!recv_bytes(ctrl_fd, receiver_ep.data(), len)) {
            std::cerr << "Failed to receive receiver endpoint" << std::endl;
            return;
        }

        senders.reserve(num_remote_senders);
        for (size_t i = 0; i < num_remote_senders; ++i) {
            std::string ep = receiver_ep;
            senders.emplace_back([&, ep] {
                auto push = Transport::make_push(ctx);
                Transport::sender_connect(push, ep);
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

        // Immediately ack so server can begin sending size tokens.
        send_u32(ctrl_fd, kControlAck);
    }

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
                   const std::string& server_ip,
                   const std::string& listen_ip,
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

    if (isServer) {
        // Start control listener on 62001 immediately so client can connect anytime
        int lfd_sb_rc = -1;
        if (num_remote_senders > 0) {
            lfd_sb_rc = start_control_listener_any(port + 1);
            std::cout << "[server] sb_rc control listener started on :" << (port + 1) << std::endl;
        }

        // Run local fan-in for both semantics
        {
            auto pull = Transport::make_pull(ctx);
            run_local_fanin<Transport>(ctx, pull, listen_ip, port, num_local_senders, sizes, rounds, repeats, warmup, csv, Semantics::SenderBind_ReceiverConnect);
        }
        {
            auto pull = Transport::make_pull(ctx);
            run_local_fanin<Transport>(ctx, pull, listen_ip, port, num_local_senders, sizes, rounds, repeats, warmup, csv, Semantics::SenderConnect_ReceiverBind);
        }
        // Run remote fan-in for both semantics
        if (num_remote_senders > 0) {
            // Accept sb_rc
            std::cout << "[server] waiting for sb_rc control accept..." << std::endl;
            int cfd_sb_rc = accept_control(lfd_sb_rc);
            if (cfd_sb_rc >= 0) {
                std::cout << "[server] sb_rc control accepted" << std::endl;
                auto pull = Transport::make_pull(ctx);
                run_remote_server<Transport>(pull, cfd_sb_rc, num_remote_senders, sizes, rounds, repeats, warmup, csv, Semantics::SenderBind_ReceiverConnect, server_ip, port);
                close(cfd_sb_rc);
            }
            // Listen and accept sc_rb
            int lfd_sc_rb = start_control_listener_any(port + 1);
            std::cout << "[server] sc_rb control listener started on :" << (port + 1) << std::endl;
            std::cout << "[server] waiting for sc_rb control accept..." << std::endl;
            int cfd_sc_rb = accept_control(lfd_sc_rb);
            if (cfd_sc_rb >= 0) {
                std::cout << "[server] sc_rb control accepted" << std::endl;
                auto pull = Transport::make_pull(ctx);
                run_remote_server<Transport>(pull, cfd_sc_rb, num_remote_senders, sizes, rounds, repeats, warmup, csv, Semantics::SenderConnect_ReceiverBind, server_ip, port);
                close(cfd_sc_rb);
            }
        }
    } else {
        if (num_remote_senders > 0) {
            // Establish sb_rc first
            std::cout << "[client] connecting sb_rc control to " << server_ip << ":" << (port + 1) << std::endl;
            int ctrl_fd_sb_rc = open_control_client(server_ip.c_str(), port + 1);
            if (ctrl_fd_sb_rc >= 0) {
                std::cout << "[client] sb_rc control connected" << std::endl;
                run_remote_client<Transport>(ctx, listen_ip, port, ctrl_fd_sb_rc, num_remote_senders, sizes, rounds, Semantics::SenderBind_ReceiverConnect);
                close(ctrl_fd_sb_rc);
            }
            // Then sc_rb
            std::cout << "[client] connecting sc_rb control to " << server_ip << ":" << (port + 1) << std::endl;
            int ctrl_fd_sc_rb = open_control_client(server_ip.c_str(), port + 1);
            if (ctrl_fd_sc_rb >= 0) {
                std::cout << "[client] sc_rb control connected" << std::endl;
                run_remote_client<Transport>(ctx, listen_ip, port, ctrl_fd_sc_rb, num_remote_senders, sizes, rounds, Semantics::SenderConnect_ReceiverBind);
                close(ctrl_fd_sc_rb);
            }
        }
    }
}

} // namespace

int main(int argc, char** argv) {
    auto print_usage = [] {
        std::cerr << "Usage:\n"
                  << "  ucxq_example server\n"
                  << "  ucxq_example client\n"
                  << "\nFixed config: server=10.10.2.1, client=10.10.2.2, port=62000, threads=16/16\n";
    };

    if (argc < 2) {
        print_usage();
        return 1;
    }

    bool isServer = std::string(argv[1]) == "server";
    bool isClient = std::string(argv[1]) == "client";
    if (!isServer && !isClient) {
        print_usage();
        return 1;
    }

    const int port = 62000;
    const size_t remote_threads = 16;
    const std::string server_ip = "10.10.2.1";
    const std::string client_ip = "10.10.2.2";
    const std::string advertise_ip = isServer ? server_ip : client_ip;
    const size_t local_threads = isServer ? 16 : 0; // client hosts only remote senders

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

    run_fanin_all<UcxTransport>(isServer, server_ip, advertise_ip, port, local_threads, remote_threads, sizes, rounds, isServer ? &csv : nullptr, repeats, warmup);
    // Uncomment to compare against ZeroMQ baseline using the same harness.
    // run_fanin_all<ZmqTransport>(isServer, server_ip, advertise_ip, port + 100, local_threads, remote_threads, sizes, rounds, isServer ? &csv : nullptr, repeats, warmup);

    if (csv.is_open()) {
        csv.close();
    }
    std::cout << "Benchmarks completed." << std::endl;
    return 0;
}
