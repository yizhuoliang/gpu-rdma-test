#include <arpa/inet.h>
#include <cuda_runtime.h>
#include <nccl.h>
#include <zmq.hpp>

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <fstream>

#define SERVER_IP "10.10.2.1"
#define CLIENT_IP "10.10.2.2"
#define TEST_PORT 60061
#define SMALL_BYTES (1024ULL)
#define LARGE_BYTES (1024ULL * 1024ULL * 1024ULL)

using clock_type = std::chrono::high_resolution_clock;

static void throwOnCudaError(cudaError_t status, const char* msg) {
    if (status != cudaSuccess) {
        std::cerr << msg << ": " << cudaGetErrorString(status) << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

static void throwOnNcclError(ncclResult_t status, const char* msg) {
    if (status != ncclSuccess) {
        std::cerr << msg << ": " << ncclGetErrorString(status) << std::endl;
        std::exit(EXIT_FAILURE);
    }
}

// 1 & 3: CPU<->CPU latency using ZeroMQ REQ/REP roundtrip
double runZmqRoundtrip(size_t numBytes, bool isServer) {
    zmq::context_t ctx(1);
    zmq::socket_t sock(ctx, isServer ? ZMQ_REP : ZMQ_REQ);
    std::string ep = std::string("tcp://") + (isServer ? SERVER_IP : SERVER_IP) + ":" + std::to_string(TEST_PORT);
    if (isServer) {
        sock.bind(ep);
    } else {
        // small delay to let server bind
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        sock.connect(ep);
    }

    std::vector<uint8_t> payload(numBytes, 0x5A);
    zmq::message_t msg_recv;

    // Warmup (untimed) round
    if (!isServer) {
        sock.send(zmq::buffer(payload), zmq::send_flags::none);
        sock.recv(msg_recv, zmq::recv_flags::none);
    } else {
        sock.recv(msg_recv, zmq::recv_flags::none);
        sock.send(zmq::buffer(payload), zmq::send_flags::none);
    }

    auto t0 = clock_type::now();
    if (!isServer) {
        sock.send(zmq::buffer(payload), zmq::send_flags::none);
        sock.recv(msg_recv, zmq::recv_flags::none);
    } else {
        sock.recv(msg_recv, zmq::recv_flags::none);
        sock.send(zmq::buffer(payload), zmq::send_flags::none);
    }
    auto t1 = clock_type::now();
    return std::chrono::duration<double, std::micro>(t1 - t0).count();
}

// 2 & 4: CPU->GPU->IB->GPU->CPU using NCCL send/recv and TCP rendezvous
double runNcclCpuGpuRoundtrip(size_t numBytes, bool isServer) {
    int port = TEST_PORT + 1; // different port for rendezvous
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return -1.0; }
    if (isServer) {
        int yes = 1; setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        if (bind(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("bind"); return -1.0; }
        if (listen(sockfd, 1) != 0) { perror("listen"); return -1.0; }
        int conn = accept(sockfd, nullptr, nullptr);
        if (conn < 0) { perror("accept"); return -1.0; }
        close(sockfd); sockfd = conn;
    } else {
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        // Retry connect to tolerate server not yet listening between back-to-back tests
        const int maxAttempts = 50; // ~5s total @ 100ms
        int attempt = 0;
        while (connect(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) {
            if (++attempt >= maxAttempts) { perror("connect"); return -1.0; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    size_t numFloats = numBytes / sizeof(float);
    if (numFloats == 0) numFloats = 1;

    int device = 0; throwOnCudaError(cudaSetDevice(device), "cudaSetDevice");
    float* d_send = nullptr; float* d_recv = nullptr;
    throwOnCudaError(cudaMalloc(&d_send, numFloats * sizeof(float)), "cudaMalloc send");
    throwOnCudaError(cudaMalloc(&d_recv, numFloats * sizeof(float)), "cudaMalloc recv");

    std::vector<float> h(numFloats, 1.0f);
    throwOnCudaError(cudaMemcpy(d_send, h.data(), numFloats * sizeof(float), cudaMemcpyHostToDevice), "H2D");
    throwOnCudaError(cudaMemset(d_recv, 0, numFloats * sizeof(float)), "Memset recv");

    // NCCL setup
    setenv("NCCL_DEBUG", "WARN", 0);
    setenv("NCCL_SOCKET_IFNAME", "ibs3", 0);
    setenv("NCCL_IB_HCA", "mlx5", 0);
    setenv("NCCL_IB_DISABLE", "0", 0);

    ncclUniqueId id{};
    if (isServer) {
        throwOnNcclError(ncclGetUniqueId(&id), "ncclGetUniqueId");
        if (send(sockfd, &id, sizeof(id), 0) != (ssize_t)sizeof(id)) { perror("send id"); return -1.0; }
    } else {
        if (recv(sockfd, &id, sizeof(id), MSG_WAITALL) != (ssize_t)sizeof(id)) { perror("recv id"); return -1.0; }
    }

    int rank = isServer ? 0 : 1;
    ncclComm_t comm{};
    throwOnNcclError(ncclCommInitRank(&comm, 2, id, rank), "ncclCommInitRank");
    cudaStream_t stream; throwOnCudaError(cudaStreamCreate(&stream), "stream");

    // Warmup (untimed) round
    throwOnNcclError(ncclGroupStart(), "warmup group start");
    if (isServer) {
        throwOnNcclError(ncclSend(d_send, numFloats, ncclFloat, 1, comm, stream), "warmup send");
        throwOnNcclError(ncclRecv(d_recv, numFloats, ncclFloat, 1, comm, stream), "warmup recv");
    } else {
        throwOnNcclError(ncclRecv(d_recv, numFloats, ncclFloat, 0, comm, stream), "warmup recv");
        throwOnNcclError(ncclSend(d_send, numFloats, ncclFloat, 0, comm, stream), "warmup send");
    }
    throwOnNcclError(ncclGroupEnd(), "warmup group end");
    throwOnCudaError(cudaStreamSynchronize(stream), "warmup sync");

    // Timed section includes cudaMalloc, H2D before send, NCCL transfer, and D2H after recv
    float* d_send_t = nullptr; float* d_recv_t = nullptr;
    std::vector<float> h_send_t(numFloats, 2.0f);
    std::vector<float> h_recv_t(numFloats);

    auto t0 = clock_type::now();
    // Allocate device buffers (counted)
    throwOnCudaError(cudaMalloc(&d_send_t, numFloats * sizeof(float)), "cudaMalloc timed send");
    throwOnCudaError(cudaMalloc(&d_recv_t, numFloats * sizeof(float)), "cudaMalloc timed recv");

    // Host to device before network (counted)
    throwOnCudaError(cudaMemcpyAsync(d_send_t, h_send_t.data(), numFloats * sizeof(float), cudaMemcpyHostToDevice, stream), "H2D timed");

    if (isServer) {
        // Phase 1: server sends to client
        throwOnNcclError(ncclGroupStart(), "group start phase1");
        throwOnNcclError(ncclSend(d_send_t, numFloats, ncclFloat, 1, comm, stream), "send phase1");
        throwOnNcclError(ncclGroupEnd(), "group end phase1");

        // Phase 2: server receives from client
        throwOnNcclError(ncclGroupStart(), "group start phase2");
        throwOnNcclError(ncclRecv(d_recv_t, numFloats, ncclFloat, 1, comm, stream), "recv phase2");
        throwOnNcclError(ncclGroupEnd(), "group end phase2");

        // Device to host after network (counted)
        throwOnCudaError(cudaMemcpyAsync(h_recv_t.data(), d_recv_t, numFloats * sizeof(float), cudaMemcpyDeviceToHost, stream), "D2H timed");
    } else {
        // Phase 1: client receives from server
        throwOnNcclError(ncclGroupStart(), "group start phase1");
        throwOnNcclError(ncclRecv(d_recv_t, numFloats, ncclFloat, 0, comm, stream), "recv phase1");
        throwOnNcclError(ncclGroupEnd(), "group end phase1");

        // Bring to host, then echo back same payload and push to device
        throwOnCudaError(cudaMemcpyAsync(h_recv_t.data(), d_recv_t, numFloats * sizeof(float), cudaMemcpyDeviceToHost, stream), "D2H after recv");
        // Ensure no overlap between D2H and H2D on the client
        throwOnCudaError(cudaStreamSynchronize(stream), "sync after D2H before H2D");
        throwOnCudaError(cudaMemcpyAsync(d_send_t, h_recv_t.data(), numFloats * sizeof(float), cudaMemcpyHostToDevice, stream), "H2D before send");

        // Phase 2: client sends to server
        throwOnNcclError(ncclGroupStart(), "group start phase2");
        throwOnNcclError(ncclSend(d_send_t, numFloats, ncclFloat, 0, comm, stream), "send phase2");
        throwOnNcclError(ncclGroupEnd(), "group end phase2");
    }

    throwOnCudaError(cudaStreamSynchronize(stream), "sync timed");
    auto t1 = clock_type::now();

    double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();

    cudaFree(d_send_t); cudaFree(d_recv_t);

    cudaStreamDestroy(stream);
    ncclCommDestroy(comm);
    cudaFree(d_send); cudaFree(d_recv);
    close(sockfd);
    return usec;
}

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "Usage: latency_test server|client" << std::endl; return 1; }
    bool isServer = std::string(argv[1]) == "server";

    auto appendCsv = [&](const std::string& csvPath, size_t sizeBytes, const char* pattern, int roundIdx, double usec) {
        if (!isServer) return; // only server records
        bool exists = static_cast<bool>(std::ifstream(csvPath));
        std::ofstream ofs(csvPath, std::ios::app);
        if (!exists) {
            ofs << "size_bytes,pattern,round,latency_usec\n";
        }
        ofs << sizeBytes << "," << pattern << "," << roundIdx << "," << usec << "\n";
    };

    const std::string csvPath = "results.csv";

    // Sizes (bytes): 0.5KB, 1KB, 2KB, 4KB, 8KB, 16KB, 1GB
    const std::vector<size_t> sizes = {
        512ULL,
        1024ULL,
        2048ULL,
        4096ULL,
        8192ULL,
        16384ULL,
        1073741824ULL
    };

    for (size_t sz : sizes) {
        // ZMQ: 3 rounds
        for (int i = 1; i <= 3; ++i) {
            double us = runZmqRoundtrip(sz, isServer);
            if (isServer) std::cout << "ZMQ size=" << sz << "B round=" << i << " usec=" << us << std::endl;
            appendCsv(csvPath, sz, "ZMQ", i, us);
        }
        // NCCL: 3 rounds
        for (int i = 1; i <= 3; ++i) {
            double us = runNcclCpuGpuRoundtrip(sz, isServer);
            if (isServer) std::cout << "NCCL size=" << sz << "B round=" << i << " usec=" << us << std::endl;
            appendCsv(csvPath, sz, "NCCL", i, us);
        }
    }

    return 0;
}



