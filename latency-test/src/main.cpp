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
    zmq::message_t msg_send(payload.data(), payload.size());
    zmq::message_t msg_recv;

    auto t0 = clock_type::now();
    if (!isServer) {
        sock.send(msg_send, zmq::send_flags::none);
        sock.recv(msg_recv, zmq::recv_flags::none);
    } else {
        sock.recv(msg_recv, zmq::recv_flags::none);
        sock.send(msg_send, zmq::send_flags::none);
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
        if (connect(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("connect"); return -1.0; }
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

    auto t0 = clock_type::now();
    throwOnNcclError(ncclGroupStart(), "group start");
    if (isServer) {
        throwOnNcclError(ncclSend(d_send, numFloats, ncclFloat, 1, comm, stream), "send");
        throwOnNcclError(ncclRecv(d_recv, numFloats, ncclFloat, 1, comm, stream), "recv");
    } else {
        throwOnNcclError(ncclRecv(d_recv, numFloats, ncclFloat, 0, comm, stream), "recv");
        throwOnNcclError(ncclSend(d_send, numFloats, ncclFloat, 0, comm, stream), "send");
    }
    throwOnNcclError(ncclGroupEnd(), "group end");
    throwOnCudaError(cudaStreamSynchronize(stream), "sync");
    auto t1 = clock_type::now();

    double usec = std::chrono::duration<double, std::micro>(t1 - t0).count();

    std::vector<float> back(numFloats);
    throwOnCudaError(cudaMemcpy(back.data(), d_recv, numFloats * sizeof(float), cudaMemcpyDeviceToHost), "D2H");

    cudaStreamDestroy(stream);
    ncclCommDestroy(comm);
    cudaFree(d_send); cudaFree(d_recv);
    close(sockfd);
    return usec;
}

int main(int argc, char** argv) {
    if (argc != 2) { std::cerr << "Usage: latency_test server|client" << std::endl; return 1; }
    bool isServer = std::string(argv[1]) == "server";

    // ZMQ small
    double zmq_small = runZmqRoundtrip(SMALL_BYTES, isServer);
    if (isServer) std::cout << "ZMQ 1KB RT usec: " << zmq_small << std::endl;

    // ZMQ large
    double zmq_large = runZmqRoundtrip(LARGE_BYTES, isServer);
    if (isServer) std::cout << "ZMQ 1GB RT usec: " << zmq_large << std::endl;

    // NCCL small via GPU path
    double nccl_small = runNcclCpuGpuRoundtrip(SMALL_BYTES, isServer);
    if (isServer) std::cout << "NCCL CPU->GPU->IB->GPU->CPU 1KB RT usec: " << nccl_small << std::endl;

    // NCCL large via GPU path
    double nccl_large = runNcclCpuGpuRoundtrip(LARGE_BYTES, isServer);
    if (isServer) std::cout << "NCCL CPU->GPU->IB->GPU->CPU 1GB RT usec: " << nccl_large << std::endl;

    return 0;
}



