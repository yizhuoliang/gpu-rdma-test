#include <arpa/inet.h>
#include <cuda_runtime.h>
#include <nccl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

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

int runServer(const std::string& bindIp, int port, size_t numFloats) {
    int serverFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (serverFd < 0) { perror("socket"); return 1; }

    int yes = 1;
    setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, bindIp.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "Invalid bind IP" << std::endl; return 1;
    }

    if (bind(serverFd, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("bind"); return 1; }
    if (listen(serverFd, 1) != 0) { perror("listen"); return 1; }
    std::cout << "Server listening on " << bindIp << ":" << port << std::endl;

    int conn = accept(serverFd, nullptr, nullptr);
    if (conn < 0) { perror("accept"); return 1; }
    close(serverFd);

    // Create NCCL unique ID and send to client
    ncclUniqueId id{};
    throwOnNcclError(ncclGetUniqueId(&id), "ncclGetUniqueId");
    if (::send(conn, &id, sizeof(id), 0) != (ssize_t)sizeof(id)) { perror("send nccl id"); return 1; }

    int myRank = 0;
    int worldSize = 2;
    int device = 0;
    throwOnCudaError(cudaSetDevice(device), "cudaSetDevice");

    ncclComm_t comm{};
    throwOnNcclError(ncclCommInitRank(&comm, worldSize, id, myRank), "ncclCommInitRank");

    size_t count = numFloats;
    float* sendBuf = nullptr;
    float* recvBuf = nullptr;
    throwOnCudaError(cudaMalloc(&sendBuf, count * sizeof(float)), "cudaMalloc send");
    throwOnCudaError(cudaMalloc(&recvBuf, count * sizeof(float)), "cudaMalloc recv");

    // Initialize send buffer on GPU
    std::vector<float> host(count);
    for (size_t i = 0; i < count; ++i) host[i] = static_cast<float>(i);
    throwOnCudaError(cudaMemcpy(sendBuf, host.data(), count * sizeof(float), cudaMemcpyHostToDevice), "cudaMemcpy H2D");
    throwOnCudaError(cudaMemset(recvBuf, 0, count * sizeof(float)), "cudaMemset recv");

    cudaStream_t stream;
    throwOnCudaError(cudaStreamCreate(&stream), "cudaStreamCreate");

    auto t0 = std::chrono::high_resolution_clock::now();
    throwOnNcclError(ncclGroupStart(), "ncclGroupStart");
    throwOnNcclError(ncclSend(sendBuf, (size_t)count, ncclFloat, 1, comm, stream), "ncclSend");
    throwOnNcclError(ncclRecv(recvBuf, (size_t)count, ncclFloat, 1, comm, stream), "ncclRecv");
    throwOnNcclError(ncclGroupEnd(), "ncclGroupEnd");
    throwOnCudaError(cudaStreamSynchronize(stream), "cudaStreamSynchronize");
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double gb = (count * sizeof(float)) / 1e9;
    std::cout << "Server: exchange completed, size=" << gb << " GB, time=" << ms << " ms, bw ~ " << (gb / (ms / 1000.0)) << " GB/s" << std::endl;

    // Validate recv buffer matches client's pattern (count - i)
    std::vector<float> recvHost(count, -1.0f);
    throwOnCudaError(cudaMemcpy(recvHost.data(), recvBuf, count * sizeof(float), cudaMemcpyDeviceToHost), "cudaMemcpy D2H");
    size_t mism = 0; for (size_t i = 0; i < count; ++i) if (recvHost[i] != static_cast<float>(count - i)) { mism = i + 1; break; }
    std::cout << (mism ? "Validation FAILED" : "Validation OK") << std::endl;

    ncclCommDestroy(comm);
    cudaStreamDestroy(stream);
    cudaFree(sendBuf);
    cudaFree(recvBuf);
    close(conn);
    return mism ? 2 : 0;
}

int runClient(const std::string& serverIp, int port, size_t numFloats) {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (::inet_pton(AF_INET, serverIp.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "Invalid server IP" << std::endl; return 1;
    }
    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("connect"); return 1; }

    ncclUniqueId id{};
    ssize_t got = ::recv(sock, &id, sizeof(id), MSG_WAITALL);
    if (got != (ssize_t)sizeof(id)) { perror("recv nccl id"); return 1; }

    int myRank = 1;
    int worldSize = 2;
    int device = 0;
    throwOnCudaError(cudaSetDevice(device), "cudaSetDevice");

    ncclComm_t comm{};
    throwOnNcclError(ncclCommInitRank(&comm, worldSize, id, myRank), "ncclCommInitRank");

    size_t count = numFloats;
    float* sendBuf = nullptr;
    float* recvBuf = nullptr;
    throwOnCudaError(cudaMalloc(&sendBuf, count * sizeof(float)), "cudaMalloc send");
    throwOnCudaError(cudaMalloc(&recvBuf, count * sizeof(float)), "cudaMalloc recv");

    // Initialize send buffer with a different pattern
    std::vector<float> host(count);
    for (size_t i = 0; i < count; ++i) host[i] = static_cast<float>(count - i);
    throwOnCudaError(cudaMemcpy(sendBuf, host.data(), count * sizeof(float), cudaMemcpyHostToDevice), "cudaMemcpy H2D");
    throwOnCudaError(cudaMemset(recvBuf, 0, count * sizeof(float)), "cudaMemset recv");

    cudaStream_t stream;
    throwOnCudaError(cudaStreamCreate(&stream), "cudaStreamCreate");

    auto t0 = std::chrono::high_resolution_clock::now();
    throwOnNcclError(ncclGroupStart(), "ncclGroupStart");
    throwOnNcclError(ncclRecv(recvBuf, (size_t)count, ncclFloat, 0, comm, stream), "ncclRecv");
    throwOnNcclError(ncclSend(sendBuf, (size_t)count, ncclFloat, 0, comm, stream), "ncclSend");
    throwOnNcclError(ncclGroupEnd(), "ncclGroupEnd");
    throwOnCudaError(cudaStreamSynchronize(stream), "cudaStreamSynchronize");
    auto t1 = std::chrono::high_resolution_clock::now();

    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    double gb = (count * sizeof(float)) / 1e9;
    std::cout << "Client: exchange completed, size=" << gb << " GB, time=" << ms << " ms, bw ~ " << (gb / (ms / 1000.0)) << " GB/s" << std::endl;

    // Validate
    std::vector<float> recvHost(count, -1.0f);
    throwOnCudaError(cudaMemcpy(recvHost.data(), recvBuf, count * sizeof(float), cudaMemcpyDeviceToHost), "cudaMemcpy D2H");
    size_t mism = 0; for (size_t i = 0; i < count; ++i) if (recvHost[i] != i) { mism = i + 1; break; }
    std::cout << (mism ? "Validation FAILED" : "Validation OK") << std::endl;

    ncclCommDestroy(comm);
    cudaStreamDestroy(stream);
    cudaFree(sendBuf);
    cudaFree(recvBuf);
    close(sock);
    return mism ? 2 : 0;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " server <bind_ip> [port] [num_floats]" << std::endl;
        std::cerr << "   or: " << argv[0] << " client <server_ip> [port] [num_floats]" << std::endl;
        return 1;
    }

    std::string mode = argv[1];
    int port = (argc >= 4) ? std::atoi(argv[3]) : 50051;
    size_t numFloats = (argc >= 5) ? static_cast<size_t>(std::atoll(argv[4])) : (256 * 1024 * 1024) / sizeof(float); // 256 MB default

    // NCCL tuning for IB/RoCE
    setenv("NCCL_DEBUG", "INFO", 0);
    setenv("NCCL_IB_HCA", "mlx5", 0);
    setenv("NCCL_NET_GDR_LEVEL", "PHB", 0);
    setenv("NCCL_SOCKET_IFNAME", "ibs3,ens1np0", 0);
    setenv("NCCL_IB_DISABLE", "0", 0);

    if (mode == "server") {
        if (argc < 3) { std::cerr << "server mode requires <bind_ip>" << std::endl; return 1; }
        std::string bindIp = argv[2];
        return runServer(bindIp, port, numFloats);
    } else if (mode == "client") {
        if (argc < 3) { std::cerr << "client mode requires <server_ip>" << std::endl; return 1; }
        std::string serverIp = argv[2];
        return runClient(serverIp, port, numFloats);
    } else {
        std::cerr << "Unknown mode: " << mode << std::endl; return 1;
    }
}


