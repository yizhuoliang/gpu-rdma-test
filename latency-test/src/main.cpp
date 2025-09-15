#include <arpa/inet.h>
#include <cuda_runtime.h>
#include <nccl.h>
#include <zmq.hpp>
#include <ucp/api/ucp.h>
#include <ucs/type/status.h>

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

// 1 & 3: CPU<->CPU latency using ZeroMQ REQ/REP roundtrip (warmup + N rounds inside)
std::vector<double> runZmqRoundtrips(size_t numBytes, bool isServer, int rounds) {
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

    std::vector<double> results;
    results.reserve(rounds);
    for (int i = 0; i < rounds; ++i) {
        auto t0 = clock_type::now();
        if (!isServer) {
            sock.send(zmq::buffer(payload), zmq::send_flags::none);
            sock.recv(msg_recv, zmq::recv_flags::none);
        } else {
            sock.recv(msg_recv, zmq::recv_flags::none);
            sock.send(zmq::buffer(payload), zmq::send_flags::none);
        }
        auto t1 = clock_type::now();
        results.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }
    return results;
}

// 3: CPU<->CPU latency using UCX UCP TAG roundtrip over IB (address exchange via TCP)
static void ucxWait(ucp_worker_h worker, void* request) {
    if (request == nullptr) return;
    if (!UCS_PTR_IS_PTR(request)) {
        ucs_status_t status = (ucs_status_t)UCS_PTR_STATUS(request);
        if (status != UCS_OK) {
            std::cerr << "UCX request error: " << ucs_status_string(status) << std::endl;
            std::exit(EXIT_FAILURE);
        }
        return;
    }
    ucs_status_t status;
    do {
        ucp_worker_progress(worker);
        status = ucp_request_check_status(request);
    } while (status == UCS_INPROGRESS);
    if (status != UCS_OK) {
        std::cerr << "UCX request complete with error: " << ucs_status_string(status) << std::endl;
        std::exit(EXIT_FAILURE);
    }
    ucp_request_free(request);
}

std::vector<double> runUcxRoundtrips(size_t numBytes, bool isServer, int rounds) {
    const uint64_t TAG1 = 0x1111ULL;
    const uint64_t TAG2 = 0x2222ULL;

    // OOB TCP for address exchange
    int port = TEST_PORT + 2;
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return {}; }
    if (isServer) {
        int yes = 1; setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        if (bind(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("bind"); return {}; }
        if (listen(sockfd, 1) != 0) { perror("listen"); return {}; }
        int conn = accept(sockfd, nullptr, nullptr);
        if (conn < 0) { perror("accept"); return {}; }
        close(sockfd); sockfd = conn;
    } else {
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        const int maxAttempts = 50;
        int attempt = 0;
        while (connect(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) {
            if (++attempt >= maxAttempts) { perror("connect"); return {}; }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    // UCX init
    ucp_config_t* config = nullptr;
    if (ucp_config_read(nullptr, nullptr, &config) != UCS_OK) { std::cerr << "ucp_config_read failed" << std::endl; return {}; }
    ucp_params_t params{}; params.field_mask = UCP_PARAM_FIELD_FEATURES; params.features = UCP_FEATURE_TAG;
    ucp_context_h context{};
    if (ucp_init(&params, config, &context) != UCS_OK) { std::cerr << "ucp_init failed" << std::endl; return {}; }
    ucp_config_release(config);

    ucp_worker_params_t wparams{}; wparams.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE; wparams.thread_mode = UCS_THREAD_MODE_SINGLE;
    ucp_worker_h worker{};
    if (ucp_worker_create(context, &wparams, &worker) != UCS_OK) { std::cerr << "ucp_worker_create failed" << std::endl; return {}; }

    ucp_address_t* my_addr{}; size_t my_addr_len{};
    if (ucp_worker_get_address(worker, &my_addr, &my_addr_len) != UCS_OK) { std::cerr << "ucp_worker_get_address failed" << std::endl; return {}; }

    // Exchange addresses (two-phase: lengths then addresses)
    auto send_all = [&](const void* buf, size_t len) {
        const char* p = (const char*)buf; size_t sent = 0; while (sent < len) { ssize_t n = send(sockfd, p + sent, len - sent, 0); if (n <= 0) { perror("send"); std::exit(EXIT_FAILURE); } sent += (size_t)n; }
    };
    auto recv_all = [&](void* buf, size_t len) {
        char* p = (char*)buf; size_t recvd = 0; while (recvd < len) { ssize_t n = recv(sockfd, p + recvd, len - recvd, 0); if (n <= 0) { perror("recv"); std::exit(EXIT_FAILURE); } recvd += (size_t)n; }
    };

    uint32_t l_my = (uint32_t)my_addr_len; uint32_t l_peer = 0;
    if (isServer) {
        uint32_t netlen = htonl(l_my); send_all(&netlen, sizeof(netlen));
        recv_all(&l_peer, sizeof(l_peer)); l_peer = ntohl(l_peer);
    } else {
        recv_all(&l_peer, sizeof(l_peer)); l_peer = ntohl(l_peer);
        uint32_t netlen = htonl(l_my); send_all(&netlen, sizeof(netlen));
    }
    std::vector<uint8_t> peer_addr(l_peer);
    if (isServer) {
        // After exchanging lengths, send our address then receive peer's address
        send_all(my_addr, my_addr_len);
        recv_all(peer_addr.data(), peer_addr.size());
    } else {
        // Receive server address then send our address
        recv_all(peer_addr.data(), peer_addr.size());
        send_all(my_addr, my_addr_len);
    }

    // Create endpoint to peer
    ucp_ep_params_t epp{}; epp.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS; epp.address = (ucp_address_t*)peer_addr.data();
    ucp_ep_h ep{}; if (ucp_ep_create(worker, &epp, &ep) != UCS_OK) { std::cerr << "ucp_ep_create failed" << std::endl; return {}; }

    // Ensure endpoint is ready before first send
    ucp_request_param_t fparam{}; fparam.op_attr_mask = 0;
    void* freq = ucp_ep_flush_nbx(ep, &fparam);
    ucxWait(worker, freq);

    // Warmup
    std::vector<uint8_t> buf(numBytes, 0xAB);
    ucp_request_param_t sp{}; sp.op_attr_mask = 0;
    ucp_request_param_t rp{}; rp.op_attr_mask = 0;
    if (isServer) {
        void* sreq = ucp_tag_send_nbx(ep, buf.data(), buf.size(), TAG1, &sp);
        ucxWait(worker, sreq);
        void* rreq = ucp_tag_recv_nbx(worker, buf.data(), buf.size(), TAG2, (uint64_t)-1, &rp);
        ucxWait(worker, rreq);
    } else {
        void* rreq = ucp_tag_recv_nbx(worker, buf.data(), buf.size(), TAG1, (uint64_t)-1, &rp);
        ucxWait(worker, rreq);
        void* sreq = ucp_tag_send_nbx(ep, buf.data(), buf.size(), TAG2, &sp);
        ucxWait(worker, sreq);
    }

    std::vector<double> results; results.reserve(rounds);
    for (int i = 0; i < rounds; ++i) {
        auto t0 = clock_type::now();
        if (isServer) {
            void* sreq = ucp_tag_send_nbx(ep, buf.data(), buf.size(), TAG1, &sp);
            ucxWait(worker, sreq);
            void* rreq = ucp_tag_recv_nbx(worker, buf.data(), buf.size(), TAG2, (uint64_t)-1, &rp);
            ucxWait(worker, rreq);
        } else {
            void* rreq = ucp_tag_recv_nbx(worker, buf.data(), buf.size(), TAG1, (uint64_t)-1, &rp);
            ucxWait(worker, rreq);
            void* sreq = ucp_tag_send_nbx(ep, buf.data(), buf.size(), TAG2, &sp);
            ucxWait(worker, sreq);
        }
        auto t1 = clock_type::now();
        results.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());
    }

    // Cleanup
    ucp_ep_destroy(ep);
    ucp_worker_release_address(worker, my_addr);
    ucp_worker_destroy(worker);
    ucp_cleanup(context);
    close(sockfd);
    return results;
}

// 2 & 4: CPU->GPU->IB->GPU->CPU using NCCL send/recv and TCP rendezvous
std::vector<double> runNcclCpuGpuRoundtrips(size_t numBytes, bool isServer, bool noCopy, int rounds) {
    int port = TEST_PORT + 1; // different port for rendezvous
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return {}; }
    if (isServer) {
        int yes = 1; setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        if (bind(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("bind"); return {}; }
        if (listen(sockfd, 1) != 0) { perror("listen"); return {}; }
        int conn = accept(sockfd, nullptr, nullptr);
        if (conn < 0) { perror("accept"); return {}; }
        close(sockfd); sockfd = conn;
    } else {
        sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
        inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
        // Retry connect to tolerate server not yet listening between back-to-back tests
        const int maxAttempts = 50; // ~5s total @ 100ms
        int attempt = 0;
        while (connect(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) {
            if (++attempt >= maxAttempts) { perror("connect"); return {}; }
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
    if (!noCopy) {
        throwOnCudaError(cudaMemcpy(d_send, h.data(), numFloats * sizeof(float), cudaMemcpyHostToDevice), "H2D");
    }
    throwOnCudaError(cudaMemset(d_recv, 0, numFloats * sizeof(float)), "Memset recv");

    // NCCL setup
    setenv("NCCL_DEBUG", "WARN", 1);
    setenv("NCCL_SOCKET_IFNAME", "ibs3", 0);
    setenv("NCCL_IB_HCA", "mlx5", 0);
    setenv("NCCL_IB_DISABLE", "0", 0);

    ncclUniqueId id{};
    if (isServer) {
        throwOnNcclError(ncclGetUniqueId(&id), "ncclGetUniqueId");
        if (send(sockfd, &id, sizeof(id), 0) != (ssize_t)sizeof(id)) { perror("send id"); return {}; }
    } else {
        if (recv(sockfd, &id, sizeof(id), MSG_WAITALL) != (ssize_t)sizeof(id)) { perror("recv id"); return {}; }
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

    std::vector<double> results; results.reserve(rounds);
    for (int iter = 0; iter < rounds; ++iter) {
        // Timed section includes cudaMalloc, H2D before send, NCCL transfer, and D2H after recv
        float* d_send_t = nullptr; float* d_recv_t = nullptr;
        std::vector<float> h_send_t(numFloats, 2.0f);
        std::vector<float> h_recv_t(numFloats);

        auto t0 = clock_type::now();
        // Allocate device buffers (counted)
        throwOnCudaError(cudaMalloc(&d_send_t, numFloats * sizeof(float)), "cudaMalloc timed send");
        throwOnCudaError(cudaMalloc(&d_recv_t, numFloats * sizeof(float)), "cudaMalloc timed recv");

        // Host to device before network (counted)
        if (!noCopy) {
            throwOnCudaError(cudaMemcpyAsync(d_send_t, h_send_t.data(), numFloats * sizeof(float), cudaMemcpyHostToDevice, stream), "H2D timed");
        }

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
            if (!noCopy) {
                throwOnCudaError(cudaMemcpyAsync(h_recv_t.data(), d_recv_t, numFloats * sizeof(float), cudaMemcpyDeviceToHost, stream), "D2H timed");
            }
        } else {
            // Phase 1: client receives from server
            throwOnNcclError(ncclGroupStart(), "group start phase1");
            throwOnNcclError(ncclRecv(d_recv_t, numFloats, ncclFloat, 0, comm, stream), "recv phase1");
            throwOnNcclError(ncclGroupEnd(), "group end phase1");

            // Bring to host, then echo back same payload and push to device
            if (!noCopy) {
                throwOnCudaError(cudaMemcpyAsync(h_recv_t.data(), d_recv_t, numFloats * sizeof(float), cudaMemcpyDeviceToHost, stream), "D2H after recv");
                // Ensure no overlap between D2H and H2D on the client
                throwOnCudaError(cudaStreamSynchronize(stream), "sync after D2H before H2D");
                throwOnCudaError(cudaMemcpyAsync(d_send_t, h_recv_t.data(), numFloats * sizeof(float), cudaMemcpyHostToDevice, stream), "H2D before send");
            }

            // Phase 2: client sends to server
            throwOnNcclError(ncclGroupStart(), "group start phase2");
            throwOnNcclError(ncclSend(d_send_t, numFloats, ncclFloat, 0, comm, stream), "send phase2");
            throwOnNcclError(ncclGroupEnd(), "group end phase2");
        }

        throwOnCudaError(cudaStreamSynchronize(stream), "sync timed");
        auto t1 = clock_type::now();
        results.push_back(std::chrono::duration<double, std::micro>(t1 - t0).count());

        cudaFree(d_send_t); cudaFree(d_recv_t);
    }

    cudaStreamDestroy(stream);
    ncclCommDestroy(comm);
    cudaFree(d_send); cudaFree(d_recv);
    close(sockfd);
    return results;
}

int main(int argc, char** argv) {
    if (argc < 2) { std::cerr << "Usage: latency_test server|client [nocopy]" << std::endl; return 1; }
    bool isServer = std::string(argv[1]) == "server";
    bool noCopy = (argc >= 3 && std::string(argv[2]) == "nocopy");

    auto appendCsv = [&](const std::string& csvPath, size_t sizeBytes, const char* pattern, int roundIdx, double usec) {
        if (!isServer) return; // only server records
        std::ofstream ofs(csvPath, std::ios::app);
        ofs << sizeBytes << "," << pattern << "," << roundIdx << "," << usec << "\n";
    };

    const std::string csvPath = "results.csv";
    // Truncate CSV and write header at start of each run (server only)
    if (isServer) {
        std::ofstream ofs(csvPath, std::ios::trunc);
        ofs << "size_bytes,pattern,round,latency_usec\n";
    }

    // Sizes (bytes): 0.5KB, 1KB, 2KB, 4KB, 8KB, 16KB, 1GB
    const std::vector<size_t> sizes = {
        512ULL,
        1024ULL,
        2048ULL,
        4096ULL,
        8192ULL,
        16384ULL,
        262144ULL,
        1048576ULL,
        1073741824ULL
    };

    for (size_t sz : sizes) {
        // ZMQ: warmup + N inside
        auto zmq_results = runZmqRoundtrips(sz, isServer, 20);
        if (isServer) {
            for (size_t i = 0; i < zmq_results.size(); ++i) {
                std::cout << "ZMQ size=" << sz << "B round=" << (i+1) << " usec=" << zmq_results[i] << std::endl;
                appendCsv(csvPath, sz, "ZMQ", (int)(i+1), zmq_results[i]);
            }
        }
        // NCCL: warmup + N inside
        auto nccl_results = runNcclCpuGpuRoundtrips(sz, isServer, noCopy, 20);
        if (isServer) {
            for (size_t i = 0; i < nccl_results.size(); ++i) {
                std::cout << "NCCL size=" << sz << "B round=" << (i+1) << " usec=" << nccl_results[i] << std::endl;
                appendCsv(csvPath, sz, "NCCL", (int)(i+1), nccl_results[i]);
            }
        }
        // UCX: warmup + N inside
        auto ucx_results = runUcxRoundtrips(sz, isServer, 20);
        if (isServer) {
            for (size_t i = 0; i < ucx_results.size(); ++i) {
                std::cout << "UCX size=" << sz << "B round=" << (i+1) << " usec=" << ucx_results[i] << std::endl;
                appendCsv(csvPath, sz, "UCX", (int)(i+1), ucx_results[i]);
            }
        }
    }

    return 0;
}



