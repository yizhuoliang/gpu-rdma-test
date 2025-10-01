#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cuda.h>
#include <cuda_runtime.h>
#include <nvshmem.h>
#include <nvshmemx.h>
#include <nccl.h>
#include <mpi.h>

#define ITERS 100

#define SERVER_IP "10.10.2.1"
#define BASE_PORT 60071

static int g_mype = -1;

#define CUDA_CHECK_MSG(call, msg) \
    do { \
        cudaError_t err__ = (call); \
        if (err__ != cudaSuccess) { \
            fprintf(stderr, "[PE %d] CUDA error during %s at %s:%d on call %s: %s\n", \
                    g_mype, msg, __FILE__, __LINE__, #call, cudaGetErrorString(err__)); \
            nvshmem_global_exit(1); \
        } \
    } while (0)

static inline void check_cuda_last_error(const char *api_name, const char *stage) {
    cudaError_t err = cudaGetLastError();
    if (err != cudaSuccess) {
        fprintf(stderr, "[PE %d] CUDA kernel error for %s during %s: %s\n",
                g_mype, api_name ? api_name : "(unspecified)", stage, cudaGetErrorString(err));
        nvshmem_global_exit(1);
    }
}

static inline void check_cuda_error(cudaError_t err, const char *api_name, const char *action) {
    if (err != cudaSuccess) {
        fprintf(stderr, "[PE %d] CUDA error for %s during %s: %s\n",
                g_mype, api_name ? api_name : "(unspecified)", action, cudaGetErrorString(err));
        nvshmem_global_exit(1);
    }
}

// ================= KERNELS =================

__global__ void send_block(void *dest, void *src, int size, uint64_t *sig, int peer) {
    int num_blocks = gridDim.x;
    int size_per_block = size / num_blocks;
    int x = blockIdx.x;
    int mype = nvshmem_my_pe();
    nvshmem_signal_wait_until(sig, NVSHMEM_CMP_EQ, 0);
    if (blockIdx.x == 0 && threadIdx.x == 0) {
        nvshmemx_signal_op(sig, 1, NVSHMEM_SIGNAL_SET, mype);
    }
    nvshmemx_putmem_signal_nbi_block(
        (char*)dest + x * size_per_block, (char*)src + x * size_per_block, size_per_block,
        &sig[mype], 1, NVSHMEM_SIGNAL_ADD, peer);
}

__global__ void wait_kernel(uint64_t *sig, int num_send_blocks) {
    int mype = nvshmem_my_pe();
    int npes = nvshmem_n_pes();
    if (threadIdx.x > 0 && threadIdx.x < npes) {
        nvshmem_signal_wait_until(&sig[threadIdx.x], NVSHMEM_CMP_EQ, num_send_blocks);
        nvshmemx_signal_op(&sig[threadIdx.x], 0, NVSHMEM_SIGNAL_SET, mype);
        nvshmemx_signal_op(sig, 0, NVSHMEM_SIGNAL_SET, threadIdx.x);
    }
}

class Timer {
    private:
        cudaEvent_t start_event, stop_event;
        cudaStream_t stream;
        float ms;
    public:
        explicit Timer(cudaStream_t s) : start_event(nullptr), stop_event(nullptr), stream(s), ms(0.0f) {
            CUDA_CHECK_MSG(cudaEventCreate(&start_event), "cudaEventCreate(start)");
            CUDA_CHECK_MSG(cudaEventCreate(&stop_event), "cudaEventCreate(stop)");
        }
        ~Timer() {
            cudaEventDestroy(start_event);
            cudaEventDestroy(stop_event);
        }
        void start() { CUDA_CHECK_MSG(cudaEventRecord(start_event, stream), "cudaEventRecord(start)"); }
        void stop() {
            CUDA_CHECK_MSG(cudaEventRecord(stop_event, stream), "cudaEventRecord(stop)");
            CUDA_CHECK_MSG(cudaEventSynchronize(stop_event), "cudaEventSynchronize(stop)");
            CUDA_CHECK_MSG(cudaEventElapsedTime(&ms, start_event, stop_event), "cudaEventElapsedTime");
        }
        float get_elapsed_time_ms() const { return ms; }
};

// ================= TCP helpers =================

static int tcp_listen_and_accept(int port) {
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return -1; }
    int yes = 1; setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    if (bind(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) { perror("bind"); close(sockfd); return -1; }
    if (listen(sockfd, 1) != 0) { perror("listen"); close(sockfd); return -1; }
    int conn = accept(sockfd, nullptr, nullptr);
    if (conn < 0) { perror("accept"); close(sockfd); return -1; }
    close(sockfd);
    return conn;
}

static int tcp_connect_retry(int port) {
    int sockfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return -1; }
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_port = htons(port);
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);
    const int maxAttempts = 50;
    int attempt = 0;
    while (connect(sockfd, (sockaddr*)&addr, sizeof(addr)) != 0) {
        if (++attempt >= maxAttempts) { perror("connect"); close(sockfd); return -1; }
        usleep(100000);
    }
    return sockfd;
}

// ================= Bench: NVSHMEM (cross-node, MPI bootstrap) =================

static void bench_nvshmem(int msg_size, int num_blocks) {
    int mype = nvshmem_my_pe();
    g_mype = mype;
    int npes = nvshmem_n_pes();
    if (npes != 2) {
        if (mype == 0) fprintf(stderr, "NVSHMEM test requires exactly 2 PEs\n");
        nvshmem_barrier_all();
        return;
    }

    int buffer_size = msg_size * npes;
    int sig_bytes = npes * (int)sizeof(uint64_t);
    void *buf = nvshmem_malloc(sig_bytes + buffer_size);
    uint64_t *sig = (uint64_t*)buf;
    void *data = (char*)buf + sig_bytes;
    if (!buf) {
        fprintf(stderr, "[PE %d] nvshmem_malloc failed\n", mype);
        nvshmem_global_exit(1);
    }
    CUDA_CHECK_MSG(cudaMemset((void*)sig, 0, sig_bytes), "cudaMemset(sig)");

    void *d_src = nullptr;
    CUDA_CHECK_MSG(cudaMalloc(&d_src, msg_size), "cudaMalloc(d_src)");
    if (mype == 1) {
        int *h_src = (int*)malloc(msg_size);
        for (size_t i = 0; i < (size_t)msg_size / sizeof(int); i++) h_src[i] = 1;
        CUDA_CHECK_MSG(cudaMemcpy(d_src, h_src, msg_size, cudaMemcpyHostToDevice), "H2D d_src");
        free(h_src);
    }

    cudaStream_t stream{}; CUDA_CHECK_MSG(cudaStreamCreate(&stream), "cudaStreamCreate");
    Timer timer(stream);

    if (mype == 1) {
        int num_threads = 256;
        void *dst_peer0 = (char*)data + mype * msg_size; // write to our slot on PE0's symmetric heap
        auto run_once = [&]() {
            send_block<<<num_blocks, num_threads, 0, stream>>>(dst_peer0, d_src, msg_size, sig, 0);
            check_cuda_last_error("send_block", "launch");
            check_cuda_error(cudaStreamSynchronize(stream), "send_block", "sync");
        };
        for (int i = 0; i < 10; i++) run_once();
        nvshmem_barrier_all();
        timer.start();
        for (int i = 0; i < ITERS; i++) run_once();
        timer.stop();
        printf("[PE %d] NVSHMEM send avg %.3f us per iter\n", mype, (timer.get_elapsed_time_ms() * 1000.0f) / ITERS);
    } else {
        auto run_once = [&]() {
            wait_kernel<<<1, 32, 0, stream>>>(sig, num_blocks);
            check_cuda_last_error("wait_kernel", "launch");
            check_cuda_error(cudaStreamSynchronize(stream), "wait_kernel", "sync");
        };
        for (int i = 0; i < 10; i++) run_once();
        nvshmem_barrier_all();
        timer.start();
        for (int i = 0; i < ITERS; i++) run_once();
        timer.stop();
        printf("[PE %d] NVSHMEM recv avg %.3f us per iter\n", mype, (timer.get_elapsed_time_ms() * 1000.0f) / ITERS);
    }

    cudaStreamDestroy(stream);
    cudaFree(d_src);
    nvshmem_free(buf);
}

// ================= Bench: NCCL (TCP rendezvous like latency-test) =================

static void bench_nccl(int msg_size, int mype) {
    int port = BASE_PORT + 1;
    int sockfd = -1;
    bool isServer = (mype == 0);
    if (isServer) sockfd = tcp_listen_and_accept(port);
    else sockfd = tcp_connect_retry(port);
    if (sockfd < 0) { fprintf(stderr, "[PE %d] TCP setup failed\n", mype); return; }

    setenv("NCCL_DEBUG", "WARN", 1);
    setenv("NCCL_SOCKET_IFNAME", "ibs3", 0);
    setenv("NCCL_IB_HCA", "mlx5", 0);
    setenv("NCCL_IB_DISABLE", "0", 0);

    int device = 0; CUDA_CHECK_MSG(cudaSetDevice(device), "cudaSetDevice(nccl)");

    size_t count_bytes = (size_t)msg_size;
    size_t numFloats = count_bytes / sizeof(float);
    if (numFloats == 0) numFloats = 1;
    float *d_send = nullptr, *d_recv = nullptr;
    CUDA_CHECK_MSG(cudaMalloc(&d_send, numFloats * sizeof(float)), "cudaMalloc d_send");
    CUDA_CHECK_MSG(cudaMalloc(&d_recv, numFloats * sizeof(float)), "cudaMalloc d_recv");
    CUDA_CHECK_MSG(cudaMemset(d_recv, 0, numFloats * sizeof(float)), "memset d_recv");

    ncclUniqueId id{};
    if (isServer) {
        ncclGetUniqueId(&id);
        if (send(sockfd, &id, sizeof(id), 0) != (ssize_t)sizeof(id)) { perror("send id"); close(sockfd); return; }
    } else {
        if (recv(sockfd, &id, sizeof(id), MSG_WAITALL) != (ssize_t)sizeof(id)) { perror("recv id"); close(sockfd); return; }
    }

    ncclComm_t comm{};
    ncclCommInitRank(&comm, 2, id, mype);
    cudaStream_t stream{}; CUDA_CHECK_MSG(cudaStreamCreate(&stream), "cudaStreamCreate");

    // Warmup
    ncclGroupStart();
    if (isServer) {
        ncclRecv(d_recv, numFloats, ncclFloat, 1, comm, stream);
        ncclSend(d_send, numFloats, ncclFloat, 1, comm, stream);
    } else {
        ncclSend(d_send, numFloats, ncclFloat, 0, comm, stream);
        ncclRecv(d_recv, numFloats, ncclFloat, 0, comm, stream);
    }
    ncclGroupEnd();
    CUDA_CHECK_MSG(cudaStreamSynchronize(stream), "warmup sync");

    Timer timer(stream);
    timer.start();
    for (int i = 0; i < ITERS; i++) {
        ncclGroupStart();
        if (isServer) {
            ncclRecv(d_recv, numFloats, ncclFloat, 1, comm, stream);
        } else {
            ncclSend(d_send, numFloats, ncclFloat, 0, comm, stream);
        }
        ncclGroupEnd();
        CUDA_CHECK_MSG(cudaStreamSynchronize(stream), "iter sync");
    }
    timer.stop();
    if (isServer) {
        printf("[PE %d] NCCL recv avg %.3f us per iter\n", mype, (timer.get_elapsed_time_ms() * 1000.0f) / ITERS);
    } else {
        printf("[PE %d] NCCL send avg %.3f us per iter\n", mype, (timer.get_elapsed_time_ms() * 1000.0f) / ITERS);
    }

    cudaStreamDestroy(stream);
    ncclCommDestroy(comm);
    cudaFree(d_send); cudaFree(d_recv);
    close(sockfd);
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int world_rank = 0, world_size = 1;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int local_rank = 0;
    {
        // Try to derive local rank from MPI, fallback to 0
        MPI_Comm shmcomm; MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &shmcomm);
        int shm_rank = 0; MPI_Comm_rank(shmcomm, &shm_rank);
        local_rank = shm_rank;
        MPI_Comm_free(&shmcomm);
    }

    int dev_count = 0; cudaGetDeviceCount(&dev_count);
    if (dev_count == 0) { fprintf(stderr, "No CUDA devices\n"); MPI_Abort(MPI_COMM_WORLD, 1); }
    CUDA_CHECK_MSG(cudaSetDevice(local_rank % dev_count), "cudaSetDevice(local_rank)");

    nvshmemx_init_attr_t attr{};
    attr.mpi_comm = MPI_COMM_WORLD;
    nvshmemx_init_attr(NVSHMEMX_INIT_WITH_MPI_COMM, &attr);

    int mype = nvshmem_my_pe();
    int npes = nvshmem_n_pes();
    g_mype = mype;
    if (npes != 2) {
        if (mype == 0) fprintf(stderr, "Please run with exactly 2 MPI ranks across two hosts.\n");
        nvshmem_finalize();
        MPI_Finalize();
        return 1;
    }

    const int M = 1024 * 1024;
    const int sizes[] = { 1 * M, 2 * M, 4 * M, 8 * M };

    for (int i = 0; i < (int)(sizeof(sizes)/sizeof(sizes[0])); i++) {
        int sz = sizes[i];
        if (sz >= M) {
            printf("msg_size %d MB\n", sz / M);
        } else {
            printf("msg_size %d KB\n", sz / 1024);
        }
        // NVSHMEM benchmark (cross-node)
        bench_nvshmem(sz, /*num_blocks*/8);
        // NCCL benchmark (TCP rendezvous)
        bench_nccl(sz, mype);
        if (mype == 0) printf("--------------------------------\n");
        nvshmem_barrier_all();
    }

    nvshmem_finalize();
    MPI_Finalize();
    return 0;
}


