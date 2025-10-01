#include <stdio.h>
#include <stdlib.h>
#include <cuda.h>
#include <nvshmem.h>
#include <nvshmemx.h>
#include <nccl.h>

#define MSG_SIZE (4 << 20)  // 4MB (in ints = MSG_SIZE/sizeof(int))
#define ITERS 100

#define CUDA_CHECK_MSG(call, msg) \
    do { \
        cudaError_t err__ = (call); \
        if (err__ != cudaSuccess) { \
            fprintf(stderr, "[PE %d] CUDA error during %s at %s:%d on call %s: %s\n", \
                    g_mype, msg, __FILE__, __LINE__, #call, cudaGetErrorString(err__)); \
            nvshmem_global_exit(1); \
        } \
    } while (0)

static int g_mype = -1;

static inline const char *api_name_from_index(int api) {
    switch (api) {
        case 0: return "putmem_signal_block";
        case 1: return "putmem_signal_warp";
        default: return "unknown_api";
    }
}

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

// Block-granular NVSHMEM send:
// - The grid partitions 'size' bytes evenly; block x sends its slice.
// - A one-time handshake per iteration uses sig[0]:
//   all threads wait until sig[0] == 0 (receiver reset), then one thread sets it to 1
//   to mark the iteration as in-progress.
// - Each block issues a nonblocking put of its slice to 'peer' and, after the data
//   is visible remotely, atomically increments the receiver-side counter sig[mype]
//   by 1. The receiver waits until it observes 'num_blocks' to know all blocks
//   from this PE have completed.
//   dest: symmetric destination base on 'peer'
//   src: local source base on this PE
//   size: total bytes to send (across all blocks)
//   sig: symmetric signal array used for start/reset (index 0) and per-PE completion
//   peer: target PE (rank) to receive the data
__global__ void send_block(void *dest, void *src, int size, uint64_t *sig, int peer) {
    int num_blocks = gridDim.x;
    // Compute this block's byte range
    int size_per_block = size / num_blocks;
    int x = blockIdx.x;

    int mype = nvshmem_my_pe();

    // Wait for receiver to reset start signal for the next iteration
    nvshmem_signal_wait_until(sig, NVSHMEM_CMP_EQ, 0);
    if (blockIdx.x == 0 && threadIdx.x == 0) {
        // Mark this iteration as in-progress once per grid on this PE
        nvshmemx_signal_op(sig, 1, NVSHMEM_SIGNAL_SET, mype);
    }

    // Block-scoped nonblocking put of this slice + remote completion signal increment
    nvshmemx_putmem_signal_nbi_block(
        dest + x * size_per_block, src + x * size_per_block, size_per_block,
        &sig[mype], 1, NVSHMEM_SIGNAL_ADD, peer);

    // nvshmem_quiet();
}

__device__ int ceil_div(int a, int b) {
    return (a + b - 1) / b;
}

__global__ void send_warp(void *dest, void *src, int size, int hidden_size, uint64_t *sig, int peer) {
    constexpr int threads_per_warp = 32;
    int num_blocks = gridDim.x;
    int num_warps = blockDim.x / threads_per_warp;
    int size_per_block = size / num_blocks;
    int tokens_per_block = size_per_block / hidden_size;
    int size_per_warp = size_per_block / num_warps;
    int warp_id = threadIdx.x / threads_per_warp;
    int block_base = blockIdx.x * size_per_block;

    dest += block_base;
    src += block_base;

    int mype = nvshmem_my_pe();

    nvshmem_signal_wait_until(sig, NVSHMEM_CMP_EQ, 0);
    if (blockIdx.x == 0 && threadIdx.x == 0) { // only the first thread in the first block sets the signal
        nvshmemx_signal_op(sig, 1, NVSHMEM_SIGNAL_SET, mype);
    }
    for (int p = warp_id; p < tokens_per_block; p += num_warps) {
        nvshmemx_putmem_nbi_warp(dest + p * hidden_size, src + p * hidden_size, hidden_size, peer);
    }

    if (threadIdx.x == 0) {
        nvshmemx_signal_op(&sig[mype], 1, NVSHMEM_SIGNAL_ADD, peer);
    }
    // nvshmemx_putmem_signal_nbi_warp(
    //     dest + offset, src + offset, size_per_warp,
    //     sig + blockIdx.x, 1, NVSHMEM_SIGNAL_SET, peer);

    // nvshmem_quiet();
}


__global__ void wait_kernel(uint64_t *sig, int num_send_blocks) {
    int mype = nvshmem_my_pe();
    int npes = nvshmem_n_pes();
    if (threadIdx.x > 0 && threadIdx.x < npes) {
        nvshmem_signal_wait_until(&sig[threadIdx.x], NVSHMEM_CMP_EQ, num_send_blocks);
        // copy the data out
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
        Timer(cudaStream_t stream) : stream(stream), ms(0.0f) {
            CUDA_CHECK_MSG(cudaEventCreate(&start_event), "cudaEventCreate(start)");
            CUDA_CHECK_MSG(cudaEventCreate(&stop_event), "cudaEventCreate(stop)");
        }
        ~Timer() {
            CUDA_CHECK_MSG(cudaEventDestroy(start_event), "cudaEventDestroy(start)");
            CUDA_CHECK_MSG(cudaEventDestroy(stop_event), "cudaEventDestroy(stop)");
        }
        void start() {
            CUDA_CHECK_MSG(cudaEventRecord(start_event, stream), "cudaEventRecord(start)");
        }
        void stop() {
            CUDA_CHECK_MSG(cudaEventRecord(stop_event, stream), "cudaEventRecord(stop)");
            CUDA_CHECK_MSG(cudaEventSynchronize(stop_event), "cudaEventSynchronize(stop)");
            CUDA_CHECK_MSG(cudaEventElapsedTime(&ms, start_event, stop_event), "cudaEventElapsedTime");
        }
        float get_elapsed_time() {
            return ms;
        }
};

void benchmark_nvshmem(int msg_size, int num_blocks, int num_warps, int hidden_size) {
    int mype = nvshmem_my_pe();
    g_mype = mype;
    int npes = nvshmem_n_pes();
    CUDA_CHECK_MSG(cudaSetDevice(mype), "cudaSetDevice");

    if (npes < 2) {
        if (mype == 0) fprintf(stderr, "Need at least 2 PEs\n");
        nvshmem_finalize();
        return;
    }


    int buffer_size = msg_size * npes;

    int sig_size = npes * sizeof(uint64_t);

    void *buf = (void *)nvshmem_malloc(sig_size + buffer_size);
    uint64_t *sig = (uint64_t *)buf;
    void *data = buf + sig_size;
    if (!buf || !sig) {
        fprintf(stderr, "[PE %d] Failed to allocate nvshmem buffers\n", mype);
        nvshmem_global_exit(1);
    }
    CUDA_CHECK_MSG(cudaMemset((void*)sig, 0, sig_size), "cudaMemset(sig)");

    // Source buffer on device
    void *d_src;
    CUDA_CHECK_MSG(cudaMalloc(&d_src, msg_size), "cudaMalloc(d_src)");
    if (mype > 0) {
        int *h_src = (int *)malloc(msg_size);
        for (size_t i = 0; i < msg_size / sizeof(int); i++) h_src[i] = mype;
        CUDA_CHECK_MSG(cudaMemcpy(d_src, h_src, msg_size, cudaMemcpyHostToDevice), "cudaMemcpy HtoD (d_src)");
        free(h_src);
    }

    cudaStream_t stream;
    CUDA_CHECK_MSG(cudaStreamCreate(&stream), "cudaStreamCreate");


    int warmup_iters = 10;

    Timer timer(stream);

    if (mype > 0) {

        int num_threads = 32 * num_warps;
        void *recv_buffer_addr = data + mype * msg_size;

        // Loop through APIs
        for (int api = 0; api < 2; api++) {
            nvshmem_barrier_all();

            const char *api_name = api_name_from_index(api);

            auto run_once = [&]() {
                switch (api) {
                    case 0: send_block<<<num_blocks, num_threads, 0, stream>>>(recv_buffer_addr, d_src, msg_size, sig, 0); break;
                    case 1: send_warp<<<num_blocks, num_threads, 0, stream>>>(recv_buffer_addr, d_src, msg_size, hidden_size, sig, 0); break;
                }
                check_cuda_last_error(api_name, "kernel launch");
                check_cuda_error(cudaStreamSynchronize(stream), api_name, "cudaStreamSynchronize (post send)");
            };

            for (int i = 0; i < warmup_iters; i++) {
                run_once();
            }

            nvshmem_barrier_all();

            timer.start();
            for (int i = 0; i < ITERS; i++) {
                run_once();
            }
            timer.stop();
            float ms = timer.get_elapsed_time();

            const char *name = (api == 0 ? "putmem_signal_block" :
                                api == 1 ? "putmem_signal_warp" :
                                            "unknown_api");
            printf("[PE %d] (num_blocks: %d, num_warps: %d) send: %s avg %.3f us per iter\n",
                    mype, num_blocks, num_warps, name, (ms * 1000) / ITERS);
        }

    } else {
        for (int api = 0; api < 2; api++) {
            nvshmem_barrier_all();

            const char *api_name = api_name_from_index(api);
            auto run_once = [&]() {
                wait_kernel<<<1, 32, 0, stream>>>(sig, num_blocks);
                check_cuda_last_error("wait_kernel", "kernel launch");
                check_cuda_error(cudaStreamSynchronize(stream), api_name, "cudaStreamSynchronize (post wait_kernel)");
            };

            for (int i = 0; i < warmup_iters; i++) {
                run_once();
            }

            nvshmem_barrier_all();

            timer.start();
            for (int i = 0; i < ITERS; i++) {
                run_once();
            }
            timer.stop();
            float ms = timer.get_elapsed_time();

            const char *name = (api == 0 ? "putmem_signal_block" :
                                api == 1 ? "putmem_signal_warp" :
                                            "unknown_api");
            printf("[PE %d] (num_blocks: %d, num_warps: %d) recv: %s avg %.3f us per iter\n", mype, num_blocks, num_warps, name, (ms * 1000) / ITERS);
        }
    }

    CUDA_CHECK_MSG(cudaStreamDestroy(stream), "cudaStreamDestroy");
    CUDA_CHECK_MSG(cudaFree(d_src), "cudaFree(d_src)");
    nvshmem_free(buf);
}

// ================= HOST CODE =================

void benchmark_nccl(int msg_size) {


    int mype = nvshmem_my_pe();
    g_mype = mype;
    int npes = nvshmem_n_pes();
    int peer = (mype + 1) % npes;
    // get nvshmem team
    CUDA_CHECK_MSG(cudaSetDevice(mype), "cudaSetDevice");

    ncclUniqueId id;
    ncclComm_t comm = NULL;

    if (mype == 0) {
        ncclGetUniqueId(&id);
        printf("PE %d: size of ncclUniqueId = %d\n", mype, sizeof(ncclUniqueId));
    }

    void *id_buf = (void*) nvshmem_malloc(sizeof(ncclUniqueId));
    CUDA_CHECK_MSG(cudaMemcpy(id_buf, &id, sizeof(ncclUniqueId), cudaMemcpyHostToDevice), "cudaMemcpy HtoD (id_buf)");


    nvshmem_int32_broadcast(NVSHMEM_TEAM_WORLD, (int*)id_buf, (int*)id_buf, sizeof(ncclUniqueId)/sizeof(int), 0);
    nvshmem_barrier_all();
    CUDA_CHECK_MSG(cudaMemcpy(&id, id_buf, sizeof(ncclUniqueId), cudaMemcpyDeviceToHost), "cudaMemcpy DtoH (id)");

    ncclCommInitRank(&comm, npes, id, mype);

    void *buf;
    cudaMalloc(&buf, msg_size);

    if (mype == 0) {
        int *h_src = (int *)malloc(msg_size);
        for (size_t i = 0; i < msg_size/sizeof(int); i++) h_src[i] = i;
        cudaMemcpy(buf, h_src, msg_size, cudaMemcpyHostToDevice);
        free(h_src);
    }

    cudaStream_t stream;
    cudaStreamCreate(&stream);

    int warmup_iters = 10;

    // add benchmark code here
    Timer timer(stream);

    if (mype > 0) {

        for (int i = 0; i < warmup_iters; i++) {
            ncclSend(buf, msg_size, ncclUint8, 0, comm, stream);
        }

        nvshmem_barrier_all();

        timer.start();
        for (int i = 0; i < ITERS; i++) {
            ncclSend(buf, msg_size, ncclUint8, 0, comm, stream);
        }

        timer.stop();
        float ms = timer.get_elapsed_time();
        printf("[PE %d] ncclSend avg %.3f us per iter\n", mype, (ms * 1000) / ITERS);
    } else {

        for (int i = 0; i < warmup_iters; i++) {
            for (int j = 1; j < npes; j++) {
                ncclRecv(buf, msg_size, ncclUint8, j, comm, stream);
            }
        }

        nvshmem_barrier_all();

        timer.start();
        for (int i = 0; i < ITERS; i++) {
            for (int j = 1; j < npes; j++) {
                ncclRecv(buf, msg_size, ncclUint8, j, comm, stream);
            }
        }
        timer.stop();
        float ms = timer.get_elapsed_time();
        printf("[PE %d] ncclRecv avg %.3f us per iter\n", mype, (ms * 1000) / ITERS);
    }

    cudaStreamDestroy(stream);
    cudaFree(buf);
    ncclCommDestroy(comm);
}


int main(int argc, char **argv) {
    nvshmem_init();

    int M = 1024 * 1024;
    int K = 1024;

    // parse num_blocks and num_warps from argv
    int num_blocks = 8;
    int num_warps = 8;
    if (argc > 1) {
        num_blocks = atoi(argv[1]);
    }
    if (argc > 2) {
        num_warps = atoi(argv[2]);
    }

    auto msg_sizes = {1 * M, 2 * M, 4 * M, 8 * M}; 
    int hidden_size = 4096;

    for (auto msg_size : msg_sizes) {

        if (msg_size >= M) {
            printf("msg_size %d MB\n", msg_size / M);
        } else {
            printf("msg_size %d KB\n", msg_size / K);
        }
        
        benchmark_nvshmem(msg_size, num_blocks, num_warps, hidden_size);

        benchmark_nccl(msg_size);

        printf("--------------------------------\n");
    }

    nvshmem_finalize();

    return 0;
}
