// put_signal_warp_mpi_safe.cu
#include <cstdio>
#include <cstdlib>
#include <mpi.h>
#include <cuda_runtime.h>
#include <nvshmem.h>
#include <nvshmemx.h>

#define MSG_NINT 16

__global__ void send_kernel(int *dest, const int *src, uint64_t *sig, int peer) {
    if (threadIdx.x < warpSize) {
        nvshmemx_putmem_signal_nbi_warp(
            dest, src, MSG_NINT * sizeof(int),
            sig, 1, NVSHMEM_SIGNAL_SET, peer);
    }
    nvshmem_quiet();
}

__global__ void wait_kernel(uint64_t *sig, const int *buf) {
    if (threadIdx.x == 0) {
        nvshmem_signal_wait_until(sig, NVSHMEM_CMP_EQ, 1);
        printf("PE %d received:", nvshmem_my_pe());
        for (int i = 0; i < MSG_NINT; i++) printf(" %d", buf[i]);
        printf("\n");
    }
}

static int get_local_rank(MPI_Comm world) {
    MPI_Comm local;
    MPI_Comm_split_type(world, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &local);
    int r; MPI_Comm_rank(local, &r);
    MPI_Comm_free(&local);
    return r;
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    // 1) Set CUDA device per local rank BEFORE nvshmem init
    int dev_count = 0; cudaGetDeviceCount(&dev_count);
    int local_rank = get_local_rank(MPI_COMM_WORLD);
    if (dev_count == 0) { fprintf(stderr, "No CUDA devices\n"); MPI_Abort(MPI_COMM_WORLD, 1); }
    cudaSetDevice(local_rank % dev_count);

    // 2) Bootstrap NVSHMEM with MPI
    nvshmemx_init_attr_t attr;
    attr.mpi_comm = MPI_COMM_WORLD;
    nvshmemx_init_attr(NVSHMEMX_INIT_WITH_MPI_COMM, &attr);

    int mype = nvshmem_my_pe();
    int npes = nvshmem_n_pes();
    if (npes < 2) {
        if (mype == 0) fprintf(stderr, "Run with at least 2 PEs\n");
        nvshmem_finalize(); MPI_Finalize(); return 1;
    }
    int peer = (mype + 1) % npes;

    // 3) Symmetric allocations (check for NULL)
    int *buf = (int*) nvshmem_malloc(MSG_NINT * sizeof(int));
    uint64_t *sig = (uint64_t*) nvshmem_malloc(sizeof(uint64_t));
    if (!buf || !sig) {
        fprintf(stderr, "PE %d: nvshmem_malloc failed. Increase NVSHMEM_SYMMETRIC_SIZE.\n", mype);
        nvshmem_finalize(); MPI_Finalize(); return 1;
    }
    cudaMemset(sig, 0, sizeof(uint64_t));

    // Sync before using symmetric pointers/signals
    nvshmem_barrier_all();

    if (mype == 0) {
        int h_src[MSG_NINT];
        for (int i = 0; i < MSG_NINT; i++) h_src[i] = 100 + i;
        int *d_src = nullptr;
        cudaMalloc(&d_src, MSG_NINT * sizeof(int));
        cudaMemcpy(d_src, h_src, MSG_NINT * sizeof(int), cudaMemcpyHostToDevice);

        // Optional: collective launch to ensure all PEs have kernels live
        // nvshmemx_collective_launch((const void*)send_kernel, dim3(1), dim3(32), 0, 0, buf, d_src, sig, 1);
        send_kernel<<<1, 32>>>(buf, d_src, sig, 1);
        cudaDeviceSynchronize();
        cudaFree(d_src);
    } else if (mype == 1) {
        wait_kernel<<<1, 32>>>(sig, buf);
        cudaDeviceSynchronize();
    }

    nvshmem_free(buf);
    nvshmem_free(sig);
    nvshmem_finalize();
    MPI_Finalize();
    return 0;
}
