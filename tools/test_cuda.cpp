#include <cuda.h>
#include <cuda_runtime.h>
#include <dlfcn.h>
#include <iostream>

int main() {
    // Runtime API sanity
    cudaError_t cr = cudaFree(0);
    std::cout << "cudaFree(0): " << (cr == cudaSuccess ? "OK" : cudaGetErrorString(cr)) << "\n";

    // Driver API via dynamic load
    void* h = dlopen("libcuda.so.1", RTLD_NOW);
    if (!h) {
        std::cerr << "dlopen libcuda.so.1 failed: " << dlerror() << "\n";
        return 2;
    }
    typedef CUresult (*PFN_cuInit)(unsigned int);
    auto pCuInit = reinterpret_cast<PFN_cuInit>(dlsym(h, "cuInit"));
    if (!pCuInit) {
        std::cerr << "dlsym cuInit failed\n";
        return 3;
    }
    CUresult dr = pCuInit(0);
    std::cout << "cuInit(0): " << dr << "\n";
    return 0;
}


