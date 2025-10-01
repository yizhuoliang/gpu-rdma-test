cmake_minimum_required(VERSION 3.18)

set(CMAKE_CUDA_COMPILER nvcc)

project(put LANGUAGES CUDA CXX)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0")
set(CMAKE_CUDA_FLAGS "${CMAKE_CUDA_FLAGS} -O0")

# CUDA setup
set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_EXTENSIONS OFF)
# Required for device-side NVSHMEM calls (separable compilation)
set(CMAKE_CUDA_SEPARABLE_COMPILATION ON)

set(CMAKE_CUDA_ARCHITECTURES 80)

# Find CUDA toolkit (for cudart)
find_package(CUDAToolkit REQUIRED)

# Locate NVSHMEM
# Try common environment variables first
set(NCCL_ROOT "$ENV{NCCL_HOME}" CACHE PATH "Path to NCCL installation (root)")
if(NOT NCCL_ROOT AND DEFINED ENV{NCCL_DIR})
	set(NCCL_ROOT "$ENV{NCCL_DIR}" CACHE PATH "Path to NCCL installation (root)" FORCE)
endif()

set(NCCL_INCLUDE_DIR "${NCCL_ROOT}/include")
set(NCCL_LIB_DIR "${NCCL_ROOT}/lib")

find_library(NCCL_LIB
	NAMES nccl
	PATHS ${NCCL_LIB_DIR}
	REQUIRED)

find_library(NCCL_INCLUDE_DIR
	NAMES nccl
	PATHS ${NCCL_INCLUDE_DIR}
	REQUIRED)

set(NVSHMEM_ROOT "$ENV{NVSHMEM_HOME}" CACHE PATH "Path to NVSHMEM installation (root)")
if(NOT NVSHMEM_ROOT AND DEFINED ENV{NVSHMEM_DIR})
	set(NVSHMEM_ROOT "$ENV{NVSHMEM_DIR}" CACHE PATH "Path to NVSHMEM installation (root)" FORCE)
endif()

# Fallback to common install prefix if env vars are not set
if(NOT NVSHMEM_ROOT)
	set(NVSHMEM_ROOT "/usr/local/nvshmem" CACHE PATH "Path to NVSHMEM installation (root)" FORCE)
endif()

# Include and library directories
set(NVSHMEM_INCLUDE_DIR "${NVSHMEM_ROOT}/include")
set(NVSHMEM_LIB_DIR "${NVSHMEM_ROOT}/lib")
set(NVSHMEM_LIB64_DIR "${NVSHMEM_ROOT}/lib64")

# Find NVSHMEM libraries
find_library(NVSHMEM_LIB
	NAMES nvshmem
	PATHS ${NVSHMEM_LIB_DIR} ${NVSHMEM_LIB64_DIR}
	REQUIRED)
find_library(NVSHMEM_HOST_LIB
	NAMES nvshmem_host
	PATHS ${NVSHMEM_LIB_DIR} ${NVSHMEM_LIB64_DIR}
	REQUIRED)

# Find OpenMPI (C++ imported target is widely available)
find_package(MPI REQUIRED COMPONENTS CXX)

# Target: NVSHMEM example
add_executable(put put.cu)

# Includes and links
target_include_directories(put PRIVATE ${NVSHMEM_INCLUDE_DIR})
target_include_directories(put PRIVATE ${NCCL_INCLUDE_DIR})
# Prefer imported CUDA target for cudart
if(TARGET CUDA::cudart)
	target_link_libraries(put PRIVATE CUDA::cudart)
endif()

# Link NVSHMEM and OpenMPI (CXX target works for CUDA C++)
target_link_libraries(put PRIVATE ${NVSHMEM_LIB} ${NVSHMEM_DEVICE_LIB} ${NVSHMEM_HOST_LIB} MPI::MPI_CXX ${NCCL_LIB})

# RPATH so the binary can find NVSHMEM at runtime
set_target_properties(put PROPERTIES
	BUILD_RPATH "${NVSHMEM_LIB_DIR};${NVSHMEM_LIB64_DIR}"
)

# Enable device linking flags explicitly (safety)
if (CMAKE_CUDA_COMPILER_ID STREQUAL "NVIDIA")
	# Ensure RDC is enabled; CMake flag above should handle this
	target_compile_options(put PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
endif()

# Simple MPI hello world target
add_executable(put_nvshmem put_nvshmem.cu)
set_target_properties(put_nvshmem PROPERTIES CXX_STANDARD 11 CXX_STANDARD_REQUIRED ON)
target_include_directories(put_nvshmem PRIVATE ${NVSHMEM_INCLUDE_DIR} ${NCCL_INCLUDE_DIR})
if(TARGET CUDA::cudart)
	target_link_libraries(put_nvshmem PRIVATE CUDA::cudart)
endif()
target_link_libraries(put_nvshmem PRIVATE ${NVSHMEM_LIB} ${NVSHMEM_HOST_LIB} MPI::MPI_CXX ${NCCL_LIB})
set_target_properties(put_nvshmem PROPERTIES
	BUILD_RPATH "${NVSHMEM_LIB_DIR};${NVSHMEM_LIB64_DIR}"
)
if (CMAKE_CUDA_COMPILER_ID STREQUAL "NVIDIA")
	target_compile_options(put_nvshmem PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
endif()

# Helpful output
message(STATUS "Using NVSHMEM_ROOT: ${NVSHMEM_ROOT}")
message(STATUS "NVSHMEM include: ${NVSHMEM_INCLUDE_DIR}")
message(STATUS "NVSHMEM libs: ${NVSHMEM_LIB}; ${NVSHMEM_HOST_LIB}")
message(STATUS "CUDA version: ${CUDAToolkit_VERSION}")
message(STATUS "CUDA architectures: ${CMAKE_CUDA_ARCHITECTURES}")
message(STATUS "MPI CXX libraries: ${MPI_CXX_LIBRARIES}")
message(STATUS "NCCL include: ${NCCL_INCLUDE_DIR}")
message(STATUS "NCCL libs: ${NCCL_LIB}")

# Cross-node NVSHMEM vs NCCL benchmark
add_executable(cross_node_ib cross_node_ib.cu)
set_target_properties(cross_node_ib PROPERTIES CXX_STANDARD 11 CXX_STANDARD_REQUIRED ON)
target_include_directories(cross_node_ib PRIVATE ${NVSHMEM_INCLUDE_DIR} ${NCCL_INCLUDE_DIR})
if(TARGET CUDA::cudart)
    target_link_libraries(cross_node_ib PRIVATE CUDA::cudart)
endif()
target_link_libraries(cross_node_ib PRIVATE ${NVSHMEM_LIB} ${NVSHMEM_HOST_LIB} MPI::MPI_CXX ${NCCL_LIB})
set_target_properties(cross_node_ib PROPERTIES
    BUILD_RPATH "${NVSHMEM_LIB_DIR};${NVSHMEM_LIB64_DIR}"
)
if (CMAKE_CUDA_COMPILER_ID STREQUAL "NVIDIA")
    target_compile_options(cross_node_ib PRIVATE $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
endif()

# Usage notes:
#   module load openmpi/5.0.5
#   mkdir -p build && cd build
#   cmake -DCMAKE_BUILD_TYPE=Release ..
#   cmake --build . -j
# Run with:
#   mpirun -np 2 ./hello_mpi
#   mpirun -np 2 ./put
