cmake_minimum_required(VERSION 3.18)
project(nccl_rdma_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(CUDAToolkit REQUIRED)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-Wno-deprecated-declarations" HAS_NODEP_WARN)
if(HAS_NODEP_WARN)
  add_compile_options(-Wno-deprecated-declarations)
endif()

find_path(NCCL_INCLUDE_DIR nccl.h
  HINTS ${NCCL_INCLUDE_DIR_HINT} ${CMAKE_INSTALL_PREFIX}/include /usr/include /usr/local/include /usr/include/x86_64-linux-gnu)
find_library(NCCL_LIBRARY nccl
  HINTS ${NCCL_LIBRARY_HINT} ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu)

if(NOT NCCL_INCLUDE_DIR OR NOT NCCL_LIBRARY)
  message(FATAL_ERROR "Could not find NCCL. Pass -DNCCL_INCLUDE_DIR=... -DNCCL_LIBRARY=... or install libnccl-dev.")
endif()

message(STATUS "Found NCCL include: ${NCCL_INCLUDE_DIR}")
message(STATUS "Found NCCL lib: ${NCCL_LIBRARY}")

add_executable(nccl_rdma_test src/main.cpp)
target_include_directories(nccl_rdma_test PRIVATE ${NCCL_INCLUDE_DIR} ${CUDAToolkit_INCLUDE_DIRS})
target_link_libraries(nccl_rdma_test PRIVATE ${NCCL_LIBRARY} CUDA::cudart)

# Prefer system or conda lib directory at runtime if provided
if(DEFINED ENV{CONDA_PREFIX})
  set_target_properties(nccl_rdma_test PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
else()
  set_target_properties(nccl_rdma_test PROPERTIES BUILD_RPATH "/usr/lib/x86_64-linux-gnu")
endif()


