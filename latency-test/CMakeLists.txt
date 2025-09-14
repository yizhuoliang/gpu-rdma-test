cmake_minimum_required(VERSION 3.18)
project(latency_test LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(CUDAToolkit REQUIRED)

include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-Wno-deprecated-declarations" HAS_NODEP_WARN)
if(HAS_NODEP_WARN)
  add_compile_options(-Wno-deprecated-declarations)
endif()

# NCCL
find_path(NCCL_INCLUDE_DIR nccl.h
  HINTS ${NCCL_INCLUDE_DIR_HINT} ${CMAKE_INSTALL_PREFIX}/include /usr/include /usr/local/include /usr/include/x86_64-linux-gnu)
find_library(NCCL_LIBRARY nccl
  HINTS ${NCCL_LIBRARY_HINT} ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu)
if(NOT NCCL_INCLUDE_DIR OR NOT NCCL_LIBRARY)
  message(FATAL_ERROR "Could not find NCCL. Pass -DNCCL_INCLUDE_DIR=... -DNCCL_LIBRARY=... or install libnccl-dev.")
endif()

# ZeroMQ
find_path(ZMQ_INCLUDE_DIR zmq.hpp HINTS ${CMAKE_INSTALL_PREFIX}/include $ENV{CONDA_PREFIX}/include /usr/include)
find_library(ZMQ_LIBRARY zmq HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  message(FATAL_ERROR "Could not find ZeroMQ. Install 'zeromq' and 'cppzmq' (e.g., via conda) or apt libzmq3-dev.")
endif()

# UCX
find_path(UCX_INCLUDE_DIR ucp/api/ucp.h HINTS ${CMAKE_INSTALL_PREFIX}/include $ENV{CONDA_PREFIX}/include /usr/include)
find_library(UCX_UCX_LIBRARY ucp HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
find_library(UCX_UCS_LIBRARY ucs HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
if(NOT UCX_INCLUDE_DIR OR NOT UCX_UCX_LIBRARY OR NOT UCX_UCS_LIBRARY)
  message(FATAL_ERROR "Could not find UCX. Install 'ucx' (e.g., via conda-forge) or apt libucx-dev.")
endif()

message(STATUS "Found NCCL include: ${NCCL_INCLUDE_DIR}")
message(STATUS "Found NCCL lib: ${NCCL_LIBRARY}")
message(STATUS "Found ZMQ include: ${ZMQ_INCLUDE_DIR}")
message(STATUS "Found ZMQ lib: ${ZMQ_LIBRARY}")

add_executable(latency_test src/main.cpp)
target_include_directories(latency_test PRIVATE ${NCCL_INCLUDE_DIR} ${CUDAToolkit_INCLUDE_DIRS} ${ZMQ_INCLUDE_DIR} ${UCX_INCLUDE_DIR})
target_link_libraries(latency_test PRIVATE ${NCCL_LIBRARY} ${ZMQ_LIBRARY} ${UCX_UCX_LIBRARY} ${UCX_UCS_LIBRARY} CUDA::cudart)

if(DEFINED ENV{CONDA_PREFIX})
  set_target_properties(latency_test PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
else()
  set_target_properties(latency_test PROPERTIES BUILD_RPATH "/usr/lib/x86_64-linux-gnu")
endif()



