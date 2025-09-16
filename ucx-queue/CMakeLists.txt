cmake_minimum_required(VERSION 3.18)
project(ucx_queue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# UCX
find_path(UCX_INCLUDE_DIR ucp/api/ucp.h HINTS ${CMAKE_INSTALL_PREFIX}/include $ENV{CONDA_PREFIX}/include /usr/include)
find_library(UCX_UCP_LIBRARY ucp HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
find_library(UCX_UCS_LIBRARY ucs HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
if(NOT UCX_INCLUDE_DIR OR NOT UCX_UCP_LIBRARY OR NOT UCX_UCS_LIBRARY)
  message(FATAL_ERROR "Could not find UCX. Install 'ucx' (e.g., via conda-forge) or apt libucx-dev.")
endif()

# ZeroMQ
find_path(ZMQ_INCLUDE_DIR zmq.hpp HINTS ${CMAKE_INSTALL_PREFIX}/include $ENV{CONDA_PREFIX}/include /usr/include)
find_library(ZMQ_LIBRARY zmq HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  message(FATAL_ERROR "Could not find ZeroMQ. Install 'zeromq' and 'cppzmq' (e.g., via conda) or apt libzmq3-dev.")
endif()

add_library(ucx_queue_lib
  src/ucx_queue.cpp
)
target_include_directories(ucx_queue_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${UCX_INCLUDE_DIR})
target_link_libraries(ucx_queue_lib PUBLIC ${UCX_UCP_LIBRARY} ${UCX_UCS_LIBRARY} Threads::Threads)

add_executable(ucx_queue_test src/main.cpp)
target_include_directories(ucx_queue_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include ${UCX_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
target_link_libraries(ucx_queue_test PRIVATE ucx_queue_lib ${ZMQ_LIBRARY})

if(DEFINED ENV{CONDA_PREFIX})
  set_target_properties(ucx_queue_test PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
  set_target_properties(ucx_queue_lib PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
endif()



