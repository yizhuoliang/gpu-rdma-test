cmake_minimum_required(VERSION 3.18)
project(ucx_queue_zmq LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

# ZeroMQ (cppzmq header + libzmq)
find_path(ZMQ_INCLUDE_DIR zmq.hpp
  HINTS /usr/include /usr/local/include $ENV{CONDA_PREFIX}/include)
find_library(ZMQ_LIBRARY NAMES zmq libzmq
  HINTS /usr/lib /usr/local/lib /usr/lib/x86_64-linux-gnu $ENV{CONDA_PREFIX}/lib)
if(NOT ZMQ_INCLUDE_DIR OR NOT ZMQ_LIBRARY)
  message(FATAL_ERROR "Could not find ZeroMQ (requires zmq.hpp and libzmq). Install cppzmq headers and libzmq.")
endif()

# UCX
find_path(UCX_INCLUDE_DIR ucp/api/ucp.h HINTS ${CMAKE_INSTALL_PREFIX}/include $ENV{CONDA_PREFIX}/include /usr/include)
find_library(UCX_UCP_LIBRARY ucp HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
find_library(UCX_UCS_LIBRARY ucs HINTS ${CMAKE_INSTALL_PREFIX}/lib ${CMAKE_INSTALL_PREFIX}/lib64 $ENV{CONDA_PREFIX}/lib /usr/lib /usr/lib/x86_64-linux-gnu)
if(NOT UCX_INCLUDE_DIR OR NOT UCX_UCP_LIBRARY OR NOT UCX_UCS_LIBRARY)
  message(FATAL_ERROR "Could not find UCX. Install 'ucx' (e.g., via conda-forge) or apt libucx-dev.")
endif()

add_library(ucxq_lib
  src/ucxq.cpp
)
target_include_directories(ucxq_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${UCX_INCLUDE_DIR})
target_link_libraries(ucxq_lib PUBLIC ${UCX_UCP_LIBRARY} ${UCX_UCS_LIBRARY} Threads::Threads)

add_executable(ucxq_example src/example.cpp)
target_include_directories(ucxq_example PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include ${UCX_INCLUDE_DIR} ${ZMQ_INCLUDE_DIR})
target_link_libraries(ucxq_example PRIVATE ucxq_lib ${ZMQ_LIBRARY})

if(DEFINED ENV{CONDA_PREFIX})
  set_target_properties(ucxq_lib PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
  set_target_properties(ucxq_example PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
endif()

