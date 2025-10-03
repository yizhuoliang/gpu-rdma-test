cmake_minimum_required(VERSION 3.18)
project(ucx_push_pull LANGUAGES CXX)

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

add_library(ucxpp_lib
  src/ucxq.cpp
)
target_include_directories(ucxpp_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include ${UCX_INCLUDE_DIR})
target_link_libraries(ucxpp_lib PUBLIC ${UCX_UCP_LIBRARY} ${UCX_UCS_LIBRARY} Threads::Threads)

if(DEFINED ENV{CONDA_PREFIX})
  set_target_properties(ucxpp_lib PROPERTIES BUILD_RPATH "$ENV{CONDA_PREFIX}/lib")
endif()
