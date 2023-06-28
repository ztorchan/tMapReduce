cmake_minimum_required(VERSION 3.16.0)
project(word_count)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

find_package(cpr REQUIRED)

include_directories(${CMAKE_SOURCE_DIR}/../../src/)
include_directories(${CMAKE_SOURCE_DIR}/../../third-party/rapidjson/include/)

set(LIBWORDCOUNT_SRC 
    word_count.cc
    ${CMAKE_SOURCE_DIR}/../../src/tmapreduce/wrapper.cc
)

set(DEPENDENCIES
  gflags
  stdc++fs
  cpr::cpr
)

add_library(wordcount SHARED ${LIBWORDCOUNT_SRC})
add_executable(client ${CMAKE_SOURCE_DIR}/word_count_client.cc)
target_link_libraries(client ${DEPENDENCIES})