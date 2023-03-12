cmake_minimum_required(VERSION 3.16.0)
project(word_count)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

include(FindProtobuf)
find_package(gflags REQUIRED)
find_package(OpenSSL)
find_package(Threads)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
find_library(LEVELDB_LIB NAMES leveldb)

set(LIBWORDCOUNT_SRC 
    word_count.cc
    ${CMAKE_SOURCE_DIR}/../../src/wrapper.cc
)

set(DEPENDENCIES
  ${BRPC_LIB}
  ${PROTOBUF_LIBRARIES}
  ${OPENSSL_SSL_LIBRARY}
  ${OPENSSL_CRYPTO_LIBRARY}
  ${LEVELDB_LIB}
  ${CMAKE_THREAD_LIBS_INIT}
  gflags
  dl
  stdc++fs
)

add_library(wordcount SHARED ${LIBWORDCOUNT_SRC})
add_executable(client ${CMAKE_SOURCE_DIR}/word_count_client.cc)
target_link_libraries(client mrmaster ${DEPENDENCIES})