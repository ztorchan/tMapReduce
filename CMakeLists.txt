cmake_minimum_required(VERSION 3.16.0)
project(mapduce C CXX)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

include(FindProtobuf)
file(GLOB_RECURSE PROTO_FILE_LIST ${CMAKE_SOURCE_DIR}/protos/*.proto)
set(PROTO_OUT_DIR ${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/)
foreach(PROTO_FILE ${PROTO_FILE_LIST})
  get_filename_component(FILE_NAME ${PROTO_FILE} NAME_WE)
  list(APPEND PROTO_CXX "${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/${FILE_NAME}.pb.cc")
  list(APPEND PROTO_H "${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/${FILE_NAME}.pb.h")
  execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${CMAKE_SOURCE_DIR}/protos/ --cpp_out=${PROTO_OUT_DIR} ${PROTO_FILE}
  )
endforeach()

include_directories(${CMAKE_SOURCE_DIR}/include/)

find_package(gflags REQUIRED)
find_package(OpenSSL)
find_package(Threads)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
find_library(LEVELDB_LIB NAMES leveldb)

set(LIBMASTER_SRC 
  src/master.cc
  src/job.cc
  ${PROTO_CXX}
  ${PROTO_H}
)

set(LIBWORKER_SRC 
  src/worker.cc
  src/job.cc
  src/wrapper.cc
  ${PROTO_CXX}
  ${PROTO_H}
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
)

add_library(master STATIC ${LIBMASTER_SRC})
add_executable(master_server server/master_server.cc)
add_dependencies(master_server master)
target_link_libraries(master_server master ${DEPENDENCIES})

add_library(worker STATIC ${LIBWORKER_SRC})
add_executable(worker_server server/worker_server.cc)
add_dependencies(worker_server worker)
target_link_libraries(worker_server worker ${DEPENDENCIES})