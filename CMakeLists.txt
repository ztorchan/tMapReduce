cmake_minimum_required(VERSION 3.16.0)
project(mapduce C CXX)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

add_subdirectory(${CMAKE_SOURCE_DIR}/third-party/brpc)
add_subdirectory(${CMAKE_SOURCE_DIR}/third-party/gflags)

include(FindProtobuf)
file(GLOB_RECURSE PROTO_FILE_LIST ${CMAKE_SOURCE_DIR}/protos/*.proto)
set(PROTO_OUT_DIR ${CMAKE_SOURCE_DIR}/include/mapreduce/rpc/)
foreach(PROTO_FILE ${PROTO_FILE_LIST})
  get_filename_component(FILE_NAME ${PROTO_FILE} NAME_WE)
  list(APPEND PROTO_CXX "${CMAKE_BINARY_DIR}/mapreduce/rpc/${FILE_NAME}.pb.cc")
  list(APPEND PROTO_H "${CMAKE_BINARY_DIR}/mapreduce/rpc/${FILE_NAME}.pb.h")
  execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${CMAKE_SOURCE_DIR}/protos/ --cpp_out=${PROTO_OUT_DIR} ${PROTO_FILE}
  )
endforeach()

message("${CMAKE_SOURCE_DIR}")
message("${CMAKE_BINARY_DIR}")

include_directories(${CMAKE_BINARY_DIR}/third-party/incubator-brpc/output/include/)
include_directories(${CMAKE_BINARY_DIR}/third-party/gflags/include/)
include_directories(${CMAKE_SOURCE_DIR}/include/)

add_library(master STATIC src/master.cc)
add_library(worker STATIC src/worker.cc)