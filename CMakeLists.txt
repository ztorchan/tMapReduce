cmake_minimum_required(VERSION 3.16.0)
project(mapduce C CXX)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

include_directories(${CMAKE_SOURCE_DIR}/include/)

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

find_package(gflags REQUIRED)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
find_package(Threads)

add_library(master STATIC src/master.cc src/job.cc src/state.cc include/mapreduce/master.h ${PROTO_CXX} ${PROTO_H})
target_link_libraries(master ${BRPC_LIB} gflags ${CMAKE_THREAD_LIBS_INIT})
