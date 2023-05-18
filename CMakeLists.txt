cmake_minimum_required(VERSION 3.16.0)
project(tmapduce C CXX)

set(CMAKE_CXX_STANDARD 17)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE ReLWithDebInfo)
endif()

if(NOT CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX /usr/local)
endif()

include(FindProtobuf)
file(GLOB_RECURSE PROTO_FILE_LIST ${CMAKE_SOURCE_DIR}/protos/*.proto)
set(PROTO_OUT_DIR ${CMAKE_SOURCE_DIR}/src/tmapreduce/rpc/)
foreach(PROTO_FILE ${PROTO_FILE_LIST})
  get_filename_component(FILE_NAME ${PROTO_FILE} NAME_WE)
  list(APPEND PROTO_CXX "${PROTO_OUT_DIR}/${FILE_NAME}.pb.cc")
  list(APPEND PROTO_H "${PROTO_OUT_DIR}/${FILE_NAME}.pb.h")
  execute_process(
    COMMAND ${PROTOBUF_PROTOC_EXECUTABLE} -I${CMAKE_SOURCE_DIR}/protos/ --cpp_out=${PROTO_OUT_DIR} ${PROTO_FILE}
  )
endforeach()

include_directories(${CMAKE_SOURCE_DIR}/include/)
include_directories(${CMAKE_SOURCE_DIR}/src/)

find_package(gflags REQUIRED)
find_package(OpenSSL)
find_package(Threads)
find_library(BRPC_LIB NAMES libbrpc.a brpc)
find_library(LEVELDB_LIB NAMES leveldb)

set(LIBMASTER_SRC 
  src/tmapreduce/master.cc
  src/tmapreduce/job.cc
  ${PROTO_CXX}
  ${PROTO_H}
)

set(LIBWORKER_SRC 
  src/tmapreduce/worker.cc
  src/tmapreduce/job.cc
  src/tmapreduce/wrapper.cc
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
  stdc++fs
)

add_library(mrmaster STATIC ${LIBMASTER_SRC})
# add_executable(mr_master_server server/master_server.cc server/validator.cc)
# add_dependencies(mr_master_server mrmaster)
# target_link_libraries(mr_master_server mrmaster ${DEPENDENCIES})

add_library(mrworker STATIC ${LIBWORKER_SRC})
# add_executable(mr_worker_server server/worker_server.cc server/validator.cc)
# add_dependencies(mr_worker_server mrworker)
# target_link_libraries(mr_worker_server mrworker ${DEPENDENCIES})

# install(
#   TARGETS mrmaster mrworker mr_master_server mr_worker_server
#   RUNTIME DESTINATION bin
#   ARCHIVE DESTINATION lib64
# )
# install(
#   DIRECTORY include/mapreduce DESTINATION include/
# )