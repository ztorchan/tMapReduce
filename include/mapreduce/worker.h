#ifndef _MAPREDUCE_WORKER_H
#define _MAPREDUCE_WORKER_H

#include <cstdint>
#include <string>
#include <mutex>

#include "brpc/channel.h"

#include "mapreduce/status.h"
#include "mapreduce/rpc/state.pb.h"
#include "mapreduce/rpc/master_service.pb.h"
#include "mapreduce/rpc/worker_service.pb.h"

namespace mapreduce {

class Worker {
public:
  Worker(std::string name, std::string address, uint32_t port);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  Status Register(std::string master_address, uint32_t master_port);

  friend class WorkerServiceImpl;

  static void BGExecutor(Worker* worker);

private:
  uint32_t id_;
  const std::string name_;
  const std::string address_;
  const uint32_t port_;

  uint32_t  master_id_;
  std::string master_address_;
  uint32_t master_port_;
  brpc::Channel channel_;
  MasterService_Stub* stub_;
  
  WorkerState state_;
  std::uint32_t cur_job_id_;
  std::uint32_t cur_subjob_id_;
  std::string cur_job_name_;
  std::string cur_job_type_; 

  bool end_;
};

class WorkerServiceImpl : public WorkerService {
public:
  WorkerServiceImpl(std::string name, std::string address, uint32_t port);
  virtual ~WorkerServiceImpl();

  Status Register(std::string master_address, uint32_t master_port);
  void Beat(::google::protobuf::RpcController* controller,
            const ::google::protobuf::Empty* request,
            ::mapreduce::WorkerReplyMsg* response,
            ::google::protobuf::Closure* done) override;

  void Map(::google::protobuf::RpcController* controller,
           const ::mapreduce::MapJobMsg* request,
           ::mapreduce::WorkerReplyMsg* response,
           ::google::protobuf::Closure* done) override;

  void Reduce(::google::protobuf::RpcController* controller,
              const ::mapreduce::ReduceJobMsg* request,
              ::mapreduce::WorkerReplyMsg* response,
              ::google::protobuf::Closure* done) override;

private:
  Worker* const worker_;
};

}

#endif