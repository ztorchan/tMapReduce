#ifndef _MAPREDUCE_WORKER_H
#define _MAPREDUCE_WORKER_H

#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>

#include "brpc/channel.h"

#include "mapreduce/status.h"
#include "mapreduce/job.h"
#include "mapreduce/rpc/state.pb.h"
#include "mapreduce/rpc/master_service.pb.h"
#include "mapreduce/rpc/worker_service.pb.h"

namespace mapreduce {

class Worker {
public:
  Worker(std::string name, uint32_t port, std::string mrf_path);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  Status Register(std::string master_address, uint32_t master_port);
  Status PrepareMap(uint32_t job_id, uint32_t subjob_id, std::string job_name,
             std::string job_type, MapIns& map_kvs);
  Status PrepareReduce(uint32_t job_id, uint32_t subjob_id, std::string job_name,
                std::string job_type, ReduceIns& reduce_kvs);
  Status TryWakeupExecutor();

  void end() { 
    end_ = true;
    job_cv_.notify_all();
  }
  friend class WorkerServiceImpl;

  static void BGExecutor(Worker* worker);

private:
  uint32_t id_;
  const std::string name_;
  const uint32_t port_;
  std::string mrf_path_;

  uint32_t  master_id_;
  std::string master_address_;
  uint32_t master_port_;
  brpc::Channel channel_;
  MasterService_Stub* master_stub_;
  
  WorkerState state_;
  std::string cur_type_;

  std::uint32_t cur_job_id_;
  std::uint32_t cur_subjob_id_;
  std::string cur_job_name_;
  std::string cur_job_type_; 
  MapIns map_kvs_;
  MapOuts map_result_;
  ReduceIns reduce_kvs_;
  ReduceOuts reduce_result_;
  std::mutex job_mtx_;
  std::condition_variable job_cv_;

  bool end_;
};

class WorkerServiceImpl : public WorkerService {
public:
  WorkerServiceImpl(std::string name, uint32_t port, std::string mrf_path);
  virtual ~WorkerServiceImpl();

  Status Register(std::string master_address, uint32_t master_port);
  void Beat(::google::protobuf::RpcController* controller,
            const ::google::protobuf::Empty* request,
            ::mapreduce::WorkerReplyMsg* response,
            ::google::protobuf::Closure* done) override;

  void PrepareMap(::google::protobuf::RpcController* controller,
                  const ::mapreduce::MapJobMsg* request,
                  ::mapreduce::WorkerReplyMsg* response,
                  ::google::protobuf::Closure* done) override;

  void PrepareReduce(::google::protobuf::RpcController* controller,
                     const ::mapreduce::ReduceJobMsg* request,
                     ::mapreduce::WorkerReplyMsg* response,
                     ::google::protobuf::Closure* done) override;

  void Start(::google::protobuf::RpcController* controller,
             const ::google::protobuf::Empty* request,
             ::mapreduce::WorkerReplyMsg* response,
             ::google::protobuf::Closure* done) override;

  void end() { worker_->end(); }
private:
  Worker* const worker_;
};

}

#endif