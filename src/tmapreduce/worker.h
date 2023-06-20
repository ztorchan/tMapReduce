#ifndef _TMAPREDUCE_WORKER_H
#define _TMAPREDUCE_WORKER_H

#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>

#include <butil/endpoint.h>
#include <brpc/channel.h>

#include "tmapreduce/status.h"
#include "tmapreduce/job.h"
#include "tmapreduce/rpc/state.pb.h"
#include "tmapreduce/rpc/master_service.pb.h"
#include "tmapreduce/rpc/worker_service.pb.h"

namespace tmapreduce {

class Worker {
public:
  Worker(std::string name, butil::EndPoint ep, std::string mrf_path);
  ~Worker();

  Worker(const Worker&) = delete;
  Worker& operator=(const Worker&) = delete;

  void Reset();
  void InitRegister(std::vector<butil::EndPoint> master_eps, std::vector<std::string> acceptable_job_types);
  Status Prepare(uint32_t master_id, std::string master_group, std::string master_conf, std::string job_type);
  Status PrepareMap(uint32_t master_id, uint32_t job_id, uint32_t subjob_id, 
                    std::string job_name, std::string job_type, MapIns& map_kvs);
  Status PrepareReduce(uint32_t master_id, uint32_t job_id, uint32_t subjob_id, 
                       std::string job_name, std::string job_type, ReduceIns& reduce_kvs);
  void TryWakeupExecutor();

  void end() { 
    end_ = true;
    job_cv_.notify_all();
  }
  friend class WorkerServiceImpl;

  static void BGExecutor(Worker* worker);

private:
  // baisc information
  const std::string name_;
  const butil::EndPoint ep_;
  std::string mrf_path_;
  // worker state
  WorkerState state_;
  std::string mrf_type_;
  void* mrf_handle_;
  uint32_t cur_master_id_;
  std::string cur_master_group_;
  std::mutex state_mtx_;
  // current job
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
  WorkerServiceImpl(std::string name, butil::EndPoint ep, std::string mrf_path);
  virtual ~WorkerServiceImpl();

  void InitRegister(std::vector<butil::EndPoint> master_eps, std::vector<std::string> acceptable_job_types);
  void Beat(::google::protobuf::RpcController* controller,
            const ::google::protobuf::Empty* request,
            ::tmapreduce::WorkerReplyMsg* response,
            ::google::protobuf::Closure* done) override;

  void Prepare(::google::protobuf::RpcController* controller,
               const ::tmapreduce::PrepareMsg* request,
               ::tmapreduce::WorkerReplyMsg* response,
               ::google::protobuf::Closure* done) override;

  void PrepareMap(::google::protobuf::RpcController* controller,
                  const ::tmapreduce::MapJobMsg* request,
                  ::tmapreduce::WorkerReplyMsg* response,
                  ::google::protobuf::Closure* done) override;

  void PrepareReduce(::google::protobuf::RpcController* controller,
                     const ::tmapreduce::ReduceJobMsg* request,
                     ::tmapreduce::WorkerReplyMsg* response,
                     ::google::protobuf::Closure* done) override;

  void Start(::google::protobuf::RpcController* controller,
             const ::google::protobuf::Empty* request,
             ::tmapreduce::WorkerReplyMsg* response,
             ::google::protobuf::Closure* done) override;

  void end() { worker_->end(); }
private:
  Worker* const worker_;
};

}

#endif