#ifndef MAPREDUCE_INCLUDE_MASTER_H
#define MAPREDUCE_INCLUDE_MASTER_H

#include <cstdint>
#include <unordered_map>
#include <map>
#include <queue>
#include <string>
#include <atomic>
#include <mutex>
#include <thread>
#include <condition_variable>
#include <chrono>
#include <cassert>

#include "brpc/channel.h"

#include "mapreduce/state.h"
#include "mapreduce/job.h"
#include "mapreduce/rpc/master_service.pb.h"
#include "mapreduce/rpc/worker_service.pb.h"


namespace mapreduce{

class Status;

class Slaver {
public:
  Slaver(uint32_t id, std::string address);
  ~Slaver() {}

  friend class Master;
  friend class JobDistributor;
  friend class Beater;

private:
  WorkerState state_;
  const uint32_t id_;
  const std::string address_;
  brpc::Channel channel_;
  WorkerService_Stub* stub_;

  uint32_t cur_job_id_;
  uint32_t cur_subjob_id_;
};

class Master {
public:
  Master(uint32_t id, std::string name_, uint32_t port);
  ~Master();

  Master(const Master&) = delete;
  Master& operator=(const Master&) = delete;

  Status init();
  Status Register(std::string address, uint32_t* slaver_id);
  Status Launch(const std::string& name, const std::string& type, 
                const int& map_worker_num, const int& reduce_worker_num,
                MapKVs& map_lvs, uint32_t* job_id);

  friend class MasterServiceImpl;
  friend class JobDistributor;
  friend class Beater;
  
  static void BGDistributor(Master* master);

  static void BGBeater(Master* master);

  void end() { 
    end_ = true; 
    jobs_cv_.notify_all();
  }

private:
  const uint32_t id_;
  const std::string name_;
  const uint32_t port_;
  std::atomic_uint32_t slaver_seq_num_;
  std::atomic_uint32_t job_seq_num_;

  std::unordered_map<uint32_t, Job*> jobs_;
  std::deque<uint32_t> map_queue_;
  std::deque<uint32_t> reduce_queue_;
  std::map<uint32_t, Slaver*> slavers_;
  std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> slaver_to_job_;

  std::mutex slavers_mutex_;
  std::mutex jobs_mutex_;
  std::condition_variable jobs_cv_;

  bool end_;

  uint32_t new_slaver_id() { return ++slaver_seq_num_; }
  uint32_t new_job_id() { return ++job_seq_num_; }
};

class MasterServiceImpl : public MasterService {
public:
  MasterServiceImpl(uint32_t id, std::string name, uint32_t port);
  virtual ~MasterServiceImpl();

  void Register(::google::protobuf::RpcController* controller,
                const ::mapreduce::RegisterMsg* request,
                ::mapreduce::RegisterReplyMsg* response,
                ::google::protobuf::Closure* done) override;

  void Launch(::google::protobuf::RpcController* controller,
              const ::mapreduce::JobMsg* request,
              ::mapreduce::LaunchReplyMsg* response,
              ::google::protobuf::Closure* done) override;
  
  void end() { master_->end(); }

private:
  Master* const master_;
};

} // namespace mapreduce


#endif