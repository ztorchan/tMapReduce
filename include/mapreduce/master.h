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

#include "brpc/channel.h"

#include "mapreduce/state.h"
#include "mapreduce/rpc/master_service.pb.h"
#include "mapreduce/rpc/worker_service.pb.h"


namespace mapreduce{

class Status;
class Job;

class Slaver {
public:
  Slaver(uint32_t id, std::string address);
  ~Slaver();

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

private:
  const uint32_t id_;
  const std::string name_;
  const uint32_t port_;
  std::atomic_uint32_t slaver_seq_num_;
  std::atomic_uint32_t job_seq_num_;

  std::unordered_map<uint32_t, Job*> jobs_;
  std::queue<uint32_t> map_queue_;
  std::queue<uint32_t> reduce_queue;
  std::
  std::map<uint32_t, Slaver*> slavers_;

  std::mutex slavers_mutex_;
  std::mutex jobs_mutex_;
  std::condition_variable jobs_cv_;
  std::thread* job_distributor_;        // 任务分发线程
  std::thread* beater_;        // 心跳线程

  uint32_t new_slaver_id() { return ++slaver_seq_num_; }
  uint32_t new_job_id() { return ++job_seq_num_; }
};

class MasterServiceImpl : public MasterService {
public:
  MasterServiceImpl(uint32_t id, std::string name, uint32_t port);
  virtual ~MasterServiceImpl();

  void Register(::google::protobuf::RpcController* controller,
                const ::mapreduce::RegisterInfo* request,
                ::mapreduce::RegisterReply* response,
                ::google::protobuf::Closure* done) override;

  void Launch(::google::protobuf::RpcController* controller,
              const ::mapreduce::Job* request,
              ::mapreduce::LaunchReply* response,
              ::google::protobuf::Closure* done) override;

private:
  Master* const master_;
};

class JobDistributor {
public:
  JobDistributor(Master* master) : master_(master) {}
  ~JobDistributor() {}
  
  JobDistributor(const JobDistributor&) = delete;
  JobDistributor& operator=(const JobDistributor&) = delete;

  void operator()(){
    // TODO: Distribute jobs
    uint32_t cur_slaver_id = 0;   // the last slaver assigned successfully
    std::map<uint32_t, Slaver*>& slavers = master_->slavers_;
    std::queue<Job*>& jobs = master_->jobs_queue_;
    brpc::Controller cntl;
    while(true) {
      std::unique_lock<std::mutex> lck(master_->slavers_mutex_);
      std::unique_lock<std::mutex> lck(master_->jobs_mutex_);
      master_->jobs_cv_.wait(lck, [&] { return !slavers.empty() && !jobs.empty(); });
      
      for(auto it = slavers.find(cur_slaver_id); ; ) {
        if(it->second->state_ == WorkerState::IDLE) {
          Job* job = jobs.front();
          Slaver* slaver = it->second;
          WorkerService_Stub* stub = slaver->stub_;
          MapJob map_job;
          map_job.set_name(job->name_);
          map_job.set_job_id(job->id_);
          map_job.set_sub_job_id(job->subjobs_)

          cntl.Reset();


          break;
        }
        ++it;
        if(it == slavers.end()) {
          it = slavers.begin();
        }
        if(it->first == cur_slaver_id) {
          break;
        }
      }

    }
  }
private:
  Master* const master_;
}

class Beater {
public:
  Beater(Master* master) : master_(master) {}
  ~Beater() {}
  
  Beater(const Beater&) = delete;
  Beater& operator=(const Beater&) = delete;

  void operator()(){
    brpc::Controller cntl;
    WorkerReplyMsg response;
    while(true) {
      for(auto& [_, slaver] : master_->slavers_) {
        cntl.Reset();
        WorkerService_Stub* stub = slaver->stub_;
        stub->Beat(&cntl, NULL, &response, NULL);
        if(cntl.Failed()) {
          slaver->state_ = SlaverState::UNKNOWN;
        } else {
          Slaver->state_ = WorkerStateFromRPC(response.state());
        }
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  }
private:
  Master* const master_;
}

} // namespace mapreduce


#endif