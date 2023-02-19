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
  std::deque<uint32_t> map_queue_;
  std::deque<uint32_t> reduce_queue_;
  std::map<uint32_t, Slaver*> slavers_;
  std::unordered_map<uint32_t, std::pair<uint32_t, uint32_t>> slaver_to_job_;

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
    uint32_t cur_slaver_id = UINT32_MAX;    // the last slaver assigned successfully
    uint32_t cur_job_id = UINT32_MAX;       // current job
    uint32_t cur_subjob_index = UINT32_MAX; 
    bool cur_map = true;

    std::map<uint32_t, Slaver*>& slavers = master_->slavers_;
    std::deque<uint32_t>& map_jobs = master_->map_queue_;
    std::deque<uint32_t>& reduce_jobs = master_->reduce_queue_;
    brpc::Controller cntl;

    while(true) {
      std::unique_lock<std::mutex> lck(master_->slavers_mutex_);
      std::unique_lock<std::mutex> lck(master_->jobs_mutex_);
      master_->jobs_cv_.wait(lck, [&] { 
        return !slavers.empty() 
               && (!cur_job_id != UINT32_MAX || !map_jobs.empty() || !reduce_jobs.empty()); 
      });

      // Init
      if(cur_job_id == UINT32_MAX) {
        if(!reduce_jobs.empty()){
          cur_job_id = reduce_jobs.front();
          reduce_jobs.pop_front();
        } else {
          cur_job_id = map_jobs.front();
          map_jobs.pop_front();
        }
      }
      if(cur_slaver_id == UINT32_MAX) {
        cur_slaver_id = slavers.begin()->first;
      }
      if(cur_subjob_index == UINT32_MAX) {
        cur_subjob_index = 0;
      }

      assert(job->stage_ == JobStage::WAIT2MAP || job->stage_ == JobStage::WAIT2REDUCE);
      auto it = slavers.find(cur_slaver_id);
      if(it == slavers.end()) {
        it = slavers.begin();
        cur_slaver_id = it->first;
      }
      do {
        // find idle slaver and distribute job
        if(it->second->state_ == WorkerState::IDLE) {
          Slaver* slaver = it->second;
          WorkerService_Stub* stub = slaver->stub_;
          Job* job = master_->jobs_[cur_slaver_id];
          SubJob& subjob = job->subjobs_[cur_subjob_index];
          WorkerReplyMsg response;
          cntl.Reset();

          if(job->stage_ == JobStage::WAIT2MAP) {
            MapJob rpc_map_job;
            rpc_map_job.set_job_id(job->id_);
            rpc_map_job.set_sub_job_id(subjob.subjob_id_);
            rpc_map_job.set_name(job->name_);
            rpc_map_job.set_type(job->type_);
            for(size_t i = 0; i < subjob.size_; i++) {
              const MapKV& kv = job->map_kvs_[subjob.head_ + i];
              auto rpc_kv = rpc_map_job.add_map_kvs();
              rpc_kv->set_key(kv.first);
              rpc_kv->set_value(kv.second);
            }
            stub->Map(&cntl, &rpc_map_job, &response, NULL);
          } else if(job->stage_ == JobStage::WAIT2REDUCE) {
            ReduceJob rpc_reduce_job;
            rpc_reduce_job.set_job_id(job->id_);
            rpc_reduce_job.set_sub_job_id(subjob.subjob_id_);
            rpc_reduce_job.set_name(job->name_);
            rpc_reduce_job.set_type(job->type_);
            for(size_t i = 0; i < subjob.size_; i++) {
              const ReduceKV& kv = job->reduce_kvs_[subjob.head_ + i];
              auto rpc_kv = rpc_reduce_job.add_reduce_kvs();
              rpc_kv->set_key(kv.first);
              for(size_t j = 0; j < kv.second.size(); j++) {
                rpc_kv->add_value(kv.second[j]);
              }
            }
            stub->Reduce(&cntl, &rpc_reduce_job, &response, NULL);
          }

          if(!cntl.Fail()) {
            slaver->state_ = WorkerStateFromRPC(response.state());
            if(slaver->state_ == WorkerState::WORKING && response.ok()) {
              cur_slaver_id = slaver->id_;
              subjob.worker_id_ = slaver->id_;
              master_->slaver_to_job_[slaver->id_] = std::make_pair<uint32_t, uint32_t>(cur_job_id, cur_subjob_index);
              while(cur_subjob_index < job->subjobs_.size() && job->subjobs_[cur_subjob_index].worker_id_ != UINT32_MAX) {
                cur_subjob_index++;
              }
              if(cur_subjob_index == job->subjobs_.size()){
                cur_job_id = UINT32_MAX;
                cur_subjob_index = UINT32_MAX;
              }
              break;
            }
          } else {
            slaver->state_ = WorkerState::UNKNOWN;
          }
        }
        ++it;
        if(it == slavers.end()) {
          it = slavers.begin();
        }
      } while(it->first != cur_slaver_id);
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

        // TODO: Check fault slaver and re-distribute job
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
  }
private:
  Master* const master_;
}

} // namespace mapreduce


#endif