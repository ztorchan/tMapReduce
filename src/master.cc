
#include <utility>
#include <algorithm>

#include <butil/logging.h>

#include "mapreduce/master.h"
#include "mapreduce/status.h"
#include "mapreduce/job.h"


namespace mapreduce {

Slaver::Slaver(uint32_t id, std::string address) 
    : id_(id),
      address_(address),
      channel_(),
      stub_(nullptr),
      state_(WorkerState::INIT),
      cur_job_id_(UINT32_MAX),
      cur_subjob_id_(UINT32_MAX) {
  brpc::ChannelOptions options;
  options.protocol = "baidu_std";
  options.timeout_ms = 1000;
  options.max_retry = 3;
  if(channel_.Init(address_.c_str(), &options) != 0) {
    state_ = WorkerState::UNKNOWN;
    return;
  }
  
  stub_ = new WorkerService_Stub(&channel_);
}


Master::Master(uint32_t id, std::string name, uint32_t port) 
    : id_(id),
      name_(name),
      port_(port),
      slaver_seq_num_(0),
      job_seq_num_(0),
      jobs_(),
      map_queue_(),
      reduce_queue_(),
      slavers_(),
      slaver_to_job_(),
      slavers_mutex_(),
      jobs_mutex_(),
      jobs_cv_(),
      end_(false) {
  std::thread job_distributor(Master::BGDistributor, this);
  job_distributor.detach();
  std::thread beater(Master::BGBeater, this);
  beater.detach();
}

Master::~Master() {
  for(auto& [_, slaver] : slavers_) {
    delete slaver;
    slaver = nullptr;
  }
  for(auto& [_, job] : jobs_) {
    delete job;
    job = nullptr;
  }
}

Status Master::Register(std::string address, uint32_t* slaver_id) {
  Slaver* s = new Slaver(new_slaver_id(), address);
  if(s->state_ != WorkerState::INIT){
    delete s;
    return Status::Error("Register Failed: slaver initial failed.");
  }
  WorkerService_Stub* stub = s->stub_;
  
  brpc::Controller ctl;
  google::protobuf::Empty request;
  WorkerReplyMsg response;
  stub->Beat(&ctl, &request, &response, NULL);
  if (!ctl.Failed()) {
    delete s;
    return Status::Error("Register Failed: beat slaver failed.");
  }

  s->state_ = WorkerState::IDLE;
  std::unique_lock<std::mutex> lck(slavers_mutex_);
  slavers_[s->id_] = s;
  *slaver_id = s->id_;
  return Status::Ok("");
}

Status Master::Launch(const std::string& name, const std::string& type, 
                      const int& map_worker_num, const int& reduce_worker_num, 
                      MapKVs& map_kvs, uint32_t* job_id) {
  // TODO: Check whether job is legal
  if(map_worker_num <= 0 || reduce_worker_num <= 0) {
    return Status::Error("Map worker and Reduce worker must be greater than 0.");
  }
  if(map_kvs.size() <= 0) {
    return Status::Error("Empty key-value.");
  }

  std::unique_lock<std::mutex> lck(jobs_mutex_);
  Job* new_job = new Job(new_job_id(), name, type, map_worker_num, reduce_worker_num, std::move(map_kvs));
  new_job->stage_ = JobStage::MAPPING;
  new_job->Partition();
  jobs_[new_job->id_] = new_job;
  map_queue_.push_back(new_job->id_);

  *job_id = new_job->id_;
  lck.unlock();
  jobs_cv_.notify_all();
  return Status::Ok("");
}

void Master::BGDistributor(Master* master) {
  uint32_t cur_slaver_id = UINT32_MAX;    // the last slaver assigned successfully
  uint32_t cur_job_id = UINT32_MAX;       // current job
  uint32_t cur_subjob_index = UINT32_MAX; 
  bool cur_map = true;

  std::map<uint32_t, Slaver*>& slavers = master->slavers_;
  std::deque<uint32_t>& map_jobs = master->map_queue_;
  std::deque<uint32_t>& reduce_jobs = master->reduce_queue_;
  brpc::Controller cntl;

  while(!master->end_) {
    std::unique_lock<std::mutex> jobs_lck(master->jobs_mutex_);
    master->jobs_cv_.wait(jobs_lck, [&] { 
      return (!slavers.empty() && (!cur_job_id != UINT32_MAX || !map_jobs.empty() || !reduce_jobs.empty())) 
             || master->end_; 
    });
    if(master->end_) { 
      jobs_lck.unlock();
      break;
    }

    std::unique_lock<std::mutex> slavers_lck(master->slavers_mutex_);

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
        Job* job = master->jobs_[cur_slaver_id];
        SubJob& subjob = job->subjobs_[cur_subjob_index];
        WorkerReplyMsg response;
        cntl.Reset();

        assert(job->stage_ == JobStage::MAPPING || job->stage_ == JobStage::REDUCING);

        if(job->stage_ == JobStage::MAPPING) {
          MapJobMsg rpc_map_job;
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
        } else if(job->stage_ == JobStage::REDUCING) {
          ReduceJobMsg rpc_reduce_job;
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

        if(!cntl.Failed()) {
          slaver->state_ = WorkerStateFromRPC(response.state());
          if(slaver->state_ == WorkerState::WORKING && response.ok()) {
            cur_slaver_id = slaver->id_;
            subjob.worker_id_ = slaver->id_;
            master->slaver_to_job_[slaver->id_] = std::pair<uint32_t, uint32_t>(cur_job_id, cur_subjob_index);
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
  LOG(INFO) << "JobDistributor Stop";
}

void Master::BGBeater(Master* master) {
  brpc::Controller cntl;
  WorkerReplyMsg response;
  while(!master->end_) {
    for(auto& [_, slaver] : master->slavers_) {
      cntl.Reset();
      WorkerService_Stub* stub = slaver->stub_;
      stub->Beat(&cntl, NULL, &response, NULL);
      if(cntl.Failed()) {
        slaver->state_ = WorkerState::UNKNOWN;
      } else {
        slaver->state_ = WorkerStateFromRPC(response.state());
      }

      // TODO: Check fault slaver and re-distribute job
      if(slaver->state_ != WorkerState::IDLE || slaver->state_ != WorkerState::WORKING) {
        auto it = master->slaver_to_job_.find(slaver->id_);
        if(it != master->slaver_to_job_.end()) {
          uint32_t job_id = it->second.first;
          uint32_t subjob_index = it->second.second;
          Job* job = master->jobs_[job_id];
          job->subjobs_[subjob_index].worker_id_ = UINT32_MAX;

          std::unique_lock<std::mutex> lck(master->jobs_mutex_);
          if(job->stage_ == JobStage::MAPPING) {
            master->map_queue_.push_front(job_id);
          } else if(job->stage_ == JobStage::REDUCING) {
            master->reduce_queue_.push_front(job_id);
          }
          lck.unlock();
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  LOG(INFO) << "Beater Stop";
}

MasterServiceImpl::MasterServiceImpl(uint32_t id, std::string name, uint32_t port) 
    : MasterService(),
      master_(new Master(id, name, port))  {}

MasterServiceImpl::~MasterServiceImpl() {
  delete master_;
}

void MasterServiceImpl::Register(::google::protobuf::RpcController* controller,
                                 const ::mapreduce::RegisterMsg* request,
                                 ::mapreduce::RegisterReplyMsg* response,
                                 ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  uint32_t worker_id;
  std::string worker_address = request->address();
  Status s = master_->Register(worker_address, &worker_id);
  if(s.ok()) {
    response->set_worker_id(worker_id);
    response->mutable_reply()->set_ok(true);
    response->mutable_reply()->set_msg(std::move(s.msg_));
  } else {
    response->mutable_reply()->set_ok(false);
    response->mutable_reply()->set_msg(std::move(s.msg_));
  }

  return ;
}

void MasterServiceImpl::Launch(::google::protobuf::RpcController* controller,
                               const ::mapreduce::JobMsg* request,
                               ::mapreduce::LaunchReplyMsg* response,
                               ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  
  uint32_t job_id;
  MapKVs map_kvs;
  for(int i = 0; i < request->kvs_size(); ++i) {
    const JobMsg_KV& kv = request->kvs(i);
    map_kvs.emplace_back(kv.key(), kv.value());
  }
  std::sort(map_kvs.begin(), map_kvs.end(), [](const MapKV& lhs, const MapKV& rhs) -> bool {
    return lhs.first < rhs.first;
  });

  Status s = master_->Launch(request->name(), request->type(), request->mapper_num(), request->reducer_num(), map_kvs, &job_id);
  if(s.ok()) {
    response->set_job_id(job_id);
    response->mutable_reply()->set_ok(true);
    response->mutable_reply()->set_msg(std::move(s.msg_));
  } else {
    response->mutable_reply()->set_ok(false);
    response->mutable_reply()->set_msg(std::move(s.msg_));
  }

  return ;
}


}

