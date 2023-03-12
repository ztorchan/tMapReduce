
#include <utility>
#include <algorithm>

#include <butil/logging.h>

#include "mapreduce/master.h"
#include "mapreduce/status.h"
#include "mapreduce/job.h"

namespace mapreduce {

Slaver::Slaver(uint32_t id, std::string name, std::string address, uint32_t port) 
    : id_(id),
      name_(name),
      address_(address),
      port_(port),
      channel_(),
      stub_(nullptr),
      state_(WorkerState::INIT),
      cur_job_id_(UINT32_MAX),
      cur_subjob_id_(UINT32_MAX) {
  brpc::ChannelOptions options;
  options.protocol = "baidu_std";
  options.timeout_ms = 1000;
  options.max_retry = 3;
  if(channel_.Init(address_.c_str(), port_, &options) != 0) {
    state_ = WorkerState::UNKNOWN;
    return;
  }
  stub_ = new WorkerService_Stub(&channel_);
}

Slaver::~Slaver() {
  if(stub_ != nullptr)
    delete stub_;
}


Master::Master(uint32_t id, std::string name, uint32_t port) 
    : id_(id),
      name_(name),
      port_(port),
      slaver_seq_num_(0),
      job_seq_num_(0),
      jobs_(),
      distribute_queue_(),
      merge_queue_(),
      finish_set_(),
      slavers_(),
      slaver_to_job_(),
      slavers_mutex_(),
      jobs_mutex_(),
      merge_mutex_(),
      jobs_cv_(),
      merge_cv_(),
      end_(false) {
  std::thread job_distributor(Master::BGDistributor, this);
  job_distributor.detach();
  std::thread beater(Master::BGBeater, this);
  beater.detach();
  std::thread merger(Master::BGMerger, this);
  merger.detach();
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

Status Master::Register(std::string name, std::string address, uint32_t port, uint32_t* slaver_id) {
  Slaver* s = new Slaver(new_slaver_id(), name, address, port);
  if(s->state_ != WorkerState::INIT){
    delete s;
    return Status::Error("Slaver initial failed.");
  }
  WorkerService_Stub* stub = s->stub_;
  
  brpc::Controller ctl;
  google::protobuf::Empty request;
  WorkerReplyMsg response;
  stub->Beat(&ctl, &request, &response, NULL);
  if (ctl.Failed()) {
    delete s;
    return Status::Error("Beat slaver failed.");
  }

  s->state_ = WorkerState::IDLE;
  std::unique_lock<std::mutex> lck(slavers_mutex_);
  slavers_[s->id_] = s;
  *slaver_id = s->id_;
  return Status::Ok("");
}

Status Master::Launch(const std::string& name, const std::string& type, 
                      int map_worker_num, int reduce_worker_num, 
                      MapIns& map_kvs, uint32_t* job_id) {
  // Check whether job is legal
  if(map_worker_num <= 0 || reduce_worker_num <= 0) {
    return Status::Error("Map worker and Reduce worker must be greater than 0.");
  }
  if(map_kvs.size() <= 0) {
    return Status::Error("Empty key-value.");
  }

  Job* new_job = new Job(new_job_id(), name, type, map_worker_num, reduce_worker_num, std::move(map_kvs));
  new_job->stage_ = JobStage::WAIT2MAP;
  new_job->Partition();
  std::unique_lock<std::mutex> lck(jobs_mutex_);
  jobs_[new_job->id_] = new_job;
  for(size_t i = 0; i < new_job->subjobs_.size(); i++) {
    distribute_queue_.emplace_back(new_job->id_, i);
  }

  *job_id = new_job->id_;
  lck.unlock();
  jobs_cv_.notify_all();
  return Status::Ok("");
}

Status Master::CompleteMap(uint32_t job_id, uint32_t subjob_id, uint32_t worker_id, WorkerState worker_state,
                           MapOuts& map_result) {
  // Check Job exists
  if(jobs_.find(job_id) == jobs_.end()) {
    return Status::Error("Job does not exist.");
  }

  Job* job = jobs_[job_id];
  std::unique_lock<std::mutex> lck(job->mtx_);
  // Check job state
  if(job->stage_ != JobStage::MAPPING) {
    return Status::Error("Job is not in MAPPING stage.");
  }
  // Check subjob id is legal 
  if(job->subjobs_.size() <= subjob_id) {
    return Status::Error("Subjob id is not legal");
  }
  // Check subjob has being distributed to the worker
  if(job->subjobs_[subjob_id].worker_id_ != worker_id) {
    return Status::Error("Subjob has not distributed to this worker.");
  }
  if(job->subjobs_[subjob_id].finished_ != false) {
    return Status::Error("SUbjob has been finished.");
  }
  
  job->subjobs_[subjob_id].result_ 
    = reinterpret_cast<void*>(new MapOuts(std::move(map_result)));
  job->subjobs_[subjob_id].finished_ = true;
  slaver_to_job_.erase(worker_id);
  slavers_[worker_id]->state_ = worker_state;
  job->unfinished_job_num_--;

  if(job->unfinished_job_num_ == 0) {
    job->stage_ = JobStage::WAIT2MERGE;
    merge_queue_.push_back(job_id);
    merge_cv_.notify_all();
  }
  return Status::Ok("");
}

Status Master::CompleteReduce(uint32_t job_id, uint32_t subjob_id, uint32_t worker_id, WorkerState worker_state,
                              ReduceOuts& reduce_result) {
  // Check Job exists
  if(jobs_.find(job_id) == jobs_.end()) {
    return Status::Error("Job does not exist.");
  }

  Job* job = jobs_[job_id];
  std::unique_lock<std::mutex> lck(job->mtx_);
  // Check job state
  if(job->stage_ != JobStage::REDUCING) {
    return Status::Error("Job is not in REDUCING stage.");
  }
  // Check subjob id is legal 
  if(job->subjobs_.size() <= subjob_id) {
    return Status::Error("Subjob id is not legal");
  }
  // Check subjob has being distributed to the worker
  if(job->subjobs_[subjob_id].worker_id_ != worker_id) {
    return Status::Error("Subjob has not distributed to this worker.");
  }
  if(job->subjobs_[subjob_id].finished_ != false) {
    return Status::Error("Subjob has been finished.");
  }

  job->subjobs_[subjob_id].result_
    = reinterpret_cast<void*>(new ReduceOuts(std::move(reduce_result)));
  job->subjobs_[subjob_id].finished_ = true;
  slaver_to_job_.erase(worker_id);
  slavers_[worker_id]->state_ = worker_state;
  job->unfinished_job_num_--;

  if(job->unfinished_job_num_ == 0) {
    job->stage_ = JobStage::WAIT2FINISH;
    job->Finish();
    job->stage_ = JobStage::FINISHED;
    finish_set_.insert(job_id);
  }
  return Status::Ok("");
}

Status Master::GetResult(uint32_t job_id, ReduceOuts* result) {
  auto job_iter = jobs_.find(job_id);
  if(job_iter == jobs_.end()) {
    return Status::Error("Not such job");
  }
  Job* job = jobs_[job_id];
  if(job->stage_ != JobStage::FINISHED) {
    return Status::Error("Job is unfinished.");
  }
  *result = job->results_;
  return Status::Ok("");
}

void Master::BGDistributor(Master* master) {
  LOG(INFO) << "Distributor Start";

  uint32_t cur_slaver_id = UINT32_MAX;    // the last slaver assigned successfully

  std::map<uint32_t, Slaver*>& slavers = master->slavers_;
  std::deque<std::pair<uint32_t, uint32_t>>& distribute_jobs = master->distribute_queue_;
  brpc::Controller cntl;

  while(!master->end_) {
    std::unique_lock<std::mutex> jobs_lck(master->jobs_mutex_);
    master->jobs_cv_.wait(jobs_lck, [&] { 
      return (!slavers.empty() && !distribute_jobs.empty()) 
             || master->end_; 
    });
    if(master->end_) { 
      break;
    }

    std::unique_lock<std::mutex> slavers_lck(master->slavers_mutex_);

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
        WorkerReplyMsg response;
        cntl.Reset();

        // Get subjob
        uint32_t job_id = distribute_jobs.front().first;
        uint32_t subjob_id = distribute_jobs.front().second;
        Job* job = master->jobs_[job_id];
        SubJob& subjob = job->subjobs_[subjob_id];
        if(job->stage_ == JobStage::WAIT2MAP) {
          job->stage_ = JobStage::MAPPING;
        } else if(job->stage_ == JobStage::WAIT2REDUCE) {
          job->stage_ = JobStage::REDUCING;
        }

        if(job->stage_ == JobStage::MAPPING) {
          MapJobMsg rpc_map_job;
          rpc_map_job.set_job_id(job->id_);
          rpc_map_job.set_subjob_id(subjob.subjob_id_);
          rpc_map_job.set_name(job->name_);
          rpc_map_job.set_type(job->type_);
          for(size_t i = 0; i < subjob.size_; i++) {
            const MapIn& kv = job->map_kvs_[subjob.head_ + i];
            auto rpc_kv = rpc_map_job.add_map_kvs();
            rpc_kv->set_key(kv.first);
            rpc_kv->set_value(kv.second);
          }
          stub->PrepareMap(&cntl, &rpc_map_job, &response, NULL);
        } else if(job->stage_ == JobStage::REDUCING) {
          ReduceJobMsg rpc_reduce_job;
          rpc_reduce_job.set_job_id(job->id_);
          rpc_reduce_job.set_subjob_id(subjob.subjob_id_);
          rpc_reduce_job.set_name(job->name_);
          rpc_reduce_job.set_type(job->type_);
          for(size_t i = 0; i < subjob.size_; i++) {
            const ReduceIn& kv = job->reduce_kvs_[subjob.head_ + i];
            auto rpc_kv = rpc_reduce_job.add_reduce_kvs();
            rpc_kv->set_key(kv.first);
            for(size_t j = 0; j < kv.second.size(); j++) {
              rpc_kv->add_value(kv.second[j]);
            }
          }
          stub->PrepareReduce(&cntl, &rpc_reduce_job, &response, NULL);
        }

        if(!cntl.Failed()) {
          slaver->state_ = response.state();
          if((slaver->state_ == WorkerState::MAPPING || slaver->state_ == WorkerState::REDUCING) && response.ok()) {
            // Successfully distributed subjob, change job state
            std::unique_lock<std::mutex> lck(job->mtx_);
            cur_slaver_id = slaver->id_;
            subjob.worker_id_ = slaver->id_;
            master->slaver_to_job_[slaver->id_] = std::pair<uint32_t, uint32_t>(job_id, subjob_id);
            distribute_jobs.pop_front();
            LOG(INFO) << "Successfully distributed subjob: "
                      << "[Job Id]" << job_id << ", "
                      << "[Subjob Id]" << subjob_id << ", "
                      << "[Name]" << job->name_ << ", "
                      << "[Type]" << job->type_ << ", "
                      << "[Size]" << subjob.size_ << ", "
                      << "[Stage]" << (job->stage_ == JobStage::MAPPING ? "map" : "reduce") << ", "
                      << "[Worker Id]" << slaver->id_ << ".";
            break;
          }
          LOG(INFO) << "Failed to distributed subjob: " << response.msg();
        } else {
          slaver->state_ = WorkerState::UNKNOWN;
          LOG(INFO) << "Failed to distributed subjob: failed to connect worker " << slaver->id_ << ".";
        }
      }
      ++it;
      if(it == slavers.end()) {
        it = slavers.begin();
      }
    } while(it->first != cur_slaver_id);
  }
  LOG(INFO) << "Distributor Stop";
}

void Master::BGBeater(Master* master) {
  LOG(INFO) << "Beater Start";

  brpc::Controller cntl;
  google::protobuf::Empty request;
  WorkerReplyMsg response;
  while(!master->end_) {
    for(auto& [_, slaver] : master->slavers_) {
      cntl.Reset();
      WorkerService_Stub* stub = slaver->stub_;
      stub->Beat(&cntl, &request, &response, NULL);
      if(cntl.Failed()) {
        slaver->state_ = WorkerState::UNKNOWN;
        LOG(INFO) << "Failed to beat worker " << slaver->id_ << ".";
      } else {
        slaver->state_ = response.state();
        if(slaver->state_ == WorkerState::IDLE){
          master->jobs_cv_.notify_all();
        }
      }

      LOG(INFO) << "Worker " << slaver->id_ << " state: " << slaver->state_;

      // Check fault slaver and re-distribute job
      if(slaver->state_ != WorkerState::IDLE && slaver->state_ != WorkerState::MAPPING && slaver->state_ != WorkerState::REDUCING) {
        LOG(INFO) << "Worker " << slaver->id_ << "state: " << slaver->state_;
        auto it = master->slaver_to_job_.find(slaver->id_);
        if(it != master->slaver_to_job_.end()) {
          uint32_t job_id = it->second.first;
          uint32_t subjob_id = it->second.second;
          Job* job = master->jobs_[job_id];
          std::unique_lock<std::mutex> job_lck(job->mtx_);
          job->subjobs_[subjob_id].worker_id_ = UINT32_MAX;

          std::unique_lock<std::mutex> jobs_lck(master->jobs_mutex_);
          master->distribute_queue_.emplace_front(job_id, subjob_id);
          master->slaver_to_job_.erase(slaver->id_);
          master->jobs_cv_.notify_all();
          LOG(INFO) << "Re-distribute job: [Job Id]" << job_id << ", [Subjob Id]" << subjob_id << ".";
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
  LOG(INFO) << "Beater Stop";
}

void Master::BGMerger(Master* master) {
  LOG(INFO) << "Merger Start";
  std::deque<uint32_t>& merge_queue = master->merge_queue_;
  while(!master->end_) {
    std::unique_lock<std::mutex> merge_lck(master->merge_mutex_);
    master->merge_cv_.wait(merge_lck, [&] {
      return !merge_queue.empty() || master->end_;
    });
    if(master->end_)
      break;
    uint32_t job_id = merge_queue.front();
    merge_queue.pop_front();
    Job* job = master->jobs_[job_id];
    assert(job->stage_ == JobStage::WAIT2MERGE);
    job->Merge();

    if(job->reduce_kvs_.size() == 0) {
      // if empty reduce kvs, job finish
      job->stage_ = JobStage::FINISHED;
      master->finish_set_.insert(job->id_);
    } else {
      job->stage_ = JobStage::WAIT2REDUCE;
      job->Partition();
      for(size_t i = 0; i < job->subjobs_.size(); i++) {
        master->distribute_queue_.emplace_back(job_id, i);
      }
      master->jobs_cv_.notify_all();
    }
  }
  LOG(INFO) << "Merger Stop";
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
  std::string worker_address = butil::ip2str(ctl->remote_side().ip).c_str();
  Status s = master_->Register(request->name(), worker_address, request->port(), &worker_id);
  if(s.ok()) {
    response->set_worker_id(worker_id);
    response->set_master_id(master_->id_);
    response->mutable_reply()->set_ok(true);
    response->mutable_reply()->set_msg(s.msg_);
    LOG(INFO) << "Worker register successfully: "
              << "[Worker Id]" << worker_id << ", "
              << "[Worker Name]" << request->name() << ", "
              << "[Worker Address]" << worker_address << ", "
              << "[Worker Port]" << request->port();
  } else {
    response->mutable_reply()->set_ok(false);
    response->mutable_reply()->set_msg(s.msg_);
    LOG(ERROR) << "Failed to register worker"
               << "[Name: " << request->name() << ", "
               << "Address: " << worker_address << ", "
               << "Port: " << request->port() << "]"
               << ": " << s.msg_;
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
  MapIns map_kvs;
  for(int i = 0; i < request->kvs_size(); ++i) {
    const JobMsg_KV& kv = request->kvs(i);
    map_kvs.emplace_back(kv.key(), kv.value());
  }
  std::sort(map_kvs.begin(), map_kvs.end(), [](const MapIn& lhs, const MapIn& rhs) -> bool {
    return lhs.first < rhs.first;
  });

  Status s = master_->Launch(request->name(), request->type(), request->mapper_num(), request->reducer_num(), map_kvs, &job_id);
  if(s.ok()) {
    response->set_job_id(job_id);
    response->mutable_reply()->set_ok(true);
    response->mutable_reply()->set_msg(std::move(s.msg_));
    LOG(INFO) << "Launch job from [" << ctl->remote_side() << "]. "
              << "[Id]" << job_id << ", "
              << "[Name]" << request->name() << ", "
              << "[Type]" << request->type() << ", "
              << "[Mapper Num]" << request->mapper_num() << ", "
              << "[Reducer Num]" << request->reducer_num() << ", "
              << "[Size]" << request->kvs_size() << ".";
  } else {
    response->mutable_reply()->set_ok(false);
    response->mutable_reply()->set_msg(s.msg_);
    LOG(INFO) << "Failed to launch job: " << s.msg_;
  }

  return ;
}

void MasterServiceImpl::CompleteMap(::google::protobuf::RpcController* controller,
                                    const ::mapreduce::MapResultMsg* request,
                                    ::mapreduce::MasterReplyMsg* response,
                                    ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  MapOuts map_result;
  for(int i = 0; i < request->map_result_size(); ++i) {
    const auto& kv = request->map_result(i);
    map_result.emplace_back(kv.key(), kv.value());
  }
  Status s = master_->CompleteMap(request->job_id(), request->subjob_id(), request->worker_id(), 
                                  request->state(), map_result);
  if(s.ok()) {
    response->set_ok(true);
    response->set_msg(s.msg_);
    LOG(INFO) << "Receive map result: "
              << "[Job Id]" << request->job_id() << ", "
              << "[Subjob Id]" << request->subjob_id() << ", "
              << "[Worker Id]" << request->worker_id() << ", "
              << "[Result Size]" << request->map_result_size() << ".";
  } else {
    response->set_ok(false);
    response->set_msg(s.msg_); 
    LOG(INFO) << "Failed to receive map result: " << s.msg_;
  }
  
  return ;
}

void MasterServiceImpl::CompleteReduce(::google::protobuf::RpcController* controller,
                                       const ::mapreduce::ReduceResultMsg* request,
                                       ::mapreduce::MasterReplyMsg* response,
                                       ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  ReduceOuts reduce_result;
  for(int i = 0; i < request->reduce_result_size(); ++i) {
    reduce_result.emplace_back(request->reduce_result(i));
  }

  Status s = master_->CompleteReduce(request->job_id(), request->subjob_id(), request->worker_id(), 
                                     request->state(), reduce_result);
  if(s.ok()) {
    response->set_ok(true);
    response->set_msg(s.msg_);
    LOG(INFO) << "Receive reduce result: "
              << "[Job Id]" << request->job_id() << ", "
              << "[Subjob Id]" << request->subjob_id() << ", "
              << "[Worker Id]" << request->worker_id() << ", "
              << "[Result Size]" << request->reduce_result_size() << ".";
  } else {
    response->set_ok(false);
    response->set_msg(s.msg_);
    LOG(INFO) << "Failed to receive reduce result: " << s.msg_;
  }

  return;
}

void MasterServiceImpl::GetResult(::google::protobuf::RpcController* controller,
                                  const ::mapreduce::GetResultMsg* request,
                                  ::mapreduce::GetResultReplyMsg* response,
                                  ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  ReduceOuts result;
  Status s = master_->GetResult(request->job_id(), &result);
  for(std::string& s :result) {
    response->add_results(std::move(s));
  }
  response->mutable_reply()->set_ok(s.ok());
  response->mutable_reply()->set_msg(s.msg_);
  return ;
}

}

