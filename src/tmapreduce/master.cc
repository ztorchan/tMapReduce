#include <sys/types.h>
#include <fcntl.h>
#include <ctime>
#include <random>
#include <algorithm>

#include <butil/sys_byteorder.h>
#include <butil/endpoint.h>
#include <brpc/controller.h>
#include <brpc/server.h>
#include <braft/raft.h>
#include <braft/storage.h>
#include <braft/util.h>
#include <spdlog/spdlog.h>
#include <boost/uuid/uuid.hpp>
#include <cpr/cpr.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>

#include "tmapreduce/status.h"
#include "tmapreduce/closure.h"
#include "tmapreduce/master.h"
#include "tmapreduce/base64.h"

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "/data", "Path of data stored on");
DEFINE_string(group, "tMapReduce", "Id of the replication group");
DEFINE_string(this_endpoint, "", "Endpoint of this node");

namespace tmapreduce
{

Slaver::Slaver(std::string name, butil::EndPoint endpoint) 
  : name_(name)
  , endpoint_(endpoint)
  , channel_(nullptr)
  , stub_(nullptr)
  , beat_retry_times_(0)
  , state_(WorkerState::INIT)
  , cur_job_(UINT32_MAX)
  , cur_subjob_(UINT32_MAX) {}

Slaver::~Slaver() {
  if(stub_ != nullptr) {
    delete stub_;
  }
  if(channel_ != nullptr) {
    delete channel_;
  }
}

Master::Master(uint32_t id, std::string name, butil::EndPoint etcd_ep)
  : id_(id)
  , name_(name)
  , job_seq_num_(0)
  , etcd_ep_(etcd_ep)
  , slavers_()
  , jobs_()
  , jobs_waiting_dist_()
  , jobs_finished_()
  , slavers_mtx_()
  , jobs_mtx_()
  , dist_mtx_()
  , dist_cv_()
  , beat_cv_()
  , scan_cv_()
  , raft_node_(nullptr)
  , raft_leader_term_(-1)
  , end_(false) {
  std::thread job_distributor(Master::BGDistributor, this);
  job_distributor.detach();
  std::thread beater(Master::BGBeater, this);
  beater.detach();
  std::thread scaner(Master::BGScaner, this);
  scaner.detach();
}

Master::~Master() {
  for(auto& [_, job] : jobs_) {
    delete job;
    job = nullptr;
  }
}

int Master::start() {
  butil::EndPoint addr;
  butil::str2endpoint(FLAGS_this_endpoint.c_str(), &addr);
  braft::NodeOptions node_options;
  if(node_options.initial_conf.parse_from(FLAGS_conf) != 0) {
      spdlog::error("[start] failed to parse configuration from {} ", FLAGS_conf.c_str());
      return -1;
  }
  node_options.election_timeout_ms = FLAGS_election_timeout_ms;
  node_options.fsm = this;
  node_options.node_owns_fsm = false;
  node_options.snapshot_interval_s = FLAGS_snapshot_interval;
  std::string prefix = "local://" + FLAGS_data_path;
  node_options.log_uri = prefix + "/log";
  node_options.raft_meta_uri = prefix + "/raft_meta";
  node_options.snapshot_uri = prefix + "/snapshot";
  node_options.disable_cli = FLAGS_disable_cli;
  braft::Node* node = new braft::Node(FLAGS_group, braft::PeerId(addr));
  if (node->init(node_options) != 0) {
      LOG(ERROR) << "Fail to init raft node";
      delete node;
      return -1;
  }
  raft_node_ = node;
  return 0;
}

void Master::Register(const RegisterMsg* request,
                      RegisterReplyMsg* response,
                      google::protobuf::Closure* done) {
  // no need to log
  brpc::ClosureGuard done_guard(done);

  std::string name = request->name();
  std::string ep_str = request->ep();
  std::vector<std::string> acceptable_job_type;
  butil::EndPoint ep;
  butil::str2endpoint(ep_str.c_str(), &ep);
  for(size_t i = 0; i < request->acceptable_job_type_size(); i++) {
    acceptable_job_type.push_back(request->acceptable_job_type(i));
  }

  Status s = handle_register(name, ep, acceptable_job_type);
  if(s.ok()) {
    spdlog::info("[register] successfully register worker to etcd");
    response->mutable_reply()->set_ok(true);
  } else {
    spdlog::info("[register] fail to register worker to etcd : {}", s.msg_.c_str());
    response->mutable_reply()->set_ok(false);
    response->mutable_reply()->set_msg(s.msg_);
  }

  return ;
}

void Master::Launch(const LaunchMsg* request,
                    LaunchReplyMsg* response,
                    google::protobuf::Closure* done) {
  apply_from_rpc(OP_LAUNCH, request, response, done);
}

void Master::CompleteMap(const CompleteMapMsg* request,
                        MasterReplyMsg* response,
                        google::protobuf::Closure* done) {
  apply_from_rpc(OP_COMPLETEMAP, request, response, done);
}
void Master::CompleteReduce(const CompleteReduceMsg* request,
                            MasterReplyMsg* response,
                            google::protobuf::Closure* done) {
  apply_from_rpc(OP_COMPLETEREDUCE, request, response, done);
}
void Master::GetResult(const GetResultMsg* request,
                       GetResultReplyMsg* response,
                       google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  uint32_t job_id = request->job_id();
  std::string token = request->token();
  ReduceOuts results;
  Status s = handle_get_result(job_id, token, results);
  for(std::string& r : results) {
    response->add_results(std::move(r));
  }
  response->mutable_reply()->set_ok(s.ok());
  response->mutable_reply()->set_msg(s.msg_);
  return ;
}

void Master::BGDistributor(Master* master) {
  spdlog::info("[bgdistributor] distributor start");

  auto& jobs_waiting_dist = master->jobs_waiting_dist_;
  brpc::Controller cntl;

  while(!master->end_) {
    std::unique_lock<std::mutex> dist_lck(master->dist_mtx_);
    master->dist_cv_.wait(dist_lck, [&] { 
      return (!jobs_waiting_dist.empty() && master->raft_node_->is_leader()) || master->end_; 
    });
    if(master->end_) {
      break;
    }

    // get job
    uint32_t job_id = jobs_waiting_dist.front().first;
    uint32_t subjob_id = jobs_waiting_dist.front().second;
    jobs_waiting_dist.pop_front();
    Job* job = master->jobs_[job_id];
    SubJob& subjob = job->subjobs_[subjob_id];
    spdlog::info("[bgdistributor] try to distribute job {} subjob {}", job_id, subjob_id);

    // get job type and find worker
    std::unordered_map<std::string, butil::EndPoint> workers;
    Status s = master->etcd_get_workers_from_type(job->type_, workers);
    if(!s.ok()) {
      spdlog::info("[bgdistributor] fail to find workers that can process \"{}\" job", job->type_.c_str());
      jobs_waiting_dist.emplace_back(job_id, subjob_id);
      continue;
    }

    // try to distribute
    spdlog::info("[bgdistributor] find {} workers", workers.size());
    std::vector<std::pair<std::string, butil::EndPoint>> workers_vec(workers.begin(), workers.end());
    std::random_shuffle(workers_vec.begin(), workers_vec.end());
    bool success_dist = false;
    for(const auto& w : workers_vec) {
      if(master->slavers_.count(w.first) != 0) {
        continue;
      }
      spdlog::info("[bgdistributor] try to distribute subjob to woker@{}[{}]", w.first.c_str(), butil::endpoint2str(w.second).c_str());
      
      // build brpc channel
      brpc::Channel* chan = new brpc::Channel;
      brpc::ChannelOptions chan_options;
      chan_options.protocol = brpc::PROTOCOL_BAIDU_STD;
      chan_options.timeout_ms = 3000;
      if(chan->Init(w.second, &chan_options) != 0) {
        spdlog::info("[bgdistributor] fail to initial channel");
        delete chan;
        continue;
      }
      WorkerService_Stub* stub = new WorkerService_Stub(chan);

      // Prepare
      cntl.Reset();
      PrepareMsg prepare_request;
      WorkerReplyMsg response;
      prepare_request.set_master_id(master->id_);
      prepare_request.set_master_group(FLAGS_group);
      prepare_request.set_master_conf(FLAGS_conf);
      prepare_request.set_job_type(job->type_);
      stub->Prepare(&cntl, &prepare_request, &response, NULL);
      if(cntl.Failed()) {
        spdlog::info("[bgdistributor] fail to prepare: connect worker error");
        delete stub;
        delete chan;
        continue;
      } 
      if(!response.ok()) {
        spdlog::info("[bgdistributor] fail to prepare: {}", response.msg());
        delete stub;
        delete chan;
        continue;
      }
      spdlog::info("[bgdistributor] successfully prepare");

      // transfer job data
      cntl.Reset();
      response.Clear();
      if(job->stage_ == JobStage::WAIT2MAP || job->stage_ == JobStage::MAPPING) {
        // transfer map key-values to worker
        MapJobMsg map_request;
        map_request.set_master_id(master->id_);
        map_request.set_job_id(job_id);
        map_request.set_subjob_id(subjob_id);
        map_request.set_name(job->name_);
        map_request.set_type(job->type_);
        for(size_t i = 0; i < subjob.size_; i++) {
          const MapIn& kv = job->map_kvs_[subjob.head_ + i];
          auto rpc_kv = map_request.add_map_kvs();
          rpc_kv->set_key(kv.first);
          rpc_kv->set_value(kv.second);
        }
        stub->PrepareMap(&cntl, &map_request, &response, NULL);
      } else if(job->stage_ == JobStage::WAIT2REDUCE || job->stage_ == JobStage::REDUCING) {
        ReduceJobMsg reduce_request;
        reduce_request.set_master_id(master->id_);
        reduce_request.set_job_id(job_id);
        reduce_request.set_subjob_id(subjob_id);
        reduce_request.set_name(job->name_);
        reduce_request.set_type(job->type_);
        for(size_t i = 0; i < subjob.size_; i++) {
          const ReduceIn& kv = job->reduce_kvs_[subjob.head_ + i];
          auto rpc_kv = reduce_request.add_reduce_kvs();
          rpc_kv->set_key(kv.first);
          for(size_t j = 0; j < kv.second.size(); j++) {
            rpc_kv->add_value(kv.second[j]);
          }
        }
        stub->PrepareReduce(&cntl, &reduce_request, &response, NULL);
      }
      if(cntl.Failed()) {
        spdlog::info("[bgdistributor] fail to transfer job data: connect worker error");
        delete stub;
        delete chan;
        continue;
      } 
      if(!response.ok()) {
        spdlog::info("[bgdistributor] fail to transfer job data: {}", response.msg());
        delete stub;
        delete chan;
        continue;
      }

      // successfully transfer job data
      // add slaver
      spdlog::info("[bgdistributor] successfully transfer job data");
      std::unique_ptr<Slaver> new_slaver = std::make_unique<Slaver>(w.first, w.second);
      new_slaver->channel_ = chan;
      new_slaver->stub_ = stub;
      new_slaver->state_ = response.state();
      new_slaver->cur_job_ = job_id;
      new_slaver->cur_job_ = subjob_id;
      std::unique_lock<std::mutex> slavers_lck(master->slavers_mtx_);
      master->slavers_[w.first] = std::move(new_slaver);
      slavers_lck.unlock();
      success_dist = true;

      // raft log 
      butil::IOBuf log;
      braft::Task task;
      log.push_back(OP_DISTRIBUTE);
      log.append(&job_id, sizeof(uint32_t));
      log.append(&subjob_id, sizeof(uint32_t));
      log.append(w.first);
      task.data = &log;
      if(FLAGS_check_term) {
        // avoid ABA
        task.expected_term = master->raft_leader_term_.load(butil::memory_order_acquire);
      }
      master->raft_node_->apply(task);

      // try to start job
      google::protobuf::Empty start_request;
      cntl.Reset();
      stub->Start(&cntl, &start_request, &response, NULL);
      break;
    }

    if(!success_dist) {
      spdlog::info("[bgdistributor] fail to distribute job {} subjob {}", job_id, subjob_id);
      jobs_waiting_dist.emplace_back(job_id, subjob_id);
      continue;
    }
  }
}

void Master::BGBeater(Master* master) {
  spdlog::info("[bgbeater] beater start");

  brpc::Controller cntl;
  google::protobuf::Empty beat_request;
  WorkerReplyMsg beat_response;
  while(!master->end_) {
    std::this_thread::sleep_for(std::chrono::seconds(BEAT_PERIOD_SECOND));
    std::unique_lock<std::mutex> slavers_lck(master->slavers_mtx_);
    master->beat_cv_.wait(slavers_lck, [&] {
      return (!master->slavers_.empty() && master->raft_node_->is_leader() || master->end_);
    });
    if(master->end_) {
      break;
    }

    std::vector<std::string> error_slavers;
    for(auto& [_, slaver] : master->slavers_) {
      spdlog::info("[bgbeater] beat worker {}", slaver->name_);
      cntl.Reset();
      slaver->stub_->Beat(&cntl, &beat_request, &beat_response, NULL);
      if(cntl.Failed()) {
        spdlog::info("[bgbeater] fail to beat: connected error");
        slaver->state_ = WorkerState::UNKNOWN;
        slaver->beat_retry_times_--;
        if(slaver->beat_retry_times_ == 0) {
          // abandon the slaver and cancel the job
          butil::IOBuf log;
          braft::Task task;
          log.push_back(OP_CANCEL);
          log.append(&(slaver->cur_job_), sizeof(uint32_t));
          log.append(&(slaver->cur_subjob_), sizeof(uint32_t));
          task.data = &log;
          if(FLAGS_check_term) {
            // avoid ABA
            task.expected_term = master->raft_leader_term_.load(butil::memory_order_acquire);
          }
          master->raft_node_->apply(task);
          error_slavers.push_back(slaver->name_);
        }
      } else if(!beat_response.ok()) {
        // it is not slaver
        spdlog::info("[bgbeater] fail to beat: not slaver");
        butil::IOBuf log;
        braft::Task task;
        log.push_back(OP_CANCEL);
        log.append(&(slaver->cur_job_), sizeof(uint32_t));
        log.append(&(slaver->cur_subjob_), sizeof(uint32_t));
        task.data = &log;
        if(FLAGS_check_term) {
          // avoid ABA
          task.expected_term = master->raft_leader_term_.load(butil::memory_order_acquire);
        }
        master->raft_node_->apply(task);
        error_slavers.push_back(slaver->name_);
      } else {
        spdlog::info("[bgbeater] successfully beat, worker state: {}", (int)beat_response.state());
        slaver->state_ = beat_response.state();
        if(slaver->state_ == WorkerState::CLOSE) {
          butil::IOBuf log;
          braft::Task task;
          log.push_back(OP_CANCEL);
          log.append(&(slaver->cur_job_), sizeof(uint32_t));
          log.append(&(slaver->cur_subjob_), sizeof(uint32_t));
          task.data = &log;
          if(FLAGS_check_term) {
            // avoid ABA
            task.expected_term = master->raft_leader_term_.load(butil::memory_order_acquire);
          }
          master->raft_node_->apply(task);
          error_slavers.push_back(slaver->name_);
        } else if(slaver->state_ == WorkerState::WAIT2MAP || slaver->state_ == WorkerState::WAIT2REDUCE) {
          google::protobuf::Empty start_request;
          WorkerReplyMsg start_response;
          cntl.Reset();
          slaver->stub_->Start(&cntl, &start_request, &start_response, NULL);
        }
      }
    }
    for(const std::string& worker_name : error_slavers) {
      master->slavers_.erase(worker_name);
    }
  }
}

void Master::BGScaner(Master* master) {
  spdlog::info("[bgscaner] scaner start");

  while(!master->end_) {
    std::this_thread::sleep_for(std::chrono::seconds(SCAN_PERIOD_SECOND));
    std::unique_lock<std::mutex> scan_lck(master->dist_mtx_);
    master->scan_cv_.wait(scan_lck, [&] {
      return (!master->jobs_.empty() && master->raft_node_->is_leader()) || master->end_;
    });
    if(master->end_) {
      break;
    }

    for(auto& [job_id, job] : master->jobs_) {
      // check result timeout
      if(job->stage_ == JobStage::FINISHED) {
        if(time(NULL) - job->ftime > JOB_RESULT_TIMEOUT_SECOND) {
          butil::IOBuf log;
          braft::Task task;
          log.push_back(OP_DELETEJOB);
          log.append(&job_id, sizeof(uint32_t));
          task.data = &log;
          if(FLAGS_check_term) {
            // avoid ABA
            task.expected_term = master->raft_leader_term_.load(butil::memory_order_acquire);
          }
          master->raft_node_->apply(task);
        }
      } else {
        // check subjob working timeout
        for(SubJob& subjob : job->subjobs_) {
          if(subjob.dtime != -1 && time(NULL) - subjob.dtime > SUBJOB_WORKING_TIMEOUT_SECOND) {
            std::unique_lock<std::mutex> slavers_lck(master->slavers_mtx_);
            master->slavers_.erase(subjob.worker_name_);
            slavers_lck.unlock();
            butil::IOBuf log;
            braft::Task task;
            log.push_back(OP_CANCEL);
            log.append(&job_id, sizeof(uint32_t));
            log.append(&(subjob.subjob_id_), sizeof(uint32_t));
            task.data = &log;
            if(FLAGS_check_term) {
              // avoid ABA
              task.expected_term = master->raft_leader_term_.load(butil::memory_order_acquire);
            }
            master->raft_node_->apply(task);
          }
        }
      }
    }
  }
}

void Master::apply_from_rpc(OpType op_type, 
                            const google::protobuf::Message* request, 
                            google::protobuf::Message* response, 
                            google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  if(!raft_node_->is_leader()) {
    return redirect(op_type, response);
  }
  const uint64_t term = raft_leader_term_.load(butil::memory_order_acquire);
  butil::IOBuf log;
  log.push_back(op_type);
  butil::IOBufAsZeroCopyOutputStream wrapper(&log);
  if(!request->SerializeToZeroCopyStream(&wrapper)) {
    LOG(ERROR) << "Fail to serialize request";
    set_reply_ok(op_type, response, false);
    return ;
  }
  braft::Task task;
  task.data = &log;
  task.done = get_closure(op_type, request, response, done_guard.release());
  if(FLAGS_check_term) {
    // avoid ABA
    task.expected_term = term;
  }
  return raft_node_->apply(task);
}

Status Master::handle_register(const std::string name, const butil::EndPoint ep, 
                               const std::vector<std::string> acceptable_job_type) {
  for(const std::string& type : acceptable_job_type) {
    Status s = etcd_add_type_worker(type, name, ep);
    if(!s.ok()) {
      return s;
    }
  }
  return Status::Ok("");
}

Status Master::handle_launch(const std::string& name, const std::string& type, 
                             const std::string& token, uint32_t map_worker_num, 
                             uint32_t reduce_worker_num, MapIns& map_kvs, _OUT uint32_t* job_id) {
  if(map_worker_num <= 0 || reduce_worker_num <= 0) {
    return Status::Error("Map worker and Reduce worker must be greater than 0.");
  }
  if(map_kvs.size() <= 0) {
    return Status::Error("Empty key-value.");
  }

  Job* new_job = new Job(new_job_id(), name, type, token, map_worker_num, reduce_worker_num, std::move(map_kvs));
  spdlog::info("[handle launch] create a new job {}", new_job->id_);
  new_job->stage_ = JobStage::WAIT2PARTITION4MAP;
  new_job->Partition();
  // add new job to set
  spdlog::debug("[handle launch] add new job to set");
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  jobs_[new_job->id_] = new_job;
  jobs_lck.unlock();
  // leader add subjob to dist waiting list
  if(raft_node_->is_leader()) {
    spdlog::info("[handle launch] leader add {} subjobs to dist waiting list", new_job->subjobs_.size());
    std::unique_lock<std::mutex> dist_lck(jobs_mtx_);
    for(size_t i = 0; i < new_job->subjobs_.size(); i++) {
      jobs_waiting_dist_.emplace_back(new_job->id_, i);
    } 
    dist_lck.unlock();
    dist_cv_.notify_all();
  }

  *job_id = new_job->id_;
  return Status::Ok("");
}

Status Master::handle_distribute(const uint32_t job_id, const uint32_t subjob_id, const std::string& worker_name) {
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  if(jobs_.count(job_id) != 0 && jobs_[job_id]->subjobs_.size() > subjob_id) {
    jobs_[job_id]->subjobs_[subjob_id].worker_name_ = worker_name;
    jobs_[job_id]->subjobs_[subjob_id].dtime = time(NULL);
  } else {
    return Status::Error("no such job or subjob");
  }
  return Status::Ok("");
}

Status Master::handle_cancel(const uint32_t job_id, const uint32_t subjob_id) {
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  if(jobs_.count(job_id) != 0 && jobs_[job_id]->subjobs_.size() > subjob_id && !jobs_[job_id]->subjobs_[subjob_id].finished_) {
    jobs_[job_id]->subjobs_[subjob_id].worker_name_.clear();
    jobs_[job_id]->subjobs_[subjob_id].dtime = -1;
    jobs_lck.unlock();
    std::unique_lock<std::mutex> dist_lck(dist_mtx_);
    jobs_waiting_dist_.emplace_front(job_id, subjob_id);
  } else {
    return Status::Error("no such job or subjob");
  }
  return Status::Ok("");
}

Status Master::handle_complete_map(const uint32_t job_id, const uint32_t subjob_id, const std::string worker_name, MapOuts& map_result) {
  // check job exists
  if(jobs_.count(job_id) == 0) {
    return Status::Error("job does not exist");
  }
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  Job* job = jobs_[job_id];
  // check job state
  if(job->stage_ != JobStage::MAPPING) {
    return Status::Error("job is not in MAPPING stage");
  }
  // check subjob is legal
  if(job->subjobs_.size() <= subjob_id) {
    return Status::Error("Subjob id is not legal");
  }
  // check subjob has been distributed to the worker 
  if(job->subjobs_[subjob_id].worker_name_ != worker_name) {
    return Status::Error("subjob has not distribute to this worker");
  }
  // check subjob has not been finished
  if(job->subjobs_[subjob_id].finished_) {
    return Status::Error("subjob has been finished");
  }

  job->subjobs_[subjob_id].result_ = reinterpret_cast<void*>(new MapOuts(std::move(map_result)));
  job->subjobs_[subjob_id].finished_ = true;
  job->unfinished_job_num_--;
  jobs_lck.unlock();
  if(job->unfinished_job_num_ == 0) {
    // merge
    spdlog::info("[handle complete map] all subjobs have been finished, start merge and partition");
    job->stage_ = JobStage::WAIT2MERGE;
    job->Merge();
    if(job->reduce_kvs_.size() == 0) {
      job->stage_ = JobStage::FINISHED;
      jobs_finished_.insert(job->id_);
    } else {
      job->stage_ = JobStage::WAIT2PARTITION4REDUCE;
      job->Partition();
      if(raft_node_->is_leader()) {
        std::unique_lock<std::mutex> slavers_lck(slavers_mtx_);
        slavers_.erase(worker_name);
        slavers_lck.unlock();
        std::unique_lock<std::mutex> dist_lck(jobs_mtx_);
        for(size_t i = 0; i < job->subjobs_.size(); i++) {
          jobs_waiting_dist_.emplace_back(job->id_, i);
        } 
        dist_lck.unlock();
        dist_cv_.notify_all();
      }
    }
  }
  return Status::Ok("");
}

Status Master::handle_complete_reduce(const uint32_t job_id, const uint32_t subjob_id, const std::string worker_name, ReduceOuts& reduce_result) {
  // check job exists
  if(jobs_.count(job_id) == 0) {
    return Status::Error("job does not exist");
  }
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  Job* job = jobs_[job_id];
  // check job state
  if(job->stage_ != JobStage::MAPPING) {
    return Status::Error("job is not in MAPPING stage");
  }
  // check subjob is legal
  if(job->subjobs_.size() <= subjob_id) {
    return Status::Error("Subjob id is not legal");
  }
  // check subjob has been distributed to the worker 
  if(job->subjobs_[subjob_id].worker_name_ != worker_name) {
    return Status::Error("subjob has not distribute to this worker");
  }
  // check subjob has not been finished
  if(job->subjobs_[subjob_id].finished_) {
    return Status::Error("subjob has been finished");
  }

  job->subjobs_[subjob_id].result_ = reinterpret_cast<void*>(new ReduceOuts(std::move(reduce_result)));
  job->subjobs_[subjob_id].finished_ = true;
  job->unfinished_job_num_--;
  jobs_lck.unlock();
  if(job->unfinished_job_num_ == 0) {
    // merge
    spdlog::info("[handle map map] all subjobs have been finished, finish job");
    job->stage_ = JobStage::WAIT2FINISH;
    job->Finish();
    jobs_finished_.insert(job->id_);
    if(raft_node_->is_leader()) {
      std::unique_lock<std::mutex> slavers_lck(slavers_mtx_);
      slavers_.erase(worker_name);
    }
  }
  return Status::Ok("");
}

Status Master::handle_get_result(const uint32_t job_id, const std::string token, _OUT ReduceOuts& result) {
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  if(jobs_.count(job_id) == 0) {
    return Status::Error("not such job");
  }
  Job* job = jobs_[job_id];
  if(!job->check_token(token)) {
    return Status::Error("error token");
  }
  if(job->stage_ != JobStage::FINISHED) {
    return Status::Error("job has not been finished");
  }
  result = job->results_;
  return Status::Ok("");
}

Status Master::handle_delete_job(uint32_t job_id) {
  std::unique_lock<std::mutex> jobs_lck(jobs_mtx_);
  if(jobs_.count(job_id) != 0) {
    delete jobs_[job_id];
    jobs_.erase(job_id);
  } else {
    return Status::Error("no such job");
  }
  return Status::Ok("");
}

void Master::on_apply(braft::Iterator& iter) {
  for(; iter.valid(); iter.next()) {
    // async invoke iter.done()->Run(0)
    braft::AsyncClosureGuard done_guard(iter.done());

    butil::IOBuf data = iter.data();
    OpType op_type = OP_UNKNOWN;
    data.cutn(&op_type, sizeof(OpType));

    if(iter.done()) {
      // task is applied by this node
      spdlog::info("[on_apply] get log applied by this node");
      switch(op_type) {
      // launch job
      case OP_LAUNCH: {
        uint32_t job_id;
        MapIns map_kvs;
        LaunchClosure* c = dynamic_cast<LaunchClosure*>(iter.done());
        const LaunchMsg* request = c->request();
        for(size_t i = 0; i < request->kvs_size(); i++) {
          // get map key-values
          const auto& kv = request->kvs(i);
          map_kvs.emplace_back(kv.key(), kv.value());
        }
        std::sort(map_kvs.begin(), map_kvs.end(), [](const MapIn& lhs, const MapIn& rhs) -> bool {
          return lhs.first < rhs.first;
        });
        Status s = handle_launch(request->name(), request->type(), request->token(), request->mapper_num(), request->reducer_num(), map_kvs, &job_id);
        // response
        if(s.ok()) {
          spdlog::info("[apply : launch] successfully launch a job");
          set_reply_ok(OP_LAUNCH, c->response(), true);
        } else {
          spdlog::info("[apply : launch] fail to launch a job: {}", s.msg_);
          set_reply_ok(OP_LAUNCH, c->response(), false);
          set_reply_msg(OP_LAUNCH, c->response(), s.msg_);
        }
      }
        break;
      // complete map
      case OP_COMPLETEMAP: {
        MapOuts map_result;
        CompleteMapClosure* c = dynamic_cast<CompleteMapClosure*>(iter.done());
        const CompleteMapMsg* request = c->request();
        for(size_t i = 0; i < request->map_result_size(); ++i) {
          const auto& kv = request->map_result(i);
          map_result.emplace_back(kv.key(), kv.value());
        }
        Status s = handle_complete_map(request->job_id(), request->subjob_id(), request->worker_name(), map_result);
        if(s.ok()) {
          spdlog::info("[apply : completemap] successfully complete job {} subjob {} from worker {}", request->job_id(), request->subjob_id(), request->worker_name());
          set_reply_ok(OP_COMPLETEMAP, c->response(), true);
        } else {
          spdlog::info("[apply : completemap] fail to complete job {} subjob {} from worker {}", request->job_id(), request->subjob_id(), request->worker_name());
          set_reply_ok(OP_COMPLETEMAP, c->response(), false);
          set_reply_msg(OP_COMPLETEMAP, c->response(), s.msg_);
        }
      }
        break;
      // complete reduce
      case OP_COMPLETEREDUCE: {
        ReduceOuts reduce_result;
        CompleteReduceClosure* c = dynamic_cast<CompleteReduceClosure*>(iter.done());
        const CompleteReduceMsg* request = c->request();
        for(size_t i = 0; i < request->reduce_result_size(); ++i) {
          reduce_result.emplace_back(request->reduce_result(i));
        }
        Status s = handle_complete_reduce(request->job_id(), request->subjob_id(), request->worker_name(), reduce_result);
        if(s.ok()) {
          spdlog::info("[apply : completereduce] successfully complete job {} subjob {} from worker {}", request->job_id(), request->subjob_id(), request->worker_name());
          set_reply_ok(OP_COMPLETEREDUCE, c->response(), true);
        } else {
          spdlog::info("[apply : completereduce] fail to complete job {} subjob {} from worker {}", request->job_id(), request->subjob_id(), request->worker_name());
          set_reply_ok(OP_COMPLETEREDUCE, c->response(), false);
          set_reply_msg(OP_COMPLETEREDUCE, c->response(), s.msg_);
        }
      }
      }
    } else {
      // task is applied by others
      spdlog::info("[on_apply] get log applied by other node");
      switch(op_type) {
      // launch job
      case OP_LAUNCH: {
        uint32_t job_id;
        MapIns map_kvs;
        LaunchMsg request;
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        request.ParseFromZeroCopyStream(&wrapper);
        for(size_t i = 0; i < request.kvs_size(); i++) {
          // get map key-values
          const auto& kv = request.kvs(i);
          map_kvs.emplace_back(kv.key(), kv.value());
        }
        std::sort(map_kvs.begin(), map_kvs.end(), [](const MapIn& lhs, const MapIn& rhs) -> bool {
          return lhs.first < rhs.first;
        });
        Status s = handle_launch(request.name(), request.type(), request.token(), request.mapper_num(), request.reducer_num(), map_kvs, &job_id);
        if(s.ok()) {
          spdlog::info("[apply : launch] successfully launch a job");
        } else {
          spdlog::info("[apply : launch] fail to launch a job: {}", s.msg_);
        }
      }
        break;
      // distribute subjob
      case OP_DISTRIBUTE: {
        uint32_t job_id;
        uint32_t subjob_id;
        std::string worker_name;
        data.cutn(&job_id, sizeof(uint32_t));
        data.cutn(&subjob_id, sizeof(uint32_t));
        worker_name = data.to_string();
        Status s = handle_distribute(job_id, subjob_id, worker_name);
        if(s.ok()) {
          spdlog::info("[apply : distribute] distribute job {} subjob {} to worker {}", job_id, subjob_id, worker_name.c_str());
        } else {
          spdlog::info("[apply : distribute] fail to distribute job {} subjob {} to worker {} : {}", job_id, subjob_id, worker_name.c_str(), s.msg_.c_str());
        }
      }
        break;
      // cancel subjob
      case OP_CANCEL: {
        uint32_t job_id;
        uint32_t subjob_id;
        data.cutn(&job_id, sizeof(uint32_t));
        data.cutn(&subjob_id, sizeof(uint32_t));
        Status s = handle_cancel(job_id, subjob_id);
        if(s.ok()) {
          spdlog::info("[apply : cancel] cancel job {} subjob {} and prepare to redistribute", job_id, subjob_id);
          if(raft_node_->is_leader()) {
            dist_cv_.notify_all();
          }
        } else {
          spdlog::info("[apply : cancel] fail to cancel job {} subjob {} : {}", job_id, subjob_id, s.msg_.c_str());
        }
      }
        break;
      // complete map
      case OP_COMPLETEMAP: {
        CompleteMapMsg request;
        MapOuts map_result;
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        request.ParseFromZeroCopyStream(&wrapper);
        for(size_t i = 0; i < request.map_result_size(); ++i) {
          const auto& kv = request.map_result(i);
          map_result.emplace_back(kv.key(), kv.value());
        }
        Status s = handle_complete_map(request.job_id(), request.subjob_id(), request.worker_name(), map_result);
        if(s.ok()) {
          spdlog::info("[apply : completemap] successfully complete job {} subjob {} from worker {}", request.job_id(), request.subjob_id(), request.worker_name());
        } else {
          spdlog::info("[apply : completemap] fail to complete job {} subjob {} from worker {}", request.job_id(), request.subjob_id(), request.worker_name());
        }
      }
        break;
      // complete reduce
      case OP_COMPLETEREDUCE: {
        CompleteReduceMsg request;
        ReduceOuts reduce_result;
        butil::IOBufAsZeroCopyInputStream wrapper(iter.data());
        request.ParseFromZeroCopyStream(&wrapper);
        for(size_t i = 0; i < request.reduce_result_size(); ++i) {
          reduce_result.emplace_back(request.reduce_result(i));
        }
        Status s = handle_complete_reduce(request.job_id(), request.subjob_id(), request.worker_name(), reduce_result);
        if(s.ok()) {
          spdlog::info("[apply : completereduce] successfully complete job {} subjob {} from worker {}", request.job_id(), request.subjob_id(), request.worker_name());
        } else {
          spdlog::info("[apply : completereduce] fail to complete job {} subjob {} from worker {}", request.job_id(), request.subjob_id(), request.worker_name());
        }
      }
        break;
      // delete job
      case OP_DELETEJOB: {
        uint32_t job_id;
        data.cutn(&job_id, sizeof(uint32_t));
        Status s = handle_delete_job(job_id);
        if(s.ok()) {
          spdlog::info("[apply : deletejob] delete job {}", job_id);
        } else {
          spdlog::info("[apply : deletejob] failed to delete job {}", job_id);
        }
      }
        break;
      }
    }
  }
}

void Master::on_shutdown() {
  return ;
}

void Master::on_leader_start(int64_t term) {
  spdlog::info("[on_leader_start] leader start, term {}", term);
  raft_leader_term_.store(term, butil::memory_order_acquire);
  // recover the waiting list and slavers

  spdlog::info("[on_leader_start] scan jobs and recovery");
  for(const auto& [_, job] : jobs_) {
    for(SubJob& subjob : job->subjobs_) {
      if(!subjob.finished_) {
        // unfinished subjob
        if(subjob.worker_name_.empty()) {
          // subjobs that waiting for distributing, recover waiting list
          spdlog::info("[on_leader_start] push job {} subjob {} in distribution waiting list", job->id_, subjob.subjob_id_);
          std::unique_lock<std::mutex> dist_lck(dist_mtx_);
          jobs_waiting_dist_.emplace_back(job->id_, subjob.subjob_id_);
        } else {
          // subjobs that have been dirstributed, recover slaver
          spdlog::info("[on_leader_start] job {} subjob {} has been distributed to worker {}, add slaver", job->id_, subjob.subjob_id_, subjob.worker_name_.c_str());
          butil::EndPoint ep;
          Status s = etcd_get_type_worker(job->type_, subjob.worker_name_, ep);
          if(!s.ok()) {
            // failed to get worker endpoint, push into waiting list and redistribute
            spdlog::info("[on_leader_start] fail to get worker endpoint, push subjob into waiting list and redistribute");
            subjob.worker_name_.clear();
            std::unique_lock<std::mutex> dist_lck(dist_mtx_);
            jobs_waiting_dist_.emplace_back(job->id_, subjob.subjob_id_);
            continue;
          }

          // build channel
          brpc::Channel* chan = new brpc::Channel;
          brpc::ChannelOptions chan_options;
          chan_options.protocol = brpc::PROTOCOL_BAIDU_STD;
          chan_options.timeout_ms = 3000;
          if(chan->Init(ep, &chan_options) != 0) {
            spdlog::info("[on_leader_start] fail to initial channel, push subjob into waiting list and redistribute");
            delete chan;
            subjob.worker_name_.clear();
            std::unique_lock<std::mutex> dist_lck(dist_mtx_);
            jobs_waiting_dist_.emplace_back(job->id_, subjob.subjob_id_);
            continue;
          }
          WorkerService_Stub* stub = new WorkerService_Stub(chan);

          // add slaver
          std::unique_ptr<Slaver> new_slaver = std::make_unique<Slaver>(subjob.worker_name_, ep);
          new_slaver->channel_ = chan;
          new_slaver->stub_ = stub;
          new_slaver->cur_job_ = job->id_;
          new_slaver->cur_subjob_ = subjob.subjob_id_;
          std::unique_lock<std::mutex> slavers_lck(slavers_mtx_);
          slavers_[new_slaver->name_] = std::move(new_slaver);
        }
      }
    }
  }
}

void Master::on_leader_stop(const butil::Status& status) {
  raft_leader_term_.store(-1, butil::memory_order_acquire);
  slavers_.clear();
  jobs_waiting_dist_.clear();
}

void Master::redirect(OpType op_type, google::protobuf::Message* response) {
  set_reply_ok(op_type, response, false);
  if(raft_node_) {
    braft::PeerId leader = raft_node_->leader_id();
    if(!leader.is_empty()) {
      set_reply_redirect(op_type, response, leader.to_string());
    }
  }
}

Status Master::etcd_add_type_worker(std::string type, std::string worker_name, butil::EndPoint worker_ep) {
  std::string etcd_url = std::string("http://") + butil::endpoint2str(etcd_ep_).c_str() + "/v3/kv/put";
  std::string post_body, key, value;
  cpr::Response r;
  // (1) types/{type} = {}
  key = std::string("types/") + type;
  value = "";
  post_body = std::string()
              + "{"
              + "\"key\": \"" + tmapreduce::to_base64(key) + "\", "
              + "\"value\": \"" + tmapreduce::to_base64(value) + "\""
              + "}";
  if(cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body}).status_code != 200) {
    return Status::Error("Fail to add type to etcd");
  }
  
  // (2) {type}/{worker name}={worker endpoint}
  key = type + "/" + worker_name;
  value = butil::endpoint2str(worker_ep).c_str();
  post_body = std::string()
              + "{"
              + "\"key\": \"" + tmapreduce::to_base64(key) + "\", "
              + "\"value\": \"" + tmapreduce::to_base64(value) + "\""
              + "}";
  if(cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body}).status_code != 200) {
    return Status::Error("Fail to add worker to etcd");
  }

  return Status::Ok("");
}

Status Master::etcd_get_type_worker(std::string type, std::string worker_name, _OUT butil::EndPoint& worker_ep) {
  std::string etcd_url = std::string("http://") + butil::endpoint2str(etcd_ep_).c_str() + "/v3/kv/range";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64(type + "/" + worker_name) + "\", "
                          + "}";
  cpr::Response r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  if(r.status_code != 200) {
    return Status::Error("Fail to find worker from etcd");
  }

  // Parse
  rapidjson::Document doc;
  doc.Parse(r.text.c_str());
  if(doc.HasMember("kvs")) {
    auto kv = doc["kvs"].GetArray()[0].GetObject();
    std::string value = tmapreduce::from_base64(kv["value"].GetString());
    worker_ep.reset();
    butil::str2endpoint(value.c_str(), &worker_ep);
  } else {
    return Status::Error("Fail to find worker from etcd");
  }
  return Status::Ok("");
}

Status Master::etcd_get_workers_from_type(std::string type, _OUT std::unordered_map<std::string, butil::EndPoint>& workers) {
  std::string etcd_url = std::string("http://") + butil::endpoint2str(etcd_ep_).c_str() + "/v3/kv/range";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64(type + "/") + "\", "
                          + "\"range_end\": \"" + tmapreduce::to_base64(type + "/\xff") + "\""
                          + "}";
  cpr::Response r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  if(r.status_code != 200) {
    return Status::Error("Fail to find type workers from etcd");
  }

  // Parse
  workers.clear();
  rapidjson::Document doc;
  doc.Parse(r.text.c_str());
  if(doc.HasMember("kvs")) {
    auto kvs = doc["kvs"].GetArray();
    for(uint32_t i = 0; i < kvs.Size(); i++) {
      auto kv = kvs[i].GetObject();
      std::string key = tmapreduce::from_base64(kv["key"].GetString());
      std::string value = tmapreduce::from_base64(kv["value"].GetString());
      std::string worker_name(key.c_str() + type.size() + 1);   // get worker_name from {type}/{worker_name}
      butil::EndPoint worker_ep;
      butil::str2endpoint(value.c_str(), &worker_ep);
      workers[worker_name] = worker_ep;
    }
  } else {
    return Status::Error("Fail to find type workers from etcd");
  }
  return Status::Ok("");             
}

Status Master::etcd_delete_type_worker(std::string type, std::string worker_name) {
  std::string etcd_url = std::string("http://") + butil::endpoint2str(etcd_ep_).c_str() + "/v3/kv/deleterange";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64(type + "/" + worker_name) + "\""
                          + "}";
  if(cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body}).status_code != 200) {
    return Status::Error("Fail to delete worker in etcd");
  }
  return Status::Ok("");
}

Status Master::etcd_get_all_types(_OUT std::vector<std::string> types) {
  std::string etcd_url = std::string("http://") + butil::endpoint2str(etcd_ep_).c_str() + "/v3/kv/range";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64("types/") + "\", "
                          + "\"range_end\": \"" + tmapreduce::to_base64("types/\xff") + "\""
                          + "}";
  cpr::Response r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  if(r.status_code != 200) {
    return Status::Error("Fail to get all types from etcd");
  }

  // Parse
  types.clear();
  rapidjson::Document doc;
  doc.Parse(r.text.c_str());
  if(doc.HasMember("kvs")) {
    auto kvs = doc["kvs"].GetArray();
    for(uint32_t i = 0; i < kvs.Size(); i++) {
      auto kv = kvs[i].GetObject();
      std::string key = tmapreduce::from_base64(kv["key"].GetString());
      std::string type(key.c_str() + 6);   // get worker_name from {type}/{worker_name}
      types.push_back(type);
    }
  }
  return Status::Ok("");  
}

void Master::set_reply_ok(OpType op_type, google::protobuf::Message* response, bool ok) {
  switch(op_type) {
    case OP_REGISTER:
      reinterpret_cast<RegisterReplyMsg*>(response)->mutable_reply()->set_ok(ok);
      break;
    case OP_LAUNCH:
      reinterpret_cast<LaunchReplyMsg*>(response)->mutable_reply()->set_ok(ok);
      break;
    case OP_COMPLETEMAP: case OP_COMPLETEREDUCE:
      reinterpret_cast<MasterReplyMsg*>(response)->set_ok(ok);
      break;
    default:
      break;
  }
  return;
}

void Master::set_reply_msg(OpType op_type, google::protobuf::Message* response, std::string msg) {
  switch(op_type) {
    case OP_REGISTER:
      reinterpret_cast<RegisterReplyMsg*>(response)->mutable_reply()->set_msg(msg);
      break;
    case OP_LAUNCH:
      reinterpret_cast<LaunchReplyMsg*>(response)->mutable_reply()->set_msg(msg);
      break;
    case OP_COMPLETEMAP: case OP_COMPLETEREDUCE:
      reinterpret_cast<MasterReplyMsg*>(response)->set_msg(msg);
      break;
    default:
      break;
  }
  return;
}

void Master::set_reply_redirect(OpType op_type, google::protobuf::Message* response, std::string redirect) {
  switch(op_type) {
    case OP_REGISTER:
      reinterpret_cast<RegisterReplyMsg*>(response)->mutable_reply()->set_redirect(redirect);
      break;
    case OP_LAUNCH:
      reinterpret_cast<LaunchReplyMsg*>(response)->mutable_reply()->set_redirect(redirect);
      break;
    case OP_COMPLETEMAP: case OP_COMPLETEREDUCE:
      reinterpret_cast<MasterReplyMsg*>(response)->set_redirect(redirect);
      break;
    default:
      break;
  }
  return;
}

braft::Closure* Master::get_closure(OpType op_type, const google::protobuf::Message* request, google::protobuf::Message* response, google::protobuf::Closure* done) {
  switch(op_type) {
    case OP_LAUNCH:
      return new LaunchClosure(this, reinterpret_cast<const LaunchMsg*>(request), reinterpret_cast<LaunchReplyMsg*>(response), done);
    case OP_COMPLETEMAP: 
      return new CompleteMapClosure(this, reinterpret_cast<const CompleteMapMsg*>(request), reinterpret_cast<MasterReplyMsg*>(response), done);
    case OP_COMPLETEREDUCE:
      return new CompleteReduceClosure(this, reinterpret_cast<const CompleteReduceMsg*>(request), reinterpret_cast<MasterReplyMsg*>(response), done);
    default:
      return nullptr;
  }
}

MasterServiceImpl::MasterServiceImpl(uint32_t id, std::string name, butil::EndPoint etcd_ep)
  : master_(new Master(id, name, etcd_ep)) {}

MasterServiceImpl::~MasterServiceImpl() {
  if(master_ != nullptr) {
    delete master_;
  }
}

void MasterServiceImpl::Register(::google::protobuf::RpcController* controller,
                                 const ::tmapreduce::RegisterMsg* request,
                                 ::tmapreduce::RegisterReplyMsg* response,
                                 ::google::protobuf::Closure* done) {
  return master_->Register(request, response, done);
}

void MasterServiceImpl::Launch(::google::protobuf::RpcController* controller,
                               const ::tmapreduce::LaunchMsg* request,
                               ::tmapreduce::LaunchReplyMsg* response,
                               ::google::protobuf::Closure* done) {
  return master_->Launch(request, response, done);
}

void MasterServiceImpl::CompleteMap(::google::protobuf::RpcController* controller,
                                    const ::tmapreduce::CompleteMapMsg* request,
                                    ::tmapreduce::MasterReplyMsg* response,
                                    ::google::protobuf::Closure* done) {
  return master_->CompleteMap(request, response, done);
}

void MasterServiceImpl::CompleteReduce(::google::protobuf::RpcController* controller,
                                       const ::tmapreduce::CompleteReduceMsg* request,
                                       ::tmapreduce::MasterReplyMsg* response,
                                       ::google::protobuf::Closure* done) {
  return master_->CompleteReduce(request, response, done);
}

void MasterServiceImpl::GetResult(::google::protobuf::RpcController* controller,
                                  const ::tmapreduce::GetResultMsg* request,
                                  ::tmapreduce::GetResultReplyMsg* response,
                                  ::google::protobuf::Closure* done) {
  return master_->GetResult(request, response, done);
}

} // namespace tmapreduce
