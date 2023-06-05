#include <sys/types.h>
#include <fcntl.h>
#include <ctime>
#include <random>

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

int Master::Start() {
  butil::EndPoint addr(butil::my_ip(), FLAGS_port);
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
  apply_from_rpc(OP_DELETEJOB, request, response, done);
}

void Master::BGDistributor(Master* master) {
  spdlog::info("[bgdistributor] distributor start");

  auto& jobs_waiting_dist = master->jobs_waiting_dist_;
  brpc::Controller cntl;

  while(!master->end_) {
    std::unique_lock<std::mutex> dist_lck(master->dist_mtx_);
    master->dist_cv_.wait(dist_lck, [&] { 
      return (!jobs_waiting_dist.empty() || master->end_) && master->raft_node_->is_leader(); 
    });
    if(master->end_) {
      break;
    }

    // get job
    uint32_t job_id = jobs_waiting_dist.front().first;
    uint32_t subjob_id = jobs_waiting_dist.front().second;
    Job* job = master->jobs_[job_id];
    spdlog::info("[bgdistributor] try to distribute job {} subjob {}", job_id, subjob_id);

    // get job type and find worker
    std::unordered_map<std::string, butil::EndPoint> workers;
    Status s = master->etcd_get_type_worker(job->type_, workers);
    if(!s.ok()) {
      spdlog::info("[bgdistributor] fail to find workers that can process \"{}\" job", job->type_.c_str());
      continue;
    }

    // try to distribute
    spdlog::info("[bgdistributor] find {} workers", workers.size());
    bool success_dist = false;
    for(const auto& w : workers) {
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
      PrepareMsg request;
      WorkerReplyMsg response;
      request.set_master_id(master->id_);
      request.set_job_type(job->type_);
      stub->Prepare(&cntl, &request, &response, NULL);
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

Status Master::handle_register(std::string name, butil::EndPoint ep, 
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
  spdlog::info("[handle launch] create a new job {}", job_id);
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
    for(uint32_t i = 0; i < new_job->subjobs_.size(); i++) {
      jobs_waiting_dist_.emplace_back(new_job->id_, i);
    } 
    dist_lck.unlock();
    dist_cv_.notify_all();
  }

  *job_id = new_job->id_;
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
      // launch task
      case OP_LAUNCH:
      {
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

      case OP_DISTRIBUTE:
        break;
      }
    } else {
      // task is applied by others
      spdlog::info("[on_apply] get log applied by other node");
      switch(op_type) {

      case OP_LAUNCH:
      {
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
      }
    }

  }
}

void Master::on_leader_start(int64_t term) {
  raft_leader_term_.store(term, butil::memory_order_acquire);
  // recovery the waiting and slavers
  for(const auto& [_, job] : jobs_) {
    for(const SubJob& subjob : job->subjobs_) {
      if(!subjob.finished_) {
        if(subjob.worker_id_ == UINT32_MAX) {

        } else {
          
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

Status Master::etcd_get_type_worker(std::string type, _OUT std::unordered_map<std::string, butil::EndPoint>& workers) {
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
    case OP_LAUNCH:
      reinterpret_cast<LaunchReplyMsg*>(response)->mutable_reply()->set_ok(ok);
      break;
    case OP_DELETEJOB:
      reinterpret_cast<GetResultReplyMsg*>(response)->mutable_reply()->set_ok(ok);
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


} // namespace tmapreduce
