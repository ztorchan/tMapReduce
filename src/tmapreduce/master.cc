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

Slaver::Slaver(uint32_t id, std::string name, butil::EndPoint endpoint) 
  : id_(id)
  , name_(name)
  , endpoint_(endpoint)
  , channel_()
  , stub_(nullptr)
  , beat_retry_times_(0)
  , state_(WorkerState::INIT)
  , cur_job_(UINT32_MAX)
  , cur_subjob_(UINT32_MAX) {
  brpc::ChannelOptions options;
  options.protocol = brpc::PROTOCOL_BAIDU_STD;
  options.timeout_ms = 5000;
  options.max_retry = 3;
  if(channel_.Init(endpoint_, &options) != 0) {
    state_ = WorkerState::UNKNOWN;
    return;
  }
  stub_ = new WorkerService_Stub(&channel_);
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
  // directly apply rpc content
  apply_from_rpc(OP_REGISTER, request, response, done);
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
  log.push_back((uint8_t)op_type);
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

Status Master::handle_register(std::string name, butil::EndPoint ep, const std::vector<std::string> acceptable_job_type, _OUT uint32_t* slaver_id) {
  Slaver* s = new Slaver(new_slaver_id(), name, ep);
  if(s->state_ != WorkerState::INIT) {
    delete s;
    return Status::Error("Slaver initial failed");
  }
  WorkerService_Stub* stub = s->stub_;
  std::unique_lock<std::mutex> lck(slavers_mtx_);
  slavers_[s->id_] = s;
  *slaver_id = s->id_;

  if(raft_node_->is_leader()) {
    // leader need to save acceptable_job_type in etcd
    std::string etcd_basic_url = std::string("http://") + butil::endpoint2str(etcd_ep_) + "/v3/kv/put";
    for(const string& job_type : acceptable_job_type) {
      std::string post_body = std::string("")
                              + "{"
                              + "\"key\": \"" + job_type + "/" + std::to_string(s->id_) + "/" + "endpoint" + "\","
                              + "\"value\": \"" + butil::endpoint2str(s->endpoint_) + "\""
                              + "}";
      cpr::Response r = cpr::Post(cpr::Url{etcd_basic_url},
                                  cpr::Body{post_body},
                                  cpr::Header{{"Content-Type", "application/json"}});
      r.S
      
    }
  }

  return Status::Ok("");
}

Status Master::real_Launch(const std::string& name, const std::string& type, const std::string& job_secret, uint32_t map_worker_num, uint32_t reduce_worker_num, MapIns& map_kvs, _OUT uint32_t* job_id) {
  if(map_worker_num <= 0 || reduce_worker_num <= 0) {
    return Status::Error("Map worker and Reduce worker must be greater than 0.");
  }
  if(map_kvs.size() <= 0) {
    return Status::Error("Empty key-value.");
  }

  Job* new_job = new Job(new_job_id(), name, type, job_secret, map_worker_num, reduce_worker_num, std::move(map_kvs));
  new_job->stage_ = JobStage::WAIT2MAP;
  new_job->Partition();
  std::unique_lock<std::mutex> lck(jobs_mtx_);
  jobs_[new_job->id_] = new_job;
  for(size_t i = 0; i < new_job->subjobs_.size(); i++) {
    jobs_waiting_dist_.emplace_back(new_job->id_, i);
  }
  
  *job_id = new_job->id_;
  lck.unlock();
  dist_cv_.notify_all();
  return Status::Ok("");
}

void Master::on_apply(braft::Iterator& iter) {
  for(; iter.valid(); iter.next()) {
    // async invoke iter.done()->Run(0)
    braft::AsyncClosureGuard done_guard(iter.done());

    butil::IOBuf data = iter.data();
    uint8_t op_type = OP_UNKNOWN;
    data.cutn(&op_type, sizeof(uint8_t));


  }
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
    case OP_REGISTER:
      reinterpret_cast<RegisterReplyMsg*>(response)->mutable_reply()->set_msg(msg);
      break;
    case OP_LAUNCH:
      reinterpret_cast<LaunchReplyMsg*>(response)->mutable_reply()->set_msg(msg);
      break;
    case OP_GETRESULT:
      reinterpret_cast<GetResultReplyMsg*>(response)->mutable_reply()->set_msg(msg);
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
    case OP_GETRESULT:
      reinterpret_cast<GetResultReplyMsg*>(response)->mutable_reply()->set_redirect(redirect);
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
    case OP_REGISTER:
      return new RegisterClosure(this, reinterpret_cast<const RegisterMsg*>(request), reinterpret_cast<RegisterReplyMsg*>(response), done);
    case OP_LAUNCH:
      return new LaunchClosure(this, reinterpret_cast<const LaunchMsg*>(request), reinterpret_cast<LaunchReplyMsg*>(response), done);
    case OP_GETRESULT:
      return new GetResultClosure(this, reinterpret_cast<const GetResultMsg*>(request), reinterpret_cast<GetResultReplyMsg*>(response), done);
    case OP_COMPLETEMAP: 
      return new CompleteMapClosure(this, reinterpret_cast<const CompleteMapMsg*>(request), reinterpret_cast<MasterReplyMsg*>(response), done);
    case OP_COMPLETEREDUCE:
      return new CompleteReduceClosure(this, reinterpret_cast<const CompleteReduceMsg*>(request), reinterpret_cast<MasterReplyMsg*>(response), done);
    default:
      return nullptr;
  }
}


} // namespace tmapreduce
