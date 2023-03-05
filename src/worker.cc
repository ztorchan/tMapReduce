#include <dlfcn.h>
#include <filesystem>

#include <butil/logging.h>

#include "mapreduce/worker.h"
#include "mapreduce/wrapper.h"

namespace mapreduce {

Worker::Worker(std::string name, std::string address, uint32_t port, 
               std::string mrf_path) :
    id_(UINT32_MAX),
    name_(name),
    address_(address),
    port_(port),
    mrf_path_(mrf_path),
    master_id_(UINT32_MAX),
    master_address_(""),
    master_port_(UINT32_MAX),
    channel_(),
    stub_(nullptr),
    state_(WorkerState::INIT),
    cur_type_(""),
    cur_job_id_(UINT32_MAX),
    cur_subjob_id_(UINT32_MAX),
    cur_job_name_(""),
    cur_job_type_(""),
    map_kvs_(),
    map_result_(),
    reduce_kvs_(),
    reduce_result_(),
    job_mtx_(),
    job_cv_(),
    end_(false) {}

Worker::~Worker() {
  if(stub_ != nullptr) 
    delete stub_;
}

Status Worker::Register(std::string master_address, uint32_t master_port) {
  // Connect
  if(stub_ != nullptr)
    delete stub_;
  brpc::ChannelOptions options;
  options.protocol = "baidu_std";
  options.timeout_ms = 1000;
  options.max_retry = 3;
  if(channel_.Init(master_address.c_str(), master_port, &options) != 0) {
    return Status::Error("Channel initial failed.");
  }
  MasterService_Stub* stub = new MasterService_Stub(&channel_);

  // Register
  brpc::Controller cntl;
  RegisterMsg request;
  RegisterReplyMsg response;
  request.set_address(address_);
  request.set_port(port_);
  stub->Register(&cntl, &request, &response, NULL);
  if(!cntl.Failed()) {
    if(response.reply().ok()) {
      id_ = response.worker_id();
      master_id_ = response.master_id();
      master_address_ = master_address;
      master_port_ = master_port;
      stub_ = stub;
      state_ = WorkerState::IDLE;
      return Status::Ok(response.reply().msg());
    } else {
      delete stub;
      return Status::Error(response.reply().msg());
    }
  }

  return Status::Error("Connected Failed.");
}

Status Worker::Map(uint32_t job_id, uint32_t subjob_id, std::string job_name,
                   std::string job_type, MapIns& map_kvs) {
  // Check worker state
  if(state_ != WorkerState::IDLE) {
    return Status::Error("Worker is not idle.");
  }
  // Check so file exist
  std::string so_path = mrf_path_ + cur_type_ + ".so";
  if(!std::filesystem::exists(so_path)) {
    return Status::Error("Corresponding shared library is not existing.");
  }

  // Prepare to map
  std::unique_lock<std::mutex> lck(job_mtx_);
  cur_job_id_ = job_id;
  cur_subjob_id_ = subjob_id;
  cur_job_name_ = job_name;
  cur_job_type_ = job_type;
  map_kvs_ = std::move(map_kvs);
  map_result_.clear();
  
  // start to map
  state_ = WorkerState::MAPPING;
  job_cv_.notify_all();

  return Status::Ok("");
}

Status Worker::Reduce(uint32_t job_id, uint32_t subjob_id, std::string job_name,
                      std::string job_type, ReduceIns& reduce_kvs) {
  // Check worker state
  if(state_ != WorkerState::IDLE) {
    return Status::Error("Worker is not idle.");
  }

  // Prepare to reduce
  std::unique_lock<std::mutex> lck(job_mtx_);
  cur_job_id_ = job_id;
  cur_subjob_id_ = subjob_id;
  cur_job_name_ = job_name;
  cur_job_type_ = job_type;
  reduce_kvs_ = std::move(reduce_kvs_);
  reduce_result_.clear();
  
  // start to reduce
  state_ = WorkerState::REDUCING;
  job_cv_.notify_all();

  return Status::Ok("");
}

void Worker::BGExecutor(Worker* worker) {
  MapIns& map_kvs = worker->map_kvs_;
  MapOuts& map_result = worker->map_result_;
  ReduceIns& reduce_kvs = worker->reduce_kvs_;
  ReduceOuts& reduce_result = worker->reduce_result_;

  void* mrf_handle;
  while(!worker->end_) {
    std::unique_lock<std::mutex> lck(worker->job_mtx_);
    worker->job_cv_.wait(lck);

    // Load so
    std::string so_path = worker->mrf_path_ + worker->cur_type_ + ".so";
    if(worker->cur_type_ != worker->cur_job_type_) {
      if(!std::filesystem::exists(so_path)) {
        // Re-distribute
      } else {
        mrf_handle = dlopen(so_path.c_str(), RTLD_NOW);
      }
    }
  }
}

WorkerServiceImpl::WorkerServiceImpl(std::string name, std::string address, uint32_t port,
                                     std::string mrf_path) :
    WorkerService(),
    worker_(new Worker(name, address, port, mrf_path)) {}

WorkerServiceImpl::~WorkerServiceImpl() {
  delete worker_;
}

Status WorkerServiceImpl::Register(std::string master_address, uint32_t master_port) {
  Status s = worker_->Register(master_address, master_port);
  if(!s.ok()) {
    LOG(INFO) << "Worker register successfully.";
  } else {
    LOG(INFO) << "Worker register failed: " << s.msg_ ;
  }
  return  s;
}

void WorkerServiceImpl::Beat(::google::protobuf::RpcController* controller,
          const ::google::protobuf::Empty* request,
          ::mapreduce::WorkerReplyMsg* response,
          ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  response->set_ok(true);
  response->set_msg("");
  response->set_state(worker_->state_);

  return ;
}

void WorkerServiceImpl::Map(::google::protobuf::RpcController* controller,
                            const ::mapreduce::MapJobMsg* request,
                            ::mapreduce::WorkerReplyMsg* response,
                            ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  MapIns map_kvs;
  for(int i = 0; i < request->map_kvs_size(); i++) {
    auto& kv = request->map_kvs(i);
    map_kvs.emplace_back(std::move(kv.key()), std::move(kv.value()));
  }
  Status s = worker_->Map(request->job_id(), request->subjob_id(), request->name(),
                          request->type(), map_kvs);

  response->set_ok(s.ok());
  response->set_msg(s.msg_);
  response->set_state(worker_->state_);
  return ;
}

void WorkerServiceImpl::Reduce(::google::protobuf::RpcController* controller,
                               const ::mapreduce::ReduceJobMsg* request,
                               ::mapreduce::WorkerReplyMsg* response,
                               ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  ReduceIns reduce_kvs;
  for(int i = 0; i < request->reduce_kvs_size(); i++) {
    auto& kv = request->reduce_kvs(i);
    ReduceIn tmp_kv(std::move(kv.key()), std::vector<std::string>());
    for(int j = 0; j < kv.value_size(); j++) {
      tmp_kv.second.emplace_back(std::move(kv.value(j)));
    }
    reduce_kvs.push_back(std::move(tmp_kv));
  }
  Status s = worker_->Reduce(request->job_id(), request->subjob_id(), request->name(),
                             request->type(), reduce_kvs);

  response->set_ok(s.ok());
  response->set_msg(s.msg_);
  response->set_state(worker_->state_);
  return ;
}

}