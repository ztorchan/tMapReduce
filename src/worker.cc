#include <dlfcn.h>
#include <filesystem>
#include <thread>

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
    master_stub_(nullptr),
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
    end_(false) {
  if(mrf_path_[mrf_path_.size() - 1] != '/') {
    mrf_path_.push_back('/');
  }
  std::thread executor(Worker::BGExecutor, this);
  executor.detach();
}

Worker::~Worker() {
  if(master_stub_ != nullptr) 
    delete master_stub_;
}

Status Worker::Register(std::string master_address, uint32_t master_port) {
  // Connect
  if(master_stub_ != nullptr)
    delete master_stub_;
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
      master_stub_ = stub;
      state_ = WorkerState::IDLE;
      return Status::Ok(response.reply().msg());
    } else {
      delete stub;
      return Status::Error(response.reply().msg());
    }
  }

  return Status::Error("Connected Failed.");
}

Status Worker::PrepareMap(uint32_t job_id, uint32_t subjob_id, std::string job_name,
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

Status Worker::PrepareReduce(uint32_t job_id, uint32_t subjob_id, std::string job_name,
                      std::string job_type, ReduceIns& reduce_kvs) {
  // Check worker state
  if(state_ != WorkerState::IDLE) {
    return Status::Error("Worker is not idle.");
  }

  // Check so file exist
  std::string so_path = mrf_path_ + cur_type_ + ".so";
  if(!std::filesystem::exists(so_path)) {
    return Status::Error("Corresponding shared library is not existing.");
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

  brpc::Controller cntl;

  void* mrf_handle;
  MAP_FUNC map_f = NULL;
  REDUCE_FUNC reduce_f = NULL;

  while(!worker->end_) {
    std::unique_lock<std::mutex> lck(worker->job_mtx_);
    worker->job_cv_.wait(lck);

    if(worker->end_) {
      break;
    }

    // Load so
    std::string so_path = worker->mrf_path_ + worker->cur_type_ + ".so";
    if(worker->cur_type_ != worker->cur_job_type_) {
      mrf_handle = dlopen(so_path.c_str(), RTLD_NOW);
      if(!mrf_handle) {
        // TODO : load failed, notify master
        continue;
      }
      dlerror();
      *(void**)(&map_f) = dlsym(mrf_handle, "c_Map");
      *(void**)(&reduce_f) = dlsym(mrf_handle, "c_Reduce");
    }

    // Map
    if(worker->state_ == WorkerState::MAPPING) {
      assert(map_kvs.size() != 0);
      uint32_t output_size;
      const char **input_keys = nullptr, **input_values = nullptr;
      char **output_keys = nullptr, **output_values = nullptr;

      input_keys = new const char*[map_kvs.size()];
      input_values = new const char*[map_kvs.size()];
      for(size_t i = 0; i < map_kvs.size(); i++) {
        input_keys[i] = map_kvs[i].first.c_str();
        input_values[i] = map_kvs[i].second.c_str();
      }

      map_f(input_keys, input_values, map_kvs.size(),
            output_keys, output_values, &output_size);

      // commit result
      cntl.Reset();
      MapResultMsg request;
      MasterReplyMsg response;
      request.set_job_id(worker->cur_job_id_);
      request.set_subjob_id(worker->cur_subjob_id_);
      request.set_worker_id(worker->id_);
      request.set_state(WorkerState::IDLE);
      for(uint32_t i = 0; i < output_size; i++) {
        auto kv_ptr = request.add_map_result();
        kv_ptr->set_key(output_keys[i]);
        kv_ptr->set_value(output_values[i]);
      }
      worker->master_stub_->CompleteMap(&cntl, &request, &response, NULL);
      worker->state_ = WorkerState::IDLE;  // TODO : process failed commit

      // release mem
      for(uint32_t i = 0; i < output_size; i++) {
        delete output_keys[i];
        delete output_values[i];
        output_keys[i] = nullptr;
        output_values[i] = nullptr;
      }
      delete []input_keys;
      delete []input_values;
      delete []output_keys;
      delete []output_values;
    } else {
      assert(reduce_kvs.size() != 0);
      uint32_t output_size;
      uint32_t* sizes = nullptr;
      const char **input_keys = nullptr, **input_values = nullptr;
      char** output_values = nullptr;

      uint32_t total_input_values_size = 0;
      input_keys = new const char*[reduce_kvs.size()];
      sizes = new uint32_t[reduce_kvs.size()];
      for(size_t i = 0; i < reduce_kvs.size(); i++) {
        const auto& kvs = reduce_kvs[i];
        input_keys[i] = kvs.first.c_str();
        sizes[i] = kvs.second.size();
        total_input_values_size += kvs.second.size();
      }
      input_values = new const char*[total_input_values_size];
      uint32_t sizes_ptr = 0;
      for(size_t i = 0; i < reduce_kvs.size(); i++) {
        for(size_t j = 0; j < sizes[j]; j++) {
          input_values[sizes_ptr++] = reduce_kvs[i].second[j].c_str();
        }
      }

      reduce_f(input_keys, input_values, sizes, reduce_kvs.size(),
               output_values, &output_size);
      
      // commit result
      cntl.Reset();
      ReduceResultMsg request;
      MasterReplyMsg response;
      request.set_job_id(worker->cur_job_id_);
      request.set_subjob_id(worker->cur_subjob_id_);
      request.set_worker_id(worker->id_);
      request.set_state(WorkerState::IDLE);
      for(uint32_t i = 0; i < output_size; i++) {
        request.add_reduce_result(output_values[i]);
      }
      worker->master_stub_->CompleteReduce(&cntl, &request, &response, NULL);

      // release mem
      for(uint32_t i = 0; i < output_size; i++) {
        delete output_values[i];
        output_values[i] = nullptr;
      }
      delete []input_keys;
      delete []input_values;
      delete []sizes;
      delete []output_values;
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
  if(s.ok()) {
    LOG(INFO) << "Register successfully. " 
              << "Get worker id " << worker_->id_
              << " and master id " << worker_->master_id_ << ".";
  } else {
    LOG(INFO) << "Register failed: " << s.msg_ ;
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

  LOG(INFO) << "Beat from master.";

  return ;
}

void WorkerServiceImpl::Map(::google::protobuf::RpcController* controller,
                            const ::mapreduce::MapJobMsg* request,
                            ::mapreduce::WorkerReplyMsg* response,
                            ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  LOG(INFO) << "Receive map job: "
            << "[Job Id] " << request->job_id() << ", "
            << "[Subjob Id] " << request->subjob_id() << ", "
            << "[Job Name] " << request->name() << ", "
            << "[Job Type] " << request->type();

  MapIns map_kvs;
  for(int i = 0; i < request->map_kvs_size(); i++) {
    auto& kv = request->map_kvs(i);
    map_kvs.emplace_back(std::move(kv.key()), std::move(kv.value()));
  }

  Status s = worker_->PrepareMap(request->job_id(), request->subjob_id(), request->name(),
                          request->type(), map_kvs);
  response->set_ok(s.ok());
  response->set_msg(s.msg_);
  response->set_state(worker_->state_);
  if(s.ok()) {
    LOG(INFO) << "Successfully prepare map job.";
  } else {
    LOG(ERROR) << "Failed to prepare map job: " << s.msg_;
  }
  return ;
}

void WorkerServiceImpl::Reduce(::google::protobuf::RpcController* controller,
                               const ::mapreduce::ReduceJobMsg* request,
                               ::mapreduce::WorkerReplyMsg* response,
                               ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  LOG(INFO) << "Receive reduce job: "
            << "[Job Id] " << request->job_id() << ", "
            << "[Subjob Id] " << request->subjob_id() << ", "
            << "[Job Name] " << request->name() << ", "
            << "[Job Type] " << request->type();

  ReduceIns reduce_kvs;
  for(int i = 0; i < request->reduce_kvs_size(); i++) {
    auto& kv = request->reduce_kvs(i);
    ReduceIn tmp_kv(std::move(kv.key()), std::vector<std::string>());
    for(int j = 0; j < kv.value_size(); j++) {
      tmp_kv.second.emplace_back(std::move(kv.value(j)));
    }
    reduce_kvs.push_back(std::move(tmp_kv));
  }

  Status s = worker_->PrepareReduce(request->job_id(), request->subjob_id(), request->name(),
                             request->type(), reduce_kvs);
  response->set_ok(s.ok());
  response->set_msg(s.msg_);
  response->set_state(worker_->state_);

  if(s.ok()) {
    LOG(INFO) << "Successfully prepare reduce job.";
  } else {
    LOG(ERROR) << "Failed to prepare reduce job: " << s.msg_;
  }
  return ;
}

}