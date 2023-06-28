#include <dlfcn.h>
#include <filesystem>
#include <thread>

#include <spdlog/spdlog.h>
#include <braft/raft.h>
#include <braft/route_table.h>

#include "tmapreduce/worker.h"
#include "tmapreduce/wrapper.h"

namespace tmapreduce {

Worker::Worker(std::string name, butil::EndPoint ep, std::string mrf_path) 
  : name_(name)
  , ep_(ep)
  , mrf_path_(mrf_path)
  , state_(WorkerState::INIT)
  , mrf_type_()
  , mrf_handle_(nullptr)
  , cur_master_id_(UINT32_MAX)
  , cur_master_group_()
  , state_mtx_()
  , cur_job_id_(UINT32_MAX)
  , cur_subjob_id_(UINT32_MAX)
  , cur_job_name_("")
  , cur_job_type_("")
  , map_kvs_()
  , map_result_()
  , reduce_kvs_()
  , reduce_result_()
  , job_mtx_()
  , job_cv_()
  , end_(false) {
  if(mrf_path_[mrf_path_.size() - 1] != '/') {
    mrf_path_.push_back('/');
  }
  std::thread executor(Worker::BGExecutor, this);
  executor.detach();
  state_ = WorkerState::IDLE;
}

Worker::~Worker() {
  if(mrf_handle_ != nullptr) {
    dlclose(mrf_handle_);
  }
}

void Worker::Reset() {
  state_ = WorkerState::IDLE;
  mrf_type_.clear();
  if(mrf_handle_ != NULL) {
    dlclose(mrf_handle_);
  }
  mrf_handle_ = nullptr;
  cur_master_id_ = UINT32_MAX;
  cur_master_group_.clear();
  cur_job_id_ = UINT32_MAX;
  cur_subjob_id_ = UINT32_MAX;
  cur_job_name_.clear();
  cur_job_type_.clear();
  map_kvs_.clear();
  map_result_.clear();
  reduce_kvs_.clear();
  reduce_result_.clear();
}

void Worker::InitRegister(std::vector<butil::EndPoint> master_eps, std::vector<std::string> acceptable_job_types) {
  brpc::Controller cntl;
  RegisterMsg request;
  RegisterReplyMsg response;
  brpc::ChannelOptions options;
  options.protocol = brpc::PROTOCOL_BAIDU_STD;
  for(butil::EndPoint ep : master_eps) {
    spdlog::info("[register] try to register to master {}", butil::endpoint2str(ep).c_str());
    brpc::Channel channel;
    if(channel.Init(ep, &options) != 0) {
      spdlog::info("[register] fail to initial channel");
      continue;
    }
    MasterService_Stub stub(&channel);

    // Register
    cntl.Reset();
    request.Clear();
    response.Clear();
    request.set_name(name_);
    request.set_ep(butil::endpoint2str(ep_).c_str());
    for(const std::string& type : acceptable_job_types) {
      request.add_acceptable_job_type(type);
    }
    stub.Register(&cntl, &request, &response, NULL);
  }
}

Status Worker::Prepare(uint32_t master_id, std::string master_group, std::string master_conf, std::string job_type) {
  std::unique_lock<std::mutex> lck(state_mtx_);
  // check
  if(cur_master_id_ != UINT32_MAX) {
    return Status::Error("The worker already has a master");
  }
  std::string so_path = mrf_path_ + job_type + ".so";
  if(!std::filesystem::exists(so_path)) {
    return Status::Error("Corresponding shared library is not existing.");
  }
  // load so
  mrf_handle_ = dlopen(so_path.c_str(), RTLD_NOW);
  if(mrf_handle_ == NULL) {
    mrf_handle_ = nullptr;
    return Status::Error("Fail to load so library");
  }
  if(braft::rtb::update_configuration(master_group, master_conf) != 0) {
    dlclose(mrf_handle_);
    mrf_handle_ = nullptr;
    return Status::Error("Fail to update braft configuration");
  }
  cur_master_id_ = master_id;
  cur_master_group_ = master_group;
  mrf_type_ = job_type;
  return Status::Ok("");
}

Status Worker::PrepareMap(uint32_t master_id, uint32_t job_id, uint32_t subjob_id, 
                          std::string job_name, std::string job_type, MapIns& map_kvs) {
  // Check master
  if(master_id != cur_master_id_) {
    return Status::Error("The worker already has a master");
  }
  // Check worker state
  if(state_ != WorkerState::IDLE) {
    return Status::Error("Worker is not idle.");
  }
  // Check job type
  if(job_type != mrf_type_) {
    return Status::Error("Job type is different from the given type in prepare stage");
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
  state_ = WorkerState::WAIT2MAP;
  return Status::Ok("");
}

Status Worker::PrepareReduce(uint32_t master_id, uint32_t job_id, uint32_t subjob_id, 
                             std::string job_name, std::string job_type, ReduceIns& reduce_kvs) {
  // Check master
  if(master_id != cur_master_id_) {
    return Status::Error("The worker already has a master");
  }
  // Check worker state
  if(state_ != WorkerState::IDLE) {
    return Status::Error("Worker is not idle.");
  }
  // Check job type
  if(job_type != mrf_type_) {
    return Status::Error("Job type is different from the given type in prepare stage");
  }

  // Prepare to reduce
  std::unique_lock<std::mutex> lck(job_mtx_);
  cur_job_id_ = job_id;
  cur_subjob_id_ = subjob_id;
  cur_job_name_ = job_name;
  cur_job_type_ = job_type;
  reduce_kvs_ = std::move(reduce_kvs);
  reduce_result_.clear();
  
  // start to reduce
  state_ = WorkerState::WAIT2REDUCE;
  return Status::Ok("");
}

void Worker::TryWakeupExecutor() {
  job_cv_.notify_all();
}

void Worker::BGExecutor(Worker* worker) {
  LOG(INFO) << "Executor Start.";
  MapIns& map_kvs = worker->map_kvs_;
  MapOuts& map_result = worker->map_result_;
  ReduceIns& reduce_kvs = worker->reduce_kvs_;
  ReduceOuts& reduce_result = worker->reduce_result_;

  brpc::Controller cntl;

  MAP_FUNC map_f = NULL;
  REDUCE_FUNC reduce_f = NULL;

  while(!worker->end_) {
    std::unique_lock<std::mutex> lck(worker->job_mtx_);
    worker->job_cv_.wait(lck, [&]() {
      return worker->state_ == WorkerState::WAIT2MAP 
             || worker->state_ == WorkerState::WAIT2REDUCE 
             || worker->state_ == WorkerState::WAIT2SUBMITMAP 
             || worker->state_ == WorkerState::WAIT2SUBMITREDUCE
             || worker->end_;
    });
    if(worker->end_) {
      break;
    }

    if(worker->state_ == WorkerState::WAIT2MAP) {
      assert(map_kvs.size() != 0);
      worker->state_ = WorkerState::MAPPING;
      uint32_t output_size;
      const char **input_keys = nullptr, **input_values = nullptr;
      char **output_keys = nullptr, **output_values = nullptr;

      input_keys = new const char*[map_kvs.size()];
      input_values = new const char*[map_kvs.size()];
      for(size_t i = 0; i < map_kvs.size(); i++) {
        input_keys[i] = map_kvs[i].first.c_str();
        input_values[i] = map_kvs[i].second.c_str();
      }

      map_result.clear();
      *(void**)(&map_f) = dlsym(worker->mrf_handle_, "c_Map");
      map_f(input_keys, input_values, map_kvs.size(), &output_keys, &output_values, &output_size);
      for(uint32_t i = 0; i < output_size; i++) {
        map_result.emplace_back(std::string(output_keys[i]), std::string(output_values[i]));
      }
      worker->state_ = WorkerState::WAIT2SUBMITMAP;
      spdlog::info("[bgexecutor] finish map job, job id: {}, subjob id: {}", worker->cur_job_id_, worker->cur_subjob_id_);

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
    } else if(worker->state_ == WorkerState::WAIT2REDUCE) {
      assert(reduce_kvs.size() != 0);
      worker->state_ = WorkerState::REDUCING;
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
        for(size_t j = 0; j < sizes[i]; j++) {
          input_values[sizes_ptr++] = reduce_kvs[i].second[j].c_str();
        }
      }

      reduce_result.clear();
      *(void**)(&reduce_f) = dlsym(worker->mrf_handle_, "c_Reduce");
      reduce_f(input_keys, input_values, sizes, reduce_kvs.size(), &output_values, &output_size);
      for(uint32_t i = 0; i < output_size; i++) {
        reduce_result.push_back(std::string(output_values[i]));
      }
      worker->state_ = WorkerState::WAIT2SUBMITREDUCE;
      spdlog::info("[bgexecutor] finish reduce job, job id: {}, subjob id: {}", worker->cur_job_id_, worker->cur_subjob_id_);
      
      // release mem
      for(uint32_t i = 0; i < output_size; i++) {
        delete output_values[i];
        output_values[i] = nullptr;
      }
      delete []input_keys;
      delete []input_values;
      delete []sizes;
      delete []output_values;
    } else if(worker->state_ == WorkerState::WAIT2SUBMITMAP) {
      // select leader master
      braft::PeerId leader;
      if(braft::rtb::select_leader(worker->cur_master_group_, &leader) != 0) {
        spdlog::info("[bgexecutor] fail to select leader in {}, refresh", worker->cur_master_group_.c_str());
        braft::rtb::refresh_leader(worker->cur_master_group_, 1000);
        continue;
      }

      // initial channel
      brpc::Channel channel;
      if(channel.Init(leader.addr, NULL) != 0) {
        spdlog::info("[bgexecutor] fail to initial channel");
        continue;
      }
      MasterService_Stub stub(&channel);

      // submit map result
      cntl.Reset();
      CompleteMapMsg request;
      MasterReplyMsg response;
      request.set_job_id(worker->cur_job_id_);
      request.set_subjob_id(worker->cur_subjob_id_);
      request.set_worker_name(worker->name_);
      for(size_t i = 0; i < map_result.size(); i++) {
        auto kv_ptr = request.add_map_result();
        kv_ptr->set_key(map_result[i].first);
        kv_ptr->set_value(map_result[i].second);
      }
      stub.CompleteMap(&cntl, &request, &response, NULL);
      if(!response.ok()) {
        spdlog::info("[bgexecutor] fail to submit map, job id: {}, subjob id: {}", worker->cur_job_id_, worker->cur_subjob_id_);
        continue;
      }
      spdlog::info("[bgexecutor] successfully to submit map, job id: {}, subjob id: {}", worker->cur_job_id_, worker->cur_subjob_id_);

      // change worker state
      std::unique_lock<std::mutex> state_lck(worker->state_mtx_);
      worker->Reset();
    } else if(worker->state_ == WorkerState::WAIT2SUBMITREDUCE) {
      braft::PeerId leader;
      if(braft::rtb::select_leader(worker->cur_master_group_, &leader) != 0) {
        spdlog::info("[bgexecutor] fail to select leader in {}, refresh", worker->cur_master_group_.c_str());
        braft::rtb::refresh_leader(worker->cur_master_group_, 1000);
        continue;
      }

      // initial channel
      brpc::Channel channel;
      if(channel.Init(leader.addr, NULL) != 0) {
        spdlog::info("[bgexecutor] fail to initial channel");
        continue;
      }
      MasterService_Stub stub(&channel);

      // submit map result
      cntl.Reset();
      CompleteReduceMsg request;
      MasterReplyMsg response;
      request.set_job_id(worker->cur_job_id_);
      request.set_subjob_id(worker->cur_subjob_id_);
      request.set_worker_name(worker->name_);
      for(size_t i = 0; i < reduce_result.size(); i++) {
        request.add_reduce_result(reduce_result[i]);
      }
      stub.CompleteReduce(&cntl, &request, &response, NULL);
      if(!response.ok()) {
        spdlog::info("[bgexecutor] fail to submit reduce, job id: {}, subjob id: {}", worker->cur_job_id_, worker->cur_subjob_id_);
        continue;
      }
      spdlog::info("[bgexecutor] successfully submit reduce, job id: {}, subjob id: {}", worker->cur_job_id_, worker->cur_subjob_id_);
      // change worker state
      std::unique_lock<std::mutex> state_lck(worker->state_mtx_);
      worker->Reset();
    }
  }
  LOG(INFO) << "Executor Stop.";
}

WorkerServiceImpl::WorkerServiceImpl(std::string name, butil::EndPoint ep, std::string mrf_path)
  : worker_(new Worker(name, ep, mrf_path)) {}

WorkerServiceImpl::~WorkerServiceImpl() {
  if(worker_ != nullptr) {
    delete worker_;
  }
}

void WorkerServiceImpl::InitRegister(std::vector<butil::EndPoint> master_eps, std::vector<std::string> acceptable_job_types) {
  worker_->InitRegister(master_eps, acceptable_job_types);
}

void WorkerServiceImpl::Beat(::google::protobuf::RpcController* controller,
                             const ::google::protobuf::Empty* request,
                             ::tmapreduce::WorkerReplyMsg* response,
                             ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  response->set_ok(true);
  response->set_msg("");
  response->set_state(worker_->state_);

  return ;
}

void WorkerServiceImpl::Prepare(::google::protobuf::RpcController* controller,
                                const ::tmapreduce::PrepareMsg* request,
                                ::tmapreduce::WorkerReplyMsg* response,
                                ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  spdlog::info("Prepare to worker: [Master Id] {}, [Master Group] {}, [Job Type] {}", request->master_id(), request->master_group().c_str(), request->job_type().c_str());

  Status s = worker_->Prepare(request->master_id(), request->master_group(), request->master_conf(), request->job_type());
  if(s.ok()) {
    spdlog::info("Successfully prepare");
    response->set_ok(true);
    response->set_state(worker_->state_);
  } else {
    spdlog::info("Fail to prepare: {}", s.msg_);
    response->set_ok(false);
    response->set_msg(s.msg_);
    response->set_state(worker_->state_);
  }
  return ;
}

void WorkerServiceImpl::PrepareMap(::google::protobuf::RpcController* controller,
                                   const ::tmapreduce::MapJobMsg* request,
                                   ::tmapreduce::WorkerReplyMsg* response,
                                   ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  spdlog::info("Receive map job: [Job Id] {}, [Subjob Id] {}, [Job Name] {}, [Job Type] {}, [Size] {}", request->job_id(), request->subjob_id(), request->name().c_str(), request->type().c_str(), request->map_kvs_size());

  MapIns map_kvs;
  for(int i = 0; i < request->map_kvs_size(); i++) {
    auto& kv = request->map_kvs(i);
    map_kvs.emplace_back(std::move(kv.key()), std::move(kv.value()));
  }

  Status s = worker_->PrepareMap(request->master_id(), request->job_id(), request->subjob_id(), request->name(), request->type(), map_kvs);
  if(s.ok()) {
    spdlog::info("Successfully prepare map job");
    response->set_ok(true);
    response->set_state(worker_->state_);
  } else {
    spdlog::info("Fail to prepare map job: {}", s.msg_);
    response->set_ok(false);
    response->set_msg(s.msg_);
    response->set_state(worker_->state_);
  }
  return ;
}

void WorkerServiceImpl::PrepareReduce(::google::protobuf::RpcController* controller,
                                      const ::tmapreduce::ReduceJobMsg* request,
                                      ::tmapreduce::WorkerReplyMsg* response,
                                      ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  spdlog::info("Receive map job: [Job Id] {}, [Subjob Id] {}, [Job Name] {}, [Job Type] {}, [Size] {}", request->job_id(), request->subjob_id(), request->name().c_str(), request->type().c_str(), request->reduce_kvs_size());

  ReduceIns reduce_kvs;
  for(int i = 0; i < request->reduce_kvs_size(); i++) {
    auto& kv = request->reduce_kvs(i);
    ReduceIn tmp_kv(std::move(kv.key()), std::vector<std::string>());
    for(int j = 0; j < kv.value_size(); j++) {
      tmp_kv.second.emplace_back(std::move(kv.value(j)));
    }
    reduce_kvs.push_back(std::move(tmp_kv));
  }

  Status s = worker_->PrepareReduce(request->master_id(), request->job_id(), request->subjob_id(), request->name(), request->type(), reduce_kvs);
  if(s.ok()) {
    spdlog::info("Successfully prepare reduce job");
    response->set_ok(true);
    response->set_state(worker_->state_);
  } else {
    spdlog::info("Fail to prepare reduce job: {}", s.msg_);
    response->set_ok(false);
    response->set_msg(s.msg_);
    response->set_state(worker_->state_);
  }
  return ;
}

void WorkerServiceImpl::Start(::google::protobuf::RpcController* controller,
                              const ::google::protobuf::Empty* request,
                              ::tmapreduce::WorkerReplyMsg* response,
                              ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  spdlog::info("Try to start.");
  worker_->TryWakeupExecutor();
  response->set_ok(true);
  response->set_state(worker_->state_);

  return ;  
}

}