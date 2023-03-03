#include <butil/logging.h>

#include "mapreduce/worker.h"

namespace mapreduce {

Worker::Worker(std::string name, std::string address, uint32_t port) :
    id_(UINT32_MAX),
    name_(name),
    address_(address),
    port_(port),
    master_id_(UINT32_MAX),
    master_address_(""),
    master_port_(UINT32_MAX),
    channel_(),
    stub_(nullptr),
    state_(WorkerState::INIT),
    cur_job_id_(UINT32_MAX),
    cur_subjob_id_(UINT32_MAX),
    cur_job_name_(""),
    cur_job_type_(""),
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

Status WorkerServiceImpl::Register(std::string master_address, uint32_t master_port) {
  Status s = worker_->Register(master_address, master_port);
  if(!s.ok()) {
    LOG(INFO) << "Worker register successfully.";
  } else {
    LOG(INFO) << "Worker register failed: " << s.msg_ ;
  }
  return  s;
}

}