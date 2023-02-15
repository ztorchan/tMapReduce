
#include "mapreduce/master.h"
#include "mapreduce/status.h"
#include "mapreduce/job.h"


namespace mapreduce {

Slaver::Slaver(uint32_t wid, std::string waddress) 
    : wid_(wid),
      waddress_(waddress),
      channel_(),
      stub_(nullptr),
      state_(SlaverState::INIT){
  brpc::ChannelOptions options;
  options.protocol = "baidu_std";
  options.timeout_ms = 1000;
  options.max_retry = 3;
  if(channel_.Init(waddress_.c_str(), &options) != 0) {
    state_ = SlaverState::UNKNOWN;
    return;
  }
  
  stub_ = new WorkerService_Stub(&channel_);
}


Master::Master(uint32_t id, std::string name, uint32_t port) 
    : id_(id),
      name_(name),
      port_(port),
      slaver_seq_num_(0),
      slavers_() {
  job_distributor_ = new thread(JobDistributor, this);
  job_distributor_->detach();
  beater_ = new thread(Beater, this);
  beater_->detach();
}

Master::~Master() {
  delete job_distributor_;
}

Status Master::Register(std::string waddress, uint32_t* wid) {
  Slaver* s = new Slaver(new_slaver_id(), waddress);
  if(s->state_ != SlaverState::INIT){
    return Status.Error("Register Failed: slaver initial failed.");
  }
  WorkerService_Stub* stub = s->stub_;
  
  brpc::Controller ctl;
  google::protobuf::Empty request;
  ReplyMsg response;
  stub->Beat(&ctl, &request, &response);
  if (!ctl.Failed()) {
    s->state_ = SlaverState::IDLE;
    std::unique_lock<std::mutex> lck(slavers_mutex_);
    slavers_[s->id_] = s;
    *wid = s->id_;
    return Status.Ok("");
  }

  return Status.Error("Register Failed: beat slaver failed.");
}

Status Master::Launch(const std::string& name, const std::string& type, 
                      const int& map_worker_num, const int& reduce_worker_num, uint32_t* job_id) {
  std::unique_lock<std::mutex> lck(jobs_mutex_);
  Job* new_job = new Job(new_job_id(), name, type, map_worker_num, reduce_worker_num);
  jobs_queue_.push(new_job);
  job_id = new_job->id_;
  lck.unlock();
  jobs_cv_.notify_all();
  return Status.Ok("");
}

MasterServiceImpl::MasterServiceImpl(uint32_t id, std::string name, uint32_t port) 
    : MasterService(),
      master_(new Master(id, name, port))  {}

MasterServiceImpl::~MasterServiceImpl() {
  delete master_;
}

void MasterServiceImpl::Register(::google::protobuf::RpcController* controller,
                                 const ::mapreduce::RegisterInfo* request,
                                 ::mapreduce::RegisterReply* response,
                                 ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);

  uint32_t worker_id;
  std::string worker_address = request->address();
  Status s = master_->Register(worker_address, *worker_id);
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
                               const ::mapreduce::Job* request,
                               ::mapreduce::LaunchReply* response,
                               ::google::protobuf::Closure* done) {
  brpc::ClosureGuard done_guard(done);
  brpc::Controller* ctl = static_cast<brpc::Controller*>(controller);
  
  uint32_t job_id;
  Status s = master_->Launch(request->name(), request->type(), request->mapper_num(), request->reducer_num(), &job_id);
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

