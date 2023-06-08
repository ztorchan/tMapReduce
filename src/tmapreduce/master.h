#ifndef _TMAPREDUCE_MASTER_H
#define _TMAPREDUCE_MASTER_H

#include <cstdint>
#include <string>
#include <mutex>
#include <atomic>
#include <deque>
#include <unordered_set>
#include <unordered_map>
#include <condition_variable>

#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <braft/raft.h>
#include <gflags/gflags.h>

#include "tmapreduce/job.h"
#include "tmapreduce/rpc/state.pb.h"
#include "tmapreduce/rpc/worker_service.pb.h"
#include "tmapreduce/rpc/master_service.pb.h"

#define _OUT

DEFINE_bool(check_term, true, "Check if the leader changed to another term");
DEFINE_bool(disable_cli, false, "Don't allow raft_cli access this node");
DEFINE_bool(log_applied_task, false, "Print notice log when a task is applied");
DEFINE_int32(election_timeout_ms, 5000, 
            "Start election in such milliseconds if disconnect with the leader");
DEFINE_int32(port, 8100, "Listen port of this peer");
DEFINE_int32(snapshot_interval, 30, "Interval between each snapshot");
DEFINE_string(conf, "", "Initial configuration of the replication group");
DEFINE_string(data_path, "./data", "Path of data stored on");
DEFINE_string(group, "tMapReduce", "Id of the replication group");

namespace tmapreduce
{

class Status;
class RegisterClosure;
class LaunchClosure;
class CompleteMapClosure;
class CompleteReduceClosure;
class GetResultClosure;

class Slaver {
public:
  Slaver(std::string name, butil::EndPoint endpoint);
  ~Slaver();

  friend class Master;
private:
  // identifier releated
  const std::string name_;
  // connection releated
  const butil::EndPoint endpoint_;
  brpc::Channel* channel_;
  WorkerService_Stub* stub_;
  uint32_t beat_retry_times_;
  // state releated
  WorkerState state_;
  std::uint32_t cur_job_;
  std::uint32_t cur_subjob_;
};

class Master : public braft::StateMachine {
public:
  Master();
  ~Master() noexcept;
  Master(const Master&) = delete;
  Master& operator=(const Master&) = delete;
  
  int Start();
  // mapreduce master related
  void Register(const RegisterMsg* request,
                RegisterReplyMsg* response,
                google::protobuf::Closure* done);
  void Launch(const LaunchMsg* request,
              LaunchReplyMsg* response,
              google::protobuf::Closure* done);
  void CompleteMap(const CompleteMapMsg* request,
                   MasterReplyMsg* response,
                   google::protobuf::Closure* done);
  void CompleteReduce(const CompleteReduceMsg* request,
                      MasterReplyMsg* response,
                      google::protobuf::Closure* done);
  void GetResult(const GetResultMsg* request,
                 GetResultReplyMsg* response,
                 google::protobuf::Closure* done);
  
  static void BGDistributor(Master* master);

  static void BGBeater(Master* master);

private:
  enum OpType : uint8_t {
    OP_UNKNOWN = 0,
    OP_LAUNCH = 1,          // user launch a job. 
    OP_DISTRIBUTE = 2,      // master distribute a subjob
    OP_CANCEL= 3,           // cancel a subjob on a worker and back to waiting list
    OP_COMPLETEMAP = 4,     // a worker complete a map subjob
    OP_COMPLETEREDUCE = 5,  // a worker complete a reduce subjob
    OP_DELETEJOB = 6,       // master delete a job
  };
  // log handler
  // Principle: handler should be deterministic operation, communication with workers should not be involved
  Status handle_register(std::string name, butil::EndPoint ep, const std::vector<std::string> acceptable_job_type);
  Status handle_launch(const std::string& name, const std::string& type, const std::string& token, uint32_t map_worker_num, uint32_t reduce_worker_num, MapIns& map_kvs, _OUT uint32_t* job_id);
  Status handle_distribute();
  Status handle_cancel();
  Status handle_complete_map(uint32_t job_id, uint32_t subjob_id, uint32_t worker_id, WorkerState worker_state, MapOuts& map_result);
  Status handle_complete_reduce(uint32_t job_id, uint32_t subjob_id, uint32_t worker_id, WorkerState worker_state, ReduceOuts& reduce_result);
  Status handle_delete_job(uint32_t job_id, uint64_t token, _OUT ReduceOuts* result);

  // raft state machine related
  void apply_from_rpc(OpType op_type, 
                      const google::protobuf::Message* request, 
                      google::protobuf::Message* response, 
                      google::protobuf::Closure* done);
  void on_apply(braft::Iterator& iter) override;
  void on_shutdown() override;
  void on_leader_start(int64_t term) override;
  void on_leader_stop(const butil::Status& status) override;
  void redirect(OpType op_type, google::protobuf::Message* response);

  // util function
  Status etcd_add_type_worker(std::string type, std::string worker_name, butil::EndPoint worker_ep);
  Status etcd_get_type_worker(std::string type, std::string worker_name, _OUT butil::EndPoint& worker_ep);
  Status etcd_get_workers_from_type(std::string type, _OUT std::unordered_map<std::string, butil::EndPoint>& workers);
  Status etcd_delete_type_worker(std::string type, std::string worker_name);
  Status etcd_get_all_types(_OUT std::vector<std::string> types);
  void set_reply_ok(OpType op_type, google::protobuf::Message* response, bool ok);
  void set_reply_msg(OpType op_type, google::protobuf::Message* response, std::string msg);
  void set_reply_redirect(OpType op_type, google::protobuf::Message* response, std::string redirect);
  braft::Closure* get_closure(OpType op_type, const google::protobuf::Message* request, google::protobuf::Message* response, google::protobuf::Closure* done);

  friend class RegisterClosure;
  friend class LaunchClosure;
  friend class CompleteMapClosure;
  friend class CompleteReduceClosure;
  friend class GetResultClosure;

private:
  uint32_t new_job_id() { return ++job_seq_num_; }

private:
  // master attribute
  const uint32_t id_;       // master id
  const std::string name_;  // master name
  std::atomic_uint32_t job_seq_num_;    // job no sequence
  butil::EndPoint etcd_ep_;
  // slavers list
  std::unordered_map<std::string, std::unique_ptr<Slaver>> slavers_;     // slavers
  // jobs list
  std::map<uint32_t, Job*> jobs_;                       // job id to job pointer
  std::deque<std::pair<uint32_t, uint32_t>> jobs_waiting_dist_;   // <job id, subjob id> queue that waiting to distribute
  std::unordered_set<uint32_t> jobs_finished_;                    // job id set that has been finished
  // mutex and condition variable
  std::mutex slavers_mtx_;
  std::mutex jobs_mtx_;
  std::mutex dist_mtx_;
  std::condition_variable dist_cv_;
  // raft state machine related
  braft::Node* volatile raft_node_;
  butil::atomic<int64_t> raft_leader_term_; 
  // if master is end
  bool end_;
};

class MasterServiceImpl : public MasterService {
public:
  MasterServiceImpl();
  ~MasterServiceImpl();
  void Register(::google::protobuf::RpcController* controller,
                const ::tmapreduce::RegisterMsg* request,
                ::tmapreduce::RegisterReplyMsg* response,
                ::google::protobuf::Closure* done) override;
  void Launch(::google::protobuf::RpcController* controller,
              const ::tmapreduce::LaunchMsg* request,
              ::tmapreduce::LaunchReplyMsg* response,
              ::google::protobuf::Closure* done) override; 
  void CompleteMap(::google::protobuf::RpcController* controller,
                   const ::tmapreduce::CompleteMapMsg* request,
                   ::tmapreduce::MasterReplyMsg* response,
                   ::google::protobuf::Closure* done) override;
  void CompleteReduce(::google::protobuf::RpcController* controller,
                      const ::tmapreduce::CompleteReduceMsg* request,
                      ::tmapreduce::MasterReplyMsg* response,
                      ::google::protobuf::Closure* done) override;
  void GetResult(::google::protobuf::RpcController* controller,
                 const ::tmapreduce::GetResultMsg* request,
                 ::tmapreduce::GetResultReplyMsg* response,
                 ::google::protobuf::Closure* done) override;
private:
  Master* master_;
};


} // namespace tmapreduce

#endif  // _TMAPREDUCE_MASTER_H