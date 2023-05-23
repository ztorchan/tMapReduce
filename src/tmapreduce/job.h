#ifndef _TMAPREDUCE_JOB_H
#define _TMAPREDUCE_JOB_H

#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <memory>

namespace tmapreduce{

class SubJob;

using MapIn = std::pair<std::string, std::string>;
using MapOut = std::vector<std::pair<std::string, std::string>>;
using ReduceIn = std::pair<std::string, std::vector<std::string>>;
using ReduceOut = std::vector<std::string>;
using MapIns = std::vector<MapIn>;
using MapOuts = MapOut;
using ReduceIns = std::vector<ReduceIn>;
using ReduceOuts = ReduceOut;

enum class JobStage {
  INIT,
  WAIT2MAP,
  MAPPING,
  WAIT2MERGE,
  MERGING,
  WAIT2REDUCE,
  REDUCING,
  WAIT2FINISH,
  FINISHED
};

class Job {
public:
  Job(uint32_t id, std::string name, std::string type, int map_worker_num, int reduce_worker_num,
      MapIns&& map_kvs) : 
      id_(id),
      name_(name),
      type_(type),
      map_worker_num_(map_worker_num),
      reduce_worker_num_(reduce_worker_num),
      stage_(JobStage::INIT),
      map_kvs_(std::forward<MapIns>(map_kvs)),
      reduce_kvs_(),
      results_(),
      unfinished_job_num_(0),
      subjobs_(),
      mtx_() {}
  Job(const Job&) = delete;
  ~Job() {}

  Job& operator=(const Job&) = delete;

  // Partition job in several subjobs which will be sent to worker
  void Partition();
  // Merge map result
  void Merge();
  // Finish job
  void Finish();

public:
  const uint32_t id_;
  const std::string name_;
  const std::string type_;
  const int map_worker_num_ = 0;
  const int reduce_worker_num_ = 0;

  JobStage stage_;
  MapIns map_kvs_;
  ReduceIns reduce_kvs_;
  ReduceOuts results_;
  
  std::atomic_uint32_t unfinished_job_num_;
  std::vector<SubJob> subjobs_;

  std::mutex mtx_;
};

class SubJob {
public:
  SubJob(Job* job_ptr, const uint32_t subjob_id, const uint32_t head, const uint32_t size, bool is_map) :
    job_ptr_(job_ptr),
    subjob_id_(subjob_id),
    is_map_(is_map),
    head_(head),
    size_(size),
    worker_id_(UINT32_MAX),
    finished_(false),
    result_(nullptr) {}
  SubJob& operator=(const SubJob&) = delete;

  ~SubJob() {
    if(result_ != nullptr) {
      if(is_map_){
        delete reinterpret_cast<MapOuts*>(result_);
      }
      else {
        delete reinterpret_cast<ReduceOuts*>(result_);
      }
      result_ = nullptr;
    }
  }

public:
  uint32_t subjob_id_;    // subjob id
  Job* job_ptr_;          // job that belong to
  uint32_t head_;         // the head index in job
  uint32_t size_;         // kv number 
  bool is_map_;           // it is a map subjob or a reduce subjob

  uint32_t worker_id_;    // id of worker the subjob has been distributed to
  bool finished_;         // if subjob is completed
  void* result_;          // subjob result
};

}

#endif