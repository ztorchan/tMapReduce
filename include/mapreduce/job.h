#ifndef _MAPREDUCE_JOB_H
#define _MAPREDUCE_JOB_H

#include <string>
#include <vector>
#include <mutex>
#include <atomic>
#include <cstdint>
#include <memory>

namespace mapreduce{

class SubJob;

using MapKV = std::pair<std::string, std::string>;
using ReduceKV = std::pair<std::string, std::vector<std::string>>;
using MapKVs = std::vector<MapKV>;
using ReduceKVs = std::vector<ReduceKV>;

enum class JobStage {
    INIT,
    MAPPING,
    MERGING,
    REDUCING,
    FINISHED
  };

class Job {
public:
  Job(uint32_t id, std::string name, std::string type, int map_worker_num, int reduce_worker_num,
      MapKVs&& map_kvs) : 
      id_(id),
      name_(name),
      type_(type),
      map_worker_num_(map_worker_num),
      reduce_worker_num_(reduce_worker_num),
      stage_(JobStage::INIT),
      map_kvs_(std::forward<MapKVs>(map_kvs)),
      reduce_kvs_(),
      unfinished_job_num_(0),
      subjobs_(),
      mtx_() {}
  Job(const Job&) = delete;
  ~Job() {}

  Job& operator=(const Job&) = delete;

  // Partition job in several subjobs which will be sent to worker
  void Partition();

  void Merge();

public:
  const uint32_t id_;
  const std::string name_;
  const std::string type_;
  const int map_worker_num_ = 0;
  const int reduce_worker_num_ = 0;

  JobStage stage_;
  MapKVs map_kvs_;
  ReduceKVs reduce_kvs_;
  
  uint32_t unfinished_job_num_;
  std::vector<SubJob> subjobs_;

  std::mutex mtx_;
};

class SubJob {
public:
  SubJob(Job* job_ptr, const uint32_t subjob_id, const uint32_t head, const uint32_t size) :
    job_ptr_(job_ptr),
    subjob_id_(subjob_id),
    head_(head),
    size_(size),
    worker_id_(UINT32_MAX),
    finished_(false),
    result_(nullptr) {}

  ~SubJob() {
    if(result_ != nullptr) {
      if(job_ptr_->stage_ == JobStage::REDUCING)
        delete reinterpret_cast<std::vector<std::pair<std::string, std::string>>*>(result_);
      else if(job_ptr_->stage_ == JobStage::FINISHED)
        delete reinterpret_cast<std::vector<std::string>*>(result_);
      result_ = nullptr;
    }
  }

  SubJob& operator=(const SubJob&) = delete;

public:
  Job* job_ptr_;
  uint32_t subjob_id_;
  uint32_t head_;
  uint32_t size_;

  uint32_t worker_id_;
  bool finished_;
  void* result_;
};

}

#endif