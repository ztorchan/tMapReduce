#include <cassert>
#include <map>
#include <algorithm>

#include "mapreduce/job.h"

namespace mapreduce {

void Job::Partition() {
  assert(stage_ == JobStage::WAIT2MAP || stage_ == JobStage::WAIT2REDUCE);
  subjobs_.clear();
  uint32_t subjob_seq_num = 0;
  if(stage_ == JobStage::WAIT2MAP) {
    subjobs_.reserve(map_worker_num_ + 1);
    uint32_t base_kvs_per_worker = map_kvs_.size() / map_worker_num_;
    uint32_t rest_kvs = map_kvs_.size() % map_worker_num_;
    for(uint32_t head = 0; head < map_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size, true);
      head += size; 
    }
  } else if (stage_ == JobStage::WAIT2REDUCE) {
    subjobs_.reserve(reduce_worker_num_ + 1);
    uint32_t base_kvs_per_worker = reduce_kvs_.size() / reduce_worker_num_;
    uint32_t rest_kvs = reduce_kvs_.size() % reduce_worker_num_;
    for(uint32_t head = 0; head < reduce_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size, false);
      head += size; 
    }
  }
  unfinished_job_num_ = subjobs_.size();
}

void Job::Merge() {
  assert(stage_ == JobStage::WAIT2MERGE);
  stage_ = JobStage::MERGING;
  size_t total_size = 0;
  std::map<std::string, ReduceIn> tmp_map;
  for(const SubJob& subjob : subjobs_) {
    MapOuts* map_result = reinterpret_cast<MapOuts*>(subjob.result_);
    for(auto& [key, value] : *map_result) {
      if(tmp_map.find(key) == tmp_map.end()) {
        tmp_map[key] = ReduceIn(key, std::vector<std::string>());
      }
      tmp_map[key].second.push_back(std::move(value));
    }
  }
  for(auto& [_, kv] : tmp_map) {
    reduce_kvs_.push_back(std::move(kv));
  }
  std::sort(reduce_kvs_.begin(), reduce_kvs_.end(), [](const ReduceIn& lhs, const ReduceIn& rhs) -> bool {
    return lhs.first < rhs.first;
  });
}

void Job::Finish() {
  assert(stage_ == JobStage::WAIT2FINISH);
  size_t total_size = 0;
  for(const SubJob& subjob : subjobs_) {
    std::vector<std::string>* reduce_result = 
      reinterpret_cast<std::vector<std::string>*>(subjob.result_);
    results_.insert(results_.end(), reduce_result->begin(), reduce_result->end());
  }
}

}