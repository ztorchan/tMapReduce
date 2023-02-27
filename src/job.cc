#include <cassert>
#include <map>

#include "mapreduce/job.h"

namespace mapreduce {

void Job::Partition() {
  assert(stage_ == JobStage::MAPPING || stage_ == JobStage::REDUCING);
  subjobs_.clear();
  uint32_t subjob_seq_num = 0;
  if(stage_ == JobStage::MAPPING) {
    subjobs_.reserve(map_worker_num_ + 1);
    uint32_t base_kvs_per_worker = map_kvs_.size() / map_worker_num_;
    uint32_t rest_kvs = map_kvs_.size() % map_worker_num_;
    for(uint32_t head = 0; head < map_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size);
      head += size; 
    }
  } else if (stage_ == JobStage::REDUCING) {
    subjobs_.reserve(reduce_worker_num_ + 1);
    uint32_t base_kvs_per_worker = reduce_kvs_.size() / reduce_worker_num_;
    uint32_t rest_kvs = reduce_kvs_.size() % reduce_worker_num_;
    for(uint32_t head = 0; head < reduce_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size);
      head += size; 
    }
  }
  unfinished_job_num_ = subjobs_.size();
}

void Job::Merge() {
  assert(stage_ == JobStage::MERGING);
  size_t total_size = 0;
  std::map<std::string, ReduceKV> tmp_map;
  for(const SubJob& subjob : subjobs_) {
    std::vector<std::pair<std::string, std::string>>* map_result = 
      reinterpret_cast<std::vector<std::pair<std::string, std::string>>*>(subjob.result_);
    for(auto& [key, value] : *map_result) {
      if(tmp_map.find(key) == tmp_map.end()) {
        tmp_map[key] = ReduceKV(key, std::vector<std::string>());
      }
      tmp_map[key].second.push_back(std::move(value));
    }
  }
  for(auto& [_, kv] : tmp_map) {
    reduce_kvs_.push_back(std::move(kv));
  }
}

}