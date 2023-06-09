#include <cassert>
#include <map>
#include <algorithm>
#include <spdlog/spdlog.h>

#include "tmapreduce/job.h"

namespace tmapreduce {

void Job::Partition() {
  // Job should be in WAIT2PARTITION when partition
  spdlog::debug("[Job::Partition] now job stage: {}", stage_);
  assert(stage_ == JobStage::WAIT2PARTITION4MAP || stage_ == JobStage::WAIT2PARTITION4REDUCE);
  subjobs_.clear();
  uint32_t subjob_seq_num = 0;
  if(stage_ == JobStage::WAIT2PARTITION4MAP) {
    spdlog::debug("[Job::Partition] partitioning map job {}", id_);
    stage_ = JobStage::PARTITIONING;
    subjobs_.reserve(map_worker_num_ + 1);
    uint32_t base_kvs_per_worker = map_kvs_.size() / map_worker_num_;
    int rest_kvs = map_kvs_.size() % map_worker_num_;
    for(uint32_t head = 0; head < map_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size, SubJobType::MAP);
      head += size; 
    }
    spdlog::debug("[Job::Partition] successfully partition job to {} subjobs, change job stage", subjobs_.size());
    stage_ = JobStage::WAIT2MAP;
  } else if (stage_ == JobStage::WAIT2PARTITION4REDUCE) {
    spdlog::debug("[Job::Partition] partitioning reduce job {}", id_);
    subjobs_.reserve(reduce_worker_num_ + 1);
    uint32_t base_kvs_per_worker = reduce_kvs_.size() / reduce_worker_num_;
    int rest_kvs = reduce_kvs_.size() % reduce_worker_num_;
    for(uint32_t head = 0; head < reduce_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size, SubJobType::REDUCE);
      head += size; 
    }
    spdlog::debug("[Job::Partition] successfully partition job to {} subjobs, change job stage", subjobs_.size());
    stage_ = JobStage::WAIT2REDUCE;
  }
  unfinished_job_num_ = subjobs_.size();
}

void Job::Merge() {
  // Job should be in WAIT2MERGE when merge
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
    std::vector<std::string>* reduce_result =  reinterpret_cast<std::vector<std::string>*>(subjob.result_);
    results_.insert(results_.end(), reduce_result->begin(), reduce_result->end());
  }
  stage_ = JobStage::FINISHED;
}

}