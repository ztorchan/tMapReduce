#include "mapreduce/job.h"

namespace mapreduce {

void Job::Partition() {
  subjobs_.clear();
  uint32_t subjob_seq_num = 0;
  if(stage_ == JobStage::WAIT2MAP) {
    uint32_t base_kvs_per_worker = map_kvs_.size() / map_worker_num_;
    uint32_t rest_kvs = map_kvs_.size() % map_worker_num_;
    for(uint32_t head = 0; head < map_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size);
      head += size; 
    }
  } else if (stage_ == JobStage::WAIT2REDUCE) {
    uint32_t base_kvs_per_worker = reduce_kvs_.size() / reduce_worker_num_;
    uint32_t rest_kvs = reduce_kvs_.size() % reduce_worker_num_;
    for(uint32_t head = 0; head < reduce_kvs_.size(); ) {
      uint32_t size = (rest_kvs-- > 0 ? base_kvs_per_worker + 1 : base_kvs_per_worker);
      subjobs_.emplace_back(this, subjob_seq_num++, head, size);
      head += size; 
    }
  }
}

}