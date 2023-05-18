#ifndef _TMAPREDUCE_MASTER_H
#define _TMAPREDUCE_MASTER_H

#include <cstdint>
#include <string>
#include <atomic>
#include <unordered_map>
#include <deque>

#include <braft/raft.h>

#include "mapreduce/job.h"

namespace tmapreduce
{


class Master : public braft::StateMachine {
public:
  Master();
  ~Master();

private:
  const uint32_t id_;
  const std::string name_; 
  std::atomic_uint32_t slaver_seq_num_;
  std::atomic_uint32_t job_seq_num_;

  std::unordered_map<uint32_t, Job*> jobs_;
  std::

};



} // namespace tmapreduce

#endif  // _TMAPREDUCE_MASTER_H