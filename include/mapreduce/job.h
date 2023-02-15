#ifndef _MAPREDUCE_JOB_H
#define _MAPREDUCE_JOB_H

#include <string>
#include <cstdint>

namespace mapreduce{

class Job {
public:
  
  Job() {}

  Job(uint32_t id, std::string name, std::string type, int map_worker_num, int reduce_worker_num) : 
      id_(id),
      name_(name),
      type_(type),
      map_worker_num_(map_worker_num),
      reduce_worker_num_(reduce_worker_num){}

  ~Job() {}

  uint32_t id_;
  std::string name_;
  std::string type_;
  int map_worker_num_ = 0;
  int reduce_worker_num_ = 0;
};

}

#endif