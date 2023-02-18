#ifndef _MAPREDUCE_MRER_H
#define _MAPREDUCE_MRER_H

#include <vector>
#include <string>
namespace mapreduce{

struct Mapper {
  virtual void operator()(std::string key, std::string value) = 0; 
};

struct Reducer {
  virtual void operator()(std::string key, std::string value) = 0;
};

}
#endif