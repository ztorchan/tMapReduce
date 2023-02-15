#ifndef _MAPREDUCE_MRER_H
#define _MAPREDUCE_MRER_H

#include <vector>

namespace mapreduce{

template<typename K, typename V>
struct Mapper {
  virtual void operator()(K key, V value) = 0; 
};

template<typename K, typename V>
struct Reducer {
  virtual void operator()(K key, std::vector<V> value) = 0;
};

}
#endif