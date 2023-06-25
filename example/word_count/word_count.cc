#include "tmapreduce/mrf.h"

#include <unordered_map>

bool isLetterOrNum(const char& c) {
  return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); 
}

tmapreduce::MapOut Map(const tmapreduce::MapIn& map_kv) {
  tmapreduce::MapOut result;
  std::unordered_map<char, int> counter;
  for(const char& c : map_kv.second) {
    if(isLetterOrNum(c))
      counter[c]++;
  }
  for(const auto& [c, num] : counter) {
    std::string key, value(std::to_string(num));
    key.push_back(c);
    result.emplace_back(std::move(key), std::move(value));
  }
  return result;
}

tmapreduce::ReduceOut Reduce(const tmapreduce::ReduceIn& reduce_kv) {
  tmapreduce::ReduceOut result;
  int total = 0;
  result.push_back(reduce_kv.first);
  for(const auto& v : reduce_kv.second) {
    total += std::atoi(v.c_str());
  }
  result.push_back(std::to_string(total));

  return result;
}