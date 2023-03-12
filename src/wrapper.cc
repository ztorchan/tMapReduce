#include <stdint.h>
#include <vector>
#include <string>
#include <cstring>

#include "mapreduce/job.h"
#include "mapreduce/wrapper.h"
#include "mapreduce/mrf.h"

void c_Map(const char** input_keys, const char** input_values, const uint32_t input_size,
           char*** output_keys, char*** output_values, uint32_t* output_size) {
  mapreduce::MapOuts map_results;
  for(uint32_t i = 0; i < input_size; i++) {
    mapreduce::MapIn in_kv(input_keys[i], input_values[i]);
    mapreduce::MapOut tmp_result = Map(in_kv);
    map_results.insert(map_results.end(), tmp_result.begin(), tmp_result.end());
  }
  
  *output_size = map_results.size();
  (*output_keys) = new char*[*output_size];
  (*output_values) = new char*[*output_size];
  for(size_t i = 0; i < *output_size; i++) {
    const std::pair<std::string, std::string>& result = map_results[i];
    (*output_keys)[i] = new char[result.first.size() + 1];
    (*output_values)[i] = new char[result.second.size() + 1];
    strcpy((*output_keys)[i], result.first.c_str());
    strcpy((*output_values)[i], result.second.c_str());
  }

  return ;
}

void c_Reduce(const char** input_keys, const char** input_values, const uint32_t* sizes, const uint32_t input_key_size,
              char*** output_values, uint32_t* output_size) {
  mapreduce::ReduceOuts reduce_results;
  uint32_t input_values_index = 0;
  for(uint32_t i = 0; i < input_key_size; i++) {
    mapreduce::ReduceIn in_kv(input_keys[i], std::vector<std::string>());
    for(uint32_t j = 0; j < sizes[i]; j++) {
      in_kv.second.push_back(input_values[input_values_index++]);
    }
    mapreduce::ReduceOut tmp_result = Reduce(in_kv);
    reduce_results.insert(reduce_results.end(), tmp_result.begin(), tmp_result.end());
  }

  *output_size = reduce_results.size();
  (*output_values) = new char*[*output_size];
  for(size_t i = 0; i < *output_size; i++) {
    const std::string& result = reduce_results[i];
    (*output_values)[i] = new char[result.size() + 1];
    strcpy((*output_values)[i], result.c_str());
  }

  return ;
}