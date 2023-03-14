#include <iostream>
#include <fstream>
#include <filesystem>

#include "mapreduce/master.h"

std::string dir_path = "../text/";

int main() {
  brpc::ChannelOptions options;
  options.protocol = "baidu_std";
  options.timeout_ms = 1000;
  options.max_retry = 3;

  brpc::Channel channel;
  if(channel.Init("127.0.0.1", 9023, &options) != 0) {
    std::cout << "Channel initial failed." << std::endl;
    exit(1);
  }
  mapreduce::MasterService_Stub stub(&channel);

  brpc::Controller cntl;
  mapreduce::JobMsg launch_request;
  mapreduce::LaunchReplyMsg launch_response;
  launch_request.set_name("test_word_count");
  launch_request.set_type("wordcount");
  launch_request.set_mapper_num(2);
  launch_request.set_reducer_num(2);
  
  for(auto& p : std::filesystem::directory_iterator(dir_path)) {
    std::ifstream in_file(p.path());
    std::ostringstream buf;
    char ch;
    while(buf && in_file.get(ch)) {
      buf.put(ch);
    }

    auto new_kv = launch_request.add_kvs();
    new_kv->set_key(p.path());
    new_kv->set_value(buf.str());
  }

  uint32_t job_id;
  stub.Launch(&cntl, &launch_request, &launch_response, NULL);
  if(!cntl.Failed() && launch_response.reply().ok()) {
    std::cout << "Successfully launch job." << std::endl;
    job_id = launch_response.job_id();
  } else if(cntl.Failed()) {
    std::cout << "Failed to launch: Connection error." << std::endl;
  } else {
    std::cout << "Failed to launch: " << launch_response.reply().msg() << std::endl;
  }

  bool finished = false;
  mapreduce::ReduceOuts results;
  while(!finished) {
    mapreduce::GetResultMsg results_request;
    mapreduce::GetResultReplyMsg results_response;
    results_request.set_job_id(launch_response.job_id());

    cntl.Reset();
    stub.GetResult(&cntl, &results_request, &results_response, NULL);
    if(!cntl.Failed() && results_response.reply().ok()) {
      finished = true;
      for(int i = 0; i < results_response.results_size(); i += 2) {
        std::cout << results_response.results(i) << " : " << results_response.results(i+1) << std::endl;
      }
    } else if(cntl.Failed()) {
      std::cout << "Failed to get result: Connection error." << std::endl;
    } else {
      std::cout << "Failed to get result: " << launch_response.reply().msg() << std::endl;
    }
  }

  return 0;
}