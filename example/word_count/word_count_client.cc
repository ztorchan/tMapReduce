#include <iostream>
#include <fstream>
#include <filesystem>
#include <gflags/gflags.h>
#include <cpr/cpr.h>
#include <chrono>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>


DEFINE_string(txt_path, "", "Text path");
DEFINE_string(gateway, "", "The endpoint of gateway");

bool isLetterOrNum(const char& c) {
  return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); 
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  std::string launch_url = std::string("http://") + FLAGS_gateway + "/launch";
  std::string launch_body = std::string("")
                            + "{"
                            + "\"name\": \"wc-test\","
                            + "\"type\": \"wordcount\","
                            + "\"mapper_num\": 2,"
                            + "\"reducer_num\": 2,"
                            + "\"token\": \"ztorchan\","
                            + "\"kvs\": [";
  for(auto& p : std::filesystem::directory_iterator(FLAGS_txt_path)) {
    std::ifstream in_file(p.path());
    std::ostringstream buf;
    char ch;
    while(buf && in_file.get(ch)) {
      if(isLetterOrNum(ch))
        buf.put(ch);
    }
    launch_body = launch_body
                  +"{"
                  + "\"key\":\"" + p.path().c_str() + "\","
                  + "\"value\":\"" + buf.str() + "\""
                  + "},";
  }
  launch_body.pop_back();
  launch_body = launch_body + "]}";
  cpr::Response launch_r = cpr::Post(cpr::Url{launch_url}, cpr::Body{launch_body});
  std::cout << launch_r.text << std::endl;
  rapidjson::Document doc;
  doc.Parse(launch_r.text.c_str());
  uint32_t job_id = doc["job_id"].GetUint();

  // get result
  std::this_thread::sleep_for(std::chrono::seconds(2));
  std::string get_url = std::string("http://") + FLAGS_gateway + "/getresult?" + "job_id=" + std::to_string(job_id) + "&token=ztorchan";
  cpr::Response get_r = cpr::Get(cpr::Url{get_url});
  std::cout << get_r.text << std::endl;
  doc.Parse(get_r.text.c_str());
  auto result = doc["result"].GetArray();
  for(size_t i = 0; i < result.Size(); i += 2) {
    std::cout << result[i].GetString() << " : " << result[i+1].GetString() << std::endl;
  }

  return 0;
}