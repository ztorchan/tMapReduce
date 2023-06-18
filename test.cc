#include <cpr/cpr.h>
#include <string>
#include <cstdint>
#include <iostream>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <tmapreduce/base64.h>
#include <butil/endpoint.h>

int etcd_add_type_worker(std::string type, std::string worker_name, butil::EndPoint worker_ep) {
  std::string etcd_url = std::string("http://") + "127.0.0.1:2379" + "/v3/kv/put";
  std::string post_body, key, value;
  cpr::Response r;
  // (1) types/{type} = {}
  key = std::string("types/") + type;
  value = "";
  post_body = std::string()
              + "{"
              + "\"key\": \"" + tmapreduce::to_base64(key) + "\", "
              + "\"value\": \"" + tmapreduce::to_base64(value) + "\""
              + "}";
  r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  std::cout << r.text << std::endl;
  
  // (2) {type}/{worker name}={worker endpoint}
  key = type + "/" + worker_name;
  value = butil::endpoint2str(worker_ep).c_str();
  post_body = std::string()
              + "{"
              + "\"key\": \"" + tmapreduce::to_base64(key) + "\", "
              + "\"value\": \"" + tmapreduce::to_base64(value) + "\""
              + "}";
  r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  std::cout << r.text << std::endl;

  return r.status_code;
}

int etcd_get_type_worker(std::string type) {
  std::string etcd_url = std::string("http://") + "127.0.0.1:2379" + "/v3/kv/range";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64(type + "/") + "\", "
                          + "\"range_end\": \"" + tmapreduce::to_base64(type + "/\xff\xff") + "\""
                          + "}";
  cpr::Response r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  std::cout << r.text << std::endl;

  // Parse
  std::unordered_map<std::string, butil::EndPoint> workers;
  workers.clear();
  rapidjson::Document doc;
  doc.Parse(r.text.c_str());
  if(doc.HasMember("kvs")) {
    auto kvs = doc["kvs"].GetArray();
    for(uint32_t i = 0; i < kvs.Size(); i++) {
      auto kv = kvs[i].GetObject();
      std::string key = tmapreduce::from_base64(kv["key"].GetString());
      std::string value = tmapreduce::from_base64(kv["value"].GetString());
      std::string worker_name(key.c_str() + type.size() + 1);   // get worker_name from {type}/{worker_name}
      butil::EndPoint worker_ep;
      butil::str2endpoint(value.c_str(), &worker_ep);
      workers[worker_name] = worker_ep;
    }
  }

  for(auto& [worker_name, worker_ep] : workers) {
    std::cout << worker_name << " : " << butil::endpoint2str(worker_ep).c_str() << std::endl;
  }
  return r.status_code;             
}

int etcd_delete_type_worker(std::string type, std::string worker_name) {
  std::string etcd_url = std::string("http://") + "127.0.0.1:2379" + "/v3/kv/deleterange";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64(type + "/" + worker_name) + "\""
                          + "}";
  cpr::Response r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});
  std::cout << r.text << std::endl;

  return r.status_code;
}

int etcd_get_all_types() {
  std::string etcd_url = std::string("http://") + "127.0.0.1:2379" + "/v3/kv/range";
  std::string post_body = std::string("")
                          + "{"
                          + "\"key\": \"" + tmapreduce::to_base64("types/") + "\", "
                          + "\"range_end\": \"" + tmapreduce::to_base64("types/\xff") + "\""
                          + "}";
  cpr::Response r = cpr::Post(cpr::Url{etcd_url}, cpr::Body{post_body});

  // Parse
  std::vector<std::string> types;
  rapidjson::Document doc;
  doc.Parse(r.text.c_str());
  if(doc.HasMember("kvs")) {
    auto kvs = doc["kvs"].GetArray();
    for(uint32_t i = 0; i < kvs.Size(); i++) {
      auto kv = kvs[i].GetObject();
      std::string key = tmapreduce::from_base64(kv["key"].GetString());
      std::string type(key.c_str() + 6);   // get worker_name from {type}/{worker_name}
      types.push_back(type);
    }
  }
  for(auto& type : types) {
    std::cout << type  << std::endl;
  }
  return r.status_code;  
}



int main() {
  butil::EndPoint ep;
  butil::str2endpoint("127.1.1.1:2333", &ep);
  std::cout << std::endl << "=== Add ===" << std::endl;
  etcd_add_type_worker("type1", "worker1", ep);
  etcd_add_type_worker("type2", "worker1", ep);
  etcd_add_type_worker("type1", "worker2", ep);
  etcd_add_type_worker("type2", "worker2", ep);
  etcd_add_type_worker("type3", "worker2", ep);
  std::cout << std::endl << "=== Get ===" << std::endl;
  etcd_get_type_worker("type1");
  etcd_get_type_worker("type2");
  etcd_get_type_worker("type3");
  std::cout << std::endl << "=== Delete ===" << std::endl;
  etcd_delete_type_worker("type2", "worker2");
  std::cout << std::endl << "=== Get ===" << std::endl;
  etcd_get_type_worker("type1");
  etcd_get_type_worker("type2");
  etcd_get_type_worker("type3");
  std::cout << std::endl << "=== Get Types ===" << std::endl;
  etcd_get_all_types();
  return 0;
}

// int main() {
//   butil::EndPoint ep;
//   butil::str2endpoint("127.0.0.1:2379", &ep);
//   std::cout << butil::endpoint2str(ep).c_str() << std::endl;
// }