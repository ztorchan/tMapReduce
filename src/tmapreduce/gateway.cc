#include <brpc/channel.h>
#include <braft/route_table.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/document.h>
#include <gflags/gflags.h>
#include <spdlog/spdlog.h>
#include <crow_all.h>

#include "tmapreduce/master.h"

DEFINE_string(master_group, "", "Group name of masters");
DEFINE_string(master_conf, "", "Endpoints of master");
DEFINE_int32(gateway_port, 2335, "Port of gateway");

bool check_launch_request(const rapidjson::Document& doc) {
  if(!doc.IsObject()) {
    return false;
  }
  if(!doc.HasMember("name") || !doc["name"].IsString()) {
    return false;
  }
  if(!doc.HasMember("type") || !doc["type"].IsString()) {
    return false;
  }
  if(!doc.HasMember("mapper_num") || !doc["mapper_num"].IsUint()) {
    return false;
  }
  if(!doc.HasMember("reducer_num") || !doc["reducer_num"].IsUint()) {
    return false;
  }
  if(!doc.HasMember("token") || !doc["token"].IsString()) {
    return false;
  }
  if(!doc.HasMember("kvs") || !doc["kvs"].IsArray()) {
    return false;
  }
  const auto& kvs = doc["kvs"].GetArray();
  for(auto it = kvs.begin(); it != kvs.end(); it++) {
    if(!it->IsObject()) {
      return false;
    }
    auto kv = it->GetObject();
    if(!kv.HasMember("key") || !kv["key"].IsString()) {
      return false;
    }
    if(!kv.HasMember("value") || !kv["value"].IsString()) {
      return false;
    }
  }
  return true;
}

int main() {

  crow::SimpleApp app;
  if(braft::rtb::update_configuration(FLAGS_master_group, FLAGS_master_conf) != 0) {
    spdlog::error("Fail to update braft route table");
    return -1;
  }

  CROW_ROUTE(app, "/helloworld")([](){
    return "Hello world.";
  });

  CROW_ROUTE(app, "/launch")
  .methods("POST"_method)
  ([](const crow::request& req){
    // select leader master
    braft::PeerId leader;
    if(braft::rtb::select_leader(FLAGS_master_group, &leader) != 0) {
      spdlog::info("[launch] fail to select leader in {}, refresh", FLAGS_master_group.c_str());
      braft::rtb::refresh_leader(FLAGS_master_group, 1000);
      return crow::response(502);
    }

    // initial channel
    brpc::Controller cntl;
    brpc::Channel channel;
    if(channel.Init(leader.addr, NULL) != 0) {
      spdlog::info("[launch] fail to initial channel");
      return crow::response(500);
    }
    tmapreduce::MasterService_Stub stub(&channel);

    // get input
    tmapreduce::LaunchMsg request;
    tmapreduce::LaunchReplyMsg response;
    rapidjson::Document doc;
    doc.Parse(req.body.c_str());
    if(!check_launch_request(doc)) {
      spdlog::info("[launch] bad request");
      return crow::response(crow::status::BAD_REQUEST);
    }
    request.set_name(doc["name"].GetString());
    request.set_type(doc["type"].GetString());
    request.set_mapper_num(doc["mapper_num"].GetUint());
    request.set_reducer_num(doc["reducer_num"].GetUint());
    request.set_token(doc["toker"].GetString());
    const auto& kvs = doc["kvs"].GetArray();
    for(auto it = kvs.begin(); it != kvs.end(); it++) {
      auto kv = it->GetObject();
      auto new_kv = request.add_kvs();
      new_kv->set_key(kv["key"].GetString());
      new_kv->set_value(kv["value"].GetString());
    }
    
    // launch
    stub.Launch(&cntl, &request, &response, NULL);
    crow::response res;
    crow::json::wvalue res_body;
    if(cntl.Failed()) {
      res_body["ok"] = false;
      res_body["message"] = "fail to connect to master";
      res = crow::response(500, res_body);
    } else if(response.reply().ok()) {
      spdlog::info("[launch] successfully launch, get job id {}", response.job_id());
      res_body["ok"] = true;
      res_body["message"] = "";
      res_body["job_id"] = response.job_id();
      res = crow::response(200, res_body);
    } else {
      spdlog::info("[launch] fail to launch: {}", response.reply().msg());
      res_body["ok"] = false;
      res_body["message"] = response.reply().msg();
      res = crow::response(500, res_body);
    }
    return res;
  });

  CROW_ROUTE(app, "/getresult")
  .methods("GET"_method)
  ([](const crow::request& req){
    // select leader master
    braft::PeerId leader;
    if(braft::rtb::select_leader(FLAGS_master_group, &leader) != 0) {
      spdlog::info("[get result] fail to select leader in {}, refresh", FLAGS_master_group.c_str());
      braft::rtb::refresh_leader(FLAGS_master_group, 1000);
      return crow::response(502);
    }

    // initial channel
    brpc::Controller cntl;
    brpc::Channel channel;
    if(channel.Init(leader.addr, NULL) != 0) {
      spdlog::info("[get result] fail to initial channel");
      return crow::response(500);
    }
    tmapreduce::MasterService_Stub stub(&channel);

    // get input
    tmapreduce::GetResultMsg request;
    tmapreduce::GetResultReplyMsg response;
    const auto& query_string = req.url_params;
    uint32_t job_id = std::stoul(query_string.get("job_id"));
    request.set_job_id(job_id);
    request.set_token(query_string.get("token"));
    
    // get result
    stub.GetResult(&cntl, &request, &response, NULL);
    crow::json::wvalue res_body;
    crow::response res;
    if(cntl.Failed()) {
      res_body["ok"] = false;
      res_body["message"] = "fail to connect to master";
      res = crow::response(500, res_body);
    } else if(response.reply().ok()) {
      spdlog::info("[get result] successfully get job {} result", job_id);
      crow::json::wvalue::list values;
      for(size_t i = 0; i < response.results_size(); i++) {
        values.emplace_back(response.results(i));
      }
      res_body["ok"] = true;
      res_body["message"] = "";
      res_body["result"] = std::move(values);
      res = crow::response(200, res_body);
    } else {
      spdlog::info("[get result] fail to get result: {}", response.reply().msg());
      res_body["ok"] = false;
      res_body["message"] = response.reply().msg();
      res = crow::response(500, res_body);
    }
    return res;
  });

  app.port(FLAGS_gateway_port).run();
  return 0;
}