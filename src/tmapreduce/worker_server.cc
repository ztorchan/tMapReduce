#include <vector>
#include <filesystem>

#include <spdlog/spdlog.h>
#include <butil/endpoint.h>
#include <butil/string_splitter.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>

#include "tmapreduce/worker.h"

DEFINE_string(name, "", "Worker name");
DEFINE_int32(port, 2334, "Listen port of this worker");
DEFINE_string(this_ep, "", "Worker endpoint");
DEFINE_string(master_eps, "", "Master endpoint for initial register");
DEFINE_string(mrf_path, "", "MapReduce function library path");

std::vector<butil::EndPoint> str2endpoints(std::string master_eps) {
  std::vector<butil::EndPoint> res;
  std::string ep_str;
  butil::EndPoint ep;
  for(butil::StringSplitter sp(FLAGS_master_eps.c_str(), FLAGS_master_eps.c_str() + FLAGS_master_eps.size(), ','); sp; ++sp) {
    ep_str.assign(sp.field(), sp.length());
    if(butil::str2endpoint(ep_str.c_str(), &ep) != 0) {
      spdlog::info("[strendpoints] fail to parse endpoint string: {}", ep_str.c_str());
      continue;
    }
    res.push_back(ep);
  }
  return res;
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // get all mrf library
  std::vector<std::string> acceptable_job_types;
  for(auto& p : std::filesystem::directory_iterator(FLAGS_mrf_path)) {
    if(p.is_regular_file() && p.path().extension() == ".so") {
      spdlog::info("[worker server] find acceptable job type: {}", p.path().stem().c_str());
      acceptable_job_types.push_back(p.path().stem());
    }
  }
  spdlog::info("[worker server] totally find {} acceptable job types", acceptable_job_types.size());

  // add service
  brpc::Server worker_server;
  butil::EndPoint worker_ep;
  butil::str2endpoint(FLAGS_this_ep.c_str(), &worker_ep);
  tmapreduce::WorkerServiceImpl worker_service(FLAGS_name, worker_ep, FLAGS_mrf_path);
  if(worker_server.AddService(&worker_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    spdlog::info("[worker server] fail to add service");
    return -1;
  } 

  // start server
  brpc::ServerOptions options;
  if(worker_server.Start(butil::EndPoint(butil::IP_ANY, FLAGS_port), &options) != 0) {
    spdlog::info("[worker server] fail to start worker");
  }
  spdlog::info("[worker server] worker start: [name] {}, [port] {}", FLAGS_name, FLAGS_port);

  // register
  worker_service.InitRegister(str2endpoints(FLAGS_master_eps), acceptable_job_types);
  worker_server.RunUntilAskedToQuit();
  worker_service.end();

  spdlog::info("[worker server] worke stop");
  return 0;
}
