#include <brpc/server.h>
#include <gflags/gflags.h>
#include <butil/logging.h>

#include "mapreduce/worker.h"
#include "mapreduce/validator.h"

DEFINE_string(name, "", "Worker name.");
DEFINE_string(worker_address, "", "Worker IPv4 address.");
DEFINE_uint64(worker_port, 9024, "Worker RPC port.");
DEFINE_string(master_address, "", "Master IPv4 address.");
DEFINE_uint64(master_port, 9023, "Master RPC port.");
DEFINE_string(mrf_path, "", "MapReduce function library path.");

DEFINE_validator(worker_address, &mapreduce::ValidateIPv4);
DEFINE_validator(worker_port, &mapreduce::ValidatePort);
DEFINE_validator(master_address, &mapreduce::ValidateIPv4);
DEFINE_validator(master_port, &mapreduce::ValidatePort);
DEFINE_validator(mrf_path, &mapreduce::ValidateDirectory);


int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  brpc::Server worker_server;
  mapreduce::WorkerServiceImpl worker_service(FLAGS_name, FLAGS_worker_address, (uint32_t)FLAGS_worker_port, FLAGS_mrf_path);
  if(worker_server.AddService(&worker_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    return -1;
  }
  
  butil::EndPoint point = butil::EndPoint(butil::IP_ANY, FLAGS_worker_port);
  brpc::ServerOptions options;
  if(worker_server.Start(point, &options) != 0) {
    LOG(ERROR) << "Fail to start Master";
    return -1;
  } else {
    LOG(INFO) << "Worker Start: "
              << "[name] " << FLAGS_name << ", "
              << "[port] " << FLAGS_worker_port << ", "
              << "[ipv4 address] " << FLAGS_worker_address;
  }
  worker_service.Register(FLAGS_master_address, FLAGS_master_port);
  worker_server.RunUntilAskedToQuit();
  worker_service.end();
  LOG(INFO) << "Worker Stop";
  return 0;
}