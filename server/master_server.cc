#include <brpc/server.h>
#include <gflags/gflags.h>
#include <butil/logging.h>

#include "mapreduce/master.h"
#include "mapreduce/validator.h"

DEFINE_uint64(id, 0, "Master ID");
DEFINE_string(name, "", "Master name");
DEFINE_uint64(port, 9023, "Master RPC port.");
DEFINE_validator(port, &mapreduce::ValidatePort);

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  brpc::Server master_server;
  mapreduce::MasterServiceImpl master_service((uint32_t)FLAGS_id, FLAGS_name, (uint32_t)FLAGS_port);
  if(master_server.AddService(&master_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    LOG(ERROR) << "Fail to add service";
    return -1;
  }
  
  butil::EndPoint point = butil::EndPoint(butil::IP_ANY, FLAGS_port);
  brpc::ServerOptions options;
  if(master_server.Start(point, &options) != 0) {
    LOG(ERROR) << "Fail to start Master";
    return -1;
  } else {
    LOG(INFO) << "Master Start: "
              << "[id] " << FLAGS_id << ", "
              << "[name] " << FLAGS_name << ", "
              << "[port] " << FLAGS_port << ", ";
  }
  master_server.RunUntilAskedToQuit();
  master_service.end();
  LOG(INFO) << "Master Stop";
  return 0;
}