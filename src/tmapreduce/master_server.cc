#include <spdlog/spdlog.h>
#include <brpc/server.h>
#include <brpc/channel.h>
#include <gflags/gflags.h>

#include "tmapreduce/master.h"

DEFINE_uint64(id, 0, "Master ID");
DEFINE_string(name, "", "Master name");
DEFINE_uint64(master_port, 0, "Listen port of the master rpc");
DEFINE_string(etcd_ep, "127.0.0.1:2379", "Etcd endpoint");
DEFINE_string(log_path, "","Log path. Default: empty, which means the logs will only be output as standard stream.");

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  butil::EndPoint etcd_ep;
  butil::str2endpoint(FLAGS_etcd_ep.c_str(), &etcd_ep);

  brpc::Server master_server;
  tmapreduce::MasterServiceImpl master_service(FLAGS_id, FLAGS_name, etcd_ep);
  if(master_server.AddService(&master_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    spdlog::info("[master server] fail to add service ");
    return -1;
  }

  butil::EndPoint point = butil::EndPoint(butil::IP_ANY, FLAGS_master_port);
  brpc::ServerOptions options;
  if(master_server.Start(point, &options) != 0) {
    spdlog::info("[master server] fail to start server");
    return -1;
  } else {
    spdlog::info("[master server] server start: [id] {}, [name] {}, [port] {}", FLAGS_id, FLAGS_name.c_str(), FLAGS_master_port);
  }

  if (master_service.start() != 0) {
    spdlog::info("[master server] fail to start raft node");
    return -1;
  } else {
    spdlog::info("[master server] start raft node");
  }
  
  master_server.RunUntilAskedToQuit();
  master_service.end();
  return 0;
}