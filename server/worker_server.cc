#include <brpc/server.h>
#include <gflags/gflags.h>
#include <butil/logging.h>

#include "mapreduce/worker.h"
#include "mapreduce/validator.h"

DEFINE_string(name, "", "Worker name. Default: empty");
DEFINE_uint64(worker_port, 0, "Worker RPC port. Default: 9024");
DEFINE_string(master_address, "", "Master IPv4 address(necessary).");
DEFINE_uint64(master_port, 0, "Master RPC port. Default: 9023");
DEFINE_string(mrf_path, "", "MapReduce function library path(necessary).");
DEFINE_string(log_path, "","Log path. Default: empty, which means the logs will only be output as standard stream.");

DEFINE_validator(worker_port, &mapreduce::ValidatePort);
DEFINE_validator(master_address, &mapreduce::ValidateIPv4);
DEFINE_validator(master_port, &mapreduce::ValidatePort);
DEFINE_validator(mrf_path, &mapreduce::ValidateDirectory);
// DEFINE_validator(log_path, &mapreduce::ValidateDirectoryOrEmpty);

bool InitLog() {
  logging::LoggingSettings log_setting;
  if(FLAGS_log_path.empty()) {
    log_setting.logging_dest = logging::LoggingDestination::LOG_TO_SYSTEM_DEBUG_LOG;
  } else {
    log_setting.logging_dest = logging::LoggingDestination::LOG_TO_ALL;
  }
  log_setting.log_file = FLAGS_log_path.c_str();
  log_setting.lock_log = logging::LogLockingState::LOCK_LOG_FILE;
  log_setting.delete_old = logging::OldFileDeletionState::APPEND_TO_OLD_LOG_FILE;

  return logging::InitLogging(log_setting);
}

int main(int argc, char* argv[]) {
  // parse flags
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // log init
  InitLog();

  brpc::Server worker_server;
  mapreduce::WorkerServiceImpl worker_service(FLAGS_name, (uint32_t)FLAGS_worker_port, FLAGS_mrf_path);
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
              << "[port] " << FLAGS_worker_port;
  }
  worker_service.Register(FLAGS_master_address, FLAGS_master_port);
  worker_server.RunUntilAskedToQuit();
  worker_service.end();
  LOG(INFO) << "Worker Stop";
  return 0;
}