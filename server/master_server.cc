#include <brpc/server.h>
#include <gflags/gflags.h>
#include <butil/logging.h>

#include "mapreduce/master.h"
#include "mapreduce/validator.h"

DEFINE_uint64(id, 0, "Master ID");
DEFINE_string(name, "", "Master name");
DEFINE_uint64(port, 9023, "Master RPC port.");
DEFINE_string(log_path, "","Log path. Default: empty, which means the logs will only be output as standard stream.");

DEFINE_validator(port, &mapreduce::ValidatePort);
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
              << "[id]" << FLAGS_id << ", "
              << "[name]" << FLAGS_name << ", "
              << "[port]" << FLAGS_port << ", ";
  }
  master_server.RunUntilAskedToQuit();
  master_service.end();
  LOG(INFO) << "Master Stop";
  return 0;
}