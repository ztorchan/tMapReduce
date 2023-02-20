#ifndef MAPREDUCE_INCLUDE_STATE_H
#define MAPREDUCE_INCLUDE_STATE_H

#include "mapreduce/rpc/worker_service.pb.h"

namespace mapreduce{

enum class WorkerState {
  UNKNOWN = -0x1,
  INIT = 0x0,
  IDLE = 0x1,
  WORKING = 0x2,
  CLOSE = 0x3
};

WorkerState WorkerStateFromRPC(WorkerReplyMsg_WorkerState rpc_worker_state);

WorkerReplyMsg_WorkerState WorkerStateToRPC(WorkerState worker_state);
}
#endif 