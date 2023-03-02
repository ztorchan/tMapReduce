#ifndef _MAPREDUCE_STATE_H
#define _MAPREDUCE_STATE_H

#include "mapreduce/rpc/worker_service.pb.h"

namespace mapreduce{

enum class WorkerState {
  UNKNOWN = -0x1,
  INIT = 0x0,
  IDLE = 0x1,
  MAPPING = 0x2,
  REDUCING = 0x3,
  CLOSE = 0x4
};

WorkerState WorkerStateFromRPC(WorkerReplyMsg_WorkerState rpc_worker_state);

WorkerReplyMsg_WorkerState WorkerStateToRPC(WorkerState worker_state);
}
#endif 