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

WorkerState WorkerStateFromRPC(WorkerReplyMsg_WorkerState rpc_worker_state) {
  switch (rpc_worker_state) {
    case WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_UNKNOWN:
      return WorkerState::UNKNOWN;
    case WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_INIT:
      return WorkerState::INIT;
    case WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_IDLE:
      return WorkerState::IDLE;
    case WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_WORKING:
      return WorkerState::WORKING;
    case WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_CLOSE:
      return WorkerState::CLOSE;
  }
  return WorkerState::UNKNOWN;
}

WorkerReplyMsg_WorkerState WorkerStateToRPC(WorkerState worker_state) {
  switch (worker_state) {
    case WorkerState::UNKNOWN:
      return WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_UNKNOWN;
    case WorkerState::INIT:
      return WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_INIT;
    case WorkerState::IDLE:
      return WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_IDLE;
    case WorkerState::WORKING:
      return WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_WORKING;
    case WorkerState::CLOSE:
      return WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_CLOSE;
  }
  return WorkerReplyMsg_WorkerState::WorkerReplyMsg_WorkerState_UNKNOWN;
}

}
#endif 