#ifndef MAPREDUCE_INCLUDE_STATE_H
#define MAPREDUCE_INCLUDE_STATE_H

#include "mapreduce/rpc/worker_service.pb.h"

namespace mapreduce{

enum WorkerState {
  UNKNOWN = -0x1,
  INIT = 0x0,
  IDLE = 0x1,
  WORKING = 0x2,
  CLOSE = 0x3
};

WorkerState WorkerStateFromRPC(BeatMsg_WorkerState rpc_worker_state) {
  switch rpc_worker_state {
    case BeatMsg_WorkerState::BeatMsg_WorkerState_UNKNOWN:
      return WorkerState::UNKNOWN;
    case BeatMsg_WorkerState::BeatMsg_WorkerState_INIT:
      return WorkerState::INIT;
    case BeatMsg_WorkerState::BeatMsg_WorkerState_IDLE:
      return WorkerState::IDLE;
    case BeatMsg_WorkerState::BeatMsg_WorkerState_WORKING:
      return WorkerState::WORKING;
    case BeatMsg_WorkerState::BeatMsg_WorkerState_CLOSE:
      return WorkerState::CLOSE;
  }
  return WorkerState::UNKNOWN;
}

BeatMsg_WorkerState WorkerStateToRPC(WorkerState worker_state) {
  switch worker_state {
    case WorkerState::UNKNOWN:
      return BeatMsg_WorkerState::BeatMsg_WorkerState_UNKNOWN;
    case WorkerState::INIT:
      return BeatMsg_WorkerState::BeatMsg_WorkerState_INIT;
    case WorkerState::IDLE:
      return BeatMsg_WorkerState::BeatMsg_WorkerState_IDLE;
    case WorkerState::WORKING:
      return BeatMsg_WorkerState::BeatMsg_WorkerState_WORKING;
    case WorkerState::CLOSE:
      return BeatMsg_WorkerState::BeatMsg_WorkerState_CLOSE;
  }
  return BeatMsg_WorkerState::BeatMsg_WorkerState_UNKNOWN;
}

}
#endif 