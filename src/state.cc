#include "mapreduce/state.h"

namespace mapreduce {

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
