#include <brpc/closure_guard.h>

#include "tmapreduce/master.h"
#include "tmapreduce/closure.h"

namespace tmapreduce
{

// void RegisterClosure::Run() {
//   std::unique_ptr<RegisterClosure> self_guard(this);
//   brpc::ClosureGuard done_guard(done_);
//   if(status().ok()) {
//     return ;
//   }
//   master_->redirect(Master::OpType::OP_REGISTER, response_->mutable_reply());
// }

void LaunchClosure::Run() {
  std::unique_ptr<LaunchClosure> self_guard(this);
  brpc::ClosureGuard done_guard(done_);
  if(status().ok()) {
    return ;
  }
  master_->redirect(Master::OpType::OP_LAUNCH, response_);
}

void CompleteMapClosure::Run() {
  std::unique_ptr<CompleteMapClosure> self_guard(this);
  brpc::ClosureGuard done_guard(done_);
  if(status().ok()) {
    return ;
  }
  master_->redirect(Master::OpType::OP_COMPLETEMAP, response_);
}

void CompleteReduceClosure::Run() {
  std::unique_ptr<CompleteReduceClosure> self_guard(this);
  brpc::ClosureGuard done_guard(done_);
  if(status().ok()) {
    return ;
  }
  master_->redirect(Master::OpType::OP_COMPLETEREDUCE, response_);
}

// void GetResultClosure::Run() {
//   std::unique_ptr<GetResultClosure> self_guard(this);
//   brpc::ClosureGuard done_guard(done_);
//   if(status().ok()) {
//     return ;
//   }
//   master_->redirect(response_->mutable_reply());
// }

} // namespace tmapreduce
