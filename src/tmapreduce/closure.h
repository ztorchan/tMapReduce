#ifndef _TMAPREDUCE_CLOSURE_H
#define _TMAPREDUCE_CLOSURE_H

#include "braft/raft.h"
#include "tmapreduce/rpc/master_service.pb.h"

namespace tmapreduce
{

class Master;

class RegisterClosure : public braft::Closure {
public: 
  RegisterClosure(Master* master, 
                  const RegisterMsg* request, 
                  RegisterReplyMsg* response, 
                  google::protobuf::Closure* done) 
    : master_(master)
    , request_(request)
    , response_(response)
    , done_(done) {}
  ~RegisterClosure() {}

  const RegisterMsg* request() { return request_; }
  RegisterReplyMsg* response() { return response_; }
  void Run();
private:
  Master* master_;
  const RegisterMsg* request_;
  RegisterReplyMsg* response_;
  google::protobuf::Closure* done_;
};

class LaunchClosure : public braft::Closure {
public: 
  LaunchClosure(Master* master, 
                const LaunchMsg* request, 
                LaunchReplyMsg* response, 
                google::protobuf::Closure* done) 
    : master_(master)
    , request_(request)
    , response_(response)
    , done_(done) {}
  ~LaunchClosure() {}

  const LaunchMsg* request() { return request_; }
  LaunchReplyMsg* response() { return response_; }
  void Run();
private:
  Master* master_;
  const LaunchMsg* request_;
  LaunchReplyMsg* response_;
  google::protobuf::Closure* done_;
};

class CompleteMapClosure : public braft::Closure {
public: 
  CompleteMapClosure(Master* master, 
                     const CompleteMapMsg* request, 
                     MasterReplyMsg* response, 
                     google::protobuf::Closure* done) 
    : master_(master)
    , request_(request)
    , response_(response)
    , done_(done) {}
  ~CompleteMapClosure() {}

  const CompleteMapMsg* request() { return request_; }
  MasterReplyMsg* response() { return response_; }
  void Run();
private:
  Master* master_;
  const CompleteMapMsg* request_;
  MasterReplyMsg* response_;
  google::protobuf::Closure* done_;
};

class CompleteReduceClosure : public braft::Closure {
public: 
  CompleteReduceClosure(Master* master, 
                        const CompleteReduceMsg* request, 
                        MasterReplyMsg* response, 
                        google::protobuf::Closure* done) 
    : master_(master)
    , request_(request)
    , response_(response)
    , done_(done) {}
  ~CompleteReduceClosure() {}

  const CompleteReduceMsg* request() { return request_; }
  MasterReplyMsg* response() { return response_; }
  void Run();
private:
  Master* master_;
  const CompleteReduceMsg* request_;
  MasterReplyMsg* response_;
  google::protobuf::Closure* done_;
};

class GetResultClosure : public braft::Closure {
public: 
  GetResultClosure(Master* master, 
                   const GetResultMsg* request, 
                   GetResultReplyMsg* response, 
                   google::protobuf::Closure* done) 
    : master_(master)
    , request_(request)
    , response_(response)
    , done_(done) {}
  ~GetResultClosure() {}

  const GetResultMsg* request() { return request_; }
  GetResultReplyMsg* response() { return response_; }
  void Run();
private:
  Master* master_;
  const GetResultMsg* request_;
  GetResultReplyMsg* response_;
  google::protobuf::Closure* done_;
};

} // namespace tmapreduce


#endif