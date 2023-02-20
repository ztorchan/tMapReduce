#ifndef MAPREDUCE_INCLUDE_STATUS_H
#define MAPREDUCE_INCLUDE_STATUS_H

#include <string>

namespace mapreduce{

enum class Code {
    OK = 0x0,
    ERROR = 0x1,
    NOTFOUND = 0x2
  };

class Status {
public:
  Status(Code code, std::string msg) : code_(code), msg_(msg) {}

  static Status Ok(std::string msg) {
    return Status(Code::OK, msg);
  }

  static Status Error(std::string msg) {
    return Status(Code::ERROR, msg);
  }

  static Status NotFound(std::string msg) {
    return Status(Code::NOTFOUND, msg);
  }

  bool ok() { return code_ == Code::OK; }

  Code code_;
  std::string msg_;
};

}

#endif