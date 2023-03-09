#ifndef _MAPREDUCE_Validator_H
#define _MAPREDUCE_Validator_H

#include <string>

#include <gflags/gflags.h>

namespace mapreduce{

bool ValidateIPv4(const char* flagname, const std::string& ipv4_address);
bool ValidatePort(const char* flagname, google::uint64 port);
bool ValidateDirectory(const char* flagname, const std::string& path);

}

#endif