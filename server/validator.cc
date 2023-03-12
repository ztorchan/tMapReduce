#include <regex>
#include <filesystem>

#include "mapreduce/validator.h"

namespace mapreduce{

bool ValidateIPv4(const char* flagname, const std::string& ipv4_address) {
  std::regex reg(R"(^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$)");
  return std::regex_match(ipv4_address, reg);
}

bool ValidatePort(const char* flagname, google::uint64 port) {
  if (port > 0 && port < 32768)
    return true;
  printf("Invalid value for --%s: %d\n", flagname, (int)port);
  return false;
}

bool ValidateDirectory(const char* flagname, const std::string& path) {
  return std::filesystem::is_directory(path);
}

bool ValidateDirectoryOrEmpty(const char* flagname, const std::string& path) {
  return std::filesystem::is_directory(path) || path.empty();
}

}