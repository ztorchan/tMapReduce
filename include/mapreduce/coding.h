#ifndef _MAPREDUCE_CODING_H
#define _MAPREDUCE_CODING_H

#include <cstdint>
#include <string>

namespace mapreduce{

class Coding {
  enum ValueType{
    FixedInt8 = 1,
    FixedInt16 = 2,
    FixedInt32 = 3,
    FixedInt64 = 4,
    FixedUInt8 = 5,
    FixedUInt16 = 6,
    FixedUInt32 = 7,
    FixedUInt64 = 8,
    FixFloat32 = 9,
    FixFloat64 = 10,
    Char = 11,
    Bool = 12,
  };

public:
  static void PutFixedInt8(std::string* dst, int8_t value) {
    char buf[1 + 1];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt8);
    buf[1] = static_cast<uint8_t>(value);

    dst->append(buf, sizeof(buf));
  }

  static void PutFixedInt16(std::string* dst, int16_t value) {
    char buf[1 + 2];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt16);
    buf[1] = static_cast<uint8_t>(value);
    buf[2] = static_cast<uint8_t>(value >> 8);

    dst->append(buf, sizeof(buf));
  }

  static void PutFixedInt32(std::string* dst, int32_t value) {
    char buf[1 + 4];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt32);
    buf[1] = static_cast<uint8_t>(value);
    buf[2] = static_cast<uint8_t>(value >> 8);
    buf[3] = static_cast<uint8_t>(value >> 16);
    buf[4] = static_cast<uint8_t>(value >> 24);

    dst->append(buf, sizeof(buf));
  }

  static void PutFixedInt64(std::string* dst, int64_t value) {
    char buf[1 + 8];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt64);
    buf[1] = static_cast<uint8_t>(value);
    buf[2] = static_cast<uint8_t>(value >> 8);
    buf[3] = static_cast<uint8_t>(value >> 16);
    buf[4] = static_cast<uint8_t>(value >> 24);
    buf[5] = static_cast<uint8_t>(value >> 32);
    buf[6] = static_cast<uint8_t>(value >> 40);
    buf[7] = static_cast<uint8_t>(value >> 48);
    buf[8] = static_cast<uint8_t>(value >> 56);
    
    dst->append(buf, sizeof(buf));
  }

  static void PutFixedUInt8(std::string* dst, uint8_t value) {
    char buf[1 + 1];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt8);
    buf[1] = static_cast<uint8_t>(value);

    dst->append(buf, sizeof(buf));
  }

  static void PutFixedUInt16(std::string* dst, uint16_t value) {
    char buf[1 + 2];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt16);
    buf[1] = static_cast<uint8_t>(value);
    buf[2] = static_cast<uint8_t>(value >> 8);

    dst->append(buf, sizeof(buf));
  }

  static void PutFixedUInt32(std::string* dst, uint32_t value) {
    char buf[1 + 4];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt32);
    buf[1] = static_cast<uint8_t>(value);
    buf[2] = static_cast<uint8_t>(value >> 8);
    buf[3] = static_cast<uint8_t>(value >> 16);
    buf[4] = static_cast<uint8_t>(value >> 24);

    dst->append(buf, sizeof(buf));
  }

  static void PutFixedUInt64(std::string* dst, uint64_t value) {
    char buf[1 + 8];
    uint8_t* const byte_ptr = reinterpret_cast<uint8_t*>(buf);
    buf[0] = static_cast<uint8_t>(ValueType::FixedInt64);
    buf[1] = static_cast<uint8_t>(value);
    buf[2] = static_cast<uint8_t>(value >> 8);
    buf[3] = static_cast<uint8_t>(value >> 16);
    buf[4] = static_cast<uint8_t>(value >> 24);
    buf[5] = static_cast<uint8_t>(value >> 32);
    buf[6] = static_cast<uint8_t>(value >> 40);
    buf[7] = static_cast<uint8_t>(value >> 48);
    buf[8] = static_cast<uint8_t>(value >> 56);
    
    dst->append(buf, sizeof(buf));
  }

  static int8_t GetFixedInt8(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return static_cast<int8_t>(buf[0]);
  }

  static int16_t GetFixedInt16(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<int16_t>(buf[0])) | 
           (static_cast<int16_t>(buf[1]) << 8);
  }

  static int32_t GetFixedInt32(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<int32_t>(buf[0])) | 
           (static_cast<int32_t>(buf[1]) << 8) | 
           (static_cast<int32_t>(buf[2]) << 16) | 
           (static_cast<int32_t>(buf[3]) << 24);
  }

  static int64_t GetFixedInt64(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<int64_t>(buf[0])) | 
           (static_cast<int64_t>(buf[1]) << 8) | 
           (static_cast<int64_t>(buf[2]) << 16) | 
           (static_cast<int64_t>(buf[3]) << 24) | 
           (static_cast<int64_t>(buf[4]) << 32) | 
           (static_cast<int64_t>(buf[5]) << 40) | 
           (static_cast<int64_t>(buf[6]) << 48) | 
           (static_cast<int64_t>(buf[7]) << 56);
  }

  static uint8_t GetFixedUInt8(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return static_cast<uint8_t>(buf[0]);
  }

  static uint16_t GetFixedUInt16(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint16_t>(buf[0])) | 
           (static_cast<uint16_t>(buf[1]) << 8);
  }

  static uint32_t GetFixedUInt32(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint32_t>(buf[0])) | 
           (static_cast<uint32_t>(buf[1]) << 8) | 
           (static_cast<uint32_t>(buf[2]) << 16) | 
           (static_cast<uint32_t>(buf[3]) << 24);
  }

  static uint64_t GetFixedUInt64(const char* ptr) {
    const uint8_t* const buf = reinterpret_cast<const uint8_t*>(ptr);
    return (static_cast<uint64_t>(buf[0])) | 
           (static_cast<uint64_t>(buf[1]) << 8) | 
           (static_cast<uint64_t>(buf[2]) << 16) | 
           (static_cast<uint64_t>(buf[3]) << 24) | 
           (static_cast<uint64_t>(buf[4]) << 32) | 
           (static_cast<uint64_t>(buf[5]) << 40) | 
           (static_cast<uint64_t>(buf[6]) << 48) | 
           (static_cast<uint64_t>(buf[7]) << 56);
  }
};

} // namespace mapreduce



#endif