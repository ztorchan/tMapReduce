// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: state.proto

#include "state.pb.h"

#include <algorithm>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)
#include <google/protobuf/port_def.inc>
namespace mapreduce {
}  // namespace mapreduce
static constexpr ::PROTOBUF_NAMESPACE_ID::Metadata* file_level_metadata_state_2eproto = nullptr;
static const ::PROTOBUF_NAMESPACE_ID::EnumDescriptor* file_level_enum_descriptors_state_2eproto[1];
static constexpr ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor const** file_level_service_descriptors_state_2eproto = nullptr;
const ::PROTOBUF_NAMESPACE_ID::uint32 TableStruct_state_2eproto::offsets[1] = {};
static constexpr ::PROTOBUF_NAMESPACE_ID::internal::MigrationSchema* schemas = nullptr;
static constexpr ::PROTOBUF_NAMESPACE_ID::Message* const* file_default_instances = nullptr;

const char descriptor_table_protodef_state_2eproto[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) =
  "\n\013state.proto\022\tmapreduce*s\n\013WorkerState\022"
  "\013\n\007UNKNOWN\020\000\022\010\n\004INIT\020\001\022\010\n\004IDLE\020\002\022\014\n\010WAIT"
  "2MAP\020\003\022\013\n\007MAPPING\020\004\022\017\n\013WAIT2REDUCE\020\005\022\014\n\010"
  "REDUCING\020\006\022\t\n\005CLOSE\020\007B\003\200\001\001b\006proto3"
  ;
static const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable*const descriptor_table_state_2eproto_deps[1] = {
};
static ::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase*const descriptor_table_state_2eproto_sccs[1] = {
};
static ::PROTOBUF_NAMESPACE_ID::internal::once_flag descriptor_table_state_2eproto_once;
const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable descriptor_table_state_2eproto = {
  false, false, descriptor_table_protodef_state_2eproto, "state.proto", 154,
  &descriptor_table_state_2eproto_once, descriptor_table_state_2eproto_sccs, descriptor_table_state_2eproto_deps, 0, 0,
  schemas, file_default_instances, TableStruct_state_2eproto::offsets,
  file_level_metadata_state_2eproto, 0, file_level_enum_descriptors_state_2eproto, file_level_service_descriptors_state_2eproto,
};

// Force running AddDescriptors() at dynamic initialization time.
static bool dynamic_init_dummy_state_2eproto = (static_cast<void>(::PROTOBUF_NAMESPACE_ID::internal::AddDescriptors(&descriptor_table_state_2eproto)), true);
namespace mapreduce {
const ::PROTOBUF_NAMESPACE_ID::EnumDescriptor* WorkerState_descriptor() {
  ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(&descriptor_table_state_2eproto);
  return file_level_enum_descriptors_state_2eproto[0];
}
bool WorkerState_IsValid(int value) {
  switch (value) {
    case 0:
    case 1:
    case 2:
    case 3:
    case 4:
    case 5:
    case 6:
    case 7:
      return true;
    default:
      return false;
  }
}


// @@protoc_insertion_point(namespace_scope)
}  // namespace mapreduce
PROTOBUF_NAMESPACE_OPEN
PROTOBUF_NAMESPACE_CLOSE

// @@protoc_insertion_point(global_scope)
#include <google/protobuf/port_undef.inc>
