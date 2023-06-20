#ifndef _TMAPREDUCE_MRF_WRAPPER_H
#define _TMAPREDUCE_MRF_WRAPPER_H

#include <stdint.h>

#ifdef __cplusplus
extern "C"{
#endif

typedef void (*MAP_FUNC)(const char** input_keys, const char** input_values, const uint32_t input_size,
                         char*** output_keys, char*** output_values, uint32_t* output_size);
typedef void (*REDUCE_FUNC)(const char** input_keys, const char** input_values, const uint32_t* sizes, const uint32_t input_key_size,
                            char*** output_values, uint32_t* output_size);

void c_Map(const char** input_keys, const char** input_values, const uint32_t input_size,
           char*** output_keys, char*** output_values, uint32_t* output_size);

void c_Reduce(const char** input_keys, const char** input_values, const uint32_t* sizes, const uint32_t input_key_size,
              char*** output_values, uint32_t* output_size);

#ifdef __cplusplus
}
#endif

#endif