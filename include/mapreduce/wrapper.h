#ifndef _MAPREDUCE_MRF_WRAPPER_H
#define _MAPREDUCE_MRF_WRAPPER_H

#ifdef __cplusplus
extern "C"{
#endif

typedef void (*map_func)(char** input_keys, char** input_values, unsigned int input_size,
                         char** output_keys, char** output_values, unsigned int output_size);
typedef void (*reduce_func)(char** input_keys, char** input_values, unsigned int* sizes, unsigned int input_key_size,
                            char** output_values, unsigned int output_size);

void c_Map(char** input_keys, char** input_values, unsigned int input_size,
         char** output_keys, char** output_values, unsigned int output_size);

void c_Reduce(char** input_keys, char** input_values, unsigned int* sizes, unsigned int input_key_size,
            char** output_values, unsigned int output_size);

#ifdef __cplusplus
}
#endif

#endif