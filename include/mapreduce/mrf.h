#ifndef _MAPREDUCE_MRF_H
#define _MAPREDUCE_MRF_H

#include <vector>
#include <string>

#include "mapreduce/job.h"

MapOut Map(MapIn);

ReduceOut Reduce(ReduceIn);

#endif