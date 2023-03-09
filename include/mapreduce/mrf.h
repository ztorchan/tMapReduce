#ifndef _MAPREDUCE_MRF_H
#define _MAPREDUCE_MRF_H

#include <vector>
#include <string>

#include "mapreduce/job.h"

mapreduce::MapOut Map(const mapreduce::MapIn&);

mapreduce::ReduceOut Reduce(const mapreduce::ReduceIn&);

#endif