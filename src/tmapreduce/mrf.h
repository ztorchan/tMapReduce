#ifndef _TMAPREDUCE_MRF_H
#define _TMAPREDUCE_MRF_H

#include <vector>
#include <string>

#include "mapreduce/job.h"

tmapreduce::MapOut Map(const tmapreduce::MapIn&);

tmapreduce::ReduceOut Reduce(const tmapreduce::ReduceIn&);

#endif