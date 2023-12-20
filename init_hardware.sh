#!/bin/bash

# This script retrieves decisive properties of the CPU and inserts them into
# hardware.h.
#
# Only works on a GNU/Linux system, if you use another operating system, please
# insert the numbers manually (optional, just if you want to use them in your
# implementation of the groupByAggOperator), or just leave them at zero (in
# which case you won't be able to use them).

sed -i "s/NUM_CORES\\s*.*/NUM_CORES $(cat /proc/cpuinfo | grep 'core id' | sort | uniq | wc -l)/" hardware.h
sed -i "s/L1_DATA_CACHE_SIZE_BYTES\\s*.*/L1_DATA_CACHE_SIZE_BYTES $(getconf LEVEL1_DCACHE_SIZE)/" hardware.h
sed -i "s/L2_CACHE_SIZE_BYTES\\s*.*/L2_CACHE_SIZE_BYTES $(getconf LEVEL2_CACHE_SIZE)/" hardware.h
sed -i "s/L3_CACHE_SIZE_BYTES\\s*.*/L3_CACHE_SIZE_BYTES $(getconf LEVEL3_CACHE_SIZE)/" hardware.h