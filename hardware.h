#ifndef HARDWARE_H
#define HARDWARE_H

/**
 * This header provides a simple abstraction from important hardware features.
 * 
 * Using these in your implementation of the groupByAgg operator is optional.
 * If your implementation requires this information, it is recommended that you
 * enter it here. During the grading, we will make sure that the hardware
 * characteristics of our benchmarking servers are available through these
 * macro constants.
 * 
 * Please note that you need to manually enter the numbers here. In GNU/Linux,
 * you can obtain the information by typing `lscpu` in a terminal.
 */

// The number of physical cores.
#define NUM_CORES 0

// The size of the level 1 data cache in bytes.
#define L1_DATA_CACHE_SIZE_BYTES 0
// The size of the level 2 cache in bytes.
#define L2_CACHE_SIZE_BYTES 0
// The size of the level 3 cache in bytes.
#define L3_CACHE_SIZE_BYTES 0

#endif // HARDWARE_H