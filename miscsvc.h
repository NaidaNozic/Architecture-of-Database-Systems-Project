#ifndef MISCSVC_H
#define MISCSVC_H

#include "api.h"
#include <stdint.h>
#include <stddef.h>

extern int SPEED_TEST;
extern size_t NUM_ROWS;
extern size_t NUM_DISTINCTS;
extern size_t NUM_KEYS;
extern size_t NUM_COLS;
extern int16_t MAX_16B_KEY;
extern int32_t MAX_32B_KEY;
extern int64_t MAX_64B_KEY;
extern int32_t* RAND_BANK32;
extern int16_t* RAND_BANK16;
extern DataType GRPTYPE;

void matchResults(Relation* res, int64_t* eRes);
void sortCols(void** cols, size_t nRows, size_t nCols, DataType* dt);
void copyValInCol(void** allCols, int cIndex, int src, int dest, DataType type);
void swapValsInCol(void** allCols, int cIndex, int pos1, int pos2, DataType type);
void shuffle(Relation *rel, int nDist);
void copyColumn(void** srcCols, void** destCols, int cInd, size_t* rInds, DataType dt, size_t nR);
void randStore(int32_t num);
void allocColumnMemory(void** allCols, int cIndex, DataType dt, size_t nRows);
void initColumn(void** allCols, int cIndex, DataType* dt, size_t* offsets, int simple);
void initVal(void** allCols, int cIndex, int rIndex, DataType type);

#endif // MISCSVC_H
