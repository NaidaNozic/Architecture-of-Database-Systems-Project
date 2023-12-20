#include "miscsvc.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/**********************************
 *** Functions to match results ***
 **********************************/
int int16Comparator (const void* p1, const void* p2)
{ return (*(int16_t*)p1 > *(int16_t*)p2) - (*(int16_t*)p1 < *(int16_t*)p2); }

int int32Comparator (const void* p1, const void* p2)
{ return (*(int32_t*)p1 > *(int32_t*)p2) - (*(int32_t*)p1 < *(int32_t*)p2); }

int int64Comparator (const void* p1, const void* p2)
{ return (*(int64_t*)p1 > *(int64_t*)p2) - (*(int64_t*)p1 < *(int64_t*)p2); }

void sort(int64_t* col, size_t num)
{
  qsort(col, num, sizeof(int64_t), int64Comparator);
}

/* Sort each column individually
 * NOTE: This function destroys keys to payload mapping */
void sortCols(void **cols, size_t nRows, size_t nCols, DataType* dt) 
{
  size_t i;
  for (i=0; i<nCols; i++) {
    switch(dt[i]) {
      case INT16: 
        qsort(((int16_t *)cols[i]), nRows, sizeof(int16_t), int16Comparator);
        break;
      case INT32: 
        qsort(((int32_t *)cols[i]), nRows, sizeof(int32_t), int32Comparator);
        break;
      case INT64: 
        qsort(((int64_t *)cols[i]), nRows, sizeof(int64_t), int64Comparator);
        break;
    default:
      printf("Unknown data type (%d)\n", dt[i]);
      exit(EXIT_FAILURE);
    }
  }
}

/**********************************
 *** Functions to move values ***
 **********************************/

/* Copy value from one row index to another in a column */
void copyValInCol(void** allCols, int cIndex, int src, int dest, DataType type)
{
  switch(type) {
    case INT16:
      memcpy(&(((int16_t *)allCols[cIndex])[dest]), &(((int16_t *)allCols[cIndex])[src]), sizeof(int16_t));
      break;
    case INT32:
      memcpy(&(((int32_t *)allCols[cIndex])[dest]), &(((int32_t *)allCols[cIndex])[src]), sizeof(int32_t));
      break;
    case INT64:
      memcpy(&(((int64_t *)allCols[cIndex])[dest]), &(((int64_t *)allCols[cIndex])[src]), sizeof(int64_t));
      break;
    default:
      printf("Unknown data type (%d)\n", type);
      exit(EXIT_FAILURE);
  }
}

/* Swap values between pos1 and pos2 in a column */
void swapValsInCol(void** allCols, int cIndex, int pos1, int pos2, DataType type)
{
  switch(type) {
    case INT16: {
      int16_t tmp = ((int16_t *)allCols[cIndex])[pos1];
      copyValInCol(allCols, cIndex, pos2, pos1, type);
      ((int16_t *)allCols[cIndex])[pos2] = tmp;
      break;
    }
    case INT32: {
      int32_t tmp = ((int32_t *)allCols[cIndex])[pos1];
      copyValInCol(allCols, cIndex, pos2, pos1, type);
      ((int32_t *)allCols[cIndex])[pos2] = tmp;
      break;
    }
    case INT64: {
      int64_t tmp = ((int64_t *)allCols[cIndex])[pos1];
      copyValInCol(allCols, cIndex, pos2, pos1, type);
      ((int64_t *)allCols[cIndex])[pos2] = tmp;
      break;
    }
    default:
      printf("Unknown data type (%d)\n", type);
      exit(EXIT_FAILURE);
  }
}

/* Shuffle the rows */
void shuffle(Relation *rel, int num) {
  size_t nR = rel->numRows;
  int32_t nC = rel->numCols;
  DataType* dt = rel->colTypes;
  void** allCols = rel->cols;
  int i,j;

  srand(time(0));
  for (i=0; i<nR; i++) {
    int loc = rand() % num;
    for (j=0; j<nC; j++)
      swapValsInCol(allCols, j, i, loc, dt[j]); 
  }
}

/* Deep copy some rows of a column from one table to another */
void copyColumn(void** srcCols, void** destCols, int cInd, size_t* rInds, DataType dt, size_t nR)
{
  switch(dt) {
    case INT16:
      for (int i=0; i<nR; i++) 
        memcpy(&(((int16_t *)destCols[cInd])[i]), &(((int16_t *)srcCols[cInd])[rInds[i]]), sizeof(int16_t));
      break;
    case INT32:
      for (int i=0; i<nR; i++) 
        memcpy(&(((int32_t *)destCols[cInd])[i]), &(((int32_t *)srcCols[cInd])[rInds[i]]), sizeof(int32_t));
      break;
    case INT64:
      for (int i=0; i<nR; i++) 
        memcpy(&(((int64_t *)destCols[cInd])[i]), &(((int64_t *)srcCols[cInd])[rInds[i]]), sizeof(int64_t));
      break;
    default:
      printf("Unknown data type (%d)\n", dt);
      exit(EXIT_FAILURE);
    }
}


/**********************************************
 *** Memory allocation and data generation ***
 **********************************************/

void randStore(int32_t n) {
  // rand() returns int32_t. Hence no need to maintain 64bit bank.
  RAND_BANK32 = (int32_t *)malloc(n * sizeof(int32_t));
  RAND_BANK16 = (int16_t *)malloc(n * sizeof(int16_t));
  for (int i=0; i<n; i++) {
    RAND_BANK32[i] = (rand() % MAX_32B_KEY) + 1;
    RAND_BANK16[i] = (rand() % MAX_16B_KEY) + 1;
  }
  qsort(RAND_BANK32, n, sizeof(int32_t), int32Comparator);
  qsort(RAND_BANK16, n, sizeof(int16_t), int16Comparator);
  
  // Remove duplicates
  int j = 0;
  for (int i=0; i<n-1; i++)
    if (RAND_BANK32[i] != RAND_BANK32[i+1])
       RAND_BANK32[j++] = RAND_BANK32[i];
  RAND_BANK32[j++] = RAND_BANK32[n-1];

  j = 0;
  for (int i=0; i<n-1; i++)
    if (RAND_BANK16[i] != RAND_BANK16[i+1])
       RAND_BANK16[j++] = RAND_BANK16[i];
  RAND_BANK16[j++] = RAND_BANK16[n-1];
}

/* Allocate memory for a column */
void allocColumnMemory(void** allCols, int cIndex, DataType dt, size_t nRows) 
{
  switch(dt) {
    case INT16:
      allCols[cIndex] = malloc(nRows * sizeof(int16_t));
      memset(allCols[cIndex], 0, nRows*sizeof(int16_t));
      break;
    case INT32:
      allCols[cIndex] = malloc(nRows * sizeof(int32_t));
      memset(allCols[cIndex], 0, nRows*sizeof(int32_t));
      break;
    case INT64:
      allCols[cIndex] = malloc(nRows * sizeof(int64_t));
      memset(allCols[cIndex], 0, nRows*sizeof(int64_t));
      break;
    default:
      printf("Unknown data type (%d)\n", dt);
      exit(EXIT_FAILURE);
  }
}

/* Initialize a column with random values */
void initColumn(void** allCols, int cIndex, DataType* dt, size_t* startOff, int simple)
{
  size_t nDist = NUM_DISTINCTS;
  int i;
  switch(dt[cIndex]) {
    case INT16: {
      for (i=0; i<nDist; i++) {
        int16_t val = simple == 1 ? (RAND_BANK16[i] % 100) + 1 : RAND_BANK16[i];
        ((int16_t *)allCols[cIndex])[startOff[i]] = val;
      }
      break;
    }
    case INT32: {
      for (i=0; i<nDist; i++) {
        int32_t val = simple == 1 ? (RAND_BANK32[i] % 100) + 1 : RAND_BANK32[i];
        ((int32_t *)allCols[cIndex])[startOff[i]] = val;
      }
      break;
    }
    case INT64: {
      for (i=0; i<nDist; i++) {
        int64_t val = simple == 1 ? (RAND_BANK32[i] % 100) + 1 : RAND_BANK32[i];
        ((int64_t *)allCols[cIndex])[startOff[i]] = val;
      }
      break;
    }

    default:
      printf("Unknown data type (%d)\n", dt[cIndex]);
      exit(EXIT_FAILURE);
  }
}

/* Initialize a single cell with a random value */
void initVal(void** allCols, int cIndex, int rIndex, DataType type)
{
  switch(type) {
    case INT16:
      ((int16_t *)allCols[cIndex])[rIndex] = (rand() % MAX_16B_KEY) + 1;
      break;
    case INT32:
      ((int32_t *)allCols[cIndex])[rIndex] = (rand() % MAX_32B_KEY) + 1;
      break;
    case INT64:
      ((int64_t *)allCols[cIndex])[rIndex] = (rand() % MAX_64B_KEY) + 1;
      break;
    default:
      printf("Unknown data type (%d)\n", type);
      exit(EXIT_FAILURE);
  }
}



