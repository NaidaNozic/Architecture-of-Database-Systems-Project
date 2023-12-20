#include "api.h"
#include "hardware.h"
#include "miscsvc.h"
#include "match.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <sys/time.h>

#define NUM_TESTCASES 11

int SPEED_TEST = 1;
size_t NUM_ROWS = 12;
size_t NUM_DISTINCTS = 3;
size_t NUM_KEYS = 1;
size_t NUM_COLS = 2;
DataType GRPTYPE = INT32;
int SORT_KEYS = 0;
int SKEW_KEYS = 0;
int16_t MAX_16B_KEY = 0x7fff;
int32_t MAX_32B_KEY = 0x7FFFFFFFL;
int64_t MAX_64B_KEY = 0x7FFFFFFFFFFFFFFFLL;
int32_t* RAND_BANK32 = NULL;
int16_t* RAND_BANK16 = NULL;
int debug = 0;
int curTestNo = 0;
FILE * outFile = NULL;
FILE * refFile = NULL;
time_t refTimes[NUM_TESTCASES];
float speedups[NUM_TESTCASES];

/* Print the table */
void displayRows(Relation *rel)
{
  size_t nR = rel->numRows;
  int32_t nC = rel->numCols;
  DataType* cTypes = rel->colTypes;
  void** cols = rel->cols;
  int i,j;

  for (i=0; i<nR; i++) {
    printf("Row #%d: ", i);
    for (j=0; j<nC; j++) {
      printf("Col %d: ",j);
      switch (cTypes[j]) {
        case INT16:
          printf("[%d, %d] | ", cTypes[j], ((int16_t *)cols[j])[i]);
          break;
        case INT32:
          printf("[%d, %d] | ", cTypes[j], ((int32_t *)cols[j])[i]);
          break;
        case INT64:
          printf("[%d, %ld] | ", cTypes[j], ((int64_t *)cols[j])[i]);
          break;
      }
    }
    printf("\n");
  }
}

/* Generate offset array (starting offsets of distinct values) */
size_t initOffsetArray(size_t* distOffsets)
{
  size_t nDist = NUM_DISTINCTS;
  size_t nRows = NUM_ROWS;
  size_t i;
  
  //Equal distribution if skew is not needed
  if (!SKEW_KEYS) {
    size_t off = floor((double)nRows/nDist);
    for (i=0; i<nDist; i++) 
      distOffsets[i] = i*off;
    return NUM_DISTINCTS;
  }

  //Distribute according to Harmonic series (sarting 10%)
  distOffsets[0] = 0;
  size_t firstOff = floor((double)nRows * 0.10); 
  size_t nextOff = firstOff; 
  for (i=1; i<nDist; i++) {
    distOffsets[i] = nextOff;
    nextOff = nextOff + ceil(((float)firstOff)/(1+i));
    if (nextOff > nRows) {
      NUM_DISTINCTS = i;
      nDist = i;
      break;
    }
    if (i == nDist-1) {
      distOffsets[i] = nextOff;
      break;
    }
  }
  if (SKEW_KEYS)
    printf("distincts per key after applying skew: %ld\n", NUM_DISTINCTS);

  return NUM_DISTINCTS;
}



/* Initialize a relation */
void initRelation(Relation* inpRel, size_t nR, size_t nC, DataType* dt, void** cols)
{
  inpRel->numRows = nR;
  inpRel->numCols = nC;
  inpRel->colTypes = dt;
  inpRel->cols = cols;
}

/* Call the API 3 times, calculate avg time taken */
void benchmarkAPI(Relation* res, Relation* inpRel, Relation* expRes, size_t nKeys, size_t nCols) 
{
  struct timezone tz;
  struct timeval start, end;
  time_t avg, tot = 0;
  int RUN = 3;
  int ret = 0;
  size_t grpColInd[nKeys];
  size_t aggColInd[nCols-nKeys];
  size_t j = 0;
  for (size_t i=0; i<nCols; i++) {
    if (i < nKeys)
      grpColInd[i] = i;
    else {
      aggColInd[j] = i;
      j++;
    }
  }
  AggFunc aggF[] = {SUM};

  for (int i=0; i<RUN; i++) {
    gettimeofday(&start, &tz);
    groupByAgg(res, inpRel, nKeys, grpColInd, nCols-nKeys, aggColInd, aggF);
    gettimeofday(&end, &tz);

    if (i == 0) {
      //Print returned rows
      if (debug) {
        printf("Returned results\n");
        displayRows(res);
      }

      // Print expected results. TODO: print in a file
      if (debug) {
        printf("\nExpected results\n");
        displayRows(expRes);
      }

      ret = matchRelations(res, expRes);
      if (ret != EXIT_SUCCESS) return;   //early abort
    }
    tot += (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
    printf("Time to complete run %d: %li milliseconds.\n", i+1, (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000);
  }
  avg = ((double)tot)/RUN;
  printf("Average time to complete (%d runs): %li milliseconds.\n", RUN, avg);
  if (outFile)
    fprintf(outFile, "%d\t%ld\n", curTestNo, avg);
  if (refFile) {
    time_t refAvg = refTimes[curTestNo - 1];
    printf("Speed-up: ");
    if (refAvg == -1)
      printf("(no reference time available for this case)\n");
    else {
      float speedup = (float)refAvg / avg;
      printf("%.2fx\n", speedup);
      speedups[curTestNo - 1] = speedup;
    }
  }
}

/* Driver function */
int run_speedtest(void)
{
  void** allCols;
  void** relCols;
  Relation inpRel, expRes;
  Relation* res = (Relation*)malloc(sizeof(Relation));
  int i, j;
  size_t rIndex;
  size_t nDist = NUM_DISTINCTS;
  size_t nRows = NUM_ROWS;
  size_t nKeys = NUM_KEYS;
  size_t nCols = NUM_COLS;
  size_t *distOffsets;
  DataType dt[nCols], dtRes[nCols];

  // Set the datatypes. Agg cols have INT32 inputs and INT64 results always
  for (i=0; i<nCols; i++) {
    dt[i] = i < nKeys ? GRPTYPE : INT32;
    dtRes[i] = i < nKeys ? GRPTYPE : INT64;
  }

  // Generate offset array (starting offsets of distinct values)
  distOffsets = (size_t *)malloc(nDist * sizeof(size_t));
  memset(distOffsets, 0, nDist * sizeof(size_t));
  nDist = initOffsetArray(distOffsets); //this might change #distincts if skewed

  // Allocate memory for all the columns
  allCols = (void**)malloc(nCols * sizeof(void *));
  memset(allCols, 0, nCols*sizeof(void*));
  for (i=0; i<nCols; i++) 
    allocColumnMemory(allCols, i, dt[i], nRows);

  // Generate random type-specific distinct keys
  for (i=0; i<nKeys; i++) {
    initColumn(allCols, i, dt, distOffsets, 0);
  }

  // Allocate memory for expected result table. Always 64bit result cols.
  relCols = (void**)malloc(nCols * sizeof(void *));
  memset(relCols, 0, nCols*sizeof(void*));
  for (i=0; i<nCols; i++) 
    allocColumnMemory(relCols, i, dtRes[i], nDist);

  // Copy input key cols with nDist rows into result table
  for (i=0; i<nKeys; i++)
    copyColumn(allCols, relCols, i, distOffsets, dt[i], nDist);

  // Fill distinct values
  for (i=0; i<nKeys; i++) {
    rIndex = 0;
    j = 0;
    size_t src = 0;
    for (rIndex=0; rIndex<nRows; rIndex++) {
      if (j < nDist && rIndex == distOffsets[j]) {
        src = distOffsets[j++];
        continue;
      }
      copyValInCol(allCols, i, src, rIndex, dt[i]);
    }
  }

  // Generate aggregate columns and calcualate results for SUM
  for (i=nKeys; i<nCols; i++) {
    rIndex = 0;
    j = 0;
    size_t grp = 0;
    for (rIndex=0; rIndex<nRows; rIndex++) {
      if (j < nDist && rIndex == distOffsets[j]) {
        grp = j;
        j++;
      }
      initVal(allCols, i, rIndex, dt[i]);
      ((int64_t *)relCols[i])[grp] += ((int32_t *)allCols[i])[rIndex]; 
    }
  }
 
  // Fill the expected result table
  initRelation(&expRes, nDist, nCols, dtRes, relCols);

  // Assign keys and payload to the input relation
  initRelation(&inpRel, nRows, nCols, dt, allCols);

  // Shuffle rows
  if (!SORT_KEYS)
    shuffle(&inpRel, nRows);

  if (debug) {
    printf("\nInput rows\n");
    displayRows(&inpRel);
  }

  // Call the API here
  benchmarkAPI(res, &inpRel, &expRes, nKeys, nCols);

  // Cleanup
  freeRelation(res, 1, 1);
  for (i=0; i<nCols; i++) {
    free(allCols[i]);
    free(relCols[i]);
  }
  free(allCols);
  free(relCols);
  free(distOffsets);
  
  return EXIT_SUCCESS;
}

/* Setup the global variables and call test */
int setupAndRun(size_t nK, size_t nA, size_t nD, size_t nR, DataType grpType, int sort, int skew)
{
  NUM_KEYS = nK;        // #grp cols
  NUM_COLS = nK + nA;   // #cols
  NUM_DISTINCTS = nD;   // #distinct values in each grp col
  NUM_ROWS = nR;        // #rows
  GRPTYPE = grpType;    // data type of the grp cols
  SORT_KEYS = sort;     // if true, sort keys
  SKEW_KEYS = skew;

  return run_speedtest();
}

void printHelpAndExit(const char * execName) {
  printf("\nUsage: %s [TESTCASE] [(rec|ref) FILE]\n\n", execName);
  printf("TESTCASE: Select a single test case by 1, 2, ..., 11; or 0 for all test cases.\n\n");
  printf("rec FILE: Record measured runtimes to the specified file.\n");
  printf("          Can be used to capture the runtimes of the naive baseline only once.\n");
  printf("ref FILE: Load reference runtimes for score calculation from the specified file.\n");
  printf("          Can be used to refer to the captured runtimes of the naive baseline.\n");
  exit(EXIT_FAILURE);
}

int main(int argc, char **argv)
{
  int test = 0; // default: run all tests
  outFile = NULL;
  refFile = NULL;
  if (argc == 3 || argc > 4)
    printHelpAndExit(argv[0]);
  if (argc > 1)
    test = atoi(argv[1]);
  if (argc > 2) {
    const char * mode = argv[2];
    if (!strcmp(mode, "rec")) {
      const char * outFilename = argv[3];
      printf("Recording runtimes to %s\n", outFilename);
      outFile = fopen(outFilename, "w");
      if (!outFile) {
        printf("Error: Could not open file %s for writing\n", outFilename);
        exit(EXIT_FAILURE);
      }
    }
    else if (!strcmp(mode, "ref")) {
      const char * refFilename = argv[3];
      printf("Loading reference runtimes from %s\n", refFilename);
      refFile = fopen(refFilename, "r");
      if (!refFile) {
        printf("Error: Could not open file %s for reading\n", refFilename);
        exit(EXIT_FAILURE);
      }
      for (int i = 0; i < NUM_TESTCASES; i++)
        refTimes[i] = -1;
      for (int i = 0; i < NUM_TESTCASES; i++) {
        int testNo;
        time_t refTime;
        int ret = fscanf(refFile, "%d", &testNo);
        if (ret == EOF)
            break;
        if (ret != 1 || getc(refFile) != '\t') {
          printf("Error while reading from reference file (malformatted)\n");
          exit(EXIT_FAILURE);
        }
        ret = fscanf(refFile, "%ld", &refTime);
        if (ret != 1 || getc(refFile) != '\n') {
          printf("Error while reading from reference file (malformatted)\n");
          exit(EXIT_FAILURE);
        }
        if (refTimes[testNo - 1] != -1) {
          printf("Error while reading from reference file (duplicate test case)\n");
          exit(EXIT_FAILURE);
        }
        if (testNo < 1 || testNo > NUM_TESTCASES) {
          printf("Error while reading from reference file (invalid test number)\n");
          exit(EXIT_FAILURE);
        }
        refTimes[testNo - 1] = refTime;
      }
    }
    else {
      printf("Unknown mode for specified file: %s\n", mode);
      printHelpAndExit(argv[0]);
    }
  }
  
  for (int i = 0; i < NUM_TESTCASES; i++)
      speedups[i] = NAN;
  
  printf("Starting speed tests\n");
  
  /* Debug advice: Systematically tune the input parameters to debug a particular case */
  size_t nR = 30000000;

  srand(time(0));
  randStore(nR);

  /* Case 1. #grp = 1, #agg = 1, #distinct = 1K, #rows = 30M, func = SUM */
  curTestNo = 1;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT32), 1K dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 1000, nR, INT32, 0, 0);
  }

  /* Case 2. #grp = 1, #agg = 1, #distinct = 10K, #rows = 30M, func = SUM */
  curTestNo = 2;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT32), 10K dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 10000, nR, INT32, 0, 0);
  }

  /* Case 3. #grp = 1, #agg = 1, #distinct = 100K, #rows = 30M, func = SUM */
  curTestNo = 3;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT32), 100K dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 100000, nR, INT32, 0, 0);
  }

  /* Case 4. #grp = 1, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM */
  curTestNo = 4;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT32), 1M dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 1000000, nR, INT32, 0, 0);
  }

  /* Case 5. #grp = 2, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM */
  curTestNo = 5;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 2 grp cols (INT32), 1M dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(2, 1, 1000000, nR, INT32, 0, 0);
  }

  /* Case 6. #grp = 1, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM */
  curTestNo = 6;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT64), 1M dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 1000000, nR, INT64, 0, 0);
  }

  /* Case 7. #grp = 2, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM */
  curTestNo = 7;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 2 grp cols (INT64), 1M dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(2, 1, 1000000, nR, INT64, 0, 0);
  }

  /* Case 8. #grp = 1, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM */
  curTestNo = 8;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT16), 30K dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 30000, nR, INT16, 0, 0);
  }

  /* Case 9. #grp = 2, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM */
  curTestNo = 9;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 2 grp cols (INT16), 300K dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(2, 1, 30000, nR, INT16, 0, 0);
  }

  /* Case 10. #grp = 1, #agg = 1, #distinct = 1M, #rows = 30M, func = SUM, sort = true*/
  curTestNo = 10;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT32, sorted), 1M dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 1000000, nR, INT32, 1, 0);
  }

  /* Case 11. #grp = 1, #agg = 1, #distinct = 10K, #rows = 30M, func = SUM, skew = true */
  curTestNo = 11;
  if (test == 0 || test == curTestNo) {
    printf("\n#%d SUM: 1 grp cols (INT32, skewed), 10K dist. values (each), 1 agg cols (INT32), and %ld rows\n", curTestNo, nR);
    setupAndRun(1, 1, 10000, nR, INT32, 0, 1);
  }
  
  if (test == 0 && refFile) { // if all tests and reference times used
    float sum = 0;
    for (int i = 0; i < NUM_TESTCASES; i++)
      sum += speedups[i];
    float score = sum / NUM_TESTCASES;
    float targetScore = (float)(NUM_CORES) / 2;
    printf("\n========================================\n");
    printf("OVERALL SCORE (avg. speed-up): %.2f\n", score);
    if (NUM_CORES) {
      printf("You need a score of at least %.2f to pass the grading.\n", targetScore);
      printf("%s\n", score >= targetScore ? "ACHIEVED" : "NOT ACHIEVED");
    }
    else {
      printf("You need a score of at least #cores/2 to pass the grading.\n");
      printf("Please run ./init_hardware.sh (on GNU/Linux) or edit hardware.h manually to see the target score here.\n");
    }
  }
  
  if (outFile)
      fclose(outFile);
  if (refFile)
      fclose(refFile);

  free(RAND_BANK32);
  free(RAND_BANK16);

}

