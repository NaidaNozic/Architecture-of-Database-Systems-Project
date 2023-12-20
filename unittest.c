#include "api.h"
#include "match.h"
#include "miscsvc.h"
#include "utils.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

int SPEED_TEST = 0;
size_t NUM_ROWS = 12;
size_t NUM_DISTINCTS = 3;
size_t NUM_KEYS = 1;
size_t NUM_COLS = 2;
DataType GRPTYPE = INT32;
int16_t MAX_16B_KEY = 0x7fff;
int32_t MAX_32B_KEY = 0x7FFFFFFFL;
int64_t MAX_64B_KEY = 0x7FFFFFFFFFFFFFFFLL;
int32_t* RAND_BANK32 = NULL;
int16_t* RAND_BANK16 = NULL;

// Global variables corresponding to command line arguments.
int debug = 0;
size_t selectedMajor = 0;
size_t selectedMinor = 0;

// Global counters used during the tests.
size_t testMajor;
size_t testMinor;
size_t numTested;
size_t numPassed;

// ****************************************************************************
// Printing
// ****************************************************************************

/* Prints a relation */
void displayRows(Relation *rel) {
    printf("Relation: %ld rows x %ld columns\n", rel->numRows, rel->numCols);
    printf("columns: [");
    for (size_t c = 0; c < rel->numCols; c++) {
        printf("C%ld:%s", c, nameOfDataType(rel->colTypes[c]));
        if(c < rel->numCols - 1)
            printf(", ");
    }
    printf(")\n");
    for (size_t i = 0; i < rel->numRows; i++) {
        printf("R%ld: | ", i);
        for (size_t j = 0; j < rel->numCols; j++) {
            switch (rel->colTypes[j]) {
                case INT16:
                    printf("%d | ", ((int16_t *) (rel->cols)[j])[i]);
                    break;
                case INT32:
                    printf("%d | ", ((int32_t *) (rel->cols)[j])[i]);
                    break;
                case INT64:
                    printf("%ld | ", ((int64_t *) (rel->cols)[j])[i]);
                    break;
            }
        }
        printf("\n");
    }
}

// ****************************************************************************
// Utilities
// ****************************************************************************

Relation* expand(Relation* rel, DataType dt) {
    // Assumes that all column types in rel are INT16.
    Relation* res = (Relation*)malloc(sizeof(Relation));
    res->numRows = rel->numRows;
    res->numCols = rel->numCols;
    res->colTypes = (DataType*)malloc(res->numCols * sizeof(DataType));
    res->cols = (void**)malloc(res->numCols * sizeof(void*));
    for(size_t c = 0; c < res->numCols; c++) {
        res->colTypes[c] = dt;
        res->cols[c] = (void*)malloc(res->numRows * sizeOfDataType(dt));
        for(size_t r = 0; r < res->numRows; r++)
            switch(dt) {
                case INT16:
                    ((int16_t*)(res->cols[c]))[r] = ((int16_t*)(rel->cols[c]))[r];
                    break;
                case INT32:
                    ((int32_t*)(res->cols[c]))[r] = ((int16_t*)(rel->cols[c]))[r];
                    break;
                case INT64:
                    ((int64_t*)(res->cols[c]))[r] = ((int16_t*)(rel->cols[c]))[r];
                    break;
            }
    }
    return res;
}

// ****************************************************************************
// Creating of relations
// ****************************************************************************

/* Creates a relation from a 1d array of int64_t */
Relation* makeRelationFrom64(size_t numRows, size_t numCols, DataType* colTypes, int64_t* data) {
    Relation* rel = (Relation*)malloc(sizeof(Relation));
    rel->numRows = numRows;
    rel->numCols = numCols;
    rel->colTypes = (DataType*)malloc(numCols * sizeof(DataType));
    rel->cols = (void**)malloc(numCols * sizeof(void*));
    for(size_t c = 0; c < numCols; c++) {
        rel->colTypes[c] = colTypes[c];
        size_t size = sizeOfDataType(colTypes[c]);
        rel->cols[c] = (void*)malloc(numRows * size);
    }
    for(size_t r = 0; r < numRows; r++)
        for(size_t c = 0; c < numCols; c++) {
            size_t size = sizeOfDataType(colTypes[c]);
            memcpy(((int8_t*)(rel->cols[c])) + r * size, &data[r * numCols + c], size);
        }
    return rel;
}

/* Creates a relation from a 1d array of int16_t */
Relation* makeRelationFrom16(size_t numRows, size_t numCols, DataType* colTypes, int16_t* data) {
    Relation* rel = (Relation*)malloc(sizeof(Relation));
    rel->numRows = numRows;
    rel->numCols = numCols;
    rel->colTypes = (DataType*)malloc(numCols * sizeof(DataType));
    rel->cols = (void**)malloc(numCols * sizeof(void*));
    for(size_t c = 0; c < numCols; c++) {
        rel->colTypes[c] = colTypes[c];
        size_t size = sizeOfDataType(colTypes[c]);
        rel->cols[c] = (void*)malloc(numRows * size);
    }
    for(size_t r = 0; r < numRows; r++)
        for(size_t c = 0; c < numCols; c++) {
            size_t size = sizeOfDataType(colTypes[c]);
            memcpy(((int8_t*)(rel->cols[c])) + r * size, &data[r * numCols + c], size);
        }
    return rel;
}

/* Creates a relation from a 1d array of int64_t */
Relation* makeRelationInt64(size_t numRows, size_t numCols, int64_t* data) {
    DataType* colTypes = (DataType*)malloc(numCols * sizeof(DataType));
    for(size_t c = 0; c < numCols; c++)
        colTypes[c] = INT64;
    Relation* rel = makeRelationFrom64(numRows, numCols, colTypes, data);
    free(colTypes);
    return rel;
}

/* Creates a relation from a 1d array of int16_t */
Relation* makeRelationInt16(size_t numRows, size_t numCols, int16_t* data) {
    DataType* colTypes = (DataType*)malloc(numCols * sizeof(DataType));
    for(size_t c = 0; c < numCols; c++)
        colTypes[c] = INT16;
    Relation* rel = makeRelationFrom16(numRows, numCols, colTypes, data);
    free(colTypes);
    return rel;
}

// ****************************************************************************
// Execution of a single test case
// ****************************************************************************

/* Executes a single test case */
void checkCase(
        Relation* exp, Relation* in,
        size_t numGrpCols, size_t* grpColIdxs,
        size_t numAggCols, size_t* aggColIdxs, AggFunc* aggFuncs
) {
    ++testMinor;
    if(selectedMajor && selectedMinor && (testMajor != selectedMajor || testMinor != selectedMinor))
        return;
    printf("test case %ld.%ld: ", testMajor, testMinor);
    
    Relation* res = (Relation*)malloc(sizeof(Relation));
    groupByAgg(res, in, numGrpCols, grpColIdxs, numAggCols, aggColIdxs, aggFuncs);
    
    if(debug) {
        printf("\n\ninput:\n");
        displayRows(in);
        printf("\ngrouping on (");
        for(size_t c = 0; c < numGrpCols; c++) {
            printf("C%ld", grpColIdxs[c]);
            if(c < numGrpCols - 1)
                printf(", ");
        }
        printf("), aggregating on (");
        for(size_t c = 0; c < numAggCols; c++) {
            printf("C%ld:", aggColIdxs[c]);
            switch(aggFuncs[c]) {
                case SUM: printf("SUM"); break;
                case MIN: printf("MIN"); break;
                case MAX: printf("MAX"); break;
            }
            if(c < numAggCols - 1)
                printf(", ");
        }
        printf(")\n");
        printf("\nexpected (in any row order):\n");
        displayRows(exp);
        printf("\nfound:\n");
        displayRows(res);
        printf("\n");
    }
    
    int check = matchRelations(res, exp);
    numTested++;
    if(check == EXIT_SUCCESS) {
        printf("PASS\n");
        numPassed++;
    }
    
    freeRelation(res, 1, 1);
}

// ****************************************************************************
// Automatic variation of test cases
// ****************************************************************************

// Data types to consider.
DataType dts[] = {INT16, INT32, INT64}; // the order should be as in the enum
size_t numDTs = sizeof(dts) / sizeof(DataType);

// Aggregation functions to consider.
AggFunc afs[] = {SUM, MIN, MAX}; // the order should be as in the enum
size_t numAFs = sizeof(afs) / sizeof(AggFunc);

/* Varies the aggregation functions to use */
void varyCaseAggFuncs(Relation* expFull, Relation* in, size_t numGrpCols) {
    size_t numAggCols = in->numCols - numGrpCols;
    size_t* grpColIdxs = (size_t*)malloc(numGrpCols * sizeof(size_t));
    for(size_t c = 0; c < numGrpCols; c++)
        grpColIdxs[c] = c;
    
    // One agg func per agg col, common agg func for all cols.
    {
        size_t numAggColsUse = numAggCols;
        size_t* aggColIdxs = (size_t*)malloc(numAggColsUse * sizeof(size_t));
        AggFunc* aggFuncs = (AggFunc*)malloc(numAggColsUse * sizeof(AggFunc));
        for(size_t c = 0; c < numAggColsUse; c++)
            aggColIdxs[c] = numGrpCols + c;
        size_t numColsLess = numGrpCols + numAggColsUse;
        size_t* colIdxsLess = (size_t*)malloc(numColsLess * sizeof(size_t));
        for(size_t i = 0; i < numAFs; i++) {
            AggFunc af = afs[i];
            for(size_t c = 0; c < numAggColsUse; c++)
                aggFuncs[c] = af;
            memcpy(colIdxsLess, grpColIdxs, numGrpCols * sizeof(size_t));
            for(size_t c = 0; c < numAggColsUse; c++)
                colIdxsLess[numGrpCols + c] = numGrpCols + numAFs * c + af;
            Relation* expLess = project(expFull, numColsLess, colIdxsLess);
            checkCase(
                    expLess, in,
                    numGrpCols, grpColIdxs,
                    numAggColsUse, aggColIdxs, aggFuncs
            );
            freeRelation(expLess, 1, 0);
        }
        free(aggColIdxs);
        free(aggFuncs);
        free(colIdxsLess);
    }
    
    // All agg funcs for each col.
    {
        size_t numAggColsUse = numAggCols * numAFs;
        size_t* aggColIdxs = (size_t*)malloc(numAggColsUse * sizeof(size_t));
        AggFunc* aggFuncs = (AggFunc*)malloc(numAggColsUse * sizeof(AggFunc));
        for(size_t c = 0; c < numAggColsUse; c++) {
            aggColIdxs[c] = numGrpCols + c / numAFs;
            aggFuncs[c] = afs[c % numAFs];
        }
        checkCase(
                expFull, in,
                numGrpCols, grpColIdxs,
                numAggColsUse, aggColIdxs, aggFuncs
        );
        free(aggColIdxs);
        free(aggFuncs);
    }
    
    free(grpColIdxs);
}

/* Varies which value columns to aggregate */
void varyCaseAggCols(Relation* expFull, Relation* inFull, size_t numGrpCols) {
    size_t numAggColsFull = inFull->numCols - numGrpCols;
    
    // Use each agg col individually.
    size_t numExpColsLess = numGrpCols + numAFs;
    size_t* expColIdxsLess = (size_t*)malloc(numExpColsLess * sizeof(size_t));
    for(size_t c = 0; c < numGrpCols; c++)
        expColIdxsLess[c] = c;
    size_t numInColsLess = numGrpCols + 1;
    size_t* inColIdxsLess = (size_t*)malloc(numInColsLess * sizeof(size_t));
    for(size_t c = 0; c < numGrpCols; c++)
        inColIdxsLess[c] = c;
    for(size_t c = 0; c < numAggColsFull; c++) {
        for(size_t i = 0; i < numAFs; i++)
            expColIdxsLess[numGrpCols + i] = numGrpCols + c * numAFs + i;
        inColIdxsLess[numGrpCols] = numGrpCols + c;
        Relation* expLess = project(expFull, numExpColsLess, expColIdxsLess);
        Relation* inLess = project(inFull, numInColsLess, inColIdxsLess);
        varyCaseAggFuncs(expLess, inLess, numGrpCols);
        freeRelation(expLess, 1, 0);
        freeRelation(inLess, 1, 0);
    }
    free(expColIdxsLess);
    free(inColIdxsLess);
    
    // Use all agg cols.
    varyCaseAggFuncs(expFull, inFull, numGrpCols);
}

/* Varies the column types */
void varyCaseColTypes(Relation* exp, Relation* in, size_t numGrpCols) {
    Relation** ins = (Relation**)malloc(numDTs * sizeof(Relation*));
    Relation** exps = (Relation**)malloc(numDTs * sizeof(Relation*));
    for(size_t i = 0; i < numDTs; i++) {
        ins[i] = expand(in, dts[i]);
        exps[i] = expand(exp, dts[i]);
    }
    
    Relation* in2 = (Relation*)malloc(sizeof(Relation));
    in2->numRows = in->numRows;
    in2->numCols = in->numCols;
    in2->colTypes = (DataType*)malloc(in2->numCols * sizeof(DataType));
    in2->cols = (void**)malloc(in2->numCols * sizeof(void*));
    for(size_t c = 0; c < in2->numCols; c++) {
        in2->colTypes[c] = INT64;
        in2->cols[c] = ins[INT64]->cols[c];
    }
    
    Relation* exp2 = (Relation*)malloc(sizeof(Relation));
    exp2->numRows = exp->numRows;
    exp2->numCols = exp->numCols;
    exp2->colTypes = (DataType*)malloc(exp2->numCols * sizeof(DataType));
    exp2->cols = (void**)malloc(exp2->numCols * sizeof(void*));
    for(size_t c = 0; c < exp2->numCols; c++) {
        exp2->colTypes[c] = INT64;
        exp2->cols[c] = exps[INT64]->cols[c];
    }
    
    for(size_t c = 0; c < in->numCols; c++) {
        for(size_t i = 0; i < numDTs; i++) {
            in2->colTypes[c] = dts[i];
            in2->cols[c] = ins[dts[i]]->cols[c];
            
            if(c < numGrpCols) { // grouping cols
                DataType dt = dts[i];
                exp2->colTypes[c] = dt;
                exp2->cols[c] = exps[dt]->cols[c];
            }
            else { // agg cols
                for(size_t k = 0; k < numAFs; k++) {
                    size_t cc = numGrpCols + (c - numGrpCols) * numAFs + k;
                    DataType dt = getAggType(dts[i], afs[k]);
                    exp2->colTypes[cc] = dt;
                    exp2->cols[cc] = exps[dt]->cols[cc];
                }
            }
            
            varyCaseAggCols(exp2, in2, numGrpCols);
        }
    }
    
    for(size_t i = 0; i < numDTs; i++) {
        freeRelation(ins[i], 1, 1);
        freeRelation(exps[i], 1, 1);
    }
    free(ins);
    free(exps);
    freeRelation(exp2, 1, 0);
    freeRelation(in2, 1, 0);
}

// ****************************************************************************
// Main function
// ****************************************************************************

int main(int argc, char** argv)
{
    if(argc == 4) {
        debug = atoi(argv[1]);
        selectedMajor = atoi(argv[2]);
        selectedMinor = atoi(argv[3]);
    }
    else if(argc != 1) {
        printf("Usage: %s [PrintData TestMajor TestMinor]\n\n", argv[0]);
        printf("Invoke without arguments to run all test cases or provide\n");
        printf("all of the following arguments:\n");
        printf("\n");
        printf("- PrintData: 0 (don't print data) or 1 (print data)\n");
        printf("- TestMajor TestMinor: number of the test case to execute\n");
        printf("\n");
        printf("Examples:\n");
        printf("- To run all test cases: %s\n", argv[0]);
        printf("- To run only test case 2.34 and print the data: %s 1 2 34\n", argv[0]);
        exit(EXIT_FAILURE);
    }
    
    printf("Starting unit tests\n\n");

    numTested = 0;
    numPassed = 0;
    
    // ************************************************************************
    // Tests with small hand-crafted input/output
    // ************************************************************************
    
    { // no grouping column (whole-column aggregation)
        testMajor = 1;
        testMinor = 0;
        
        int16_t inData[] = {
             1,  50,
             2, -20,
            -5,  30,
             7,  10,
             0, -10,
             0,  20,
            -3,  30,
        };
        Relation* in = makeRelationInt16(7, 2, inData);

        int16_t expData[] = {
            2, -5, 7, 110, -20, 50,
        };
        Relation* exp = makeRelationInt16(1, 6, expData);

        varyCaseColTypes(exp, in, 0);
        
        freeRelation(in, 1, 1);
        freeRelation(exp, 1, 1);
    }
    { // one grouping column, a single input row
        testMajor = 2;
        testMinor = 0;
        
        int16_t inData[] = {
            1, 10, 200,
        };
        Relation* in = makeRelationInt16(1, 3, inData);

        int16_t expData[] = {
            1, 10, 10, 10, 200, 200, 200,
        };
        Relation* exp = makeRelationInt16(1, 7, expData);

        varyCaseColTypes(exp, in, 1);
        
        freeRelation(in, 1, 1);
        freeRelation(exp, 1, 1);
    }
    { // one grouping column, multiple input rows
        testMajor = 3;
        testMinor = 0;
        
        int16_t inData[] = {
            1,  10, 111,
            1, -20, 222,
            2,  30, 111,
            1,  40, 111,
            3,  20, 333,
            2, -10, 111,
            2, -20,   0,
            3,  10, 111,
            1, -30,   0,
            3,  40, 222,
        };
        Relation* in = makeRelationInt16(10, 3, inData);

        int16_t expData[] = {
            1,  0, -30, 40, 444,   0, 222,
            2,  0, -20, 30, 222,   0, 111,
            3, 70,  10, 40, 666, 111, 333,
        };
        Relation* exp = makeRelationInt16(3, 7, expData);

        varyCaseColTypes(exp, in, 1);
        
        freeRelation(in, 1, 1);
        freeRelation(exp, 1, 1);
    }
    { // two grouping columns, multiple input rows
        testMajor = 4;
        testMinor = 0;
        
        int16_t inData[] = {
            -1, 10,  300, 1111,
            -1, 15,  200, 2222,
            -2, 10,  100, 1111,
             3, 15,    0, 2222,
            -1, 15, -100, 1111,
            -2, 10, -200, 2222,
             3, 20, -300, 1111,
            -1, 10, -400, 2222,
            -2, 15, -300, 1111,
            -2, 15, -200, 2222,
             3, 20, -100, 1111,
        };
        Relation* in = makeRelationInt16(11, 4, inData);

        int16_t expData[] = {
            -2, 10, -100, -200,  100, 3333, 1111, 2222, 
            -2, 15, -500, -300, -200, 3333, 1111, 2222, 
            -1, 10, -100, -400,  300, 3333, 1111, 2222, 
            -1, 15,  100, -100,  200, 3333, 1111, 2222, 
             3, 15,    0,    0,    0, 2222, 2222, 2222, 
             3, 20, -400, -300, -100, 2222, 1111, 1111, 
        };
        Relation* exp = makeRelationInt16(6, 8, expData);

        varyCaseColTypes(exp, in, 2);
        
        freeRelation(in, 1, 1);
        freeRelation(exp, 1, 1);
    }
    { // three grouping columns, multiple input rows, unique groups
        testMajor = 5;
        testMinor = 0;
        
        int16_t inData[] = {
            -1, 10, 11,  300, 1111,
            -1, 15, 11,  200, 2222,
            -2, 10, 11,  100, 1111,
             3, 15, 22,    0, 2222,
            -1, 15, 22, -100, 1111,
            -2, 10, 22, -200, 2222,
             3, 20, 33, -300, 1111,
            -1, 10, 33, -400, 2222,
            -2, 15, 33, -300, 1111,
            -2, 15, 22, -200, 2222,
             3, 20, 11, -100, 1111,
        };
        Relation* in = makeRelationInt16(11, 5, inData);

        int16_t expData[] = {
            -2, 10, 11,  100,  100,  100, 1111, 1111, 1111, 
            -2, 10, 22, -200, -200, -200, 2222, 2222, 2222, 
            -2, 15, 22, -200, -200, -200, 2222, 2222, 2222, 
            -2, 15, 33, -300, -300, -300, 1111, 1111, 1111, 
            -1, 10, 11,  300,  300,  300, 1111, 1111, 1111, 
            -1, 10, 33, -400, -400, -400, 2222, 2222, 2222, 
            -1, 15, 11,  200,  200,  200, 2222, 2222, 2222, 
            -1, 15, 22, -100, -100, -100, 1111, 1111, 1111, 
             3, 15, 22,    0,    0,    0, 2222, 2222, 2222, 
             3, 20, 11, -100, -100, -100, 1111, 1111, 1111, 
             3, 20, 33, -300, -300, -300, 1111, 1111, 1111, 
        };
        Relation* exp = makeRelationInt16(11, 9, expData);

        varyCaseColTypes(exp, in, 3);
        
        freeRelation(in, 1, 1);
        freeRelation(exp, 1, 1);
    }

    // ************************************************************************
    // Tests with large inputs and small hand-crafted outputs
    // ************************************************************************
    
    { // no grouping column, pure aggregation
        testMajor = 6;
        testMinor = 0;
        
        DataType inColTypes[] = {INT16};
        Relation* in = createRelation(9000, 1, inColTypes);
        for(size_t r = 0; r < in->numRows; r++)
            ((int16_t*)(in->cols[0]))[r] = 2 + r % 3;
        
        int16_t expData[] = {
            27000, 2, 4,
        };
        Relation* exp = makeRelationInt16(1, 3, expData);

        varyCaseColTypes(exp, in, 0);
        
        freeRelation(in, 0, 1);
        freeRelation(exp, 1, 1);
    }
    { // one grouping column
        testMajor = 7;
        testMinor = 0;
        
        DataType inColTypes[] = {INT16, INT16};
        Relation* in = createRelation(30000, 2, inColTypes);
        for(size_t r = 0; r < in->numRows; r++) {
            ((int16_t*)(in->cols[0]))[r] = 77 + (r % 2) * 22;
            ((int16_t*)(in->cols[1]))[r] = 1 - 2 * (r % 2);
        }
        
        int16_t expData[] = {
            77,  15000,  1,  1,
            99, -15000, -1, -1,
        };
        Relation* exp = makeRelationInt16(2, 4, expData);

        varyCaseColTypes(exp, in, 1);
        
        freeRelation(in, 0, 1);
        freeRelation(exp, 1, 1);
    }
    
    printf("\nSUMMARY\n=======\n");
    printf("passed %ld/%ld test cases\n", numPassed, numTested);
    if(numPassed == numTested)
        printf("all tests passed\n");
    else
        printf("some tests failed\n");
}