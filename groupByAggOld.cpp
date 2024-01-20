// Students: Implement your solution in this file. Initially, it is a copy of
//           groupByAggBaseline.cpp.

#include "api.h"
#include "setmap_utils.h"
#include "utils.h"
#include "thread"

#include <functional>
#include <unordered_map>

#include <vector>
#include <cstring>
#include <iostream>
#include <mutex>

// ****************************************************************************
// Group-by operator with aggregation
// ****************************************************************************
std::mutex hashTableMutex;

void threadFunction(
    Relation& inKeys,
    Relation& inVals,
    size_t start, size_t end,
    size_t numGrpCols, size_t* grpColIdxs,
    size_t numAggCols, size_t* aggColIdxs,
    AggFunc* aggFuncs,
    std::unordered_map<Row, int64_t*>& ht,
    std::mutex& mutex
) {
    Row* keys = initRow(&inKeys);
    Row* vals = initRow(&inVals);

    for(size_t r = start; r < end; ++r) {
        getRow(keys, &inKeys, r);
        getRow(vals, &inVals, r);
        // Search the key combination in the hash-table.
        std::lock_guard<std::mutex> lock(mutex);
        int64_t*& accs = ht[*keys];
        if(accs) {
            // This key combination is already in the hash-table.
            // Update the accumulators.
            for(size_t c = 0; c < numAggCols; ++c) {
                int64_t val = getValueInt64(vals, c);
                switch(aggFuncs[c]) {
                    case AggFunc::SUM: accs[c] += val; break;
                    case AggFunc::MIN: accs[c] = std::min(accs[c], val); break;
                    case AggFunc::MAX: accs[c] = std::max(accs[c], val); break;
                    default: exit(EXIT_FAILURE);
                }
            }
        }
        else {
            // This key combination is not in the hash-table yet.
            // Allocate and initialize the accumulators.
            accs = (int64_t*)(malloc(numAggCols * sizeof(int64_t*)));
            for(size_t c = 0; c < numAggCols; ++c){
                accs[c] = getValueInt64(vals, c);
            }
            keys->values = (void**)malloc(keys->numCols * sizeof(void*));
        }
    }
    freeRow(keys);
    freeRow(vals);
}

/* Student implementation of the `groupByAgg`-operator */
void groupByAgg(
        Relation* res,
        const Relation* in,
        size_t numGrpCols, size_t* grpColIdxs,
        size_t numAggCols, size_t* aggColIdxs, AggFunc* aggFuncs
) {
    size_t numThreads = std::thread::hardware_concurrency();
    size_t chunkSize = in->numRows / numThreads;
    if(chunkSize == 0){
        chunkSize = in->numRows;
        numThreads = 1;
    }

    std::vector<std::unordered_map<Row, int64_t*>> threadHashTables(numThreads);
    std::vector<std::thread> threads;

    Relation* inKeys = project(in, numGrpCols, grpColIdxs);
    Relation* inVals = project(in, numAggCols, aggColIdxs);

    for (size_t i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? in->numRows : (i + 1) * chunkSize;

        threads.emplace_back(threadFunction, std::ref(*inKeys), std::ref(*inVals), start, end, 
                                numGrpCols, grpColIdxs, numAggCols, aggColIdxs, aggFuncs, std::ref(threadHashTables[i]), std::ref(hashTableMutex));
    }
    for (auto& thread : threads) {
        thread.join();
    }

    std::unordered_map<Row, int64_t*>& ht = threadHashTables[0];

    for (size_t i = 1; i < numThreads; i++) {
        for (auto entry : threadHashTables[i]) {  
            auto*& accs = ht[entry.first];
            if (accs) {
                for (size_t c = 0; c < numAggCols; ++c) {
                    switch (aggFuncs[c]) {
                        case AggFunc::SUM: accs[c] += entry.second[c]; break;
                        case AggFunc::MIN: accs[c] = std::min(accs[c], entry.second[c]); break;
                        case AggFunc::MAX: accs[c] = std::max(accs[c], entry.second[c]); break;
                        default: 
                            std::cout << "Error: Unknown aggregation function encountered." << std::endl;
                            exit(EXIT_FAILURE);
                    }
                }
                free(entry.second);
                free(entry.first.values);
            } else {
                accs = entry.second;
            }
        }
    }

    // Initialize the result relation.
    res->numRows = ht.size();
    res->numCols = numGrpCols + numAggCols;
    res->colTypes = (DataType*)malloc(res->numCols * sizeof(DataType));
    res->cols = (void**)malloc(res->numCols * sizeof(void*));

    for(size_t c = 0; c < numGrpCols; c++) {
        res->colTypes[c] = inKeys->colTypes[c];
        res->cols[c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[c]));
    }

    for(size_t c = 0; c < numAggCols; c++) {
        res->colTypes[numGrpCols + c] = getAggType(inVals->colTypes[c], aggFuncs[c]);
        res->cols[numGrpCols + c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[numGrpCols + c]));
    }
   
    // Populate the result with the data from the hash-table.
    size_t r = 0;
    Row* dst = initRow(res);
    for(auto entry : ht) {
        getRow(dst, res, r++);
        Row keys = entry.first;
        for(size_t c = 0; c < inKeys->numCols; c++)
            memcpy(dst->values[c], keys.values[c], sizeOfDataType(inKeys->colTypes[c]));
        free(keys.values);
        int64_t* accs = entry.second;
        for(size_t c = 0; c < inVals->numCols; c++) {
            memcpy(dst->values[numGrpCols + c], &accs[c], sizeOfDataType(res->colTypes[numGrpCols + c]));
        }
        free(accs);
    }
    freeRow(dst);
    
    freeRelation(inKeys, 1, 0);
    freeRelation(inVals, 1, 0);
}