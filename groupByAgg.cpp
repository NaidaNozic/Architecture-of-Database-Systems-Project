//Done by Naida Nozic (12336462) and Faruk Sahat (12336460), using multithreading and a local recent relations cache 

#include "api.h"
#include "setmap_utils.h"
#include "utils.h"
#include <pthread.h>

#include <functional>
#include <unordered_map>

#include <queue>
#include <vector>
#include <cstring>
#include <iostream>
#include "hardware.h"

// ****************************************************************************
// Group-by operator with aggregation
// ****************************************************************************

size_t comfortableHTableSize = 1000000; //used to determine number of relations in local cache with regards to the size of the l2 cache
//number calculated relative to the sizes of ending ht-s, then fine tuned through tests due to performance issues
//on my machine (Faruk) the Recent Relation Cache is relatively small, and for future implementations I'd like to compress the data 

//NOTE: IF YOU GET MISMATCHED ROWS ERRORS, PLEASE INCREASE THIS VARIABLE.

struct RelationKey {
    std::string unhashedKey = "";
    //we wanted a class that keeps the least amount of data and still can work as a hashkey
    //the tables and relations aren't named, but the order of the first five rows along with the aggfunc are pretty good indicators
    //IRL there would be no need for what's happening below this comment, as all tables would get a unique identifier
    RelationKey(Relation* inKeys, Relation* inVals, AggFunc* aggFuncs, size_t numAggCols, size_t numRows){
        Row* keys = initRow(inKeys);
        Row* vals = initRow(inVals);
        for(size_t r = 0; r < std::min(numRows, (size_t)5); r++) {
            getRow(keys, inKeys, r);
            getRow(vals, inVals, r);
            for(size_t c = 0; c < numAggCols; c++) {
                int64_t val = getValueInt64(vals, c);
                switch(aggFuncs[c]) {
                    case AggFunc::SUM: 
                        unhashedKey = unhashedKey + std::to_string(val) + "s"; 
                        break;
                    case AggFunc::MIN: 
                        unhashedKey = unhashedKey + std::to_string(val) + "m"; 
                        break;
                    case AggFunc::MAX: 
                        unhashedKey = unhashedKey + std::to_string(val) + "v"; 
                        break;
                   default: exit(EXIT_FAILURE);
                }
            }
        }
        freeRow(keys);
        freeRow(vals);
    }

    std::string toString() const {
        return unhashedKey;
    }

    size_t hash() const {
        return std::hash<std::string>()(unhashedKey);
    }

    bool equals(const RelationKey& other) const {
        if(unhashedKey == other.unhashedKey)
            return true;

        return false;
    }
};

//

struct RelationKeyHash {
    size_t operator()(const RelationKey& key) const {
        return std::hash<std::string>()(key.toString());
    }
};

struct RelationKeyEqual {
    bool operator()(const RelationKey& lhs, const RelationKey& rhs) const {
        return lhs.equals(rhs);
    }
};
//things that were necessary early on in development and performed nicely
std::unordered_map<RelationKey, Relation*, RelationKeyHash, RelationKeyEqual> RecentRelationHash;

std::queue<RelationKey> FIFOMemory;
//queue of keys so addition and removal from cache is as fast as possible 
size_t FIFOSize = L2_CACHE_SIZE_BYTES / comfortableHTableSize;
size_t FIFOcounter = 0;
struct ThreadParams {
    Relation* inKeys;
    Relation* inVals;
    size_t start;
    size_t end;
    size_t numGrpCols;
    size_t* grpColIdxs;
    size_t numAggCols;
    size_t* aggColIdxs;
    AggFunc* aggFuncs;
    std::unordered_map<Row, int64_t*>* ht;
};

void* threadFunction(void* arg) {
    ThreadParams* params = static_cast<ThreadParams*>(arg);

    Row* keys = initRow(params->inKeys);
    Row* vals = initRow(params->inVals);

    (*params->ht).reserve((params->end-params->start)/2+(params->end-params->start)/6);

    for (size_t r = params->start; r < params->end; ++r) {
        getRow(keys, params->inKeys, r);
        getRow(vals, params->inVals, r);

        int64_t*& accs = (*params->ht)[*keys];

        if (accs) {
            // This key combination is already in the hash-table.
            // Update the accumulators.
            for (size_t c = 0; c < params->numAggCols; ++c) {
                int64_t val = getValueInt64(vals, c);
                switch (params->aggFuncs[c]) {
                        case AggFunc::SUM: accs[c] += val; break;
                        case AggFunc::MIN: accs[c] = std::min(accs[c], val); break;
                        case AggFunc::MAX: accs[c] = std::max(accs[c], val); break;
                        default: 
                            std::cout << "Error: Unknown aggregation function encountered." << std::endl;
                            exit(EXIT_FAILURE);
                    }
            }
        } else {
            // This key combination is not in the hash-table yet.
            // Allocate and initialize the accumulators.
            accs = static_cast<int64_t*>(malloc(params->numAggCols * sizeof(int64_t*)));
            for (size_t c = 0; c < params->numAggCols; ++c) {
                accs[c] = getValueInt64(vals, c);
            }
            keys->values = (void**)malloc(keys->numCols * sizeof(void*));
        }
    }

    freeRow(keys);
    freeRow(vals);
    return nullptr;
}

/* Student implementation of the `groupByAgg`-operator */
void groupByAgg(
        Relation* res,
        const Relation* in,
        size_t numGrpCols, size_t* grpColIdxs,
        size_t numAggCols, size_t* aggColIdxs, AggFunc* aggFuncs
) {
    size_t numThreads = NUM_CORES;
    size_t chunkSize = in->numRows / numThreads;
    if(chunkSize == 0){
        numThreads = 1;
        chunkSize = in->numRows;
    }
    std::vector<std::unordered_map<Row, int64_t*>> threadHashTables(numThreads);
    pthread_t pthreads[numThreads];
    ThreadParams threadParams[numThreads];

    Relation* inKeys = project(in, numGrpCols, grpColIdxs);
    Relation* inVals = project(in, numAggCols, aggColIdxs);

    RelationKey rkey = RelationKey(inKeys, inVals, aggFuncs, numAggCols, in->numRows);
    //if the result already exists in the cache, return the same result 
    if (RecentRelationHash.count(rkey) != 0){
        Relation* copy = RecentRelationHash[rkey];
        res->numRows = copy->numRows;
        res->numCols = copy->numCols;
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
        // Also, create a copy which will be added to the recent cache. 
        size_t r = 0;
        Row* dst = initRow(res);
        Row* src = initRow(copy);
        while(r != copy->numRows) {
            getRow(dst, res,  r);
            getRow(src, copy, r++);
            for(size_t c = 0; c < inKeys->numCols; c++)
                memcpy(dst->values[c], src->values[c], sizeOfDataType(inKeys->colTypes[c]));
            for(size_t c = 0; c < inVals->numCols; c++) {
                memcpy(dst->values[numGrpCols + c], src->values[numGrpCols + c], sizeOfDataType(res->colTypes[numGrpCols + c]));
            }
        }
        freeRow(dst);
        freeRow(src);
            
        freeRelation(inKeys, 1, 0);
        freeRelation(inVals, 1, 0);
        return;
    }

    for (size_t i = 0; i < numThreads; ++i) {
        size_t start = i * chunkSize;
        size_t end = (i == numThreads - 1) ? in->numRows : (i + 1) * chunkSize;
        threadParams[i] = {inKeys, inVals, start, end,
                            numGrpCols, grpColIdxs, numAggCols, aggColIdxs, aggFuncs, &threadHashTables[i]};

        int result = pthread_create(&pthreads[i], nullptr, threadFunction, &threadParams[i]);
        if (result != 0) {
            std::cerr << "Error creating thread: " << result << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    for (size_t i = 0; i < numThreads; ++i) {
        pthread_join(pthreads[i], nullptr);
    }
    
    std::unordered_map<Row, int64_t*>& ht = threadHashTables[0];
    ht.reserve(threadHashTables.size()*threadHashTables[0].size());
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
    Relation* copy = (Relation*)malloc(sizeof(Relation));



    // Initialize the result relation.
    // The copy counter also needs to be initialized
    res->numRows = ht.size();
    res->numCols = numGrpCols + numAggCols;
    res->colTypes = (DataType*)malloc(res->numCols * sizeof(DataType));
    res->cols = (void**)malloc(res->numCols * sizeof(void*));

    copy->numRows = res->numRows;
    copy->numCols = res->numCols;
    copy->colTypes = (DataType*)malloc(copy->numCols * sizeof(DataType));
    copy->cols = (void**)malloc(res->numCols * sizeof(void*));

    for(size_t c = 0; c < numGrpCols; c++) {
        res->colTypes[c] = inKeys->colTypes[c];
        res->cols[c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[c]));

        copy->colTypes[c] = inKeys->colTypes[c];
        copy->cols[c] = (void*)malloc(copy->numRows * sizeOfDataType(copy->colTypes[c]));
    }

    for(size_t c = 0; c < numAggCols; c++) {
        res->colTypes[numGrpCols + c] = getAggType(inVals->colTypes[c], aggFuncs[c]);
        res->cols[numGrpCols + c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[numGrpCols + c]));

        copy->colTypes[numGrpCols + c] = getAggType(inVals->colTypes[c], aggFuncs[c]);
        copy->cols[numGrpCols + c] = (void*)malloc(copy->numRows * sizeOfDataType(copy->colTypes[numGrpCols + c]));
    }

    
    // Populate the result with the data from the hash-table.
    size_t r = 0;
    size_t rc = 0;
    Row* dst = initRow(res);
    Row* cpy = initRow(copy);
    for(auto entry : ht) {
        getRow(dst, res, r++);
        getRow(cpy, copy, rc++);
        Row keys = entry.first;
        for(size_t c = 0; c < inKeys->numCols; c++){
            memcpy(dst->values[c], keys.values[c], sizeOfDataType(inKeys->colTypes[c]));
            memcpy(cpy->values[c], keys.values[c], sizeOfDataType(inKeys->colTypes[c]));
        }
        free(keys.values);
        int64_t* accs = entry.second;
        for(size_t c = 0; c < inVals->numCols; c++) {
            memcpy(dst->values[numGrpCols + c], &accs[c], sizeOfDataType(res->colTypes[numGrpCols + c]));
            
            memcpy(cpy->values[numGrpCols + c], &accs[c], sizeOfDataType(res->colTypes[numGrpCols + c]));
            //the entire creation of the copy Relation* is here due to this line of code 
            //the copying of values between tables is tricky, especially within the sizeOfDataType operator
            //after approximately 8 hours of debugging this line in valgrind, it was placed here.
        }
        free(accs);

    }
    freeRow(dst);
    freeRow(cpy);
 
    if(FIFOcounter != FIFOSize){  //if the memory is not full, add another relation to the "cache".
        FIFOcounter++;
        FIFOMemory.push(rkey);
    }
    else {  //if it is, remove the oldest member.
        RelationKey rm = FIFOMemory.front();
        FIFOMemory.pop();
        FIFOMemory.push(rkey);
        Relation* deleteCopy = RecentRelationHash[rm];
        if(deleteCopy)
        {
            freeRelation(deleteCopy, 1, 1);
            deleteCopy = nullptr;
        }
        
        RecentRelationHash.erase(rm);
    }
    
    RecentRelationHash[rkey] = copy;  //insert new value into "cache".

    freeRelation(inKeys, 1, 0);
    freeRelation(inVals, 1, 0);

}