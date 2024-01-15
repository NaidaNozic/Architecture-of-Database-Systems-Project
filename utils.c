#include "api.h"
#include "utils.h"

#include <stdlib.h>
#include <string.h>

size_t sizeOfDataType(DataType dt) {
    switch(dt) {
        case INT16: return sizeof(int16_t);
        case INT32: return sizeof(int32_t);
        case INT64: return sizeof(int64_t);
        default: exit(EXIT_FAILURE);
    }
}

char* nameOfDataType(DataType dt) {
    switch(dt) {
        case INT16: return "INT16";
        case INT32: return "INT32";
        case INT64: return "INT64";
        default: exit(EXIT_FAILURE);
    }
}

DataType getAggType(DataType dt, AggFunc af) {
    switch(af) {
        case SUM:
            // For SUM, we always use a 64-bit integer to prevent overflows if
            // possible, without making it unnecessarily complex.
            return INT64;
        case MIN:
        case MAX:
            // For MIN and MAX, we always keep the input data type, no upgrade
            // required.
            return dt;
        default:
            exit(EXIT_FAILURE);
    };
}

Relation* createRelation(size_t numRows, size_t numCols, DataType* colTypes) {
    Relation* rel = (Relation*)malloc(sizeof(Relation));
    rel->numRows = numRows;
    rel->numCols = numCols;
    rel->colTypes = colTypes;
    rel->cols = (void**)malloc(numCols * sizeof(void*));
    for(size_t c = 0; c < numCols; c++)
        rel->cols[c] = (void*)malloc(numRows * sizeOfDataType(colTypes[c]));
    return rel;
}

void freeRelation(Relation* rel, int freeColTypes, int freeEachCol) {
    if(freeColTypes)
        free(rel->colTypes);
    if(freeEachCol)
        for(size_t c = 0; c < rel->numCols; c++)
            free(rel->cols[c]);
    free(rel->cols);
    free(rel);
}

Row* initRow(Relation* rel) {
    Row* row = (Row*)malloc(sizeof(Row));
    row->numCols = rel->numCols;
    row->types = rel->colTypes; // Reuse the column types of the relation.
    row->values = (void**)malloc(rel->numCols * sizeof(void*));
    return row;
}

void getRow(Row* row, Relation* rel, size_t r) {
    for(size_t c = 0; c < rel->numCols; c++)
        row->values[c] = (int8_t*)(rel->cols[c]) + r * sizeOfDataType(rel->colTypes[c]);
}


void freeRow(Row* row) {
    // Don't free the column types, they still belong to the relation.
    free(row->values);
    free(row);
}

int64_t getValueInt64(const Row* row, size_t c) {
    switch(row->types[c]) {
        case INT16: return (int64_t)*((int16_t*)(row->values[c]));
        case INT32: return (int64_t)*((int32_t*)(row->values[c]));
        case INT64: return (int64_t)*((int64_t*)(row->values[c]));
        default: exit(EXIT_FAILURE);
    }
}

Relation* project(const Relation* rel, size_t numCols, size_t* colIdxs) {
    Relation* res = (Relation*)malloc(sizeof(Relation));
    res->numRows = rel->numRows;
    res->numCols = numCols;
    res->colTypes = (DataType*)malloc(numCols * sizeof(DataType));
    res->cols = (void**)malloc(numCols * sizeof(void*));
    for(size_t c = 0; c < numCols; c++) {
        res->colTypes[c] = rel->colTypes[colIdxs[c]];
        res->cols[c] = rel->cols[colIdxs[c]];
    }
    return res;
}