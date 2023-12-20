#ifndef UTILS_H
#define UTILS_H

/* A row of a relation */
typedef struct {
    size_t numCols; // the number of columns
    DataType* types; // an array of `numCols` types, one for each column
    void** values; // an array of `numCols` pointers to the actual values
} Row;

#ifdef __cplusplus
extern "C" {
#endif

size_t sizeOfDataType(DataType dt);

char* nameOfDataType(DataType dt);

DataType getAggType(DataType dt, AggFunc af);

Relation* createRelation(size_t numRows, size_t numCols, DataType* colTypes);
    
void freeRelation(Relation* rel, int freeColTypes, int freeEachCol);

Row* initRow(Relation* rel);

void getRow(Row* row, Relation* rel, size_t r);

void freeRow(Row* row);

int64_t getValueInt64(const Row* row, size_t c);

Relation* project(const Relation* rel, size_t numCols, size_t* colIdxs);

#ifdef __cplusplus
}
#endif

#endif // UTILS_H