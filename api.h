#ifndef API_H
#define API_H

#include <stdint.h>
#include <stddef.h>

/* Codes for column data types */
typedef enum DataType {
    INT16, // 16-bit signed integer (int16_t)
    INT32, // 32-bit signed integer (int32_t)
    INT64  // 64-bit signed integer (int64_t)
} DataType;

/* Codes for aggregation functions  */
typedef enum AggFunc {
    SUM, MIN, MAX
} AggFunc;

/**
 * An intermediate result. This type is used for inputs and outputs of the
 * `groupByAgg`-operator. A column-major storage layout is used, whereby each
 * column is stored as a separate array of values of the column's data type.
 * For instance, values in an `INT32` column are stored using 4 bytes each.
 * Remember to cast the column pointers to the right type as necessary.
 */
typedef struct {
    size_t numRows; // the number of rows
    size_t numCols; // the number of columns
    DataType* colTypes; // an array of `numCols` types, one for each column
    void** cols; // an array of `numCols` pointers to the actual column data
} Relation;

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Performs a grouping and aggregation on the input relation. Supports grouping
 * on an arbitrary subset of the input columns and calculating an arbitrary
 * number of aggregates over the input columns for each group. If the number of
 * grouping columns is zero, all rows are treated as belonging to the same
 * group. Each input column can have an individual data type.
 * 
 * The result shall consist of all grouping columns followed by all aggregate
 * columns. The data types of the grouping columns shall be the same as in the
 * input, while the types of the aggregate columns also depend on the
 * corresponding aggregate function (see `getAggType()`). There shall be one
 * row per distinct combination of the values in the grouping columns. The
 * order of the rows in the result can be arbitrary.
 * 
 * @param res An empty `Relation` object for the output. The implementation
 * must set `numCols`/`numRows` and allocate/populate `colTypes` and `cols` of
 * this `Relation`.
 * @param in The input relation.
 * @param numGrpCols The number of columns to group on.
 * @param grpColIdxs An array of the numbers of the columns to group on. Must
 * have `numGrpCols` elements. Counting starts at zero.
 * @param numAggCols The number of aggregates to calculate.
 * @param aggColIdxs An array of the numbers of the columns to calculate an
 * aggregate on. Must have `numAggCols` elements. Counting starts at zero.
 * @param aggFuncs An array of the aggregation functions to use. Must have
 * `numAggCols` elements.
 */
void groupByAgg(
        Relation* res,
        const Relation* in,
        size_t numGrpCols, size_t* grpColIdxs,
        size_t numAggCols, size_t* aggColIdxs, AggFunc* aggFuncs
);

#ifdef __cplusplus
}
#endif

#endif // API_H