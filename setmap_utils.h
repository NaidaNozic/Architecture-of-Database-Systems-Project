#ifndef SETMAP_UTILS_H
#define SETMAP_UTILS_H

#include "utils.h"

#include <functional>

// ****************************************************************************
// Utilities so that we can use Row as in std::unordered_set/map.
// ****************************************************************************

/* Calculates a hash value of a Row */
template<>
struct std::hash<Row> {
    size_t operator()(const Row& row) const {
        size_t hash = 0;
        for(size_t c = 0; c < row.numCols; c++)
            hash += getValueInt64(&row, c);
        return hash;
    }
};

/* Checks if two Rows are equal */
static bool operator==(const Row& row1, const Row& row2) {
    // Check number of columns.
    if(row1.numCols != row2.numCols)
        return false;
    // Check column types.
    if(row1.types != row2.types)
        for(size_t c = 0; c < row1.numCols; c++)
            if(row1.types[c] != row2.types[c])
                return false;
    // Check stored values.
    for(size_t c = 0; c < row1.numCols; c++)
        if(getValueInt64(&row1, c) != getValueInt64(&row2, c))
            return false;
    // All checks succeeded.
    return true;
}

#endif /* SETMAP_UTILS_H */