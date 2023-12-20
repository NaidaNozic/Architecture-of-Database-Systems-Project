#include "match.h"
#include "api.h"
#include "setmap_utils.h"
#include "utils.h"

#include <unordered_set>
#include <iostream>

// ****************************************************************************
// Comparison of two relations
// ****************************************************************************

int matchRelations(Relation* oRes, Relation* eRes) {
    size_t onR = oRes->numRows;
    size_t enR = eRes->numRows;
    size_t onC = oRes->numCols;
    size_t enC = eRes->numCols;

    // The number of columns must match.
    if (onC != 0 && onC != enC) {
        printf("FAIL: Returned %ld columns. Expected %ld columns.\n", onC, enC);
        return EXIT_FAILURE;
    }

    // The number of rows must match.
    if (onR != 0 && onR != enR) {
        printf("FAIL: Returned %ld rows. Expected %ld rows.\n", onR, enR);
        return EXIT_FAILURE;
    }
    
    // The column types must match.
    if (oRes->colTypes != eRes->colTypes)
        for (size_t c = 0; c < onC; c++) {
            DataType oDt = oRes->colTypes[c];
            DataType eDt = eRes->colTypes[c];
            if (oDt != eDt) {
                printf(
                        "FAIL: Column %ld has type %s. Expected %s.\n",
                        c, nameOfDataType(oDt), nameOfDataType(eDt)
                );
                return EXIT_FAILURE;
            }
        }

    // The rows themselves must match (in any order).
    Row* oRow = initRow(oRes);
    std::unordered_set<Row> hs;
    for (size_t r = 0; r < onR; r++) {
        getRow(oRow, oRes, r);
        auto inserted = hs.insert(*oRow);
        if(inserted.second)
            oRow->values = (void**)malloc(oRow->numCols * sizeof(void*));
    }
    freeRow(oRow);
    Row* eRow = initRow(eRes);
    for (size_t r = 0; r < enR; r++) {
        getRow(eRow, eRes, r);
        if (hs.find(*eRow) == hs.end()) {
            printf("FAIL: Result mismatch: expected row %ld not found.\n", r);
            return EXIT_FAILURE;
        }
    }
    freeRow(eRow);
    
    for(const Row& row : hs)
        free(row.values);

    // All tests succeeded.
    return EXIT_SUCCESS;
}