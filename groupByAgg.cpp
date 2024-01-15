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
#include <cmath>

// ****************************************************************************
// Group-by operator with aggregation
// ****************************************************************************
struct TreeNode {
    Relation* inKeys;
    Relation* inVals;
    size_t start;
    size_t end;
    size_t numAggCols;
    AggFunc* aggFuncs;
    std::unordered_map<Row, int64_t*> ht;
    std::vector<TreeNode*> children;
    std::unordered_map<Row, int64_t*>* ht1;

    TreeNode(
        Relation* inKeys,
        Relation* inVals,
        size_t start,
        size_t end,
        size_t numAggCols,
        AggFunc* aggFuncs
    ) : inKeys(inKeys),
        inVals(inVals),
        start(start),
        end(end),
        numAggCols(numAggCols),
        aggFuncs(aggFuncs),
        ht1(&ht) {}

    ~TreeNode() {
        for (auto child : children) {
            delete child;
        }
    }
};

void threadMerge(TreeNode* node){
    if(node->children.size() == 2){
        for (auto entry : *node->children[1]->ht1){
            auto*& accs = (*node->children[0]->ht1)[entry.first];
            if (accs) {
                for (size_t c = 0; c < node->numAggCols; ++c) {
                    switch (node->aggFuncs[c]) {
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
    node->ht1 = node->children[0]->ht1;
}

void threadCreateHashTable(TreeNode* node) {
    Row* keys = initRow(node->inKeys);
    Row* vals = initRow(node->inVals);

    for (size_t r = node->start; r < node->end; ++r) {
        getRow(keys, node->inKeys, r);
        getRow(vals, node->inVals, r);

        int64_t*& accs = node->ht[*keys];
        if (accs) {
            for (size_t c = 0; c < node->numAggCols; ++c) {
                int64_t val = getValueInt64(vals, c);
                switch (node->aggFuncs[c]) {
                    case AggFunc::SUM: accs[c] += val; break;
                    case AggFunc::MIN: accs[c] = std::min(accs[c], val); break;
                    case AggFunc::MAX: accs[c] = std::max(accs[c], val); break;
                    default: exit(EXIT_FAILURE);
                }
            }
        } else {
            accs = (int64_t*)(malloc(node->numAggCols * sizeof(int64_t*)));
            for (size_t c = 0; c < node->numAggCols; ++c) {
                accs[c] = getValueInt64(vals, c);
            }
            keys->values = (void**)malloc(keys->numCols * sizeof(void*));
        }
    }
    freeRow(keys);
    freeRow(vals);
}

void buildTree(TreeNode* node, size_t depth) {
    if (depth == 0 || (node->end-node->start <= 100)) {
        std::thread t(threadCreateHashTable, node);
        node->ht1 = &node->ht;
        t.join();
    } else {
        size_t mid = (node->start + node->end) / 2;
        TreeNode* leftChild = new TreeNode(
            node->inKeys, node->inVals,
            node->start, mid,
            node->numAggCols,
            node->aggFuncs
        );
        TreeNode* rightChild = new TreeNode(
            node->inKeys, node->inVals,
            mid, node->end,
            node->numAggCols,
            node->aggFuncs
        );

        node->children.push_back(leftChild);
        node->children.push_back(rightChild);

        std::thread t1(buildTree, leftChild, depth - 1);
        std::thread t2(buildTree, rightChild, depth - 1);

        t1.join();
        t2.join();
        threadMerge(node);
    }
}

void groupByAgg(
        Relation* res,
        const Relation* in,
        size_t numGrpCols, size_t* grpColIdxs,
        size_t numAggCols, size_t* aggColIdxs, AggFunc* aggFuncs
) {
    Relation* inKeys = project(in, numGrpCols, grpColIdxs);
    Relation* inVals = project(in, numAggCols, aggColIdxs);
    size_t numThreads = std::thread::hardware_concurrency();
    size_t treeDepth = static_cast<size_t>(std::ceil(std::log2(numThreads)));

    TreeNode* root = new TreeNode(
        inKeys,
        inVals,
        0, in->numRows,
        numAggCols,
        aggFuncs
    );
    buildTree(root, treeDepth);

    // Initialize the result relation and copy data from the root node.
    res->numRows = (*root->ht1).size();
    res->numCols = numGrpCols + numAggCols;
    res->colTypes = (DataType*)malloc(res->numCols * sizeof(DataType));
    res->cols = (void**)malloc(res->numCols * sizeof(void*));

    for (size_t c = 0; c < numGrpCols; c++) {
        res->colTypes[c] = root->inKeys->colTypes[c];
        res->cols[c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[c]));
    }

    for (size_t c = 0; c < numAggCols; c++) {
        res->colTypes[numGrpCols + c] = getAggType(root->inVals->colTypes[c], aggFuncs[c]);
        res->cols[numGrpCols + c] = (void*)malloc(res->numRows * sizeOfDataType(res->colTypes[numGrpCols + c]));
    }

    size_t r = 0;
    Row* dst = initRow(res);
    for (auto entry : *root->ht1) {
        getRow(dst, res, r++);
        Row keys = entry.first;
        for (size_t c = 0; c < root->inKeys->numCols; c++)
            memcpy(dst->values[c], keys.values[c], sizeOfDataType(root->inKeys->colTypes[c]));
        free(keys.values);

        int64_t* accs = entry.second;
        for (size_t c = 0; c < root->inVals->numCols; c++) {
            memcpy(dst->values[numGrpCols + c], &accs[c], sizeOfDataType(res->colTypes[numGrpCols + c]));
        }
        free(accs);
    }
    freeRow(dst);

    freeRelation(inKeys, 1, 0);
    freeRelation(inVals, 1, 0);

    delete root; // Release memory used by the tree.
}