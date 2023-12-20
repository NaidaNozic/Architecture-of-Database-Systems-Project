#ifndef MATCH_H
#define MATCH_H

#include "api.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Checks if two relations represent the same data */
int matchRelations(Relation* oRes, Relation* eRes);

#ifdef __cplusplus
}
#endif

#endif /* MATCH_H */

