#ifndef _USEARCH_EXTEND_H
#define _USEARCH_EXTEND_H

#include "usearch.h"


/* usearch */
typedef char const *mo_error_t;
float mo_distance(void const* vector_first, void const* vector_second, size_t dimensions, mo_error_t* error);

void xxhash_test();

#endif

