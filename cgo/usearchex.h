#ifndef _USEARCH_EXTEND_H
#define _USEARCH_EXTEND_H

#include "bloom.h"
#include "usearch.h"

typedef char const *mo_error_t;

/* usearch */
size_t usearchex_filtered_search_with_bloomfilter(
    usearch_index_t index,
    void const* query_vector, usearch_scalar_kind_t query_kind, size_t count,
    void *bf,
    usearch_key_t* keys, usearch_distance_t* distances, usearch_error_t* error);

#endif

