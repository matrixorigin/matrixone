#include "usearchex.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>

static int filtered_search_cb(usearch_key_t key, void *data) {
    bloomfilter_t *bf = (bloomfilter_t*) data;
    if (bf) {
        return bloomfilter_test(bf, (const char *)&key, sizeof(usearch_key_t));
    }
    return 1;
}

size_t usearchex_filtered_search_with_bloomfilter(
    usearch_index_t index,
    void const* query_vector, usearch_scalar_kind_t query_kind, size_t count,
    void *bf,
    usearch_key_t* keys, usearch_distance_t* distances, usearch_error_t* error) {

    return usearch_filtered_search((usearch_index_t)index, 
            query_vector, 
            (usearch_scalar_kind_t)query_kind, 
            count, 
            filtered_search_cb, 
            bf, 
            keys, 
            distances, 
            error);
}
