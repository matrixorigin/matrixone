// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "usearchex.h"
#include "bitmap.h"
#include <string.h>
#include <stdio.h>
#include <stdint.h>

// Weak symbol declaration for usearch_filtered_search
// Allows compilation even if the function is not available in the library
__attribute__((weak)) size_t usearch_filtered_search(
    usearch_index_t, void const*, usearch_scalar_kind_t, size_t,
    usearch_filtered_search_callback_t, void*,
    usearch_key_t*, usearch_distance_t*, usearch_error_t*);

typedef struct bitmap_int_t {
    uint64_t *bitmap;
    size_t len;
} bitmap_int_t;

static int filtered_search_bf_cb(usearch_key_t key, void *data) {
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

    // Check if usearch_filtered_search is available
    if (usearch_filtered_search == NULL) {
        if (error) {
            *error = "usearch_filtered_search function not available in library";
        }
        return 0;
    }

    return usearch_filtered_search((usearch_index_t)index,
            query_vector,
            (usearch_scalar_kind_t)query_kind,
            count,
            filtered_search_bf_cb,
            bf,
            keys,
            distances,
            error);
}

static int filtered_search_bitmap_cb(usearch_key_t key, void *data) {
    bitmap_int_t *bm = (bitmap_int_t*) data;
    if (bm) {
        return bitmap_test_with_len(bm->bitmap, bm->len, key);
    }
    return 1;
}

size_t usearchex_filtered_search_with_bitmap(
    usearch_index_t index,
    void const* query_vector, usearch_scalar_kind_t query_kind, size_t count,
    uint64_t *bitmap, size_t bmlen,
    usearch_key_t* keys, usearch_distance_t* distances, usearch_error_t* error) {

    // Check if usearch_filtered_search is available
    if (usearch_filtered_search == NULL) {
        if (error) {
            *error = "usearch_filtered_search function not available in library";
        }
        return 0;
    }

    bitmap_int_t bm;
    bm.bitmap = bitmap;
    bm.len = bmlen;

    return usearch_filtered_search((usearch_index_t)index,
            query_vector,
            (usearch_scalar_kind_t)query_kind,
            count,
            filtered_search_bitmap_cb,
            &bm,
            keys,
            distances,
            error);
}
