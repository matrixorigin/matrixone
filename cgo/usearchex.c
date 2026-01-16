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
