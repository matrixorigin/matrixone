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

// Per-candidate membership predicate. `data` is a membership_filter_t carrying
// the docfilter C handle + its kind; each usearch key (a uint64) is tested
// against the structure matching that kind. A NULL handle keeps every key.
typedef struct membership_filter_t {
    void *filter;
    int kind;
} membership_filter_t;

static int filtered_search_membership_cb(usearch_key_t key, void *data) {
    membership_filter_t *mf = (membership_filter_t *)data;
    if (!mf || !mf->filter) {
        return 1; // no filter -> keep all
    }
    switch (mf->kind) {
        case USEARCHEX_FILTER_BLOOM:
            return bloomfilter_test((bloomfilter_t *)mf->filter,
                                    (const char *)&key, sizeof(usearch_key_t));
        case USEARCHEX_FILTER_CBITMAP:
            return mo_cbitmap_contain(mf->filter, (uint64_t)key) ? 1 : 0;
        case USEARCHEX_FILTER_CROARING:
            return mo_croaring_contains(mf->filter, (uint64_t)key) ? 1 : 0;
        default:
            return 1;
    }
}

size_t usearchex_filtered_search_with_membership(
    usearch_index_t index,
    void const* query_vector, usearch_scalar_kind_t query_kind, size_t count,
    void *filter, int filter_kind,
    usearch_key_t* keys, usearch_distance_t* distances, usearch_error_t* error) {

    membership_filter_t mf;
    mf.filter = filter;
    mf.kind = filter_kind;

    return usearch_filtered_search((usearch_index_t)index,
            query_vector,
            (usearch_scalar_kind_t)query_kind,
            count,
            filtered_search_membership_cb,
            &mf,
            keys,
            distances,
            error);
}
