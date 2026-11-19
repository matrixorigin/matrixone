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
#include "sorted64.h"
#include <stdint.h>

// Per-candidate membership predicate. `data` is a mo_membership_t carrying
// the docfilter C handle + its kind; each usearch key (a uint64) is tested
// against the structure matching that kind. A NULL handle keeps every key.
static int filtered_search_membership_cb(usearch_key_t key, void *data) {
    const mo_membership_t *m = (const mo_membership_t *)data;
    if (!m || !m->filter) {
        return 1; // no filter -> keep all
    }
    switch (m->kind) {
        case MO_MEMBERSHIP_BLOOM:
            return bloomfilter_test((bloomfilter_t *)m->filter,
                                    (const char *)&key, sizeof(usearch_key_t));
        case MO_MEMBERSHIP_CROARING:
            return mo_croaring_contains((void *)m->filter, (uint64_t)key) ? 1 : 0;
        case MO_MEMBERSHIP_CBITMAP:
        case MO_MEMBERSHIP_SORTED64:
            return mo_membership_exact_contains(m, (uint64_t)key) ? 1 : 0;
        default:
            // Unknown kind: fail CLOSED (exclude). Keeping the candidate here
            // would silently disable filtering for the whole search. The Go
            // caller also validates the kind before entering this path, so this
            // is defense in depth.
            return 0;
    }
}

size_t usearchex_filtered_search_with_membership(
    usearch_index_t index,
    void const* query_vector, usearch_scalar_kind_t query_kind, size_t count,
    void *filter, int filter_kind, int exact,
    usearch_key_t* keys, usearch_distance_t* distances, usearch_error_t* error) {

    mo_membership_t m;
    m.filter = filter;
    m.kind = filter_kind;

    return usearch_filtered_search((usearch_index_t)index,
            query_vector,
            (usearch_scalar_kind_t)query_kind,
            count,
            filtered_search_membership_cb,
            &m,
            exact != 0,
            keys,
            distances,
            error);
}
