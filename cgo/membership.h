// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Membership of uint64 keys (primary keys) shared by the vector index searches:
// usearch (cgo/usearchex.c) tests each candidate key, cuVS (cgo/cuvs/filter.hpp)
// ANDs the test into its per-row filter mask.
#ifndef MO_MEMBERSHIP_H
#define MO_MEMBERSHIP_H

#include <stdbool.h>
#include <stdint.h>

#include "cbitmap.h"
#include "croaring.h"
#include "sorted64.h"

#ifdef __cplusplus
extern "C" {
#endif

// Membership kinds. They MUST match the Tag* constants of pkg/common/docfilter:
// the kind is the tag of the docfilter.MembershipFilter whose C handle is passed.
#define MO_MEMBERSHIP_BLOOM    0  // bloomfilter_t* (approximate)
#define MO_MEMBERSHIP_CROARING 2  // roaring64 bitmap handle (exact)
#define MO_MEMBERSHIP_CBITMAP  3  // dense cbitmap handle (exact)
#define MO_MEMBERSHIP_SORTED64 4  // [count][sorted uint64] payload (exact)

// A membership: the C handle of a docfilter.MembershipFilter and its kind. A NULL
// filter is no membership.
typedef struct mo_membership_t {
    const void *filter;
    int kind;
} mo_membership_t;

// mo_membership_is_exact_kind reports whether kind is one mo_membership_exact_contains
// tests: cbitmap, CRoaring or sorted64.
static inline bool mo_membership_is_exact_kind(int kind) {
    return kind == MO_MEMBERSHIP_CBITMAP || kind == MO_MEMBERSHIP_CROARING ||
           kind == MO_MEMBERSHIP_SORTED64;
}

// mo_membership_exact_contains tests key against a cbitmap, CRoaring or sorted64
// membership.
static inline bool mo_membership_exact_contains(const mo_membership_t *m, uint64_t key) {
    switch (m->kind) {
    case MO_MEMBERSHIP_CBITMAP:
        return mo_cbitmap_contain(m->filter, key);
    case MO_MEMBERSHIP_CROARING:
        return mo_croaring_contains((void *)m->filter, key);
    default:
        return mo_sorted64_contains(m->filter, key);
    }
}

#ifdef __cplusplus
}
#endif

#endif  // MO_MEMBERSHIP_H
