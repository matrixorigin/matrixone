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

#ifndef _USEARCH_EXTEND_H
#define _USEARCH_EXTEND_H

#include "bloom.h"
#include "cbitmap.h"
#include "croaring.h"
#include "usearch.h"

typedef char const *mo_error_t;

/* usearch */

/* Membership-filter kinds for usearchex_filtered_search_with_membership. These
 * MUST match the Tag* constants in pkg/common/docfilter (the kind is the tag
 * byte of the docfilter.MembershipFilter being passed). */
#define USEARCHEX_FILTER_BLOOM    0  /* filter is a bloomfilter_t* (approximate) */
#define USEARCHEX_FILTER_CROARING 2  /* filter is a roaring64 bitmap (exact)     */
#define USEARCHEX_FILTER_CBITMAP  3  /* filter is a dense cbitmap (exact)        */

/* Filtered search against a doc_id membership filter. `filter` is the C handle
 * of a docfilter.MembershipFilter and `filter_kind` selects how each candidate
 * key is tested (see USEARCHEX_FILTER_* above). A NULL filter passes all keys. */
size_t usearchex_filtered_search_with_membership(
    usearch_index_t index,
    void const* query_vector, usearch_scalar_kind_t query_kind, size_t count,
    void *filter, int filter_kind,
    usearch_key_t* keys, usearch_distance_t* distances, usearch_error_t* error);

#endif

