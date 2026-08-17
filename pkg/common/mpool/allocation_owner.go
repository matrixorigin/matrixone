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

package mpool

import "strconv"

// Repository-wide allocation owner IDs are append-only. A physical
// allocation keeps the numeric ID for its entire lifetime, so IDs must never
// be reused for another owner class. Sites remain private to each owner.
const (
	AllocationOwnerHashBuild AllocationOwner = iota + 1
	AllocationOwnerIndexBuild
	AllocationOwnerFuzzyFilter
	AllocationOwnerGroup
	AllocationOwnerOrder
	AllocationOwnerTop
	AllocationOwnerFill
	AllocationOwnerCTE
	AllocationOwnerSet
	AllocationOwnerFulltext
	AllocationOwnerDML
	AllocationOwnerSample
)

const (
	// AllocationOwnerCatalogMax is the largest owner implemented by this
	// binary. Append new catalog entries immediately before it.
	AllocationOwnerCatalogMax = AllocationOwnerSample
	// AllocationOwnerMax preserves the existing public bound and reserves IDs
	// for rolling-version terminal summaries. Unknown owners remain observable
	// on the wire but cannot allocate locally until catalogued by this binary.
	AllocationOwnerMax AllocationOwner = 63
)

// String returns the stable diagnostic label for a repository owner. Unknown
// non-zero IDs remain renderable during rolling upgrades.
func (o AllocationOwner) String() string {
	switch o {
	case AllocationOwnerHashBuild:
		return "hash_build"
	case AllocationOwnerIndexBuild:
		return "index_build"
	case AllocationOwnerFuzzyFilter:
		return "fuzzy_filter"
	case AllocationOwnerGroup:
		return "group"
	case AllocationOwnerOrder:
		return "order"
	case AllocationOwnerTop:
		return "top"
	case AllocationOwnerFill:
		return "fill"
	case AllocationOwnerCTE:
		return "cte"
	case AllocationOwnerSet:
		return "set"
	case AllocationOwnerFulltext:
		return "fulltext"
	case AllocationOwnerDML:
		return "dml"
	case AllocationOwnerSample:
		return "sample"
	default:
		return "owner-" + strconv.FormatUint(uint64(o), 10)
	}
}
