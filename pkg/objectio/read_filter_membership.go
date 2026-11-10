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

package objectio

import (
	"github.com/matrixorigin/matrixone/pkg/common/docfilter"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
)

// ReadFilterMembership combines the existing PK search with an exact integer
// membership filter. It borrows the reader's filter; it neither retains nor
// frees it. Cached vectors stay inside ObjectIO and only owned offsets escape.
type ReadFilterMembership struct {
	pk     *ReadFilterSearch
	member docfilter.MembershipFilter
}

// NewReadFilterMembership only admits concrete read-only implementations.
// Accepting an arbitrary TestVector implementation would let a callback mutate
// or retain the sealed cache backing. Unsupported filters use the old snapshot
// path. A nil pk means that there is no additional primary-key predicate.
func NewReadFilterMembership(pk *ReadFilterSearch, member docfilter.MembershipFilter) *ReadFilterMembership {
	switch member.(type) {
	case *docfilter.CbitmapFilter, *docfilter.Sorted64Filter, *docfilter.CRoaringFilter:
	default:
		return nil
	}
	if !member.Valid() {
		return nil
	}
	return &ReadFilterMembership{pk: pk, member: member}
}

func (s *ReadFilterMembership) search(vectors []vector.Vector, sorted bool) []int64 {
	rowCount := vectors[0].Length()
	var selected []int64
	if s.pk != nil {
		selected = s.pk.search(&vectors[0], sorted)
		if len(selected) == 0 {
			return selected
		}
	}
	memberVector := &vectors[0]
	if len(vectors) > 1 && vectors[1].Length() != 0 {
		if vectors[1].Length() != rowCount {
			if s.pk == nil {
				return allReadFilterRows(rowCount, false)
			}
			return selected // same fail-open contract as the owned-vector path
		}
		memberVector = &vectors[1]
	}
	hits := s.member.TestVector(memberVector, nil)
	if len(hits) != rowCount {
		if s.pk == nil {
			return allReadFilterRows(rowCount, false)
		}
		return selected
	}
	if s.pk == nil {
		// Allocate only actual matches, not an entire block of offsets even
		// when membership has no hits. Count first to avoid repeated growth
		// for dense hits; the descriptor owns no reusable mutable buffer.
		count := 0
		for _, hit := range hits {
			if hit != 0 {
				count++
			}
		}
		if count == 0 {
			return nil
		}
		matched := make([]int64, 0, count)
		for row, hit := range hits {
			if hit != 0 {
				matched = append(matched, int64(row))
			}
		}
		return matched
	}
	matched := selected[:0]
	for _, row := range selected {
		if row >= 0 && row < int64(rowCount) && hits[row] != 0 {
			matched = append(matched, row)
		}
	}
	return matched
}
