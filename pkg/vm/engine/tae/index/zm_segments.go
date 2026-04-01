// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"sort"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
)

// AnySegmentOverlaps reports whether any segment in the sorted,
// non-overlapping segment list overlaps with objZM.
//
// Each segment is a 64-byte ZoneMap (same encoding as ZM) passed as
// a raw []byte slice.  This avoids forcing the engine package to
// import the index package — callers store segments as [][]byte in
// engine.PKFilter and pass them here unchanged.
//
// Segments must satisfy two invariants (guaranteed by the gap-based
// streaming construction algorithm):
//
//  1. Sorted by min:  segments[i].min ≤ segments[i+1].min
//  2. Non-overlapping: segments[i].max < segments[i+1].min
//
// Because of these invariants the max values are also monotonically
// increasing, so a single binary search on max suffices.
//
// Overlap condition for two ranges [a_min, a_max] and [b_min, b_max]:
//
//	a_max ≥ b_min  AND  a_min ≤ b_max
//
// Complexity: O(log N) where N = len(segments).
//
// Returns true when segments is empty (no filter ⇒ match everything).
func AnySegmentOverlaps(objZM ZM, segments [][]byte) bool {
	if len(segments) == 0 {
		return true
	}
	if !objZM.IsInited() {
		return false
	}

	t := objZM.GetType()
	scale := objZM.GetScale()
	objMin := objZM.GetMinBuf()
	objMax := objZM.GetMaxBuf()

	// Binary search: find the first segment whose max ≥ objMin.
	// Since max values are monotonically increasing (invariant 2),
	// this is a standard lower-bound search.
	i := sort.Search(len(segments), func(i int) bool {
		seg := ZM(segments[i])
		return compute.Compare(seg.GetMaxBuf(), objMin, t, scale, scale) >= 0
	})

	if i >= len(segments) {
		return false // every segment lies entirely below objMin
	}

	// The candidate segment's max ≥ objMin (guaranteed by the search).
	// Overlap exists iff its min ≤ objMax.
	seg := ZM(segments[i])
	return compute.Compare(seg.GetMinBuf(), objMax, t, scale, scale) <= 0
}
