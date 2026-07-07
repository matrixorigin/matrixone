// Copyright 2026 Matrix Origin
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

package wand

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// The compaction correctness property: FilterLive each segment by its liveness,
// then Merge — the result reproduces exactly the live pk set that
// SearchSegmentsLive returns over the originals (dedup, delete, reinsert), with no
// cross-segment duplicates. This is what lets the compact TVF replace the
// base+tail with a single merged base.
func TestWandCompact_FilterLiveMerge(t *testing.T) {
	q := []string{"x"}
	segs := []*WandModel{
		buildSeg(t, 1, map[int64][]string{5: {"x"}, 6: {"x"}, 7: {"x"}}),
		buildSeg(t, 2, map[int64][]string{5: {"x"}}), // UPDATE pk 5 (newer chunk)
	}
	deletes := map[any]int64{normalizeKey(int64(6)): 3} // DELETE pk 6 after both inserts
	live := ComputeLiveness(segs, deletes)

	// reference live set (SearchSegmentsLive) — expect {5, 7}, pk 6 deleted
	want := pkCounts(SearchSegmentsLive(segs, q, 10, nil, live))
	require.Equal(t, map[int64]int{5: 1, 7: 1}, want)

	// compact: FilterLive each → Merge → search the single merged model
	filtered := make([]*WandModel, len(segs))
	for i, s := range segs {
		filtered[i] = s.FilterLive(live[i])
	}
	merged := Merge("compacted", filtered...)
	got := pkCounts(SearchSegments([]*WandModel{merged}, q, 10, nil))

	require.Equal(t, want, got, "compacted result must equal the live set")
	require.Equal(t, int64(2), merged.N, "merged holds only the 2 live docs (5,7)")
}

// FilterLive(nil) is the all-live fast path — returns the receiver unchanged.
func TestWandFilterLive_NilIsIdentity(t *testing.T) {
	m := buildSeg(t, 1, map[int64][]string{5: {"x"}, 6: {"x"}})
	require.Same(t, m, m.FilterLive(nil))
}

// Split partitions a finalized model into capacity-bounded sub-models whose
// combined search results are identical to the unsplit model.
func TestWandSplit_PreservesSearch(t *testing.T) {
	docs := map[int64][]string{}
	for i := int64(0); i < 10; i++ {
		docs[i] = []string{"x", fmt.Sprintf("t%d", i%3)}
	}
	m := buildSeg(t, 0, docs) // N = 10, finalized single model
	q := []string{"x"}
	full := pkCounts(SearchSegments([]*WandModel{m}, q, 20, nil))
	require.Len(t, full, 10)

	segs := m.Split(3)
	require.Len(t, segs, 4) // ceil(10/3)
	var total int64
	for _, s := range segs {
		require.LessOrEqual(t, s.N, int64(3), "each sub-model ≤ capacity")
		total += s.N
	}
	require.Equal(t, int64(10), total)

	require.Equal(t, full, pkCounts(SearchSegments(segs, q, 20, nil)),
		"split must preserve the search result set")
}

// Split is a no-op (returns the receiver) when capacity <= 0 or N <= capacity.
func TestWandSplit_NoOpUnderCapacity(t *testing.T) {
	m := buildSeg(t, 0, map[int64][]string{1: {"x"}, 2: {"x"}})
	require.Equal(t, []*WandModel{m}, m.Split(0))
	require.Equal(t, []*WandModel{m}, m.Split(100))
}
