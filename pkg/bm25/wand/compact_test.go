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

	"github.com/matrixorigin/matrixone/pkg/container/types"
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

// Regression: a string-family (varchar/text/…) pk decodes to []byte, which is
// unhashable as a raw map key. survivingDeletes (used by CompactSegments during
// MERGE) must normalizeKey the live pks — otherwise MERGE panics ("hash of
// unhashable type []uint8"), and even for comparable pks a raw key would never
// match the normalized `deletes` keys, mis-classifying every delete as surviving.
func TestWandSurvivingDeletes_StringPk(t *testing.T) {
	// A varchar-pk tail: docs alpha/beta/gamma live; deletes of beta (re-inserted
	// live) and delta (never re-inserted). pks are []byte, the decodePk form.
	b := NewBuilder("seg1", int32(types.T_varchar))
	for _, pk := range [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")} {
		require.NoError(t, b.Add("x", pk))
	}
	filtered := []*WandModel{b.Finish()}

	// deletes map is keyed the way FoldDeleteFrame/ComputeLiveness key it: normalized.
	deletes := map[any]int64{
		normalizeKey([]byte("beta")):  5, // resolved: beta is a live insert -> dropped
		normalizeKey([]byte("delta")): 5, // surviving: delta is not a live insert
	}

	var surviving []DeleteRecord
	require.NotPanics(t, func() { surviving = survivingDeletes(filtered, deletes) })

	require.Len(t, surviving, 1, "only the unresolved delete (delta) survives")
	require.Equal(t, normalizeKey([]byte("delta")), normalizeKey(surviving[0].Pk))
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

// selectMergeRun picks the first maximal run of ADJACENT under-capacity subs (≥2), capped
// at mergeFactor / maxMergeBytes. Fullness is by doc count vs capacity (a full sub is never
// a candidate) and adjacency is the correctness property (no skipped middle sub) — both
// verified here across full/under-cap interleavings and the caps.
func TestSelectMergeRun(t *testing.T) {
	const capacity = int64(100)
	// mk builds subs from doc counts; tiny filesize so the byte budget never binds here.
	mk := func(nrows ...int64) []baseSubMeta {
		metas := make([]baseSubMeta, len(nrows))
		for i, n := range nrows {
			metas[i] = baseSubMeta{id: fmt.Sprintf("s%d", i), recency: int64(i), nrow: n, filesize: 1}
		}
		return metas
	}
	un, fl := int64(10), capacity // under-capacity vs full (nrow >= capacity)
	check := func(name string, metas []baseSubMeta, wantLo, wantHi int) {
		lo, hi := selectMergeRun(metas, capacity)
		require.Equal(t, [2]int{wantLo, wantHi}, [2]int{lo, hi}, name)
	}

	check("empty", nil, 0, 0)
	check("single under-cap", mk(un), 0, 0)
	check("all full", mk(fl, fl, fl), 0, 0)
	check("two under-cap", mk(un, un), 0, 2)
	check("full then run", mk(fl, un, un), 1, 3)
	check("run then full", mk(un, un, fl), 0, 2)
	// first under-cap is alone (followed by a full sub); the real run is the trailing pair.
	check("lone under-cap, full, run", mk(un, fl, un, un), 2, 4)

	// mergeFactor cap: 10 under-cap ⇒ first mergeFactor of them.
	ten := make([]int64, 10)
	for i := range ten {
		ten[i] = un
	}
	check("mergeFactor cap", mk(ten...), 0, mergeFactor)

	// byte-budget cap: under-cap subs but each > half maxMergeBytes ⇒ no adjacent pair fits.
	big := int64(maxMergeBytes/2 + 1)
	metas := []baseSubMeta{
		{id: "b0", recency: 0, nrow: un, filesize: big},
		{id: "b1", recency: 1, nrow: un, filesize: big},
	}
	lo, hi := selectMergeRun(metas, capacity)
	require.Equal(t, [2]int{0, 0}, [2]int{lo, hi}, "each pair exceeds the byte budget ⇒ no run")

	// capacity <= 0 (unlimited): nothing is ever full, so even large-nrow subs coalesce.
	check2 := func(name string, metas []baseSubMeta, wantLo, wantHi int) {
		lo, hi := selectMergeRun(metas, 0)
		require.Equal(t, [2]int{wantLo, wantHi}, [2]int{lo, hi}, name)
	}
	check2("unlimited coalesces all", mk(fl, fl, fl), 0, mergeFactorMin(3))
}

func mergeFactorMin(n int) int {
	if n < mergeFactor {
		return n
	}
	return mergeFactor
}
