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

package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestSearchWANDGlobalFloorMatchesSingleSegment proves the shared global-minimum-score
// floor (globalStats.minScore) is results-neutral. The SAME corpus is built as ONE
// segment -- where the floor never prunes across segments, so its top-k is the ground
// truth -- and as MANY segments, where the cross-segment floor is active. For a
// truncating k the multi-segment top-k (serial AND all-cores) must equal the
// single-segment top-k exactly, scores included.
func TestSearchWANDGlobalFloorMatchesSingleSegment(t *testing.T) {
	// Distinct per-doc scores: "rare" appears (i+1) times in docs 0..19 (distinct tf,
	// df=20), docLen held constant at L via "pad" so BM25 length-norm is equal and the
	// score is strictly monotonic in tf -> a competitive top-k with NO ties, so equality
	// is exact rather than tie-ambiguous. "alpha" is in every doc for a full-set case.
	const (
		L  = 32
		nD = 120
	)
	build := func(capacity int64) *Index {
		b := NewBuilder("idx", int32(types.T_int64))
		for i := 0; i < nD; i++ {
			words := make([]string, 0, L)
			if i < 20 {
				for j := 0; j <= i; j++ {
					words = append(words, "rare")
				}
			}
			words = append(words, "alpha")
			for len(words) < L {
				words = append(words, "pad")
			}
			feed(t, b, int64(i), words...)
		}
		segs, err := b.FinishSegments(capacity)
		require.NoError(t, err)
		return NewIndex(segs, nil)
	}

	oneSeg := build(1 << 30) // all docs in one segment -> floor-neutral ground truth
	require.Len(t, oneSeg.segments, 1, "oracle must be a single segment")
	multi := build(8) // 120/8 -> many segments, cross-segment floor active
	require.Greater(t, len(multi.segments), 1, "need multiple segments to engage the floor")

	list := func(idx *Index, threads, k int, q string) ([]any, []float32) {
		idx.threadsSearch = threads
		res, err := idx.SearchBagOfWords([]byte(q), ParserDefault, BM25, k, nil, 0)
		require.NoError(t, err)
		for i := 1; i < len(res); i++ {
			require.LessOrEqual(t, res[i].Score, res[i-1].Score)
		}
		ks := make([]any, len(res))
		ss := make([]float32, len(res))
		for i, r := range res {
			ks[i], ss[i] = normalizeKey(r.Pk), r.Score
		}
		return ks, ss
	}

	// Truncating top-k over the 20 "rare" matches: the floor's whole job is to prune the
	// globally-uncompetitive docs across segments, so this is the case that exercises it.
	// The pre-pass (ceiling-ordering + whole-segment skip) is unconditional, so these
	// multi-segment comparisons exercise floor AND pre-pass together: serial engages it,
	// and threads=0 is a multi-wave fan-out (nseg > GOMAXPROCS) that engages it too. Both
	// must match the single-segment oracle (where neither is active) exactly.
	wantK, wantS := list(oneSeg, 1, 8, "rare")
	require.Len(t, wantK, 8, "k=8 truncates the 20 'rare' matches")
	require.Equal(t, []any{int64(19), int64(18), int64(17), int64(16), int64(15), int64(14), int64(13), int64(12)}, wantK, "top-8 by 'rare' tf")

	for _, threads := range []int{1, 0} {
		gotK, gotS := list(multi, threads, 8, "rare")
		require.Equal(t, wantK, gotK, "multi-segment floor (threads=%d) must match the single-segment top-k", threads)
		require.Equal(t, wantS, gotS, "multi-segment floor (threads=%d) scores must match", threads)
	}

	// Smaller k tightens the floor sooner (more aggressive cross-segment pruning); the
	// ordered top-3 must still match the single-segment oracle exactly.
	w3K, w3S := list(oneSeg, 1, 3, "rare")
	require.Equal(t, []any{int64(19), int64(18), int64(17)}, w3K, "top-3 by 'rare' tf")
	for _, threads := range []int{1, 0} {
		gotK, gotS := list(multi, threads, 3, "rare")
		require.Equal(t, w3K, gotK, "multi-segment floor (threads=%d, k=3) must match oracle", threads)
		require.Equal(t, w3S, gotS, "multi-segment floor (threads=%d, k=3) scores must match", threads)
	}

	// Force the PARALLEL ceiling fan-out: many small segments with workers=2 clears the
	// segments-per-worker threshold (ceilPrepassMinSegPerWorker). The result must still
	// match the oracle, and this exercises the concurrent ceiling writes under -race.
	many := build(4) // 120/4 -> ~30 segments
	require.GreaterOrEqual(t, len(many.segments), 2*ceilPrepassMinSegPerWorker, "need >= 8*workers segments for workers=2")
	pK, pS := list(oneSeg, 1, 8, "rare")
	gotK, gotS := list(many, 2, 8, "rare")
	require.Equal(t, pK, gotK, "parallel ceiling pass keys must match oracle")
	require.Equal(t, pS, gotS, "parallel ceiling pass scores must match oracle")
}
