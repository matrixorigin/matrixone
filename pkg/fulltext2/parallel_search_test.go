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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// TestSearchBooleanParallelMatchesSerial proves the multi-segment parallel search
// (Index.threadsSearch > 1, the fulltext2_threads_search knob) returns the SAME
// (pk, score) set as the serial walk: parallelism is a perf-only hint. The corpus is
// split into several segments so the worker fan-out is actually exercised, with varied
// term frequencies so BM25 scores differ across docs and segments.
func TestSearchBooleanParallelMatchesSerial(t *testing.T) {
	// For a DETERMINISTIC competitive top-k the discriminating term must have a
	// positive idf (present in a MINORITY of docs — a term in every doc has df==N, so
	// idf<=0 and every score collapses to a tie) AND distinct per-doc scores. "rare"
	// is in docs 0..9 only, with (i+1) copies → distinct tf; docLen is held CONSTANT
	// (pad to L) so BM25 length-norm is equal and the score is strictly monotonic in
	// tf. "alpha" is in every doc to give the full-set disjunction 60 matches.
	const L = 24
	b := NewBuilder("idx", int32(types.T_int64))
	for i := 0; i < 60; i++ {
		words := make([]string, 0, L)
		if i < 10 {
			for j := 0; j <= i; j++ {
				words = append(words, "rare") // docs 0..9: i+1 copies → distinct tf, df=10
			}
		}
		if i%2 == 0 {
			words = append(words, "beta")
		}
		if i%7 == 0 {
			words = append(words, "gamma")
		}
		words = append(words, "alpha") // every doc → full-set disjunction match
		for len(words) < L {
			words = append(words, "pad") // filler → constant docLen, unqueried
		}
		feed(t, b, int64(i), words...)
	}
	segs, err := b.FinishSegments(9) // 60/9 => 7 segments
	require.NoError(t, err)
	require.Greater(t, len(segs), 1, "need multiple segments to exercise the fan-out")

	idx := NewIndex(segs, nil)

	runList := func(threads, k int, pattern string) []Result {
		idx.threadsSearch = threads
		res, err := idx.SearchBagOfWords([]byte(pattern), ParserDefault, BM25, k, nil, 0)
		require.NoError(t, err)
		for i := 1; i < len(res); i++ {
			require.LessOrEqual(t, res[i].Score, res[i-1].Score) // non-increasing
		}
		return res
	}
	keys := func(res []Result) []any {
		ks := make([]any, len(res))
		for i, r := range res {
			ks[i] = normalizeKey(r.Pk)
		}
		return ks
	}
	set := func(res []Result) map[any]float32 {
		m := make(map[any]float32, len(res))
		for _, r := range res {
			m[normalizeKey(r.Pk)] = r.Score
		}
		return m
	}

	// (a) COMPETITIVE TRUNCATION: "rare" matches 10 docs (0..9) with distinct scores,
	// k=5 truncates → the ordered top-5 is deterministic (docs 9,8,7,6,5 by tf), so the
	// per-segment heap cutoff + serial merge must yield the SAME ordered pk list serially
	// and in parallel. This is what actually exercises the truncated heap.
	serialTrunc := runList(1, 5, "rare")
	require.Len(t, serialTrunc, 5, "k=5 truncates the 10 'rare' matches")
	require.Equal(t, []any{int64(9), int64(8), int64(7), int64(6), int64(5)}, keys(serialTrunc), "top-5 by 'rare' tf")
	require.Equal(t, keys(serialTrunc), keys(runList(8, 5, "rare")), "8-worker truncated top-k order must match serial")
	require.Equal(t, keys(serialTrunc), keys(runList(0, 5, "rare")), "default-parallel truncated top-k order must match serial")

	// (b) FULL multi-term SET: k >= matches, so every doc is returned and the (pk->score)
	// set is tie-safe to compare across the multi-term disjunction (WAND) path.
	serialAll := set(runList(1, 100, "alpha beta gamma"))
	require.Len(t, serialAll, 60, "every doc contains alpha")
	require.Equal(t, serialAll, set(runList(8, 100, "alpha beta gamma")), "8-worker parallel search must equal the serial result")
	require.Equal(t, serialAll, set(runList(0, 100, "alpha beta gamma")), "default-parallel search must equal the serial result")
}

func TestFanOutReRaisesWorkerPanicOnCaller(t *testing.T) {
	order := []int{0, 1, 2, 3, 4, 5, 6, 7}
	var ran [8]atomic.Bool
	require.PanicsWithValue(t, "segment 3", func() {
		fanOut(order, 3, func(si int) {
			ran[si].Store(true)
			if si == 3 {
				panic("segment 3")
			}
		})
	})
	for si := range ran {
		require.True(t, ran[si].Load(), "segment %d must still run", si)
	}

	var sum atomic.Int64
	fanOut(order, 2, func(si int) { sum.Add(int64(si)) })
	require.Equal(t, int64(28), sum.Load())
}
