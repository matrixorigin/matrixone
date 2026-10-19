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

// TestSearchBooleanParallelMatchesSerial proves the multi-segment parallel search
// (Index.threadsSearch > 1, the fulltext2_threads_search knob) returns the SAME
// (pk, score) set as the serial walk: parallelism is a perf-only hint. The corpus is
// split into several segments so the worker fan-out is actually exercised, with varied
// term frequencies so BM25 scores differ across docs and segments.
func TestSearchBooleanParallelMatchesSerial(t *testing.T) {
	b := NewBuilder("idx", int32(types.T_int64))
	for i := 0; i < 60; i++ {
		words := []string{"alpha"} // every doc matches
		if i%2 == 0 {
			words = append(words, "beta")
		}
		if i%7 == 0 {
			words = append(words, "gamma") // rare → high idf
		}
		feed(t, b, int64(i), words...)
	}
	segs, err := b.FinishSegments(9) // 60/9 => 7 segments
	require.NoError(t, err)
	require.Greater(t, len(segs), 1, "need multiple segments to exercise the fan-out")

	idx := NewIndex(segs, nil)

	// k >= matches (all 60 docs contain "alpha"), so there is no top-k truncation and
	// the returned SET is deterministic; only the final tie-order (map iteration) can
	// vary, and it varies independently of threads — so compare the (pk->score) set.
	run := func(threads int) map[any]float32 {
		idx.threadsSearch = threads
		res, err := idx.SearchBagOfWords([]byte("alpha beta gamma"), ParserDefault, BM25, 100, nil)
		require.NoError(t, err)
		m := make(map[any]float32, len(res))
		for _, r := range res {
			m[normalizeKey(r.Pk)] = r.Score
		}
		// top-k scores are returned in non-increasing order.
		for i := 1; i < len(res); i++ {
			require.LessOrEqual(t, res[i].Score, res[i-1].Score)
		}
		return m
	}

	serial := run(1) // serial walk
	require.Len(t, serial, 60, "every doc contains alpha")

	require.Equal(t, serial, run(8), "8-worker parallel search must equal the serial result")
	require.Equal(t, serial, run(0), "GOMAXPROCS-default parallel search must equal the serial result")
}
