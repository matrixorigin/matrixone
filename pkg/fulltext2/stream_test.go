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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestStreamQuery pins the no-LIMIT streaming path: StreamQuery must emit EXACTLY the
// same (pk, score) set the materialized SearchQuery(k=all) produces — for both the
// heap-free disjunctive path (streamWAND) and the materialize-then-emit fallback (NL
// phrase) — and it must stop and propagate an emit error. Uses a base + CDC-tail split
// so streaming exercises the per-segment liveness + global stats.
func TestStreamQuery(t *testing.T) {
	split := NewIndex([]*Segment{
		buildFtSeg(t, "a", 0, 0, 12),
		buildFtSeg(t, "b", 1, 12, 24),
	}, nil)

	collect := func(pattern string, boolean bool) map[int64]float64 {
		m := map[int64]float64{}
		err := split.StreamQuery([]byte(pattern), boolean, ParserDefault, BM25, nil,
			func(keys []any, dists []float64) error {
				require.LessOrEqual(t, len(keys), streamBatch)
				for i, k := range keys {
					_, dup := m[k.(int64)]
					require.Falsef(t, dup, "pk %d emitted twice", k.(int64))
					m[k.(int64)] = dists[i]
				}
				return nil
			})
		require.NoError(t, err)
		return m
	}
	sameAsMaterialized := func(pattern string, boolean bool) {
		want, err := split.SearchQuery([]byte(pattern), boolean, ParserDefault, BM25, 1000, nil)
		require.NoError(t, err)
		got := collect(pattern, boolean)
		require.Equalf(t, len(want), len(got), "%q: same match count", pattern)
		for _, r := range want {
			g, ok := got[r.Pk.(int64)]
			require.Truef(t, ok, "%q: pk %v streamed", pattern, r.Pk)
			require.InDeltaf(t, r.Score, g, 1e-5, "%q: pk %v score", pattern, r.Pk)
		}
	}

	sameAsMaterialized("alpha beta gamma", true) // disjunctive → streamWAND
	sameAsMaterialized("beta delta", true)       // disjunctive → streamWAND
	sameAsMaterialized("alpha", false)           // NL single-word → materialize fallback

	// emit-error path: the callback error propagates and stops the walk.
	sentinel := errors.New("consumer aborted")
	err := split.StreamQuery([]byte("alpha beta"), true, ParserDefault, BM25, nil,
		func(keys []any, dists []float64) error { return sentinel })
	require.ErrorIs(t, err, sentinel)
}
