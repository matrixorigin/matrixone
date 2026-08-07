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

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestStreamBoolean pins the no-LIMIT boolean streaming path (Index.streamBoolean): a
// NON-disjunctive boolean query (MUST / MUST-NOT / mixed phrase) must stream EXACTLY the
// (pk, score) set the materialized SearchQuery(k=all) produces — one segment at a time,
// with no global top-K sized to globalN — must dedup a pk across the base + CDC tail
// (per-segment liveness), and must propagate an emit error. This is precisely the shape
// that used to fall back to SearchQuery(globalN) and materialize the whole live corpus
// before emitting; +alpha (matches every doc) exercises the low-selectivity case.
func TestStreamBoolean(t *testing.T) {
	split := NewIndex([]*Segment{
		buildFtSeg(t, "a", 0, 0, 12),
		buildFtSeg(t, "b", 1, 12, 24),
	}, nil)

	cases := []struct {
		pat      string
		nonEmpty bool
	}{
		{"+alpha", true},               // MUST, matches EVERY doc (low selectivity)
		{"+alpha beta", true},          // MUST + SHOULD
		{"+alpha -delta", true},        // MUST + MUST-NOT
		{"alpha -gamma", true},         // SHOULD + MUST-NOT (non-disjunctive via -)
		{"+beta gamma", true},          // MUST + SHOULD
		{`+alpha "beta gamma"`, false}, // MUST + phrase (mixed) — parity even if empty
	}

	// Guard: every case is genuinely non-disjunctive, so StreamQuery routes it to
	// streamBoolean (NOT streamWAND / streamPhrase). If a future refactor made one
	// disjunctive, this test would silently exercise the wrong path — so pin it here.
	for _, c := range cases {
		q, err := buildBooleanQuery(c.pat, normalizeParser(ParserDefault))
		require.NoErrorf(t, err, "build %q", c.pat)
		_, disj := disjunctiveTerms(q)
		require.Falsef(t, disj, "%q must be non-disjunctive (exercise streamBoolean)", c.pat)
	}

	collect := func(pattern string) map[int64]float64 {
		m := map[int64]float64{}
		err := split.StreamQuery([]byte(pattern), true, ParserDefault, BM25, nil, false,
			func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error {
				require.LessOrEqual(t, keys.N, streamBatch)
				for i, k := range int64ColumnBuffer(keys) {
					_, dup := m[k]
					require.Falsef(t, dup, "pk %d emitted twice", k) // liveness dedup
					m[k] = dists[i]
				}
				return nil
			})
		require.NoError(t, err)
		return m
	}

	for _, c := range cases {
		want, err := split.SearchQuery([]byte(c.pat), true, ParserDefault, BM25, 1000, nil)
		require.NoError(t, err)
		got := collect(c.pat)
		require.Equalf(t, len(want), len(got), "%q: same match count", c.pat)
		for _, r := range want {
			g, ok := got[r.Pk.(int64)]
			require.Truef(t, ok, "%q: pk %v streamed", c.pat, r.Pk)
			require.InDeltaf(t, r.Score, g, 1e-5, "%q: pk %v score", c.pat, r.Pk)
		}
		if c.nonEmpty {
			require.NotEmptyf(t, got, "%q: expected a non-empty streamed result", c.pat)
		}
	}

	// emit-error path: the callback error propagates and stops the boolean walk.
	sentinel := errors.New("consumer aborted")
	err := split.StreamQuery([]byte("+alpha beta"), true, ParserDefault, BM25, nil, false,
		func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error { return sentinel })
	require.ErrorIs(t, err, sentinel)
}
