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
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// syntheticCorpus builds a deterministic corpus with varied df/tf so WAND has
// something to prune: doc i includes vocab term j when i % (j+2) == 0, repeated
// (i%3)+1 times.
func syntheticCorpus(t *testing.T) *Segment {
	t.Helper()
	vocab := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	var docs []Doc
	for i := 1; i <= 60; i++ {
		var words []string
		for j, w := range vocab {
			if i%(j+2) == 0 {
				for r := 0; r <= i%3; r++ {
					words = append(words, w)
				}
			}
		}
		if len(words) == 0 {
			words = []string{"filler"}
		}
		docs = append(docs, Doc{int64(i), []byte(strings.Join(words, " "))})
	}
	s, err := BuildSegmentFromDocs("syn", int32(types.T_int64), docs, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	return s
}

func requireSameRanking(t *testing.T, label string, wand, full []Result) {
	t.Helper()
	require.Equal(t, len(full), len(wand), "%s: count", label)
	for i := range full {
		require.Equal(t, full[i].Pk, wand[i].Pk, "%s: rank %d pk", label, i)
		require.InDelta(t, full[i].Score, wand[i].Score, 1e-9, "%s: rank %d score", label, i)
	}
}

// TestWANDParity: for every disjunctive query, WAND returns the identical ranking
// (same docs, same order, same scores) as the full scan — across k and both
// scorers, on two corpora. This is the WAND correctness contract: it only avoids
// work, it never changes results.
func TestWANDParity(t *testing.T) {
	type tc struct {
		name  string
		seg   *Segment
		query string
	}
	small := fulltextCorpus(t)
	syn := syntheticCorpus(t)
	cases := []tc{
		{"small/2", small, "quick fox"},
		{"small/3", small, "quick fox dog"},
		{"small/weighted", small, ">quick fox <dog"},
		{"small/all", small, "quick brown fox jumps lazy dog high sleeps"},
		{"syn/2", syn, "alpha beta"},
		{"syn/3", syn, "beta gamma delta"},
		{"syn/all", syn, "alpha beta gamma delta epsilon"},
		{"syn/weighted", syn, ">alpha beta <gamma delta"},
	}
	for _, c := range cases {
		for _, algo := range []ScoreAlgo{TfIdf, BM25} {
			q, err := ParseBoolean([]byte(c.query), tokenizer.NewSimpleTokenizer())
			require.NoError(t, err)
			terms, ok := disjunctiveTerms(q)
			require.True(t, ok, "%s must be WAND-eligible", c.name)

			for k := 1; k <= int(c.seg.N)+1; k++ {
				wand := c.seg.searchWAND(terms, algo, k)
				full, err := c.seg.searchBooleanFull(q, algo, k)
				require.NoError(t, err)
				requireSameRanking(t, fmt.Sprintf("%s algo=%d k=%d", c.name, algo, k), wand, full)
			}
		}
	}
}

// TestWANDRouted confirms SearchBoolean actually routes a pure disjunction
// through WAND and yields the same result as the forced full path.
func TestWANDRouted(t *testing.T) {
	s := syntheticCorpus(t)
	q, err := ParseBoolean([]byte("alpha beta gamma"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)

	routed, err := s.SearchBoolean(q, BM25, 5)
	require.NoError(t, err)
	full, err := s.searchBooleanFull(q, BM25, 5)
	require.NoError(t, err)
	requireSameRanking(t, "routed", routed, full)
}

// TestWANDParityLoaded: parity also holds on a serialize→deserialize'd segment
// (term stats derived at load).
func TestWANDParityLoaded(t *testing.T) {
	data, err := syntheticCorpus(t).Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("syn", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	q, err := ParseBoolean([]byte("alpha beta gamma delta"), tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	terms, _ := disjunctiveTerms(q)
	for _, algo := range []ScoreAlgo{TfIdf, BM25} {
		for k := 1; k <= int(loaded.N)+1; k++ {
			wand := loaded.searchWAND(terms, algo, k)
			full, err := loaded.searchBooleanFull(q, algo, k)
			require.NoError(t, err)
			requireSameRanking(t, fmt.Sprintf("loaded algo=%d k=%d", algo, k), wand, full)
		}
	}
}
