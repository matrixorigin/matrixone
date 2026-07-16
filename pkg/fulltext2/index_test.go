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
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

func buildSeg(t *testing.T, id string, rec int64, docs []Doc) *Segment {
	t.Helper()
	s, err := BuildSegmentFromDocs(id, int32(types.T_int64), docs, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	s.Recency = rec
	return s
}

func iquery(t *testing.T, idx *Index, q string) []any {
	t.Helper()
	rs, err := idx.SearchText([]byte(q), tokenizer.NewSimpleTokenizer(), TfIdf, 10, nil)
	require.NoError(t, err)
	return pkslice(rs)
}

// TestIndexLiveness: a base segment + a tail that UPDATES pk 2 (new text) and
// adds pk 4, with pk 3 deleted. Queries must see only live copies.
func TestIndexLiveness(t *testing.T) {
	base := buildSeg(t, "base", 0, []Doc{
		{int64(1), []byte("alpha beta")},
		{int64(2), []byte("gamma")}, // superseded below
		{int64(3), []byte("delta")}, // deleted below
	})
	tail := buildSeg(t, "tail", 1, []Doc{
		{int64(2), []byte("omega")}, // new text for pk 2
		{int64(4), []byte("beta")},
	})
	idx := NewIndex([]*Segment{base, tail}, map[any]int64{normalizeKey(int64(3)): 1})

	require.Equal(t, int64(3), idx.NumDocs()) // pk 1, 2(new), 4 live; pk 3 deleted

	// "beta" is in base:pk1 and tail:pk4 — both live.
	require.Equal(t, []any{int64(1), int64(4)}, iquery(t, idx, "beta"))
	// pk2's OLD text ("gamma") is superseded by the tail → not visible.
	require.Empty(t, iquery(t, idx, "gamma"))
	// pk2's NEW text ("omega") is the live copy.
	require.Equal(t, []any{int64(2)}, iquery(t, idx, "omega"))
	// pk3 is deleted.
	require.Empty(t, iquery(t, idx, "delta"))
	// base-only live doc.
	require.Equal(t, []any{int64(1)}, iquery(t, idx, "alpha"))
}

// TestIndexGlobalStats: df/idf and avgDocLen span segments. A term present once
// in each of two segments has df=2 against the global N.
func TestIndexGlobalStats(t *testing.T) {
	s1 := buildSeg(t, "s1", 0, []Doc{{int64(1), []byte("cat")}, {int64(2), []byte("dog")}})
	s2 := buildSeg(t, "s2", 0, []Doc{{int64(3), []byte("cat")}, {int64(4), []byte("bird")}})
	idx := NewIndex([]*Segment{s1, s2}, nil)

	require.Equal(t, int64(4), idx.NumDocs())
	require.EqualValues(t, 1.0, idx.globalAvgDocLen) // every doc has 1 token
	// "cat" is in pk1 and pk3 across the two segments.
	require.Equal(t, []any{int64(1), int64(3)}, iquery(t, idx, "cat"))
	// idf uses the GLOBAL df (=2) and N (=4), not a per-segment count.
	rs, err := idx.SearchText([]byte("cat"), tokenizer.NewSimpleTokenizer(), TfIdf, 10, nil)
	require.NoError(t, err)
	want := idfSquared(4, 2) // tf=1
	require.InDelta(t, want, rs[0].Score, 1e-9)
}

// TestIndexPhraseAcrossSegments: an exact phrase resolves within each segment;
// the live matches across segments merge.
func TestIndexPhraseAcrossSegments(t *testing.T) {
	s1 := buildSeg(t, "s1", 0, []Doc{{int64(1), []byte("quick brown fox")}})
	s2 := buildSeg(t, "s2", 0, []Doc{{int64(2), []byte("the quick brown fox runs")}, {int64(3), []byte("brown quick")}})
	idx := NewIndex([]*Segment{s1, s2}, nil)

	require.Equal(t, []any{int64(1), int64(2)}, iquery(t, idx, "quick brown fox"))
	require.Empty(t, iquery(t, idx, "fox brown")) // not contiguous anywhere
}
