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
	"encoding/binary"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/stretchr/testify/require"
)

// TestStreamPhraseParity pins that the no-LIMIT streaming phrase path (streamPhrase)
// returns EXACTLY the same pk set and scores as the materializing SearchPhrase (the
// LIMIT path) — same idf²-scaled score, only without the globalN heap.
func TestStreamPhraseParity(t *testing.T) {
	bb := NewBuilder("p", int32(types.T_int64))
	feed(t, bb, int64(1), "alpha", "beta")      // phrase match
	feed(t, bb, int64(2), "alpha", "beta")      // phrase match
	feed(t, bb, int64(3), "x", "alpha", "beta") // phrase match (offset)
	feed(t, bb, int64(4), "beta", "alpha")      // NOT adjacent as "alpha beta"
	feed(t, bb, int64(5), "alpha", "gamma")     // no beta
	seg := loadedSeg(t, bb)
	idx := NewIndex([]*Segment{seg}, nil)

	slots, err := phraseSlots("alpha beta", ParserDefault)
	require.NoError(t, err)

	// materialized reference (LIMIT path, k above corpus so it returns every match).
	want := idx.SearchPhrase(slots, BM25, 1<<20, nil)
	wantScore := make(map[int64]float32, len(want))
	for _, r := range want {
		wantScore[r.Pk.(int64)] = r.Score
	}
	require.ElementsMatch(t, []int64{1, 2, 3}, keysOf(wantScore))

	// streamed (no-LIMIT) — decode the box-free int64 ColumnBuffer batches.
	got := make(map[int64]float32)
	emit := func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error {
		for i := 0; i < keys.N; i++ {
			pk := int64(binary.LittleEndian.Uint64(keys.Data[i*8:]))
			got[pk] = float32(dists[i])
		}
		PutColumnBuffer(keys)
		return nil
	}
	require.NoError(t, idx.StreamQuery([]byte("alpha beta"), false, ParserDefault, BM25, nil, false, emit))

	// same pk set, same idf²-scaled scores.
	require.Equal(t, len(wantScore), len(got))
	for pk, ws := range wantScore {
		gs, ok := got[pk]
		require.Truef(t, ok, "streamed result missing pk %d", pk)
		require.InEpsilonf(t, ws, gs, 1e-6, "score mismatch for pk %d", pk)
	}
}

func keysOf(m map[int64]float32) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func streamPhraseIDs(t *testing.T, idx *Index, pattern string) map[int64]float32 {
	t.Helper()
	got := make(map[int64]float32)
	emit := func(keys *vectorindex.ColumnBuffer, dists []float64, _ [][]any) error {
		for i := 0; i < keys.N; i++ {
			got[int64(binary.LittleEndian.Uint64(keys.Data[i*8:]))] = float32(dists[i])
		}
		PutColumnBuffer(keys)
		return nil
	}
	require.NoError(t, idx.StreamQuery([]byte(pattern), false, ParserDefault, BM25, nil, false, emit))
	return got
}

// TestFillBlockPositionsReuseMultiBlock stresses the Phase-1 pooled-buffer reuse across
// MULTIPLE blocks (>BlockSize docs) with varying per-doc position counts: the phrase
// term repeats a different number of times per doc, so each reused out[i] slot changes
// length block to block. A stale/aliased backing array would surface as a wrong match
// set. Runs twice so a pooled buffer is re-Got from the pool between queries.
func TestFillBlockPositionsReuseMultiBlock(t *testing.T) {
	const nDocs = 3 * BlockSize // spans several position blocks
	bb := NewBuilder("mb", int32(types.T_int64))
	wantMatch := map[int64]bool{}
	for d := 0; d < nDocs; d++ {
		pk := int64(d)
		reps := 1 + d%5 // varying position-list length per doc
		var words []string
		for r := 0; r < reps; r++ {
			words = append(words, "alpha", "beta")
		}
		if d%3 == 0 { // a third have "beta alpha" appended → still contains the phrase "alpha beta"
			words = append(words, "beta", "alpha")
		}
		if d%7 == 0 { // some docs break the phrase entirely (no adjacent alpha->beta)
			words = []string{"beta", "alpha", "gamma"}
			wantMatch[pk] = false
		} else {
			wantMatch[pk] = true
		}
		feed(t, bb, pk, words...)
	}
	seg := loadedSeg(t, bb)
	idx := NewIndex([]*Segment{seg}, nil)

	slots, err := phraseSlots("alpha beta", ParserDefault)
	require.NoError(t, err)

	run := func() {
		res := idx.SearchPhrase(slots, BM25, 1<<20, nil)
		got := map[int64]bool{}
		for _, r := range res {
			got[r.Pk.(int64)] = true
		}
		for pk, want := range wantMatch {
			require.Equalf(t, want, got[pk], "pk %d match", pk)
		}
	}
	run() // first pass populates the pool
	run() // second pass re-Gets a reused buffer — must be identical
}

// TestStreamPhraseParityMultiSegment: streamPhrase across a base + CDC-tail (with a
// delete making one base copy dead) must match SearchPhrase — same live set, same
// idf²-scaled scores — proving the multi-segment / liveness path streams correctly.
func TestStreamPhraseParityMultiSegment(t *testing.T) {
	base := NewBuilder("base", int32(types.T_int64))
	feed(t, base, int64(1), "alpha", "beta")
	feed(t, base, int64(2), "alpha", "beta")
	feed(t, base, int64(3), "beta", "alpha") // no phrase
	baseSeg := loadedSeg(t, base)
	baseSeg.Recency = 0

	tail := NewBuilder("tail", int32(types.T_int64))
	feed(t, tail, int64(4), "alpha", "beta")
	tailSeg := loadedSeg(t, tail)
	tailSeg.Recency = 10

	// delete pk 2 (its base phrase-match copy must drop out of both paths).
	deletes := foldDeleteFrame(nil, []DeleteRecord{{Pk: int64(2)}}, 20)
	idx := NewIndex([]*Segment{baseSeg, tailSeg}, deletes)

	slots, err := phraseSlots("alpha beta", ParserDefault)
	require.NoError(t, err)
	want := idx.SearchPhrase(slots, BM25, 1<<20, nil)
	wantScore := make(map[int64]float32, len(want))
	for _, r := range want {
		wantScore[r.Pk.(int64)] = r.Score
	}
	require.ElementsMatch(t, []int64{1, 4}, keysOf(wantScore)) // 2 deleted, 3 no-phrase

	got := streamPhraseIDs(t, idx, "alpha beta")
	require.Equal(t, len(wantScore), len(got))
	for pk, ws := range wantScore {
		require.InEpsilonf(t, ws, got[pk], 1e-6, "score mismatch pk %d", pk)
	}
}
