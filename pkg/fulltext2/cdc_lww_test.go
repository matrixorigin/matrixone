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

// CDC last-write-wins semantics (engine layer, no live DB): a later upsert of a pk
// must REPLACE — not merge with — its earlier terms, and an upsert to empty/NULL text
// must shadow the old version. Drives the whole write path (TailBuilder -> frames ->
// Index-with-liveness) and asserts the resolved search results, without reordering the
// delete-before-insert frame order (which CDC relies on for same-batch updates).
package fulltext2

import (
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// mkBase builds a tag=0 base segment (recency 0) from pk->text.
func mkBase(t *testing.T, pkTexts map[int64]string) *Segment {
	t.Helper()
	bb := NewBuilder("base", int32(types.T_int64))
	for pk, txt := range pkTexts {
		feed(t, bb, pk, strings.Fields(txt)...)
	}
	seg, err := bb.Finish()
	require.NoError(t, err)
	seg.Recency = 0
	return seg
}

// buildTailIndexLWW runs c through a TailBuilder at capacity, decodes the spilled
// frames in Finish order (delete frames first, then inserts — recencies above the
// base) exactly like LoadTailSegments, and assembles the queryable Index.
func buildTailIndexLWW(t *testing.T, base *Segment, c *Cdc, capacity int64) *Index {
	t.Helper()
	tb, err := NewTailBuilder(int32(types.T_int64), capacity, 0, "", wsTokenize)
	require.NoError(t, err)
	defer tb.Cleanup()
	require.NoError(t, tb.AddBatch(c))
	segs, err := tb.Finish()
	require.NoError(t, err)

	var tails []*Segment
	if base != nil {
		tails = append(tails, base)
	}
	deletes := map[any]int64{}
	recency := int64(100)
	for i, ts := range segs {
		framed, rerr := os.ReadFile(ts.Path)
		require.NoError(t, rerr)
		seg, dels, uerr := UnframeTail("tail", framed)
		require.NoError(t, uerr)
		switch {
		case seg != nil:
			seg.Recency = recency + int64(i)
			tails = append(tails, seg)
		case dels != nil:
			deletes = foldDeleteFrame(deletes, dels, recency+int64(i))
		}
	}
	return NewIndex(tails, deletes)
}

func queryIDs(t *testing.T, idx *Index, word string) []any {
	t.Helper()
	res, err := idx.SearchQuery([]byte(word), false, ParserDefault, BM25, 100, nil)
	require.NoError(t, err)
	return resultIDs(res)
}

func queryScores(t *testing.T, idx *Index, algo ScoreAlgo, word string) map[int64]float64 {
	t.Helper()
	res, err := idx.SearchQuery([]byte(word), false, ParserDefault, algo, 100, nil)
	require.NoError(t, err)
	m := make(map[int64]float64, len(res))
	for _, r := range res {
		m[r.Pk.(int64)] = float64(r.Score)
	}
	return m
}

// TestDirtyVsCompactedScoreParity pins the live-DF fix: a base+tail carrying DEAD copies
// (rows updated in the tail, their stale base copies not yet reclaimed) must score and rank a
// query IDENTICALLY to a clean index holding only the same LIVE rows. Before live-DF, df was
// summed over physical postings (dead copies included), inflating df past the live N and giving
// the dirty corpus different idf — hence a different ranking — until a MERGE reclaimed the dead
// copies. Covers both BM25 and TF-IDF.
func TestDirtyVsCompactedScoreParity(t *testing.T) {
	// The final LIVE state a compacted index holds.
	live := map[int64]string{
		1: "x y y",
		2: "x x y",
		3: "y y y",
	}
	compacted := NewIndex([]*Segment{mkBase(t, live)}, nil)

	// Dirty: a base of STALE versions (extra "x" copies inflate df(x)) + a tail that upserts
	// every pk to its live text, leaving the stale base copies dead (df-inflating until MERGE).
	dirtyBase := mkBase(t, map[int64]string{1: "x x x x", 2: "x x x x", 3: "x x x x"})
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "x y y", nil)
	c.Upsert(int64(2), "x x y", nil)
	c.Upsert(int64(3), "y y y", nil)
	dirty := buildTailIndexLWW(t, dirtyBase, c, 1000)

	require.Equal(t, compacted.NumDocs(), dirty.NumDocs(), "same live doc count")

	for _, algo := range []ScoreAlgo{BM25, TfIdf} {
		for _, q := range []string{"x", "y", "x y"} {
			cs := queryScores(t, compacted, algo, q)
			ds := queryScores(t, dirty, algo, q)
			require.Equalf(t, len(cs), len(ds), "algo=%v q=%q same match count", algo, q)
			for pk, cscore := range cs {
				dscore, ok := ds[pk]
				require.Truef(t, ok, "algo=%v q=%q pk %d present in dirty result", algo, q, pk)
				require.InDeltaf(t, cscore, dscore, 1e-6, "algo=%v q=%q pk %d dirty-vs-compacted score parity", algo, q, pk)
			}
		}
	}
}

// TestBuilderSetDocReplace pins the LWW primitive: SetDoc REPLACES a pk's terms (not
// append), and a zero-word SetDoc keeps a live 0-term doc (the empty-upsert shadow).
func TestBuilderSetDocReplace(t *testing.T) {
	b := NewBuilder("b", int32(types.T_int64))
	b.SetDoc(int64(1), []WordPos{{Word: "alpha", Pos: 0}}, nil)
	b.SetDoc(int64(1), []WordPos{{Word: "beta", Pos: 0}, {Word: "gamma", Pos: 6}}, nil)
	require.Equal(t, 1, b.NumDocs())     // same pk => one doc
	require.Equal(t, 2, b.NumPostings()) // replaced: 1 -> 2, not appended to 3
	require.Equal(t, []string{"beta", "gamma"}, b.docs[0].Terms)

	// zero-word upsert => a retained 0-term doc (carries pk + recency to shadow).
	b.SetDoc(int64(2), nil, nil)
	require.Equal(t, 2, b.NumDocs())
	require.Empty(t, b.docs[1].Terms)
	require.Equal(t, int64(2), b.docs[1].Pk)
}

// TestCdcUpsertReplaceNoMerge: two upserts of one pk in a flush must not leave BOTH
// terms searchable (the merge bug). Only the final version survives.
func TestCdcUpsertReplaceNoMerge(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha"})
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "beta", nil)
	c.Upsert(int64(1), "gamma", nil)
	idx := buildTailIndexLWW(t, base, c, 1000)

	require.Empty(t, queryIDs(t, idx, "alpha"), "base term superseded by the upsert")
	require.Empty(t, queryIDs(t, idx, "beta"), "intermediate upsert term must NOT linger (no merge)")
	require.Equal(t, []any{int64(1)}, queryIDs(t, idx, "gamma"), "only the final upsert term is live")
}

// TestCdcUpsertEmptyShadows: an upsert to empty text (also how a NULL indexed column
// arrives — rowText returns "") must shadow the old indexed version. A row with no
// searchable content must stop matching its old terms.
func TestCdcUpsertEmptyShadows(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha", 2: "delta"})
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "", nil) // empty / NULL text
	idx := buildTailIndexLWW(t, base, c, 1000)

	require.Empty(t, queryIDs(t, idx, "alpha"), "emptied row must no longer match its old term")
	require.Equal(t, []any{int64(2)}, queryIDs(t, idx, "delta"), "untouched row still matches")
}

// TestCdcNonemptyThenEmptySameFlush: nonempty upsert then empty upsert of the SAME pk
// in one flush — the final (empty) state wins via in-segment replace, so neither the
// base term nor the intermediate term matches. This case is NOT masked by the
// source-join (the pk still exists), so the engine must get it right itself.
func TestCdcNonemptyThenEmptySameFlush(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha"})
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "beta", nil)
	c.Upsert(int64(1), "", nil) // then emptied
	idx := buildTailIndexLWW(t, base, c, 1000)

	require.Empty(t, queryIDs(t, idx, "alpha"))
	require.Empty(t, queryIDs(t, idx, "beta"), "final empty state shadows the intermediate term")
}

// TestCdcUpsertReplaceAcrossCapacity: a repeated-pk upsert that straddles a sealed
// segment boundary — the newer copy (higher-recency segment) wins via liveness, the
// stale copy in the earlier sealed segment is dead.
func TestCdcUpsertReplaceAcrossCapacity(t *testing.T) {
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "aaa", nil)
	c.Upsert(int64(2), "bbb", nil) // seals seg0 (capacity 2)
	c.Upsert(int64(1), "ccc", nil) // pk 1 again, now in seg1
	idx := buildTailIndexLWW(t, nil, c, 2)

	require.Empty(t, queryIDs(t, idx, "aaa"), "stale copy of pk 1 in the sealed segment is superseded")
	require.Equal(t, []any{int64(2)}, queryIDs(t, idx, "bbb"))
	require.Equal(t, []any{int64(1)}, queryIDs(t, idx, "ccc"), "newest copy of pk 1 wins")
}

// TestCdcUpsertThenDeleteLWW: an UPSERT followed by a DELETE of the same pk in one flush
// resolves to the DELETE (last-writer-wins collapse) — the row is gone from search, no phantom.
// Previously the delete framed below the insert (delete-first) so the engine still returned the
// upserted term; that phantom was masked only by the source-table join, which the covered
// 0-JOIN path removes. The collapse fixes it at the source (no pk is both an insert and a
// delete), so BOTH the base term and the upserted term are gone.
func TestCdcUpsertThenDeleteLWW(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha"})
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "beta", nil)
	c.Delete(int64(1))
	idx := buildTailIndexLWW(t, base, c, 1000)

	require.Empty(t, queryIDs(t, idx, "alpha"), "base term superseded by the delete")
	require.Empty(t, queryIDs(t, idx, "beta"), "upsert-then-delete: the row is gone, no phantom")
}

// TestCdcDeleteThenUpsertLWW: an UPDATE (DELETE then INSERT of the same pk in one flush)
// resolves to the INSERT — the new version is live and supersedes the base. Confirms the LWW
// collapse does not break UPDATE (the case delete-first framing existed to protect).
func TestCdcDeleteThenUpsertLWW(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha"})
	c := NewCdc(int32(types.T_int64))
	c.Delete(int64(1))
	c.Upsert(int64(1), "beta", nil)
	idx := buildTailIndexLWW(t, base, c, 1000)

	require.Empty(t, queryIDs(t, idx, "alpha"), "old version superseded")
	require.Equal(t, []any{int64(1)}, queryIDs(t, idx, "beta"), "update: new version is live")
}

// TestCdcPlainDelete: a straight delete (no preceding same-flush upsert) removes the
// base doc — the delete tombstone (higher recency) shadows the base copy.
func TestCdcPlainDelete(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha", 2: "delta"})
	c := NewCdc(int32(types.T_int64))
	c.Delete(int64(1))
	idx := buildTailIndexLWW(t, base, c, 1000)

	require.Empty(t, queryIDs(t, idx, "alpha"), "deleted doc is gone")
	require.Equal(t, []any{int64(2)}, queryIDs(t, idx, "delta"))
}
