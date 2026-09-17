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

// MERGE-compaction engine: ReconstructLiveDocs recovers each live doc's ordered
// terms from the positional postings (no re-tokenize), dropping dead/superseded
// copies — the input the fulltext2_compact TVF rebuilds a fresh base from.
package fulltext2

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// loadedSeg round-trips a built segment through serialize so reconstruction runs
// over the FST/loaded path the storage loader produces (not the build-side map).
func loadedSeg(t *testing.T, b *Builder) *Segment {
	seg, err := b.Finish()
	require.NoError(t, err)
	blob, err := seg.Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize(seg.Id, bytes.NewReader(blob))
	require.NoError(t, err)
	return loaded
}

// collectLiveDocs drains the ReconstructLiveDocs iterator into a slice for
// assertions (the production consumer streams it one doc at a time).
func collectLiveDocs(idx *Index, positionFree bool) ([]TokenizedDoc, error) {
	out := make([]TokenizedDoc, 0, idx.globalN)
	for d, err := range idx.ReconstructLiveDocs(positionFree) {
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}

func TestReconstructLiveDocs(t *testing.T) {
	// base (recency 0): doc 0 "apple pie", doc 1 "banana bread".
	bb := NewBuilder("base", int32(types.T_int64))
	feed(t, bb, int64(0), "apple", "pie")
	feed(t, bb, int64(1), "banana", "bread")
	base := loadedSeg(t, bb)
	base.Recency = 0

	// tail (recency 100): insert doc 2, update doc 1's text (supersedes base copy).
	tb := NewBuilder("tail", int32(types.T_int64))
	feed(t, tb, int64(2), "cherry", "cake")
	feed(t, tb, int64(1), "blueberry", "muffin")
	tail := loadedSeg(t, tb)
	tail.Recency = 100

	// delete doc 0 at recency 100.
	idx := NewIndex([]*Segment{base, tail}, map[any]int64{normalizeKey(int64(0)): 100})

	docs, err := collectLiveDocs(idx, false)
	require.NoError(t, err)

	got := map[any][]string{}
	for _, d := range docs {
		got[normalizeKey(d.Pk)] = d.Terms
	}
	// doc 0 deleted, doc 1 = updated tail copy, doc 2 inserted. Term ORDER preserved.
	require.NotContains(t, got, normalizeKey(int64(0)))
	require.Equal(t, []string{"blueberry", "muffin"}, got[normalizeKey(int64(1))])
	require.Equal(t, []string{"cherry", "cake"}, got[normalizeKey(int64(2))])
	require.Len(t, docs, 2)

	// Rebuilding from the reconstruction gives an index with identical results.
	nb := NewBuilder("merged", int32(types.T_int64))
	for _, d := range docs {
		for i, w := range d.Terms {
			require.NoError(t, nb.Add(w, d.Positions[i], d.Pk))
		}
	}
	merged, err := nb.Finish()
	require.NoError(t, err)
	midx := NewIndex([]*Segment{merged}, nil)
	q := func(w string) []any {
		res, qerr := midx.SearchQuery([]byte(w), false, ParserDefault, BM25, 100, nil)
		require.NoError(t, qerr)
		return resultIDs(res)
	}
	require.Empty(t, q("apple"))
	require.Empty(t, q("banana"))
	require.Equal(t, []any{int64(1)}, q("blueberry"))
	require.Equal(t, []any{int64(2)}, q("cherry"))
}

// TestReconstructLiveDocsDropsZeroTermShadow verifies the reconstruction
// boundary used by MERGE/REBUILD: an all-empty upsert remains visible as a
// zero-term shadow while the tail is queried, and the reconstructed live-doc
// stream drops it before fresh base segments are built. The SQL/BVT lifecycle
// covers the production CompactSegments entry point.
func TestReconstructLiveDocsDropsZeroTermShadow(t *testing.T) {
	base := mkBase(t, map[int64]string{1: "alpha"})
	c := NewCdc(int32(types.T_int64))
	c.Upsert(int64(1), "", nil)
	dirty := buildTailIndexLWW(t, base, c, 1)

	require.Empty(t, queryIDs(t, dirty, "alpha"), "the empty upsert shadows the base before rebuild")
	docs, err := collectLiveDocs(dirty, false)
	require.NoError(t, err)
	require.Len(t, docs, 1, "the live zero-term shadow still carries the source PK")
	require.Equal(t, int64(1), docs[0].Pk)
	require.Empty(t, docs[0].Terms)

	// CompactSegments intentionally does not persist zero-term documents: once
	// the old base is replaced, there is no posting left for the shadow to hide.
	rebuilt := make([]*Segment, 0, len(docs))
	for _, d := range docs {
		if len(d.Terms) == 0 {
			continue
		}
		b := NewBuilder("rebuilt", int32(types.T_int64))
		for i, term := range d.Terms {
			require.NoError(t, b.Add(term, d.Positions[i], d.Pk))
		}
		seg, ferr := b.Finish()
		require.NoError(t, ferr)
		rebuilt = append(rebuilt, seg)
	}
	require.Empty(t, rebuilt, "a zero-term-only rebuild must not retain a stale base")
	require.Empty(t, queryIDs(t, NewIndex(rebuilt, nil), "alpha"))
}
