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

// Builder API parity with bm25.wand.Builder: NewBuilder(id, pkType) / Add(word, pk)
// / Finish / FinishSegments(capacity), so the two engines can share a core later.
// fulltext2's builder additionally records token POSITIONS (Add order within a doc).
package fulltext2

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// feed adds a doc's tokens in order (mirrors the fulltext2_create per-token Add loop).
func feed(t *testing.T, b *Builder, pk any, words ...string) {
	for _, w := range words {
		require.NoError(t, b.Add(w, pk))
	}
}

// TestBuilderSingleSegment: Finish() builds one positional segment; phrase and
// term queries resolve against it.
func TestBuilderSingleSegment(t *testing.T) {
	b := NewBuilder("idx", int32(types.T_int64))
	feed(t, b, int64(0), "quick", "brown", "fox")
	feed(t, b, int64(1), "brown", "fox", "jumps")
	feed(t, b, int64(2), "quick", "red", "fox")
	require.Equal(t, 3, b.NumDocs())

	seg, err := b.Finish()
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	// exact phrase "brown fox" is contiguous in docs 0 and 1, not 2.
	require.ElementsMatch(t, []any{int64(0), int64(1)}, resultIDs(idx.SearchPhrase([]string{"brown", "fox"}, BM25, 10)))
	// single term.
	require.ElementsMatch(t, []any{int64(0), int64(2)}, resultIDs(idx.SearchPhrase([]string{"quick"}, BM25, 10)))
}

// TestBuilderCapacitySplit: FinishSegments(capacity) splits by contiguous doc-ord
// range into ceil(n/capacity) segments; the merged Index still finds every doc.
func TestBuilderCapacitySplit(t *testing.T) {
	b := NewBuilder("idx", int32(types.T_int64))
	for i := 0; i < 10; i++ {
		feed(t, b, int64(i), "common", "brown")
	}
	segs, err := b.FinishSegments(3) // 10 docs / 3 => 4 segments
	require.NoError(t, err)
	require.Len(t, segs, 4)
	require.Equal(t, int64(3), segs[0].N)
	require.Equal(t, int64(1), segs[3].N) // remainder

	idx := NewIndex(segs, nil)
	got := resultIDs(idx.SearchPhrase([]string{"brown"}, BM25, 100))
	require.Len(t, got, 10) // all docs across the 4 bases
}

// TestBuilderEmpty: no Add → no segments.
func TestBuilderEmpty(t *testing.T) {
	b := NewBuilder("idx", int32(types.T_int64))
	segs, err := b.FinishSegments(0)
	require.NoError(t, err)
	require.Nil(t, segs)
}
