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
// These unit tests use the token index as a synthetic byte position — self-consistent
// with phr() below, which builds consecutive-offset phrase slots. (Real byte positions
// are exercised by the text-based / CJK tests via BuildSegmentFromDocsParser + SearchQuery.)
func feed(t *testing.T, b *Builder, pk any, words ...string) {
	pos := int32(0)
	for _, w := range words {
		require.NoError(t, b.Add(w, pos, pk))
		pos += int32(len(w)) + 1 // byte position of single-space-joined words
	}
}

// phr builds phrase slots for a contiguous term list at the byte offsets a
// single-space-joined query text would tokenize to — matching feed's positions and the
// production query path (tokenizing "a b" → a@0, b@len(a)+1).
func phr(terms ...string) []phraseSlot {
	s := make([]phraseSlot, len(terms))
	pos := int32(0)
	for i, t := range terms {
		s[i] = phraseSlot{term: t, off: pos}
		pos += int32(len(t)) + 1
	}
	return s
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
	require.ElementsMatch(t, []any{int64(0), int64(1)}, resultIDs(idx.SearchPhrase(phr("brown", "fox"), BM25, 10, nil)))
	// single term.
	require.ElementsMatch(t, []any{int64(0), int64(2)}, resultIDs(idx.SearchPhrase(phr("quick"), BM25, 10, nil)))
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
	got := resultIDs(idx.SearchPhrase(phr("brown"), BM25, 100, nil))
	require.Len(t, got, 10) // all docs across the 4 bases
}

// TestBuilderEmpty: no Add → no segments.
func TestBuilderEmpty(t *testing.T) {
	b := NewBuilder("idx", int32(types.T_int64))
	segs, err := b.FinishSegments(0)
	require.NoError(t, err)
	require.Nil(t, segs)
}
