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

// TestReconstructLiveDocsPositionFree: a position-free segment has no positions, so
// ReconstructLiveDocs(true) must rebuild each doc's terms from tf (each term tf times),
// preserving the term SET + tf so a position-free MERGE keeps bag-of-words results.
func TestReconstructLiveDocsPositionFree(t *testing.T) {
	b := NewBuilder("pf", int32(types.T_int64), WithPositionFree())
	feed(t, b, int64(0), "apple", "pie", "apple") // apple x2, pie x1
	feed(t, b, int64(1), "banana", "bread")
	seg := loadedSeg(t, b)
	idx := NewIndex([]*Segment{seg}, nil)

	docs, err := collectLiveDocs(idx, true)
	require.NoError(t, err)

	got := map[any]map[string]int{}
	for _, d := range docs {
		m := map[string]int{}
		for _, w := range d.Terms {
			m[w]++
		}
		got[normalizeKey(d.Pk)] = m
	}
	require.Equal(t, map[string]int{"apple": 2, "pie": 1}, got[normalizeKey(int64(0))])
	require.Equal(t, map[string]int{"banana": 1, "bread": 1}, got[normalizeKey(int64(1))])

	// Rebuild position-free from the reconstruction → bag-of-words queries still work.
	nb := NewBuilder("merged", int32(types.T_int64), WithPositionFree())
	for _, d := range docs {
		for i, w := range d.Terms {
			require.NoError(t, nb.Add(w, d.Positions[i], d.Pk))
		}
	}
	merged, err := nb.Finish()
	require.NoError(t, err)
	midx := NewIndex([]*Segment{merged}, nil)
	res, err := midx.SearchBagOfWords([]byte("apple"), ParserDefault, BM25, 10, nil)
	require.NoError(t, err)
	require.Equal(t, []any{int64(0)}, resultIDs(res))
}
