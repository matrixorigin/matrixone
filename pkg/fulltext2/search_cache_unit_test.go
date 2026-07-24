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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

func newSearchProc(t *testing.T) *sqlexec.SqlProcess {
	return sqlexec.NewSqlProcess(testutil.NewProc(t))
}

// loadedSearch builds a Fulltext2Search whose Index is assembled in memory (via the
// serialize→deserialize loadedSeg path), bypassing Load/DB.
func loadedSearch(t *testing.T) *Fulltext2Search {
	bb := NewBuilder("base", int32(types.T_int64))
	feed(t, bb, int64(0), "quick", "brown", "fox")
	feed(t, bb, int64(1), "quick", "brown", "dog")
	feed(t, bb, int64(2), "lazy", "fox", "sleeps")
	seg := loadedSeg(t, bb)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store", Parser: ParserDefault})
	s.idx = NewIndex([]*Segment{seg}, nil)
	s.loaded = true
	return s
}

func TestFulltext2SearchNewAndUnloaded(t *testing.T) {
	proc := newSearchProc(t)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store"})
	require.Equal(t, "__store", s.cfg.IndexTable)
	require.False(t, s.loaded)

	// Search before Load → "not loaded".
	_, _, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox")}, vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "not loaded")

	// SearchFloat32 is unsupported.
	require.ErrorContains(t, s.SearchFloat32(proc, nil, vectorindex.RuntimeConfig{}, nil, nil), "not supported")
}

func TestFulltext2SearchEmptyIndex(t *testing.T) {
	proc := newSearchProc(t)
	s := NewFulltext2Search(TableConfig{IndexTable: "__store", Parser: ParserDefault})
	s.idx = NewIndex(nil, nil) // loaded but doc-less
	s.loaded = true

	keys, dists, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox")}, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, dists)
}

func TestFulltext2SearchInvalidPayload(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	_, _, err := s.Search(proc, "not a query", vectorindex.RuntimeConfig{})
	require.ErrorContains(t, err, "invalid query payload")
}

func TestFulltext2SearchTopK(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	// single-term NL query with a pushed LIMIT.
	keys, dists, err := s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	ks, ok := keys.([]any)
	require.True(t, ok)
	require.Len(t, dists, len(ks))
	require.NotEmpty(t, ks) // "fox" hits docs 0 and 2

	// k <= 0 (no pushed LIMIT) falls back to NumDocs.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 0})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))

	// an absurd LIMIT past MaxInt32 is clamped, not wrapped negative.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("fox"), Algo: BM25},
		vectorindex.RuntimeConfig{Limit: uint(math.MaxInt32) + 100})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))

	// bag-of-words (IN BM25 MODE) path.
	keys, _, err = s.Search(proc, Fulltext2Query{Pattern: []byte("quick fox"), BagOfWords: true, Algo: BM25},
		vectorindex.RuntimeConfig{Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, keys.([]any))
}

func TestFulltext2SearchStreamingEmit(t *testing.T) {
	proc := newSearchProc(t)
	s := loadedSearch(t)
	defer s.Destroy()

	// Emit set + no pushed LIMIT → streaming: results handed off via Emit, empty return.
	for _, bagOfWords := range []bool{false, true} {
		emitted := 0
		emit := func(k *vectorindex.ColumnBuffer, _ []float64) error {
			emitted += k.N
			return nil
		}
		keys, dists, err := s.Search(proc,
			Fulltext2Query{Pattern: []byte("fox"), BagOfWords: bagOfWords, Algo: BM25},
			vectorindex.RuntimeConfig{Emit: emit})
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Empty(t, dists)
		require.Positive(t, emitted, "bagOfWords=%v should emit docs", bagOfWords)
	}
}

func TestFulltext2SearchDestroy(t *testing.T) {
	s := loadedSearch(t)

	// The cached config is immutable for the entry's lifetime (no UpdateConfig hook —
	// a config change evicts the entry), so Search is pure-read; here we just pin that
	// the constructed cfg is what Load queries with and that Destroy tears down cleanly.
	require.Equal(t, ParserDefault, s.cfg.Parser)

	// Destroy frees and clears the loaded index.
	s.Destroy()
	require.Nil(t, s.idx)
	require.False(t, s.loaded)
}
