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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// A small fulltext corpus (int64 pk -> text), tokenized with the dict-free
// SimpleTokenizer — the same tokenizer used for both docs and queries so phrase
// adjacency is consistent.
func fulltextCorpus(t *testing.T) *Segment {
	t.Helper()
	docs := []Doc{
		{int64(1), []byte("the quick brown fox jumps over the lazy dog")}, // len 9
		{int64(2), []byte("a quick brown fox")},                           // len 4
		{int64(3), []byte("the lazy dog sleeps all day")},                 // len 6
		{int64(4), []byte("quick brown fox jumps high")},                  // len 5
		{int64(5), []byte("the dog and the fox")},                         // len 5
	}
	s, err := BuildSegmentFromDocs("seg", int32(types.T_int64), docs, tokenizer.NewSimpleTokenizer())
	require.NoError(t, err)
	return s
}

func pkslice(rs []Result) []any {
	out := make([]any, len(rs))
	for i, r := range rs {
		out[i] = r.Pk
	}
	return out
}

func query(t *testing.T, s *Segment, q string, algo ScoreAlgo, k int) []any {
	t.Helper()
	rs, err := s.SearchText([]byte(q), tokenizer.NewSimpleTokenizer(), algo, k)
	require.NoError(t, err)
	return pkslice(rs)
}

// TestNLPhraseMatching: MATCH(col) AGAINST('...') in NL mode = exact contiguous
// phrase. Asserts the matched doc SET across representative fulltext cases.
func TestNLPhraseMatching(t *testing.T) {
	s := fulltextCorpus(t)

	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, query(t, s, "quick brown fox", TfIdf, 10))
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, query(t, s, "brown fox", TfIdf, 10))
	require.ElementsMatch(t, []any{int64(1), int64(3)}, query(t, s, "lazy dog", TfIdf, 10))
	require.ElementsMatch(t, []any{int64(1), int64(4)}, query(t, s, "fox jumps", TfIdf, 10))
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4), int64(5)}, query(t, s, "fox", TfIdf, 10))

	// "the fox" is contiguous only in doc 5 ("...the fox"); doc 1 has "the" and
	// "fox" but not adjacent.
	require.Equal(t, []any{int64(5)}, query(t, s, "the fox", TfIdf, 10))

	// non-contiguous phrase — "quick" and "fox" are never adjacent (brown between)
	require.Empty(t, query(t, s, "quick fox", TfIdf, 10))
	// absent term
	require.Empty(t, query(t, s, "dog cat", TfIdf, 10))
	require.Empty(t, query(t, s, "unicorn", TfIdf, 10))
}

// TestTfIdfOrdering: equal-tf hits tie-break by ascending ord (deterministic).
func TestTfIdfOrdering(t *testing.T) {
	s := fulltextCorpus(t)
	// The three docs contain the phrase once each → equal TfIdf, so order is unspecified.
	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, query(t, s, "quick brown fox", TfIdf, 10))
}

// TestBM25Ordering: BM25 length-normalizes, so among equal-tf phrase hits the
// shorter document ranks higher: doc2(len4) > doc4(len5) > doc1(len9).
func TestBM25Ordering(t *testing.T) {
	s := fulltextCorpus(t)
	require.Equal(t, []any{int64(2), int64(4), int64(1)}, query(t, s, "quick brown fox", BM25, 10))
}

// TestTopK caps the result count.
func TestTopK(t *testing.T) {
	s := fulltextCorpus(t)
	require.Len(t, query(t, s, "fox", TfIdf, 2), 2) // 4 hits, top-2
	require.Len(t, query(t, s, "fox", TfIdf, 10), 4)
}

// TestSearchOnLoadedSegment: build → serialize → deserialize → query, so the
// loaded (FST + ordinal postings) path produces identical results.
func TestSearchOnLoadedSegment(t *testing.T) {
	data, err := fulltextCorpus(t).Serialize()
	require.NoError(t, err)
	loaded, err := Deserialize("seg", bytes.NewReader(data))
	require.NoError(t, err)
	t.Cleanup(func() { _ = loaded.dict.Close() })

	require.ElementsMatch(t, []any{int64(1), int64(2), int64(4)}, // equal TfIdf → order unspecified
		query(t, loaded, "quick brown fox", TfIdf, 10))
	require.Equal(t, []any{int64(5)}, query(t, loaded, "the fox", TfIdf, 10))
	require.Empty(t, query(t, loaded, "quick fox", TfIdf, 10))
}
