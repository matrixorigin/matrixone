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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

// vocabWord is an all-alphabetic term (so the query tokenizer maps it 1:1).
func vocabWord(i int) string { return fmt.Sprintf("word%c%c", 'a'+byte(i/26), 'a'+byte(i%26)) }

// TestPositionFreeFootprint: WithPositionFree() drops the positional payload while
// keeping the FST + docID/tf postings, so the segment serializes smaller and still
// answers bag-of-words (WAND) queries with the same doc set + tf-based ranking.
func TestPositionFreeFootprint(t *testing.T) {
	const (
		ndoc  = 300
		ntok  = 60
		nterm = 40 // words 0..19 appear twice/doc, 20..39 once — every word in every doc
	)
	build := func(opts ...BuildOpt) *Segment {
		b := NewBuilder("idx", int32(types.T_int64), opts...)
		for d := 0; d < ndoc; d++ {
			for i := 0; i < ntok; i++ {
				require.NoError(t, b.Add(vocabWord(i%nterm), int32(i), int64(d)))
			}
		}
		seg, err := b.Finish()
		require.NoError(t, err)
		return seg
	}

	full := build()
	pf := build(WithPositionFree())

	fullBytes, err := full.Serialize()
	require.NoError(t, err)
	pfBytes, err := pf.Serialize()
	require.NoError(t, err)

	saved := 100 * float64(len(fullBytes)-len(pfBytes)) / float64(len(fullBytes))
	t.Logf("serialized: full=%d bytes  position-free=%d bytes  saved=%.1f%%",
		len(fullBytes), len(pfBytes), saved)
	require.Less(t, len(pfBytes), len(fullBytes), "position-free segment must be smaller")

	// The position-free segment round-trips and answers a bag-of-words WAND query
	// (boolean bare term → searchWAND, which never touches positions) with the full
	// doc set — proving docID/tf postings and the FST survive.
	loaded, err := Deserialize("idx", bytes.NewReader(pfBytes))
	require.NoError(t, err)
	idx := NewIndex([]*Segment{loaded}, nil)

	res, err := idx.SearchQuery([]byte(vocabWord(1)), true, ParserDefault, BM25, ndoc+10, nil)
	require.NoError(t, err)
	require.Len(t, res, ndoc, "bag-of-words query must match every doc on the position-free index")

	// Parity: the full (positional) segment returns the identical doc set for the
	// same bag-of-words query.
	fidx := NewIndex([]*Segment{full}, nil)
	fres, err := fidx.SearchQuery([]byte(vocabWord(1)), true, ParserDefault, BM25, ndoc+10, nil)
	require.NoError(t, err)
	require.ElementsMatch(t, resultIDs(fres), resultIDs(res))
}

// TestSearchBagOfWordsCJK: the IN BM25 MODE engine path (SearchBagOfWords) turns a CJK
// query into a disjunction of its gojieba tokens, so it returns the BAG-OF-WORDS
// superset — every doc containing any of 我家/有/三个/人 — whereas NL/phrase mode
// returns only the exact contiguous phrase. (Mirrors the SQL pre-flight.)
func TestSearchBagOfWordsCJK(t *testing.T) {
	docs := []Doc{
		{Pk: int64(1), Text: []byte("我家有三个人")},
		{Pk: int64(2), Text: []byte("三个人都住在我家")},
		{Pk: int64(3), Text: []byte("我家的花园很漂亮")},
		{Pk: int64(4), Text: []byte("教室里有三个人")},
		{Pk: int64(5), Text: []byte("昨天我家有三个人来吃饭")},
	}
	seg, err := BuildSegmentFromDocsParser("zh", int32(types.T_int64), docs, ParserGojieba)
	require.NoError(t, err)
	idx := NewIndex([]*Segment{seg}, nil)

	phrase, err := idx.SearchQuery([]byte("我家有三个人"), false, ParserGojieba, BM25, 10, nil)
	require.NoError(t, err)
	phraseIDs := resultIDs(phrase)

	bow, err := idx.SearchBagOfWords([]byte("我家有三个人"), ParserGojieba, BM25, 10, nil)
	require.NoError(t, err)
	bowIDs := resultIDs(bow)

	t.Logf("phrase(exact)=%v  bag-of-words=%v", phraseIDs, bowIDs)
	require.Contains(t, phraseIDs, int64(1), "doc 1 is the exact phrase")
	require.Subset(t, bowIDs, phraseIDs, "bag-of-words must be a superset of the exact phrase")
	require.Greater(t, len(bowIDs), len(phraseIDs), "bag-of-words must match strictly more docs than the phrase")
}
