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
	"github.com/matrixorigin/matrixone/pkg/fulltext2/tokenfilter"
	"github.com/stretchr/testify/require"
)

// TestTokenFilterMixedWriterReader is the old/new-writer/reader closure for the
// token_filter capability gate: it proves WHY a below-capability (non-stripping)
// writer must never maintain a token_filter index, i.e. why the write-side epoch is a
// hard lifetime gate and not a one-time CREATE-time check.
//
// Two writers build the SAME two documents:
//   - the "new" (supporting) writer strips the stopword "the" before indexing;
//   - the "old" (pre-analyzer) writer indexes every token, leaking "the" postings.
//
// A supporting reader (StopFilter on the query side) reads each build and shows:
//
//	(1) READ side is robust for a stopword query: querying "the" returns empty against
//	    BOTH builds, because the reader strips the query term, so a leaked "the" posting
//	    is unreachable (scoring consults only the query's own terms).
//	(2) WRITE side corrupts ordinary-query ranking: a document's recorded length counts
//	    every indexed token, so the leaked "the" postings enlarge doc 0's length. That
//	    length feeds BM25 length-normalization, so an ordinary query ("alpha", which is
//	    NOT a stopword and appears once in each doc) ranks the two docs EQUALLY on the
//	    stripped build but UNEQUALLY on the leaked build. The corruption reaches a query
//	    that never mentions a stopword — the corpus-wide effect the gate must prevent.
func TestTokenFilterMixedWriterReader(t *testing.T) {
	const stopword = "the"
	set, err := tokenfilter.BuiltinStopwords()
	require.NoError(t, err)
	_, isStop := set[stopword]
	require.True(t, isStop, "%q must be a builtin stopword for this test to mean anything", stopword)

	stop, err := tokenfilter.Build([]string{tokenfilter.Stop})
	require.NoError(t, err)

	// Corpus of 10 docs. docs 2..9 hold a single non-stopword "filler" and are IDENTICAL
	// on both builds (no stopwords to leak), which keeps "alpha" in a minority (df=2,
	// N=10 -> positive idf, non-zero scores) and holds the baseline length steady. Only
	// doc 0 differs: the stripping writer indexes "alpha"; the non-stripping writer also
	// indexes three leaked stopwords, enlarging doc 0's recorded length. doc 1 holds a
	// bare "alpha" on both builds, so it is the fixed reference the ranking is read against.
	buildIndex := func(strip bool) *Index {
		b := NewBuilder("idx", int32(types.T_int64), WithPositionFree())
		if strip {
			feed(t, b, int64(0), "alpha")
		} else {
			feed(t, b, int64(0), stopword, stopword, stopword, "alpha")
		}
		feed(t, b, int64(1), "alpha")
		for pk := 2; pk < 10; pk++ {
			feed(t, b, int64(pk), "filler")
		}
		segs, ferr := b.FinishSegments(0)
		require.NoError(t, ferr)
		idx := NewIndex(segs, nil)
		idx.tokenFilters = stop // supporting reader strips the query side
		return idx
	}
	stripped := buildIndex(true)
	leaked := buildIndex(false)

	score := func(idx *Index, q string) map[any]float32 {
		res, serr := idx.SearchBagOfWords([]byte(q), ParserGojieba, BM25, 10, nil, 0)
		require.NoError(t, serr)
		m := make(map[any]float32, len(res))
		for _, r := range res {
			m[normalizeKey(r.Pk)] = r.Score
		}
		return m
	}

	// (1) READ robustness: a stopword query is empty on BOTH builds. The reader strips
	// "the" from the query, so the leaked posting in doc 0 is never reached.
	require.Empty(t, score(stripped, stopword), "stopword query empty on stripped build")
	require.Empty(t, score(leaked, stopword), "stopword query empty on leaked build: leaked posting is unreachable")

	// (2) WRITE corruption: an ordinary (non-stopword) query ranks the two docs equally
	// on the consistent stripped build but unequally on the leaked build, because the
	// leaked stopword tokens enlarged doc 0's recorded length.
	sStrip := score(stripped, "alpha")
	require.Len(t, sStrip, 2, "alpha is in docs 0 and 1")
	require.Positive(t, sStrip[int64(1)], "alpha has positive idf (minority term), so a non-zero score")
	require.Equal(t, sStrip[int64(0)], sStrip[int64(1)],
		"stripped build: equal length -> identical BM25 score for the same term")

	sLeak := score(leaked, "alpha")
	require.Len(t, sLeak, 2, "alpha is in docs 0 and 1")
	require.Greater(t, sLeak[int64(1)], sLeak[int64(0)],
		"leaked build: doc 0's inflated length lowers its BM25 score -> ranking differs from the stripped build")
}
