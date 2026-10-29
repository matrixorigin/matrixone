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

	"github.com/matrixorigin/matrixone/pkg/fulltext2/tokenfilter"
	"github.com/stretchr/testify/require"
)

// The BM25 bag-of-words query path tokenizes through phraseSlots with the token_filter
// chain, so stopword terms must be dropped from the disjunction — matching the stopwords
// the build dropped. (token_filter applies ONLY in BM25 mode; NL/boolean are rejected at
// plan time for a token_filter index.)
func TestBagOfWordsStopGojieba(t *testing.T) {
	filters, err := tokenfilter.Build([]string{tokenfilter.Stop})
	require.NoError(t, err)

	q, err := buildBagOfWordsQuery("the quick and the fox", ParserGojieba, filters...)
	require.NoError(t, err)
	var terms []string
	for _, c := range q.should {
		terms = append(terms, c.terms...)
	}
	require.NotContains(t, terms, "the")
	require.NotContains(t, terms, "and")
	require.Contains(t, terms, "quick")
	require.Contains(t, terms, "fox")

	// Without the filter the stopwords stay — proving the filter is what drops them.
	q0, err := buildBagOfWordsQuery("the quick and the fox", ParserGojieba)
	require.NoError(t, err)
	var terms0 []string
	for _, c := range q0.should {
		terms0 = append(terms0, c.terms...)
	}
	require.Contains(t, terms0, "the")
	require.Contains(t, terms0, "and")
}
