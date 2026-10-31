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

package tokenfilter

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// collect flattens a tokenizer stream into (word, bytePos) pairs, stopping on error.
func collect(t *testing.T, tok tokenizer.Tokenizer, input string) ([]string, []int32) {
	var words []string
	var pos []int32
	for tk, err := range tok.Tokenize([]byte(input)) {
		require.NoError(t, err)
		slen := tk.TokenBytes[0]
		words = append(words, string(tk.TokenBytes[1:slen+1]))
		pos = append(pos, tk.BytePos)
	}
	return words, pos
}

func TestStopFilterDropsAndPreservesBytePos(t *testing.T) {
	set := map[string]struct{}{"the": {}}
	a := NewAnalyzer(tokenizer.NewSimpleTokenizer(), NewStopFilter(set))

	// "the quick the fox": the@0, quick@4, the@10, fox@14. Dropping "the" must keep
	// quick@4 and fox@14 (byte offsets unchanged) so the phrase gap is preserved.
	words, pos := collect(t, a, "the quick the fox")
	require.Equal(t, []string{"quick", "fox"}, words)
	require.Equal(t, []int32{4, 14}, pos)
}

func TestStopFilterCaseFolded(t *testing.T) {
	// SimpleTokenizer folds Latin to lower case, so an upper-case "The" is matched
	// by the lower-case stopword entry — index-time and query-time agree.
	set := map[string]struct{}{"the": {}}
	a := NewAnalyzer(tokenizer.NewSimpleTokenizer(), NewStopFilter(set))
	words, _ := collect(t, a, "The Quick THE Fox")
	require.Equal(t, []string{"quick", "fox"}, words)
}

func TestStopFilterAllDropped(t *testing.T) {
	set := map[string]struct{}{"a": {}, "the": {}}
	a := NewAnalyzer(tokenizer.NewSimpleTokenizer(), NewStopFilter(set))
	words, _ := collect(t, a, "a the a the")
	require.Empty(t, words)
}

func TestAnalyzerNoFilterIsPassthrough(t *testing.T) {
	a := NewAnalyzer(tokenizer.NewSimpleTokenizer())
	base, basePos := collect(t, tokenizer.NewSimpleTokenizer(), "quick brown fox")
	got, gotPos := collect(t, a, "quick brown fox")
	require.Equal(t, base, got)
	require.Equal(t, basePos, gotPos)
}

func TestApplyNoFilterReturnsSame(t *testing.T) {
	base := tokenizer.NewSimpleTokenizer()
	require.Equal(t, tokenizer.Tokenizer(base), Apply(base, nil))
}

func TestParseStopwords(t *testing.T) {
	set := ParseStopwords([]byte("a\r\nb\n\n  c  \nthe\n"))
	require.Equal(t, map[string]struct{}{"a": {}, "b": {}, "c": {}, "the": {}}, set)
}

func TestParseNames(t *testing.T) {
	require.Nil(t, ParseNames(""))
	require.Nil(t, ParseNames(" , ,"))
	require.Equal(t, []string{"stop"}, ParseNames("STOP"))
	require.Equal(t, []string{"stop", "synonym"}, ParseNames(" stop , Synonym "))
}

func TestBuildRejectsUnknown(t *testing.T) {
	_, err := Build([]string{"bogus"})
	require.Error(t, err)
}

func TestBuildStop(t *testing.T) {
	filters, err := Build([]string{"stop"})
	require.NoError(t, err)
	require.Len(t, filters, 1)

	// End-to-end: a "stop" filter built from the builtin list drops a real English
	// stopword and keeps content words.
	a := NewAnalyzer(tokenizer.NewSimpleTokenizer(), filters...)
	words, _ := collect(t, a, "the quick brown fox and a dog")
	require.NotContains(t, words, "the")
	require.NotContains(t, words, "and")
	require.NotContains(t, words, "a")
	require.Contains(t, words, "quick")
	require.Contains(t, words, "fox")
}

func TestBuiltinStopwords(t *testing.T) {
	set, err := BuiltinStopwords()
	require.NoError(t, err)
	require.NotEmpty(t, set)
	require.Contains(t, set, "the")
	require.Contains(t, set, "and")
}
