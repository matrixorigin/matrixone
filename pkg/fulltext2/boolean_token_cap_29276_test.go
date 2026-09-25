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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/stretchr/testify/require"
)

// #29276: SimpleTokenizer stores a Latin token capped at MAX_TOKEN_SIZE (23) bytes, but the query
// path (ngramPhraseSlots -> NL / quoted boolean / BM25) looked up the raw, untruncated word and
// missed the stored token. The query token must be truncated the same way the index truncates.
func TestLongTokenTruncationLookup(t *testing.T) {
	b26 := strings.Repeat("b", 26)         // stored as 23 b
	a23 := strings.Repeat("a", 23)         // stored whole
	ya12 := strings.Repeat("я", 12)        // 24 bytes -> stored as 11 я (22 bytes); я is Latin-class (<0x7FF)
	aya12 := "a" + strings.Repeat("я", 12) // 25 bytes -> stored a+10я (21); mixed-width boundary (#29276)
	docs := []Doc{
		{int64(1), []byte(b26)},
		{int64(2), []byte(a23)},
		{int64(3), []byte("hello " + b26)},
		{int64(4), []byte("short")},
		{int64(5), []byte(ya12)},
		{int64(6), []byte(aya12)},
	}
	seg := buildSeg(t, "cap", 0, docs)
	idx := NewIndex([]*Segment{seg}, nil)

	bm25 := func(q, parser string) []any {
		res, err := idx.SearchBagOfWords([]byte(q), parser, BM25, 100, nil)
		require.NoError(t, err)
		return resultIDs(res)
	}

	// "" / "default" / "ngram" all take the SimpleTokenizer / ngramPhraseSlots path.
	for _, parser := range []string{"", ParserDefault, ParserNgram} {
		t.Run("parser="+parser, func(t *testing.T) {
			// The three phraseSlots-based lookups (NL, quoted boolean, BM25) must find the 26-byte word,
			// stored truncated to 23. Each was empty before the fix.
			require.ElementsMatch(t, []any{int64(1), int64(3)}, nlIDs(t, idx, parser, b26),
				"NL must look up the truncated stored token (#29276)")
			require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, parser, `"`+b26+`"`),
				"quoted boolean must look up the truncated stored token")
			require.ElementsMatch(t, []any{int64(1), int64(3)}, bm25(b26, parser),
				"BM25 must look up the truncated stored token")
			// Multi-run phrase: the long run truncates, "hello" stays a hit -> doc 3.
			require.ElementsMatch(t, []any{int64(3)}, nlIDs(t, idx, parser, "hello "+b26))
			// Cyrillic 12 я (24 bytes) stored as 11 я -> the query must truncate to 11 too.
			require.ElementsMatch(t, []any{int64(5)}, nlIDs(t, idx, parser, ya12),
				"a long 2-byte-rune word must be looked up as the truncated stored token")
			// Mixed-width run a+12я (25 bytes): outputLatin over-truncates to a+10я (21), NOT the clean
			// a+11я (23). NL / quoted boolean / BM25 must reproduce that exactly, hitting only doc 6.
			require.ElementsMatch(t, []any{int64(6)}, nlIDs(t, idx, parser, aya12),
				"mixed-width run must truncate like outputLatin (a+10я), not the clean UTF-8 boundary")
			require.ElementsMatch(t, []any{int64(6)}, boolIDs(t, idx, parser, `"`+aya12+`"`),
				"quoted boolean must reproduce the mixed-width stored token")
			require.ElementsMatch(t, []any{int64(6)}, bm25(aya12, parser),
				"BM25 must reproduce the mixed-width stored token")

			// Controls that already worked.
			require.ElementsMatch(t, []any{int64(2)}, nlIDs(t, idx, parser, a23),
				"an exact 23-byte token still hits")
			require.ElementsMatch(t, []any{int64(1), int64(3)}, boolIDs(t, idx, parser, b26),
				"unquoted boolean (already truncates via tokenizeToTerms) still hits")
		})
	}
}

// TestNgramPhraseSlotsLatinMatchesStoredToken pins the invariant: the Latin term ngramPhraseSlots
// builds for a run equals exactly the token SimpleTokenizer stores for that run, across the
// truncation edge cases (ASCII over/at the cap, and 2-byte runes that back up to a rune boundary).
func TestNgramPhraseSlotsLatinMatchesStoredToken(t *testing.T) {
	tok := tokenizer.NewSimpleTokenizer()
	for _, run := range []string{
		strings.Repeat("b", 26), // ASCII over the cap
		strings.Repeat("a", 23), // exactly at the cap
		strings.Repeat("x", 22), // under the cap
		strings.Repeat("я", 12), // 24 bytes, 2-byte runes -> backs up to 22
		strings.Repeat("é", 13), // 26 bytes, 2-byte runes -> backs up to 22
		// Mixed-width: a leading ASCII byte shifts the 2-byte runes so the cap lands one byte PAST a
		// complete rune. outputLatin still drops that rune (a+12я 25B -> a+10я 21B, not the clean 23B);
		// the query truncation must match byte-for-byte (#29276).
		"a" + strings.Repeat("я", 12), // 25 bytes -> stored a+10я (21)
		"a" + strings.Repeat("é", 13), // 27 bytes -> stored a+10é (21)
		"MixedCASEword",               // lowercasing, under the cap
	} {
		terms, err := tokenizeToTerms([]byte(run), tok)
		require.NoError(t, err)
		require.Len(t, terms, 1, "run %q should be one stored token", run)

		slots := ngramPhraseSlots(run)
		require.Len(t, slots, 1, "run %q should be one query slot", run)
		require.Equal(t, terms[0], slots[0].term,
			"query token must equal the stored token for run %q", run)
	}
}
