// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fulltext

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type writerTok struct {
	word string
	pos  int32
}

// writerTokens tokenizes body exactly as the index writer (fulltext_index_tokenize) does -- through
// SimpleTokenizer -- returning each stored (word, byte-position). It is the oracle the quoted-phrase
// query must align with: SqlPhrase's positional JOIN only anchors when a phrase child's Position
// equals the byte position at which the writer stored the matching token.
func writerTokens(t *testing.T, body string) []writerTok {
	t.Helper()
	tok := tokenizer.NewSimpleTokenizer()
	out := make([]writerTok, 0, 8)
	for tk, err := range tok.Tokenize([]byte(body)) {
		require.NoError(t, err)
		slen := tk.TokenBytes[0]
		out = append(out, writerTok{word: string(tk.TokenBytes[1 : slen+1]), pos: tk.BytePos})
	}
	return out
}

func findPhrasePattern(ps []*Pattern) *Pattern {
	for _, p := range ps {
		if p.Operator == PHRASE {
			return p
		}
		if c := findPhrasePattern(p.Children); c != nil {
			return c
		}
	}
	return nil
}

// A quoted BOOLEAN phrase must be tokenized from its ORIGINAL bytes so each child lands on the same
// byte position (and carries the same truncated Latin term) the index writer stored. U+0130 (İ) folds
// from 2 bytes to 1, so folding the whole pattern first shifts every following token's byte position
// and truncates a Latin run one byte earlier than the index stored it -- the phrase then anchors
// nowhere in the row (#29271 P2). Each phrase child is checked against the writer oracle: the writer
// must hold a token at the child's exact Position, equal for a TEXT child and prefixed for a STAR.
func TestBooleanPhraseKeepsOriginalBytesForCaseFold29271(t *testing.T) {
	cases := []struct {
		body string
		want int // expected phrase-child count; an omission check — alignment alone passes when a child is silently dropped
	}{
		{"İ中", 2},                          // Latin İ (2B) then one short CJK -> CJK must stay at byte 2, not 1
		{"İstanbul-ankara", 2},             // the second Latin run must stay at byte 10, not 9
		{"İ" + strings.Repeat("a", 23), 1}, // 23-byte Latin cap AFTER the length-changing fold
		{"ABC-DEF", 2},                     // control: ASCII fold + hyphen split, no length change
		{"Ⱥ中", 2},                          // U+023A (Latin, 2B) folds to U+2C65 (>=0x7FF, 3B): class/span from the ORIGINAL, else 中@2 is dropped (#29271 P2)
		{"Ⱥb中", 2},                         // overlap defect is independent of prefix classification: the 中 suffix must survive
	}
	for _, c := range cases {
		body := c.body
		oracle := writerTokens(t, body)
		require.NotEmpty(t, oracle, body)

		ps, err := ParsePattern(`"`+body+`"`, int64(tree.FULLTEXT_BOOLEAN), "")
		require.NoError(t, err, body)
		phrase := findPhrasePattern(ps)
		require.NotNil(t, phrase, body)
		require.Lenf(t, phrase.Children, c.want, "%q: wrong phrase-child count — a required child may be silently dropped", body)

		// Overlapping CJK trigrams share a start position; keep the longest stored token per position.
		byPos := make(map[int32]writerTok, len(oracle))
		for _, o := range oracle {
			if prev, ok := byPos[o.pos]; !ok || len(o.word) > len(prev.word) {
				byPos[o.pos] = o
			}
		}
		for _, ch := range phrase.Children {
			o, ok := byPos[ch.Position]
			require.Truef(t, ok, "%q: phrase child %q at pos %d has no stored token at that position (oracle=%v)",
				body, ch.Text, ch.Position, oracle)
			switch ch.Operator {
			case TEXT:
				require.Equalf(t, o.word, ch.Text, "%q: TEXT child must equal the stored token", body)
			case STAR:
				stem := strings.TrimSuffix(ch.Text, "*")
				require.Truef(t, strings.HasPrefix(o.word, stem),
					"%q: STAR stem %q must be a prefix of stored token %q", body, stem, o.word)
			default:
				t.Fatalf("%q: unexpected phrase child operator %d", body, ch.Operator)
			}
		}
	}
}

// Exact decomposition for the length-changing İ fold, documenting the byte positions the writer
// records. Under the pre-fix whole-pattern lowercasing (İ 2B -> i 1B) these were off by one -- the
// CJK sat at 1 not 2, ankara at 9 not 10, and the 23-byte Latin cap kept one extra `a` -- so none of
// the phrase children matched the stored postings (#29271 P2).
func TestBooleanPhraseCaseFoldExactPositions29271(t *testing.T) {
	cases := []TestCase{
		{pattern: `"İ中"`, expect: "(phrase (text 0 0 i) (* 1 2 中*))"},
		{pattern: `"İstanbul-ankara"`, expect: "(phrase (text 0 0 istanbul) (text 1 10 ankara))"},
		{pattern: `"İ` + strings.Repeat("a", 23) + `"`, expect: "(phrase (text 0 0 i" + strings.Repeat("a", 21) + "))"},
		// ASCII fold and hyphen split are unchanged.
		{pattern: `"ABC-DEF"`, expect: "(phrase (text 0 0 abc) (text 1 4 def))"},
		// U+023A (Ⱥ, Latin <0x7FF, 2B) folds to U+2C65 (ⱥ, >=0x7FF, 3B). Class/span must come from the
		// ORIGINAL token, else ⱥ is misclassified CJK (STAR) and the 中 suffix is dropped as an overlap.
		{pattern: `"Ⱥ中"`, expect: "(phrase (text 0 0 ⱥ) (* 1 2 中*))"},
		{pattern: `"Ⱥb中"`, expect: "(phrase (text 0 0 ⱥb) (* 1 3 中*))"},
		// Standalone single Latin phrase: exact word (not a prefix), so it does not false-match ⱥbc.
		{pattern: `"Ⱥ"`, expect: "(phrase (text 0 0 ⱥ))"},
		// Capacity: U+023A folds to a 3-byte rune, so 8×Ⱥ (16 input bytes, fits) folds to 24 bytes and
		// must be re-capped to the 7-rune (21-byte) token the fixed buffer holds -- it previously stored
		// length 24 and panicked on TokenBytes[1:25] (#29271 P2). 7×Ⱥ folds to exactly 21 bytes and is
		// the boundary control; both decompose to the same stored token and neither panics.
		{pattern: `"` + strings.Repeat("Ⱥ", 8) + `"`, expect: "(phrase (text 0 0 " + strings.Repeat("ⱥ", 7) + "))"},
		{pattern: `"` + strings.Repeat("Ⱥ", 7) + `"`, expect: "(phrase (text 0 0 " + strings.Repeat("ⱥ", 7) + "))"},
	}
	for _, c := range cases {
		got, err := PatternToStringWithPosition(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.NoError(t, err, c.pattern)
		require.Equal(t, c.expect, got, c.pattern)
	}
}

// Non-phrase BOOLEAN word terms still fold to lowercase (the fold moved from the whole pattern to the
// CreatePattern leaf). A TEXT leaf is re-tokenized by GenTextSql and a STAR leaf is looked up by
// truncateStarPrefix (which does not fold), so both must arrive lowercased to hit the index's
// lowercased Latin tokens (#29271 P2 -- guards against a regression from the fold relocation).
func TestBooleanWordTermsStillCaseFold29271(t *testing.T) {
	cases := []TestCase{
		{pattern: `Hello`, expect: "(text 0 hello)"},
		{pattern: `+World`, expect: "(+ (text 0 world))"},
		{pattern: `HEL*`, expect: "(* 0 hel*)"},
	}
	for _, c := range cases {
		got, err := PatternToString(c.pattern, int64(tree.FULLTEXT_BOOLEAN))
		require.NoError(t, err, c.pattern)
		require.Equal(t, c.expect, got, c.pattern)
	}
}
