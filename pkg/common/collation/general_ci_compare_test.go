// Copyright 2026 Matrix Origin
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

package collation

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompareGeneralCIWeights(t *testing.T) {
	// One case-folded weight per character; values tie only when every
	// weight is equal.
	require.Zero(t, CompareGeneralCI([]byte("A"), []byte("a")))
	require.Zero(t, CompareGeneralCI([]byte("å"), []byte("a")))
	require.Positive(t, CompareGeneralCI([]byte("ａ"), []byte("a")))
	require.Zero(t, CompareGeneralCI([]byte("ß"), []byte("s")))
	require.Negative(t, CompareGeneralCI([]byte("ß"), []byte("ss")))
	require.Negative(t, CompareGeneralCI([]byte("中"), []byte("文")))
	require.Zero(t, CompareGeneralCI([]byte("😜"), []byte("😃")))

	// Issue #31174: utf8mb4_general_ci ORDER BY folds case per character, so
	// letters interleave by their folded base letter instead of sorting all
	// uppercase ASCII before every lowercase letter.
	require.Negative(t, CompareGeneralCI([]byte("b"), []byte("C")))
	require.Negative(t, CompareGeneralCI([]byte("a"), []byte("Z")))
	require.Negative(t, CompareGeneralCI([]byte("aaa"), []byte("BBB")))
	require.Zero(t, CompareGeneralCI([]byte("ABC"), []byte("abc")))
}

func TestCompareGeneralCIPadSpace(t *testing.T) {
	require.Zero(t, CompareGeneralCI([]byte("a "), []byte("a")))
	require.Zero(t, CompareGeneralCI([]byte("a  "), []byte("a")))
	// An interior space is significant and its weight sorts below letters.
	require.Negative(t, CompareGeneralCI([]byte("a b"), []byte("ab")))
	// PAD SPACE only ignores trailing spaces; comparing the exhausted key
	// against virtual spaces ties on the interior space and 'b' decides.
	require.Negative(t, CompareGeneralCI([]byte("a"), []byte("a b")))
}

// TestGeneralCISpaceWeightMatchesTable keeps the padding constant in sync
// with the weight table: PAD SPACE compares exhausted keys against the
// table's U+0020 weight.
func TestGeneralCISpaceWeightMatchesTable(t *testing.T) {
	w, size := NextGeneralCIWeight([]byte(" "))
	require.Equal(t, uint32(generalCISpaceWeight), w)
	require.Equal(t, 1, size)
}

// TestCompareGeneralCIPadsExhaustedKeyWithSpace pins MySQL's PAD SPACE
// semantics for an exhausted key (my_strnncollsp_utf8mb4 in
// mysql-server@mysql-8.0.45 strings/ctype-utf8.cc): the missing character
// keeps comparing against the space weight instead of letting the shorter
// value lose outright. Characters weighing less than space (TAB, NUL)
// therefore sort the longer value first, while letters still sort it last.
func TestCompareGeneralCIPadsExhaustedKeyWithSpace(t *testing.T) {
	// TAB (weight 0x09) is lighter than space (0x20).
	require.Positive(t, CompareGeneralCI([]byte("a"), []byte("a\t")))
	require.Negative(t, CompareGeneralCI([]byte("a\t"), []byte("a")))
	// Case folding happens before the padding comparison.
	require.Negative(t, CompareGeneralCI([]byte("A\t"), []byte("a")))
	require.Positive(t, CompareGeneralCI([]byte("a"), []byte("A\t")))
	// NUL (weight 0x00) is lighter than space too.
	require.Positive(t, CompareGeneralCI([]byte("a"), []byte("a\x00")))
	require.Negative(t, CompareGeneralCI([]byte("a\x00"), []byte("a")))
	// A trailing space after the control character is still ignored.
	require.Negative(t, CompareGeneralCI([]byte("a\t "), []byte("a")))
	// Letters weigh more than space, so the longer value still sorts last.
	require.Negative(t, CompareGeneralCI([]byte("a"), []byte("a b")))
	require.Negative(t, CompareGeneralCI([]byte("a"), []byte("ab")))
	// Leading remainder spaces compare equal and fall through to the first
	// non-space character.
	require.Negative(t, CompareGeneralCI([]byte("a"), []byte("a  b")))
}

func TestCompareGeneralCIMalformedUTF8IsTransitive(t *testing.T) {
	values := [][]byte{
		{0x80},
		[]byte("z"),
		[]byte("é"),
		{0xff},
		{0xc3},
		[]byte("A"),
		[]byte("a "),
		[]byte("a\t"),
		[]byte("a"),
	}
	for _, a := range values {
		for _, b := range values {
			require.Equal(t, -CompareGeneralCI(b, a), CompareGeneralCI(a, b))
			for _, c := range values {
				if CompareGeneralCI(a, b) > 0 && CompareGeneralCI(b, c) > 0 {
					require.Positive(t, CompareGeneralCI(a, c),
						"non-transitive ordering for %x > %x > %x", a, b, c)
				}
			}
		}
	}

	// The concrete cycle a raw-byte fallback used to create: invalid bytes
	// carry synthetic weights above the complete valid-rune range.
	require.Positive(t, CompareGeneralCI([]byte{0x80}, []byte("z")))
	require.Positive(t, CompareGeneralCI([]byte("z"), []byte("é")))
	require.Negative(t, CompareGeneralCI([]byte("é"), []byte{0x80}))
	require.Positive(t, CompareGeneralCI([]byte{0xff}, []byte{0xfe}))
}
