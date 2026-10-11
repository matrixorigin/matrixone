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

package types

import (
	"bytes"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/stretchr/testify/require"
)

func TestUnicodeCollationComparisonKeys(t *testing.T) {
	mb4 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB4UnicodeCI)

	// UCA primary weights are case and accent insensitive for these pairs.
	require.Equal(t, 0, CompareStringValues(mb4, []byte("A"), []byte("a")))
	require.Equal(t, 0, CompareStringValues(mb4, []byte("é"), []byte("e")))
	require.Equal(t, 0, CompareStringValues(mb4, []byte("ß"), []byte("ss")))
	require.Less(t, CompareStringValues(mb4, []byte("a"), []byte("b")), 0)
	legacyChar := NewWithCharset(T_char, 4, 0, CharsetUTF8MB4Bin)
	require.Equal(t, 0, CompareStringValues(legacyChar, []byte("a "), []byte("a")))
	require.NotEqual(t, 0, CompareStringOrderValues(legacyChar, []byte("a "), []byte("a")))

	key, err := CollationKey(CharsetUTF8MB3UnicodeCI, nil, []byte("😀"))
	require.ErrorIs(t, err, collation.ErrRepertoire)
	require.Nil(t, key)

	// The supplementary character is valid in the utf8mb4 domain.
	key, err = CollationKey(CharsetUTF8MB4UnicodeCI, nil, []byte("😀"))
	require.NoError(t, err)
	require.NotEmpty(t, key)
	physical, err := PhysicalCollationKey(mb4, []byte("😀"))
	require.NoError(t, err)
	require.Equal(t, byte(0), physical[0])
	require.Equal(t, key, physical[1:])
	_, err = PhysicalCollationKey(NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI), []byte("😀"))
	require.ErrorIs(t, err, collation.ErrRepertoire)

	invalid := []byte{0x5a, 0xff}
	invalidKey := CollationKeyOrOriginal(CharsetUTF8MB3UnicodeCI, invalid)
	require.Equal(t, byte(1), invalidKey[0])
	require.Equal(t, invalid, invalidKey[1:])
	unicode := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI)
	require.Equal(t, bytes.Compare(invalid, []byte{0xff}),
		CompareStringValues(unicode, invalid, []byte{0xff}))

	// Comparison-only callers can still receive an invalid value from an
	// opaque/binary source. Its comparison class must be chosen per value rather
	// than by falling back for both operands. In particular, equivalent valid
	// values must have the same ordering against every third value.
	require.Equal(t, 0, CompareStringValues(unicode, []byte("A"), []byte("a")))
	leftInvalid := CompareStringValues(unicode, []byte("A"), invalid)
	rightInvalid := CompareStringValues(unicode, []byte("a"), invalid)
	require.Equal(t, leftInvalid, rightInvalid)

	// A malformed value whose bytes happen to equal a valid UCA key must stay
	// in a separate comparison class. The comparator and hash/membership key
	// owner therefore agree on both equality and ordering.
	validKey, err := CollationKey(CharsetUTF8MB3UnicodeCI, nil, []byte("A"))
	require.NoError(t, err)
	rawKey := append([]byte(nil), validKey...)
	require.NotEqual(t, 0, CompareStringValues(unicode, rawKey, []byte("A")))
	require.NotEqual(t, 0, CompareStringValues(unicode, []byte("A"), rawKey))
}

func TestMergeStringCharsetKeepsSupplementaryRepertoire(t *testing.T) {
	mb3 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI)
	mb4 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB4UnicodeCI)
	require.Equal(t, CharsetUTF8MB4UnicodeCI,
		MergeStringCharset([]Type{mb4, mb3}, CharsetUTF8))
	require.Equal(t, CharsetUTF8MB4UnicodeCI,
		MergeStringCharset([]Type{mb3, mb4}, CharsetUTF8))
}
