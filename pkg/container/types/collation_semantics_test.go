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

	key, err := CollationKey(CharsetUTF8MB3UnicodeCI, nil, []byte("😀"))
	require.ErrorIs(t, err, collation.ErrRepertoire)
	require.Nil(t, key)

	// The supplementary character is valid in the utf8mb4 domain.
	key, err = CollationKey(CharsetUTF8MB4UnicodeCI, nil, []byte("😀"))
	require.NoError(t, err)
	require.NotEmpty(t, key)
}

func TestMergeStringCharsetKeepsSupplementaryRepertoire(t *testing.T) {
	mb3 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB3UnicodeCI)
	mb4 := NewWithCharset(T_varchar, 64, 0, CharsetUTF8MB4UnicodeCI)
	require.Equal(t, CharsetUTF8MB4UnicodeCI,
		MergeStringCharset([]Type{mb4, mb3}, CharsetUTF8))
	require.Equal(t, CharsetUTF8MB4UnicodeCI,
		MergeStringCharset([]Type{mb3, mb4}, CharsetUTF8))
}
