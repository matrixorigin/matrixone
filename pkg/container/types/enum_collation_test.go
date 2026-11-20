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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnumCollationMemberLookup(t *testing.T) {
	for _, tc := range []struct {
		name, definition, input string
		identity                uint32
		ordinal                 Enum
		truncated               bool
	}{
		{"binary upper", "a,A", "A", EnumCollationUTF8Bin, 2, false},
		{"binary miss", "a,b", "A", EnumCollationUTF8Bin, 0, true},
		{"binary input pad", "a,b", "a ", EnumCollationUTF8Bin, 1, false},
		{"general accent", "a,b", "Á", EnumCollationGeneralCI, 1, false},
		{"general first", "a,A", "A", EnumCollationGeneralCI, 1, false},
		{"unicode nbsp", "a", "a\u00a0", EnumCollationUTF8MB4UnicodeCI, 0, true},
		{"unicode padded input", "a,a\u00a0", "a ", EnumCollationUTF8MB4UnicodeCI, 1, false},
		{"unicode nbsp member", "a,a\u00a0", "a\u00a0", EnumCollationUTF8MB4UnicodeCI, 2, false},
		{"unicode invalid input", "a,b", "\xff", EnumCollationUTF8MB4UnicodeCI, 0, true},
		{"unicode invalid prefix matches empty", ",a", "\xffa", EnumCollationUTF8MB4UnicodeCI, 1, false},
		{"unicode valid prefix matches", "a,b", "a\xff", EnumCollationUTF8MB4UnicodeCI, 1, false},
		{"unicode malformed member matches", "a\xff,b", "a", EnumCollationUTF8MB4UnicodeCI, 1, false},
		{"padded member", "a ,a", "a ", EnumCollationUTF8Bin, 1, false},
		{"unpadded member", "a ,a", "a", EnumCollationUTF8Bin, 2, false},
		{"trimmed input", "a ,a", "a  ", EnumCollationUTF8Bin, 2, false},
		{"numeric", "a,b", "2", EnumCollationUTF8Bin, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, truncated, err := ParseEnumCollatedAssignment(tc.definition, tc.input, tc.identity)
			require.NoError(t, err)
			require.Equal(t, tc.ordinal, got)
			require.Equal(t, tc.truncated, truncated)
			index, err := NewEnumCollatedIndex(strings.Split(tc.definition, ","), tc.identity)
			require.NoError(t, err)
			indexed, indexedTruncated, err := index.Parse(tc.input)
			require.NoError(t, err)
			require.Equal(t, got, indexed)
			require.Equal(t, truncated, indexedTruncated)
		})
	}
}

func TestEnumUnicodeRejectsOversizedDefinitionBeforeInput(t *testing.T) {
	_, truncated, err := ParseEnumCollatedAssignment(strings.Repeat("a,", MaxEnumLen)+"b", "\xff", EnumCollationUTF8MB4UnicodeCI)
	require.Error(t, err)
	require.False(t, truncated)
}
