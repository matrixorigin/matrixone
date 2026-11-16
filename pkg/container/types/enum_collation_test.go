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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnumCollationMemberLookup(t *testing.T) {
	for _, tc := range []struct {
		name      string
		members   EnumMembers
		input     string
		collation uint32
		want      Enum
		fail      bool
	}{
		{"binary upper", EnumMembers{"a", "A"}, "A", EnumCollationUTF8Bin, 2, false},
		{"binary unmatched", EnumMembers{"a", "b"}, "A", EnumCollationUTF8Bin, 0, true},
		{"binary pad", EnumMembers{"a", "b"}, "a ", EnumCollationUTF8Bin, 1, false},
		{"case and accent", EnumMembers{"a", "b"}, "Á", EnumCollationGeneralCI, 1, false},
		{"first equal", EnumMembers{"a", "A"}, "A", EnumCollationGeneralCI, 1, false},
		{"nbsp not pad", EnumMembers{"a"}, "a\u00a0", EnumCollationGeneralCI, 0, true},
		{"padded label", EnumMembers{"a ", "a"}, "a ", EnumCollationUTF8Bin, 1, false},
		{"unpadded label", EnumMembers{"a ", "a"}, "a", EnumCollationUTF8Bin, 2, false},
		{"trimmed input", EnumMembers{"a ", "a"}, "a  ", EnumCollationUTF8Bin, 2, false},
		{"numeric", EnumMembers{"a", "b"}, "2", EnumCollationUTF8Bin, 2, false},
		{"legacy folded", EnumMembers{"a", "A"}, "A", EnumCollationLegacy, 1, false},
		{"unknown collation", EnumMembers{"a"}, "a", 257, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.members.ParseCollated(tc.input, tc.collation)
			if tc.collation == EnumCollationUTF8Bin || tc.collation == EnumCollationGeneralCI {
				indexed, buildErr := NewEnumCollatedIndex(tc.members, tc.collation)
				require.NoError(t, buildErr)
				indexedGot, indexedErr := indexed.Parse(tc.input)
				require.Equal(t, err == nil, indexedErr == nil)
				require.Equal(t, got, indexedGot)
			}
			if tc.fail {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestCollatedEnumNumericOverflowBeforeNarrowing(t *testing.T) {
	for _, identity := range []uint32{EnumCollationUTF8Bin, EnumCollationGeneralCI} {
		members := EnumMembers{"a", "b"}
		index, err := NewEnumCollatedIndex(members, identity)
		require.NoError(t, err)
		for _, input := range []string{"65535", "65535 ", "65536", "65536 ", "65537", "65537 ", "18446744073709551615", "18446744073709551616 "} {
			_, err := members.ParseCollated(input, identity)
			require.Error(t, err, "identity=%d input=%q", identity, input)
			_, err = index.Parse(input)
			require.Error(t, err, "indexed identity=%d input=%q", identity, input)
		}
		for _, input := range []string{"1", "1 ", "2", "2 "} {
			linear, err := members.ParseCollated(input, identity)
			require.NoError(t, err)
			indexed, err := index.Parse(input)
			require.NoError(t, err)
			require.Equal(t, linear, indexed)
		}
		labelMembers := EnumMembers{"65537", "a"}
		labelIndex, err := NewEnumCollatedIndex(labelMembers, identity)
		require.NoError(t, err)
		linear, err := labelMembers.ParseCollated("65537 ", identity)
		require.NoError(t, err)
		indexed, err := labelIndex.Parse("65537 ")
		require.NoError(t, err)
		require.Equal(t, Enum(1), linear)
		require.Equal(t, linear, indexed)
	}

	// A full legal domain still accepts its largest representable ordinal.
	members := make(EnumMembers, MaxEnumLen)
	for i := range members {
		members[i] = fmt.Sprintf("item_%d", i)
	}
	index, err := NewEnumCollatedIndex(members, EnumCollationUTF8Bin)
	require.NoError(t, err)
	for _, input := range []string{"65535", "65535 "} {
		linear, err := members.ParseCollated(input, EnumCollationUTF8Bin)
		require.NoError(t, err)
		indexed, err := index.Parse(input)
		require.NoError(t, err)
		require.Equal(t, Enum(MaxEnumLen), linear)
		require.Equal(t, linear, indexed)
	}
	_, err = members.ParseCollated("65536 ", EnumCollationUTF8Bin)
	require.Error(t, err)
	_, err = index.Parse("65537 ")
	require.Error(t, err)

	// Identity zero retains the existing legacy conversion behavior.
	legacy := EnumMembers{"a", "b"}
	value, err := legacy.ParseCollated("65537", EnumCollationLegacy)
	require.NoError(t, err)
	require.Equal(t, Enum(1), value)
	_, err = legacy.ParseCollated("65537 ", EnumCollationLegacy)
	require.Error(t, err)
}
