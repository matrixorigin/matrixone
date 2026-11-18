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

package external

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
)

func TestExternalEnumNumericPadding(t *testing.T) {
	for _, identity := range []uint32{types.EnumCollationUTF8Bin, types.EnumCollationGeneralCI} {
		for _, tc := range []struct {
			name    string
			members types.EnumMembers
			input   string
			want    types.Enum
			invalid bool
		}{
			{"plain ordinal", types.EnumMembers{"a", "b"}, "2", 2, false},
			{"padded ordinal", types.EnumMembers{"a", "b"}, "2 ", 2, false},
			{"numeric label before ordinal", types.EnumMembers{"2", "x"}, "2 ", 1, false},
			{"large numeric label before overflow", types.EnumMembers{"65537", "x"}, "65537 ", 1, false},
			{"padded declared label", types.EnumMembers{"2 ", "2", "x"}, "2 ", 1, false},
			{"unpadded declared label", types.EnumMembers{"2 ", "2", "x"}, "2", 2, false},
			{"numeric zero", types.EnumMembers{"a", "b"}, "0", 0, false},
			{"padded numeric zero", types.EnumMembers{"a", "b"}, "0 ", 0, false},
			{"declared zero label", types.EnumMembers{"0", "b"}, "0 ", 1, false},
			{"leading space", types.EnumMembers{"a", "b"}, " 2", 0, true},
			{"nonbreaking space", types.EnumMembers{"a", "b"}, "2\u00a0", 0, true},
			{"overflow", types.EnumMembers{"a", "b"}, "65537 ", 0, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got, err := parseExternalEnum(tc.members, tc.input, identity)
				if tc.invalid {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	}
	_, err := parseExternalEnum(types.EnumMembers{"a", "b"}, "2 ", types.EnumCollationLegacy)
	require.Error(t, err, "legacy LOAD behavior remains unchanged")
	zero, err := parseExternalEnum(types.EnumMembers{"a", "b"}, "0", types.EnumCollationLegacy)
	require.NoError(t, err)
	require.Zero(t, zero, "legacy LOAD retains its pre-existing ordinal-zero input")
}
