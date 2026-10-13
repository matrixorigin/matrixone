// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCollationMetadataStringShapeConversions(t *testing.T) {
	for _, tc := range []struct {
		oid     types.T
		width   int32
		charset uint8
	}{
		{types.T_char, 12, types.CharsetUTF8MB4Bin},
		{types.T_varchar, 12, types.CharsetUTF8MB4Bin},
		{types.T_text, types.MaxTinyTextLen, types.CharsetUTF8MB4Bin},
		{types.T_text, types.MaxMediumTextLen, types.CharsetUTF8MB4Bin},
		{types.T_varbinary, 12, types.CharsetBinary},
	} {
		source := types.NewWithCharset(tc.oid, tc.width, 0, tc.charset)
		source.CollationVersion = 1
		resolved, err := GetFunctionByName(t.Context(), "upper", []types.Type{source})
		require.NoError(t, err)
		result := resolved.GetReturnType()
		require.Equal(t, source.Charset, result.Charset)
		require.Equal(t, source.CollationVersion, result.CollationVersion)
	}
	source := types.NewWithCharset(types.T_char, 12, 0, types.CharsetUTF8MB4Bin)
	source.CollationVersion = 1
	match := fixedTypeMatch([]overload{{overloadId: 0, args: []types.T{types.T_varchar}}}, []types.Type{source})
	require.Equal(t, succeedWithCast, match.status)
	require.Len(t, match.finalType, 1)
	require.Equal(t, source.Charset, match.finalType[0].Charset)
	require.Equal(t, source.CollationVersion, match.finalType[0].CollationVersion)
}
