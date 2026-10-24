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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestVersionedCollationConsumers(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, charset := range []uint8{types.CharsetUTF8, types.CharsetUTF8MB4Bin} {
		for _, revision := range []uint8{0, 1} {
			typ := types.NewWithCharset(types.T_varchar, 16, 0, charset)
			typ.CollationVersion = revision
			for _, name := range []string{"=", "<=>", "in", "least", "greatest"} {
				resolved, err := GetFunctionByName(t.Context(), name, []types.Type{typ, typ})
				require.NoError(t, err)
				var inputs []FunctionTestInput
				var result FunctionTestResult
				switch name {
				case "=", "<=>":
					inputs = []FunctionTestInput{
						NewFunctionTestInput(typ, []string{"A", "a ", "é"}, nil),
						NewFunctionTestInput(typ, []string{"a", "a", "e"}, nil),
					}
					ci := revision == 1 && charset == types.CharsetUTF8
					result = NewFunctionTestResult(types.T_bool.ToType(), false, []bool{ci, revision == 1, ci}, nil)
				case "in":
					inputs = []FunctionTestInput{
						NewFunctionTestInput(typ, []string{"A", "a ", "b"}, nil),
						NewFunctionTestInput(types.T_varchar.ToType(), []string{"a"}, nil),
					}
					result = NewFunctionTestResult(types.T_bool.ToType(), false,
						[]bool{revision == 1 && charset == types.CharsetUTF8, revision == 1, false}, nil)
				case "least", "greatest":
					inputs = []FunctionTestInput{
						NewFunctionTestInput(typ, []string{"a"}, nil),
						NewFunctionTestInput(typ, []string{"B"}, nil),
					}
					want := "B"
					if (name == "least") == (revision == 1 && charset == types.CharsetUTF8) {
						want = "a"
					}
					result = NewFunctionTestResult(typ, false, []string{want}, nil)
				}
				fn, err := GetFunctionById(t.Context(), resolved.GetEncodedOverloadID())
				require.NoError(t, err)
				tc := NewFunctionTestCase(proc, inputs, result, fn.newOp())
				ok, info := tc.RunAndFree()
				require.True(t, ok, "charset=%d revision=%d name=%s: %s", charset, revision, name, info)
			}
			legacy := typ
			legacy.CollationVersion = 0
			cast, left, right := comparisonTypeCastRule(typ, legacy)
			require.Equal(t, revision == 1, cast)
			require.Equal(t, revision, left.CollationVersion)
			require.Equal(t, revision, right.CollationVersion)
			target := typ.PlanType()
			require.Equal(t, revision == 0, CanUseZoneMapComparison(EncodeOverloadID(EQUAL, 0),
				[]*plan.Expr{{Typ: target}, {Typ: target}}))
		}
	}
}

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
