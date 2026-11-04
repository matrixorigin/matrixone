// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestLeastGreatestCollationCharTieAndNull(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	typ := types.T_char.ToType()
	inputs := []FunctionTestInput{
		NewFunctionTestInput(typ, []string{"a ", "z", ""}, []bool{false, false, true}),
		NewFunctionTestInput(typ, []string{"a", "", "y"}, []bool{false, true, false}),
	}
	for _, id := range []int32{LEAST, GREATEST} {
		fn, err := GetFunctionById(t.Context(), EncodeOverloadID(id, LeastGreatestCollationOverload))
		require.NoError(t, err)
		result := NewFunctionTestResult(typ, false, []string{"a ", "", ""}, []bool{false, true, true})
		fc := NewFunctionTestCase(proc, inputs, result, fn.newOp())
		ok, info := fc.RunAndFree()
		require.True(t, ok, info)
	}
}

func TestLeastGreatestCollationOverloadRejectsNonString(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	input, err := vector.NewConstFixed(types.T_int64.ToType(), int64(1), 1, proc.Mp())
	require.NoError(t, err)
	defer input.Free(proc.Mp())
	for _, id := range []int32{LEAST, GREATEST} {
		for _, inputs := range [][]*vector.Vector{nil, {input}} {
			result, err := RunFunctionDirectly(proc, EncodeOverloadID(id, LeastGreatestCollationOverload), inputs, 1)
			if result != nil {
				result.Free(proc.Mp())
			}
			require.Error(t, err)
		}
	}
}

func TestLeastGreatestCollationOverloadSelection(t *testing.T) {
	native := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	native.CollationVersion = 1
	for _, tc := range []struct {
		name string
		typ  types.Type
		want int32
	}{
		{"native varchar", native, 4},
		{"native text", types.NewWithCharset(types.T_text, types.MaxLongTextLen, 0, types.CharsetUTF8MB4UnicodeCI), 4},
		{"native blob", types.NewWithCharset(types.T_blob, 0, 0, types.CharsetUTF8MB4UnicodeCI), 4},
		{"legacy char trim", types.T_char.ToType(), 4},
		{"legacy varchar", types.T_varchar.ToType(), 0},
		{"opaque binary", types.T_varbinary.ToType(), 0},
		{"numeric", types.T_int64.ToType(), 0},
	} {
		for _, name := range []string{"least", "greatest"} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				typ := tc.typ
				if types.IsUnicodeCollation(typ.Charset) {
					typ.CollationVersion = 1
				}
				resolved, err := GetFunctionByName(t.Context(), name, []types.Type{typ, typ})
				require.NoError(t, err)
				_, overload := DecodeOverloadID(resolved.GetEncodedOverloadID())
				require.Equal(t, tc.want, overload)
			})
		}
	}
}
