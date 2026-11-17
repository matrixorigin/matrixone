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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegisteredBinaryAssignmentPhysicalReplay(t *testing.T) {
	binary4 := types.New(types.T_binary, 4, 0)
	for _, tc := range []struct {
		name           string
		source, target types.Type
		input          string
		null, reject   bool
		want           string
	}{
		{name: "short physical", source: binary4, target: binary4, input: "AB", want: "AB"},
		{name: "empty physical", source: binary4, target: binary4},
		{name: "NULL physical", source: binary4, target: binary4, null: true},
		{name: "physical boundary", source: binary4, target: binary4, input: "AB\x00\xff", want: "AB\x00\xff"},
		{name: "invalid physical capacity", source: binary4, target: binary4, input: "ABCDE", reject: true},
		{name: "VARCHAR normalization", source: types.T_varchar.ToType(), target: binary4, input: "AB", want: "AB\x00\x00"},
		{name: "VARBINARY normalization", source: types.New(types.T_varbinary, 4, 0), target: binary4, input: "AB", want: "AB\x00\x00"},
		{name: "BINARY narrowing normalization", source: types.New(types.T_binary, 8, 0), target: binary4, input: "AB", want: "AB\x00\x00"},
	} {
		for _, name := range []string{"cast_assign", "cast_ignore"} {
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				proc := testutil.NewProcess(t)
				proc.SetResolveVariableFunc(func(string, bool, bool) (any, error) { return "STRICT_TRANS_TABLES", nil })
				source := vector.NewVec(tc.source)
				defer source.Free(proc.Mp())
				require.NoError(t, vector.AppendBytes(source, []byte(tc.input), tc.null, proc.Mp()))
				target := vector.NewConstNull(tc.target, 1, proc.Mp())
				defer target.Free(proc.Mp())
				fn, err := GetFunctionByName(proc.Ctx, name, []types.Type{tc.source, tc.target})
				require.NoError(t, err)
				value, err := RunFunctionDirectly(proc, fn.GetEncodedOverloadID(), []*vector.Vector{source, target}, 1)
				if tc.reject && name == "cast_assign" {
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrCastWidthExceeded))
					return
				}
				require.NoError(t, err)
				defer value.Free(proc.Mp())
				got, null := vector.GenerateFunctionStrParameter(value).GetStrValue(0)
				require.Equal(t, tc.null, null)
				if null {
					return
				}
				want := tc.want
				if tc.reject {
					want = "ABCD"
				}
				require.Equal(t, []byte(want), got)
			})
		}
	}
}
