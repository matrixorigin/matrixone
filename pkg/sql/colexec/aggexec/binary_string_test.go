// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestAggregatesPropagateBinaryStringMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte{0xe4, 0xbd, 0xa0}, false, mp))
	input.SetIsBinaryString(true)
	defer input.Free(mp)

	tests := []struct {
		name string
		new  func() AggFuncExec
	}{
		{
			name: "min",
			new: func() AggFuncExec {
				return makeMinMaxExec(mp, AggIdOfMin, true, types.T_varchar.ToType())
			},
		},
		{
			name: "any_value",
			new: func() AggFuncExec {
				return makeAnyValueExec(mp, AggIdOfAny, types.T_varchar.ToType())
			},
		},
		{
			name: "group_concat",
			new: func() AggFuncExec {
				return newGroupConcatExec(mp, multiAggInfo{
					aggID:     AggIdOfGroupConcat,
					argTypes:  []types.Type{types.T_varchar.ToType()},
					retType:   types.T_text.ToType(),
					emptyNull: true,
				}, ",")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := test.new()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))

			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.True(t, result[0].GetIsBinaryString())
			result[0].Free(mp)
			exec.Free()
		})
	}
}

func TestAggregatesRecognizeMaterializedBinaryStringType(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	input := vector.NewVec(types.T_varbinary.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte{0xe4, 0xbd, 0xa0}, false, mp))
	defer input.Free(mp)

	tests := []struct {
		name string
		new  func() AggFuncExec
	}{
		{
			name: "min",
			new: func() AggFuncExec {
				return makeMinMaxExec(mp, AggIdOfMin, true, types.T_varbinary.ToType())
			},
		},
		{
			name: "any_value",
			new: func() AggFuncExec {
				return makeAnyValueExec(mp, AggIdOfAny, types.T_varbinary.ToType())
			},
		},
		{
			name: "group_concat",
			new: func() AggFuncExec {
				return newGroupConcatExec(mp, multiAggInfo{
					aggID:     AggIdOfGroupConcat,
					argTypes:  []types.Type{types.T_varbinary.ToType()},
					retType:   types.T_text.ToType(),
					emptyNull: true,
				}, ",")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			exec := test.new()
			require.NoError(t, exec.GroupGrow(1))
			require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))

			result, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, result, 1)
			require.True(t, result[0].GetIsBinaryString())
			result[0].Free(mp)
			exec.Free()
		})
	}
}

func TestValueWindowsPropagateBinaryStringMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mp.Free(nil)

	input := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(input, []byte{0xe4, 0xbd, 0xa0}, false, mp))
	input.SetIsBinaryString(true)
	defer input.Free(mp)

	for _, aggID := range []int64{
		WinIdOfLag,
		WinIdOfLead,
		WinIdOfFirstValue,
		WinIdOfLastValue,
		WinIdOfNthValue,
	} {
		exec, err := makeValueWindowExec(mp, aggID, false, []types.Type{types.T_varchar.ToType()})
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))

		result, err := exec.Flush()
		require.NoError(t, err)
		require.Len(t, result, 1)
		require.True(t, result[0].GetIsBinaryString())
		result[0].Free(mp)
		exec.Free()
	}
}
