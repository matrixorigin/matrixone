// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-20
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

func TestGroupConcatDistinctAndHelpers(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     88,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.True(t, exec.IsDistinct())
	require.NoError(t, exec.PreAllocateGroups(1))
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation([]byte("|"), 0))

	left := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(left, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(left, []byte("a"), false, mp))
	require.NoError(t, vector.AppendBytes(left, nil, true, mp))
	require.NoError(t, vector.AppendBytes(left, []byte("b"), false, mp))

	right := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(right, []int64{1, 1, 9, 2}, nil, mp))

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{left, right}))
	require.Greater(t, exec.Size(), int64(0))

	vecs, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a1|b2", string(vecs[0].GetBytesAt(0)))

	require.Equal(t, types.T_blob.ToType(), GroupConcatReturnType([]types.Type{types.T_blob.ToType()}))
	require.Equal(t, types.T_text.ToType(), GroupConcatReturnType([]types.Type{types.T_int64.ToType()}))
	require.False(t, IsGroupConcatSupported(types.Type{Oid: types.T_tuple}))
	require.True(t, IsGroupConcatSupported(types.T_varchar.ToType()))

	left.Free(mp)
	right.Free(mp)
	vecs[0].Free(mp)
	exec.Free()
}

func TestGroupConcatDistinctMergeError(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     89,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType()}),
		emptyNull: true,
	}
	left := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	right := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))

	vec := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"x"})
	require.NoError(t, left.Fill(0, 0, []*vector.Vector{vec}))
	require.NoError(t, right.Fill(0, 0, []*vector.Vector{vec}))
	require.NoError(t, left.BulkFill(0, []*vector.Vector{vec}))
	require.Error(t, left.Merge(right, 0, 0))

	err := left.BatchMerge(right, 0, []uint64{1})
	require.Error(t, err)
	require.Contains(t, err.Error(), "distinct agg should be run in only one node")

	vec.Free(mp)
	left.Free()
	right.Free()
}
