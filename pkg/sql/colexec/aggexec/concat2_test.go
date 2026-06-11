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
	"encoding/binary"
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

func TestGroupConcatOrderByMultiArgument(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 88,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(testGroupConcatOrderConfig(3, []byte{0}, ","), 0))
	require.NoError(t, exec.GroupGrow(1))

	a := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a2", "a1"})
	colon := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{":", ":"})
	b := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"b2", "b1"})
	k := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(k, []int64{2, 1}, nil, mp))

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, []*vector.Vector{a, colon, b, k}))
	vecs, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a1:b1,a2:b2", string(vecs[0].GetBytesAt(0)))

	a.Free(mp)
	colon.Free(mp)
	b.Free(mp)
	k.Free(mp)
	vecs[0].Free(mp)
	exec.Free()
}

func TestGroupConcatOrderByPerAggregate(t *testing.T) {
	mp := mpool.MustNewZero()
	leftInfo := multiAggInfo{
		aggID:     88,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	rightInfo := multiAggInfo{
		aggID:     88,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	byX := newGroupConcatExec(mp, leftInfo, ",").(*groupConcatExec)
	byY := newGroupConcatExec(mp, rightInfo, ",").(*groupConcatExec)
	require.NoError(t, byX.SetExtraInformation(testGroupConcatOrderConfig(1, []byte{0}, ","), 0))
	require.NoError(t, byY.SetExtraInformation(testGroupConcatOrderConfig(1, []byte{0}, ","), 0))
	require.NoError(t, byX.GroupGrow(1))
	require.NoError(t, byY.GroupGrow(1))

	a := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a1", "a2"})
	b := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"b1", "b2"})
	x := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(x, []int64{1, 2}, nil, mp))
	y := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(y, []int64{2, 1}, nil, mp))

	require.NoError(t, byX.BatchFill(0, []uint64{1, 1}, []*vector.Vector{a, x}))
	require.NoError(t, byY.BatchFill(0, []uint64{1, 1}, []*vector.Vector{b, y}))
	byXVecs, err := byX.Flush()
	require.NoError(t, err)
	byYVecs, err := byY.Flush()
	require.NoError(t, err)
	require.Equal(t, "a1,a2", string(byXVecs[0].GetBytesAt(0)))
	require.Equal(t, "b2,b1", string(byYVecs[0].GetBytesAt(0)))

	a.Free(mp)
	b.Free(mp)
	x.Free(mp)
	y.Free(mp)
	byXVecs[0].Free(mp)
	byYVecs[0].Free(mp)
	byX.Free()
	byY.Free()
}

func TestGroupConcatOrderByMultipleKeysDescAndNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 88,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(testGroupConcatOrderConfig(1, []byte{0, 1}, ","), 0))
	require.NoError(t, exec.GroupGrow(1))

	vals := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vals, []byte("n1"), false, mp))
	require.NoError(t, vector.AppendBytes(vals, []byte("n2"), false, mp))
	require.NoError(t, vector.AppendBytes(vals, []byte("n3"), false, mp))
	require.NoError(t, vector.AppendBytes(vals, nil, true, mp))
	x := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(x, []int64{0, 1, 1, 0}, []bool{true, false, false, false}, mp))
	y := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(y, []int64{2, 1, 3, 9}, nil, mp))

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{vals, x, y}))
	vecs, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "n1,n3,n2", string(vecs[0].GetBytesAt(0)))

	vals.Free(mp)
	x.Free(mp)
	y.Free(mp)
	vecs[0].Free(mp)
	exec.Free()
}

func TestGroupConcatOrderByKeepsTextReturnTypeWhenOrderKeyIsBinary(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     88,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_binary.ToType()},
		retType:   types.T_blob.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(testGroupConcatOrderConfig(1, []byte{0}, ","), 0))
	require.Equal(t, types.T_text, exec.retType.Oid)
	exec.Free()
}

func testGroupConcatOrderConfig(concatArgCnt int, orderFlags []byte, separator string) []byte {
	separatorBytes := []byte(separator)
	config := make([]byte, 0, 16+len(orderFlags)+len(separatorBytes))
	config = append(config, []byte(groupConcatOrderConfigMagic)...)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(concatArgCnt))
	config = append(config, buf[:]...)
	binary.BigEndian.PutUint32(buf[:], uint32(len(orderFlags)))
	config = append(config, buf[:]...)
	config = append(config, orderFlags...)
	binary.BigEndian.PutUint32(buf[:], uint32(len(separatorBytes)))
	config = append(config, buf[:]...)
	config = append(config, separatorBytes...)
	return config
}
