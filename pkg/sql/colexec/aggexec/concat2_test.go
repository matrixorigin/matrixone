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

func TestGroupConcatOrderByMultipleArguments(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 90,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(3, []byte{groupConcatOrderAsc}, "|"),
		0,
	))
	require.NoError(t, exec.GroupGrow(1))

	left := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"b", "a"})
	colon := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{":", ":"})
	right := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(right, []int64{2, 1}, nil, mp))
	orderKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(orderKey, []int64{2, 1}, nil, mp))

	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1},
		[]*vector.Vector{left, colon, right, orderKey},
	))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a:1|b:2", string(result[0].GetBytesAt(0)))

	left.Free(mp)
	colon.Free(mp)
	right.Free(mp)
	orderKey.Free(mp)
	result[0].Free(mp)
	exec.Free()
}

func TestGroupConcatOrderByMultipleKeysAndNullPlacement(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 91,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(testGroupConcatOrderConfig(
		1,
		[]byte{
			groupConcatOrderAsc | groupConcatOrderNullsLast,
			groupConcatOrderDesc | groupConcatOrderNullsFirst,
		},
		",",
	), 0))
	require.NoError(t, exec.GroupGrow(1))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"null", "a", "b", "c"})
	firstKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		firstKey,
		[]int64{0, 1, 1, 2},
		[]bool{true, false, false, false},
		mp,
	))
	secondKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(secondKey, []int64{0, 1, 3, 2}, nil, mp))

	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1, 1, 1},
		[]*vector.Vector{values, firstKey, secondKey},
	))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "b,a,c,null", string(result[0].GetBytesAt(0)))

	values.Free(mp)
	firstKey.Free(mp)
	secondKey.Free(mp)
	result[0].Free(mp)
	exec.Free()
}

func TestGroupConcatOrderedDistinctSortsBeforeDedup(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:    92,
		distinct: true,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(2, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	require.NoError(t, exec.GroupGrow(1))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a", "b", "a", "a"})
	suffixes := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(suffixes, []int64{1, 2, 1, 2}, nil, mp))
	orderKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(orderKey, []int64{4, 3, 1, 2}, nil, mp))

	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1, 1, 1},
		[]*vector.Vector{values, suffixes, orderKey},
	))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a1,a2,b2", string(result[0].GetBytesAt(0)))

	values.Free(mp)
	suffixes.Free(mp)
	orderKey.Free(mp)
	result[0].Free(mp)
	exec.Free()
}

func TestGroupConcatOrderedDistinctCanMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     93,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	config := testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|")
	left := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	right := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, left.SetExtraInformation(config, 0))
	require.NoError(t, right.SetExtraInformation(config, 0))
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))

	leftValue := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"b"})
	leftKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(leftKey, []int64{2}, nil, mp))
	rightValue := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a"})
	rightKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(rightKey, []int64{1}, nil, mp))

	require.NoError(t, left.Fill(0, 0, []*vector.Vector{leftValue, leftKey}))
	require.NoError(t, right.Fill(0, 0, []*vector.Vector{rightValue, rightKey}))
	require.NoError(t, left.Merge(right, 0, 0))
	result, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, "a|b", string(result[0].GetBytesAt(0)))

	leftValue.Free(mp)
	leftKey.Free(mp)
	rightValue.Free(mp)
	rightKey.Free(mp)
	result[0].Free(mp)
	left.Free()
	right.Free()
}

func TestGroupConcatOrderConfigValidationAndReturnType(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 94,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_binary.ToType(),
		},
		retType:   types.T_blob.ToType(),
		emptyNull: true,
	}

	t.Run("binary order key does not change return type", func(t *testing.T) {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
			0,
		))
		require.Equal(t, types.T_text, exec.retType.Oid)
		exec.Free()
	})

	t.Run("invalid configs", func(t *testing.T) {
		cases := [][]byte{
			[]byte(groupConcatOrderConfigMagic),
			[]byte(groupConcatOrderConfigPrefix + "9"),
			testGroupConcatOrderConfig(2, nil, ","),
			testGroupConcatOrderConfig(2, []byte{groupConcatOrderAsc}, ","),
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc | groupConcatOrderDesc}, ","),
			testGroupConcatOrderConfig(
				1,
				[]byte{groupConcatOrderNullsFirst | groupConcatOrderNullsLast},
				",",
			),
		}
		for _, config := range cases {
			exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
			require.Error(t, exec.SetExtraInformation(config, 0))
			exec.Free()
		}
	})

	t.Run("legacy config clears ordered metadata", func(t *testing.T) {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
			0,
		))
		require.NoError(t, exec.SetExtraInformation([]byte("|"), 0))
		require.Equal(t, len(info.argTypes), exec.concatArgCnt)
		require.Zero(t, exec.orderArgCnt)
		require.Nil(t, exec.orderDesc)
		require.Nil(t, exec.orderNullsLast)
		require.Equal(t, []byte("|"), exec.separator)
		require.Equal(t, types.T_blob, exec.retType.Oid)
		exec.Free()
	})
}

func TestGroupConcatOrderedPayloadValidation(t *testing.T) {
	t.Run("invalid envelope", func(t *testing.T) {
		_, _, err := splitGroupConcatOrderedPayload(nil)
		require.Error(t, err)

		payload := make([]byte, 4)
		binary.BigEndian.PutUint32(payload, 1)
		_, _, err = splitGroupConcatOrderedPayload(payload)
		require.Error(t, err)
	})

	t.Run("invalid order fields release vectors", func(t *testing.T) {
		mp := mpool.MustNewZero()
		info := multiAggInfo{
			aggID:     95,
			argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
			retType:   types.T_text.ToType(),
			emptyNull: true,
		}
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
			0,
		))

		entries := []groupConcatOrderedEntry{{orderPayload: []byte{1, 0}}}
		_, err := exec.restoreOrderVectors(entries)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())

		badFixedField := appendPayloadField(nil, []byte{1}, false)
		entries[0].orderPayload = badFixedField
		_, err = exec.restoreOrderVectors(entries)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
		exec.Free()
	})
}

func testGroupConcatOrderConfig(concatArgCount int, orderFlags []byte, separator string) []byte {
	separatorBytes := []byte(separator)
	config := make([]byte, 0, len(groupConcatOrderConfigMagic)+12+len(orderFlags)+len(separatorBytes))
	config = append(config, groupConcatOrderConfigMagic...)

	var encodedUint32 [4]byte
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(concatArgCount))
	config = append(config, encodedUint32[:]...)
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(orderFlags)))
	config = append(config, encodedUint32[:]...)
	config = append(config, orderFlags...)
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(separatorBytes)))
	config = append(config, encodedUint32[:]...)
	config = append(config, separatorBytes...)
	return config
}
