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
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestGroupConcatH0OrderedSpillAndCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 100,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	newExec := func(ctx context.Context) *groupConcatExec {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|"),
			0,
		))
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 1)
		require.NoError(t, exec.GroupGrow(1))
		ConfigureGroupConcatH0Spill(exec, 80, ctx, func() (*os.File, error) {
			file, err := os.CreateTemp(t.TempDir(), "group-concat-run-")
			if err == nil {
				err = os.Remove(file.Name())
			}
			return file, err
		}, nil)
		return exec
	}

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"c", "a", "d", "b"})
	orderKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(orderKey, []int64{3, 1, 4, 2}, nil, mp))

	exec := newExec(context.Background())
	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1, 1, 1},
		[]*vector.Vector{values, orderKey},
	))
	result, err := exec.FlushWithContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, "a|b|c|d", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
	exec.Free()

	exec = newExec(context.Background())
	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1, 1, 1},
		[]*vector.Vector{values, orderKey},
	))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = exec.FlushWithContext(cancelled)
	require.ErrorIs(t, err, context.Canceled)
	exec.Free()

	values.Free(mp)
	orderKey.Free(mp)
}

func TestGroupConcatLimitAppliesAfterOrderAndDistinct(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	newExec := func(distinct bool, concatArgs int, offset, count uint64) *groupConcatExec {
		info := multiAggInfo{
			aggID:     100,
			distinct:  distinct,
			argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()}[:concatArgs],
			retType:   types.T_text.ToType(),
			emptyNull: true,
		}
		exec := newGroupConcatExec(mp, info, "|").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatLimitConfig(concatArgs, []byte{groupConcatOrderAsc}, "|", offset, count), 0))
		require.NoError(t, exec.GroupGrow(1))
		return exec
	}

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"c", "a", "b", "a"})
	orderKeys := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(orderKeys, []int64{3, 1, 2, 0}, nil, mp))
	defer values.Free(mp)
	defer orderKeys.Free(mp)

	exec := newExec(false, 2, 1, 2)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{values, orderKeys}))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a|b", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
	exec.Free()

	exec = newExec(true, 2, 1, 1)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{values, orderKeys}))
	result, err = exec.Flush()
	require.NoError(t, err)
	// Ordered DISTINCT first keeps the earliest ORDER BY candidate for "a",
	// then OFFSET is applied to the deduplicated sequence a,b,c.
	require.Equal(t, "b", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
	exec.Free()

	// LIMIT also has valid semantics without ORDER BY. A zero row count is a
	// deterministic case that verifies the no-order config path without making
	// assumptions about unordered aggregate input order.
	plainInfo := multiAggInfo{
		aggID:     100,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	plainExec := newGroupConcatExec(mp, plainInfo, ",").(*groupConcatExec)
	require.NoError(t, plainExec.SetExtraInformation(
		testGroupConcatLimitConfig(1, nil, ",", 0, 0), 0))
	require.NoError(t, plainExec.GroupGrow(1))
	require.NoError(t, plainExec.BatchFill(0, []uint64{1}, []*vector.Vector{values}))
	result, err = plainExec.Flush()
	require.NoError(t, err)
	require.Equal(t, "", string(result[0].GetBytesAt(0)))
	result[0].Free(mp)
	plainExec.Free()
}

func TestGroupConcatGroupedOrderedSpillKeepsGroupAddressing(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 100,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	require.NoError(t, exec.GroupGrow(3))
	ConfigureGroupConcatH0Spill(exec, groupConcatMinRunSize, context.Background(), nil, nil)

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"c", "b", "a", "d"})
	orderKey := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(orderKey, []int64{3, 2, 1, 4}, nil, mp))
	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 2, 1, 2},
		[]*vector.Vector{values, orderKey},
	))
	require.False(t, exec.hasOrderedSpillRuns())

	result, err := exec.FlushWithContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, "a,c", string(result[0].GetBytesAt(0)))
	require.Equal(t, "b,d", string(result[0].GetBytesAt(1)))
	require.True(t, result[0].GetNulls().Contains(2))

	result[0].Free(mp)
	values.Free(mp)
	orderKey.Free(mp)
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestGroupConcatEnumOrderPayloadUsesFixedWidthStorage(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_enum.ToType())
	require.NoError(t, vector.AppendFixed(vec, types.Enum(2), false, mp))
	data := groupConcatFieldBytes(vec, 0, types.T_enum.ToType())
	require.Len(t, data, types.T_enum.ToType().TypeSize())
	require.Equal(t, types.Enum(2), types.DecodeEnum(data))
	vec.Free(mp)
}

func TestGroupConcatH0SpillBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 101,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	newExec := func() *groupConcatExec {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
			0,
		))
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 1)
		require.NoError(t, exec.GroupGrow(1))
		return exec
	}

	empty := newExec()
	ConfigureGroupConcatH0Spill(
		empty,
		groupConcatMaxH0RunSize+1,
		nil,
		func() (*os.File, error) {
			return nil, errors.New("must not create a file for empty input")
		},
		nil,
	)
	require.Equal(t, groupConcatMaxH0RunSize, empty.h0SpillLimit)
	result, err := empty.FlushWithContext(nil)
	require.NoError(t, err)
	require.True(t, result[0].IsNull(0))
	result[0].Free(mp)
	empty.Free()

	createErr := errors.New("create spill run")
	failing := newExec()
	ConfigureGroupConcatH0Spill(
		failing,
		1,
		context.Background(),
		func() (*os.File, error) { return nil, createErr },
		nil,
	)
	value := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"x"})
	key := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(key, int64(1), false, mp))
	err = failing.BatchFill(0, []uint64{1}, []*vector.Vector{value, key})
	require.NoError(t, err)
	err = failing.spillOrderedState(context.Background())
	require.ErrorIs(t, err, createErr)
	value.Free(mp)
	key.Free(mp)
	failing.Free()

	count := makeCountStarExec(t, mp, types.T_int64.ToType())
	require.NoError(t, count.GroupGrow(1))
	result, err = FlushWithContext(nil, count)
	require.NoError(t, err)
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](result[0], 0))
	result[0].Free(mp)
	count.Free()
}

func TestGroupConcatSpillWatermarkExcludesRunMetadata(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 102,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 1)
	require.NoError(t, exec.GroupGrow(1))
	fileCreates := 0
	ConfigureGroupConcatH0Spill(exec, groupConcatMinRunSize, context.Background(), func() (*os.File, error) {
		fileCreates++
		return nil, errors.New("run metadata must not trigger an active spill")
	}, nil)

	descriptorCapacity := int(groupConcatMinRunSize/(4*8)) + 1
	exec.orderedSpillRuns[0] = make([]groupConcatSpillRun, 0, descriptorCapacity)
	require.GreaterOrEqual(t, exec.Size(), groupConcatMinRunSize)
	require.Less(t, exec.activeOrderedMemorySize(), groupConcatMinRunSize)
	require.Equal(t, exec.fixedAndSpilledMemorySize(), exec.AdditionalMemorySize())

	value := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"x"})
	key := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"k"})
	require.NoError(t, exec.BatchFill(0, []uint64{1}, []*vector.Vector{value, key}))
	require.Zero(t, fileCreates)
	require.Empty(t, exec.orderedSpillRuns[0])

	value.Free(mp)
	key.Free(mp)
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestGroupConcatInputSpillBoundsRunsAndWriteAmplification(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID: 103,
		argTypes: []types.Type{
			types.T_varchar.ToType(),
			types.T_varchar.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ""),
		0,
	))
	exec.maxLen = 4
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 1)
	require.NoError(t, exec.GroupGrow(1))

	var peakMemory, spillBytes, spillRows int64
	ConfigureGroupConcatH0Spill(exec, groupConcatMinRunSize, context.Background(), func() (*os.File, error) {
		file, err := os.CreateTemp(t.TempDir(), "group-concat-bounded-runs-")
		if err == nil {
			err = os.Remove(file.Name())
		}
		return file, err
	}, func(bytes, rows, retainedMemory int64) {
		spillBytes += bytes
		spillRows += rows
		peakMemory = max(peakMemory, retainedMemory)
	})

	const (
		rowCount = 160
		keySize  = 20 * 1024
	)
	values := make([]string, rowCount)
	keys := make([]string, rowCount)
	groups := make([]uint64, rowCount)
	for i := range rowCount {
		values[i] = fmt.Sprintf("%04d", i)
		keys[i] = fmt.Sprintf("%06d%s", rowCount-i, strings.Repeat("k", keySize-6))
		groups[i] = 1
	}
	valueVec := buildVarlenVec(t, mp, types.T_varchar.ToType(), values)
	keyVec := buildVarlenVec(t, mp, types.T_varchar.ToType(), keys)
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{valueVec, keyVec}))

	require.LessOrEqual(t, len(exec.orderedSpillRuns[0]), groupConcatMergeFanIn)
	require.LessOrEqual(t, spillRows, int64(rowCount*2))
	require.Less(t, spillBytes, int64(rowCount*keySize*3))
	require.Less(t, peakMemory, int64(groupConcatMinRunSize*10))

	result, err := exec.FlushWithContext(context.Background())
	require.NoError(t, err)
	require.Len(t, result[0].GetBytesAt(0), int(exec.maxLen))
	require.LessOrEqual(t, len(exec.orderedSpillRuns[0]), groupConcatMergeFanIn)
	require.LessOrEqual(t, spillRows, int64(rowCount*2))
	require.Less(t, spillBytes, int64(rowCount*keySize*3))

	result[0].Free(mp)
	valueVec.Free(mp)
	keyVec.Free(mp)
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestGroupConcatGroupedDistinctSpillAndCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     97,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	require.NoError(t, exec.GroupGrow(2))
	fileCreates := 0
	ConfigureGroupConcatH0Spill(exec, 1, context.Background(), func() (*os.File, error) {
		fileCreates++
		file, err := os.CreateTemp(t.TempDir(), "group-concat-grouped-")
		if err == nil {
			err = os.Remove(file.Name())
		}
		return file, err
	}, nil)

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a", "a", "b", "c"})
	keys := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{
		strings.Repeat("b", int(groupConcatMinRunSize)),
		strings.Repeat("a", int(groupConcatMinRunSize)),
		strings.Repeat("c", int(groupConcatMinRunSize)),
		strings.Repeat("a", int(groupConcatMinRunSize)),
	})
	require.NoError(t, exec.BatchFill(
		0,
		[]uint64{1, 1, 1, 2},
		[]*vector.Vector{values, keys},
	))
	require.Equal(t, 1, fileCreates)
	require.NotEmpty(t, exec.orderedSpillRuns[0])
	require.NotEmpty(t, exec.orderedSpillRuns[1])

	result, err := exec.FlushWithContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, "a,b", string(result[0].GetBytesAt(0)))
	require.Equal(t, "c", string(result[0].GetBytesAt(1)))
	result[0].Free(mp)
	values.Free(mp)
	keys.Free(mp)
	exec.Free()

	cancelExec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, cancelExec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	require.NoError(t, cancelExec.GroupGrow(1))
	value := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"x"})
	key := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"k"})
	require.NoError(t, cancelExec.BatchFill(0, []uint64{1}, []*vector.Vector{value, key}))
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(errors.New("cancel grouped ordered flush"))
	_, err = cancelExec.FlushWithContext(ctx)
	require.ErrorContains(t, err, "cancel grouped ordered flush")
	value.Free(mp)
	key.Free(mp)
	cancelExec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestGroupConcatSpillCompactsFanIn(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     98,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_varchar.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	require.NoError(t, exec.GroupGrow(1))
	fileCreates := 0
	ConfigureGroupConcatH0Spill(exec, 1, context.Background(), func() (*os.File, error) {
		fileCreates++
		file, err := os.CreateTemp(t.TempDir(), "group-concat-fanin-")
		if err == nil {
			err = os.Remove(file.Name())
		}
		return file, err
	}, nil)

	want := make([]string, groupConcatMergeFanIn+1)
	for i := range want {
		want[i] = fmt.Sprintf("%02d", i)
		entry := groupConcatOrderedEntry{
			concatPayload: appendPayloadField(nil, []byte(want[i]), false),
			orderPayload:  appendPayloadField(nil, []byte(want[i]), false),
		}
		require.NoError(t, exec.writeOrderedRun(context.Background(), 0, []groupConcatOrderedEntry{entry}))
	}
	require.Len(t, exec.orderedSpillRuns[0], groupConcatMergeFanIn+1)
	result, err := exec.FlushWithContext(context.Background())
	require.NoError(t, err)
	require.Equal(t, strings.Join(want, ","), string(result[0].GetBytesAt(0)))
	require.LessOrEqual(t, len(exec.orderedSpillRuns[0]), groupConcatMergeFanIn)
	require.Equal(t, 1, fileCreates)
	result[0].Free(mp)
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestReadGroupConcatRunEntryRejectsTruncatedData(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "group-concat-corrupt-")
	require.NoError(t, err)
	defer file.Close()

	run := groupConcatSpillRun{}
	_, err = readGroupConcatRunEntry(file, &run)
	require.NoError(t, err)

	_, err = file.Write([]byte{0, 0})
	require.NoError(t, err)
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	run = groupConcatSpillRun{end: 2}
	_, err = readGroupConcatRunEntry(file, &run)
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)

	require.NoError(t, file.Truncate(0))
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	_, err = file.Write([]byte{0, 0, 0, 3, 1, 2, 3})
	require.NoError(t, err)
	_, err = file.Seek(0, io.SeekStart)
	require.NoError(t, err)
	run = groupConcatSpillRun{end: 7}
	_, err = readGroupConcatRunEntry(file, &run)
	require.ErrorContains(t, err, "invalid group_concat ordered payload")
}

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
	require.Len(t, exec.orderedDistinct[0], 3)
	require.Zero(t, exec.state[0].argCnt[0])
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a1,a2,b2", string(result[0].GetBytesAt(0)))

	values.Free(mp)
	suffixes.Free(mp)
	orderKey.Free(mp)
	result[0].Free(mp)
	exec.Free()
}

func TestGroupConcatOrderedDistinctKeepsOneCandidatePerTuple(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     96,
		distinct:  true,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, ","),
		0,
	))
	require.NoError(t, exec.GroupGrow(1))

	const rows = 1024
	values, err := vector.NewConstBytes(types.T_varchar.ToType(), []byte("same"), rows, mp)
	require.NoError(t, err)
	orderKeys := vector.NewVec(types.T_int64.ToType())
	keys := make([]int64, rows)
	groups := make([]uint64, rows)
	for i := range rows {
		keys[i] = int64(rows - i)
		groups[i] = 1
	}
	require.NoError(t, vector.AppendFixedList(orderKeys, keys, nil, mp))
	require.NoError(t, exec.BatchFill(
		0,
		groups,
		[]*vector.Vector{values, orderKeys},
	))

	require.Len(t, exec.orderedDistinct[0], 1)
	require.Zero(t, exec.state[0].argCnt[0])
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "same", string(result[0].GetBytesAt(0)))

	values.Free(mp)
	orderKeys.Free(mp)
	result[0].Free(mp)
	exec.Free()
	require.Zero(t, mp.CurrNB())
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
		cases := []any{
			AggregateConfig{
				Type: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
				Data: []byte{groupConcatOrderConfigVersion + 1},
			},
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

	t.Run("legacy separator can use old magic prefix", func(t *testing.T) {
		exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
		separator := []byte("\x00GCORDER2")
		require.NoError(t, exec.SetExtraInformation(separator, 0))
		require.Equal(t, separator, exec.separator)
		require.Zero(t, exec.orderArgCnt)
		exec.Free()
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

func TestGroupConcatOrderedMaxLen(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     96,
		argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}
	planConfig := testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|")
	runtimeConfig := EncodeGroupConcatOrderedConfig(planConfig.Data, 5)
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(AggregateConfig{
		Type: planConfig.Type,
		Data: runtimeConfig,
	}, 0))
	require.NoError(t, exec.GroupGrow(1))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"ccc", "a", "bb"})
	orderKeys := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(orderKeys, []int64{3, 1, 2}, nil, mp))
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values, orderKeys}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a|bb|", string(results[0].GetBytesAt(0)))

	refreshed := RefreshGroupConcatConfigMaxLen(runtimeConfig, 3)
	require.Equal(t, EncodeGroupConcatOrderedConfig(planConfig.Data, 3), refreshed)

	values.Free(mp)
	orderKeys.Free(mp)
	results[0].Free(mp)
	exec.Free()
	require.Zero(t, mp.CurrNB())
}

func TestGroupConcatMaxLen(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     90,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType()}),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation(EncodeGroupConcatConfig("", 5), 0))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"aa", "bb", "cc"})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "aabbc", string(results[0].GetBytesAt(0)))

	values.Free(mp)
	results[0].Free(mp)
	exec.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestGroupConcatMaxLenCanTruncateSeparator(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     91,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType()}),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation(EncodeGroupConcatConfig("--", 3), 0))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"aa", "bb"})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "aa-", string(results[0].GetBytesAt(0)))

	values.Free(mp)
	results[0].Free(mp)
	exec.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestGroupConcatMaxLenKeepsTextWellFormed(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     92,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType()}),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation(EncodeGroupConcatConfig("", 4), 0))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"你好"})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "你", string(results[0].GetBytesAt(0)))

	values.Free(mp)
	results[0].Free(mp)
	exec.Free()
	require.Equal(t, int64(0), mp.CurrNB())
}

func TestGroupConcatMaxLenStopsAfterTruncatedUTF8Argument(t *testing.T) {
	tests := []struct {
		name    string
		ordered bool
		spill   bool
	}{
		{name: "input order"},
		{name: "ordered memory", ordered: true},
		{name: "ordered spill", ordered: true, spill: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			argTypes := []types.Type{
				types.T_varchar.ToType(),
				types.T_varchar.ToType(),
			}
			if tc.ordered {
				argTypes = append(argTypes, types.T_varchar.ToType())
			}
			info := multiAggInfo{
				aggID:     104,
				argTypes:  argTypes,
				retType:   types.T_text.ToType(),
				emptyNull: true,
			}
			exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
			if tc.ordered {
				require.NoError(t, exec.SetExtraInformation(
					testGroupConcatOrderConfig(2, []byte{groupConcatOrderAsc}, ","),
					0,
				))
			} else {
				require.NoError(t, exec.SetExtraInformation(
					EncodeGroupConcatConfig(",", 4),
					0,
				))
			}
			exec.maxLen = 4
			require.NoError(t, exec.GroupGrow(1))

			first := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"你好", "later"})
			second := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"x", "y"})
			vectors := []*vector.Vector{first, second}
			var orderKey *vector.Vector
			if tc.ordered {
				keys := []string{"a", "b"}
				if tc.spill {
					keys[0] += strings.Repeat("k", int(groupConcatMinRunSize))
				}
				orderKey = buildVarlenVec(t, mp, types.T_varchar.ToType(), keys)
				vectors = append(vectors, orderKey)
			}
			if tc.spill {
				ConfigureGroupConcatH0Spill(
					exec,
					groupConcatMinRunSize,
					context.Background(),
					func() (*os.File, error) {
						file, err := os.CreateTemp(t.TempDir(), "group-concat-utf8-")
						if err == nil {
							err = os.Remove(file.Name())
						}
						return file, err
					},
					nil,
				)
			}

			require.NoError(t, exec.BatchFill(0, []uint64{1, 1}, vectors))
			if tc.spill {
				require.True(t, exec.hasOrderedSpillRuns())
			}
			results, err := exec.FlushWithContext(context.Background())
			require.NoError(t, err)
			require.Equal(t, "你", string(results[0].GetBytesAt(0)))

			results[0].Free(mp)
			first.Free(mp)
			second.Free(mp)
			if orderKey != nil {
				orderKey.Free(mp)
			}
			exec.Free()
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestGroupConcatMaxLenStopsAfterTruncatedSeparator(t *testing.T) {
	mp := mpool.MustNewZero()
	info := multiAggInfo{
		aggID:     93,
		argTypes:  []types.Type{types.T_varchar.ToType()},
		retType:   GroupConcatReturnType([]types.Type{types.T_varchar.ToType()}),
		emptyNull: true,
	}
	exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
	require.NoError(t, exec.GroupGrow(1))
	require.NoError(t, exec.SetExtraInformation(EncodeGroupConcatConfig("你好", 2), 0))

	values := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"a", "b"})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{values}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "a", string(results[0].GetBytesAt(0)))

	values.Free(mp)
	results[0].Free(mp)
	exec.Free()
	require.Equal(t, int64(0), mp.CurrNB())
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
		_, err := exec.restoreOrderVectors(context.Background(), entries)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())

		badFixedField := appendPayloadField(nil, []byte{1}, false)
		entries[0].orderPayload = badFixedField
		_, err = exec.restoreOrderVectors(context.Background(), entries)
		require.Error(t, err)
		require.Zero(t, mp.CurrNB())
		exec.Free()
	})
}

func testGroupConcatOrderConfig(
	concatArgCount int,
	orderFlags []byte,
	separator string,
) AggregateConfig {
	separatorBytes := []byte(separator)
	config := make([]byte, 0, 13+5*len(orderFlags)+len(separatorBytes))
	config = append(config, groupConcatOrderConfigVersion)

	var encodedUint32 [4]byte
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(concatArgCount))
	config = append(config, encodedUint32[:]...)
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(orderFlags)))
	config = append(config, encodedUint32[:]...)
	config = append(config, orderFlags...)
	for i := range orderFlags {
		binary.BigEndian.PutUint32(encodedUint32[:], uint32(concatArgCount+i))
		config = append(config, encodedUint32[:]...)
	}
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(separatorBytes)))
	config = append(config, encodedUint32[:]...)
	config = append(config, separatorBytes...)
	return AggregateConfig{
		Type: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		Data: config,
	}
}

func testGroupConcatLimitConfig(
	concatArgCount int,
	orderFlags []byte,
	separator string,
	offset, count uint64,
) AggregateConfig {
	separatorBytes := []byte(separator)
	config := make([]byte, 0, 29+5*len(orderFlags)+len(separatorBytes))
	config = append(config, 3)

	var encodedUint32 [4]byte
	var encodedUint64 [8]byte
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(concatArgCount))
	config = append(config, encodedUint32[:]...)
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(orderFlags)))
	config = append(config, encodedUint32[:]...)
	config = append(config, orderFlags...)
	for i := range orderFlags {
		binary.BigEndian.PutUint32(encodedUint32[:], uint32(concatArgCount+i))
		config = append(config, encodedUint32[:]...)
	}
	binary.BigEndian.PutUint64(encodedUint64[:], offset)
	config = append(config, encodedUint64[:]...)
	binary.BigEndian.PutUint64(encodedUint64[:], count)
	config = append(config, encodedUint64[:]...)
	binary.BigEndian.PutUint32(encodedUint32[:], uint32(len(separatorBytes)))
	config = append(config, encodedUint32[:]...)
	config = append(config, separatorBytes...)
	return AggregateConfig{
		Type: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
		Data: config,
	}
}
