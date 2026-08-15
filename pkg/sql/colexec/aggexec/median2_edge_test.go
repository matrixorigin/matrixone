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

package aggexec

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

var errMedianInjectedWrite = errors.New("injected median write failure")

type medianFailAfterWriter struct {
	remaining int
	short     bool
}

func (w *medianFailAfterWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if w.remaining <= 0 {
		if w.short {
			return 0, nil
		}
		return 0, errMedianInjectedWrite
	}
	n := min(len(p), w.remaining)
	w.remaining -= n
	if n != len(p) {
		if w.short {
			return n, nil
		}
		return n, errMedianInjectedWrite
	}
	return n, nil
}

func TestMedianIntermediateRejectsEveryTruncatedPrefix(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		mp := mpool.MustNewZero()
		source, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, source.GroupGrow(2))
		input := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5, 8})
		require.NoError(t, source.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{input}))
		var encoded bytes.Buffer
		require.NoError(t, source.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &encoded))

		for cut := 0; cut < encoded.Len(); cut++ {
			target, makeErr := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
			require.NoError(t, makeErr)
			require.Error(t, target.UnmarshalFromReader(
				bytes.NewReader(encoded.Bytes()[:cut]), mp), "cut=%d", cut)
			target.Free()
		}

		input.Free(mp)
		source.Free()
		require.Zero(t, mp.CurrNB())
	})

	t.Run("accounted", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry, account, allocation := newTestAggregateAllocation(t)
		source, err := MakeAgg(mp, AggIdOfMedian, true, types.T_int64.ToType())
		require.NoError(t, err)
		sourceOwner := source.(AllocationAccountOwner)
		require.NoError(t, sourceOwner.SetAllocationAccount(allocation))
		SyncAggregatorsToChunkSize([]AggFuncExec{source}, AggBatchSize)
		require.NoError(t, source.GroupGrow(2))
		input := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5, 8})
		groups := []uint64{1, 1, 2, 2}
		require.NoError(t, source.(BatchCapacityPreflight).PreflightBatchFill(
			0, groups, []*vector.Vector{input}))
		require.NoError(t, source.BatchFill(0, groups, []*vector.Vector{input}))
		var encoded bytes.Buffer
		require.NoError(t, source.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &encoded))
		baseline := account.Snapshot().Used

		for cut := 0; cut < encoded.Len(); cut++ {
			target, makeErr := MakeAgg(mp, AggIdOfMedian, true, types.T_int64.ToType())
			require.NoError(t, makeErr)
			owner := target.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			SyncAggregatorsToChunkSize([]AggFuncExec{target}, AggBatchSize)
			require.Error(t, target.UnmarshalFromReader(
				bytes.NewReader(encoded.Bytes()[:cut]), mp), "cut=%d", cut)
			target.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			require.Equal(t, baseline, account.Snapshot().Used, "cut=%d", cut)
		}

		input.Free(mp)
		source.Free()
		require.NoError(t, sourceOwner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	})
}

func TestAccountedMedianIntermediateRejectsMalformedFrames(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	source, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	sourceOwner := source.(AllocationAccountOwner)
	require.NoError(t, sourceOwner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{source}, AggBatchSize)
	require.NoError(t, source.GroupGrow(2))
	input := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5, 8})
	groups := []uint64{1, 1, 2, 2}
	require.NoError(t, source.(BatchCapacityPreflight).PreflightBatchFill(
		0, groups, []*vector.Vector{input}))
	require.NoError(t, source.BatchFill(0, groups, []*vector.Vector{input}))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(2, [][]uint8{{1, 1}}, &encoded))

	reader := bytes.NewReader(encoded.Bytes())
	rows, err := types.ReadInt64(reader)
	require.NoError(t, err)
	require.Equal(t, int64(2), rows)
	result := vector.NewVec(types.T_float64.ToType())
	require.NoError(t, result.UnmarshalWithReader(reader, mp))
	result.Free(mp)
	emptyCountOffset := encoded.Len() - reader.Len()
	_, err = types.ReadInt64(reader)
	require.NoError(t, err)
	empty := vector.NewVec(types.T_bool.ToType())
	require.NoError(t, empty.UnmarshalWithReader(reader, mp))
	empty.Free(mp)
	distinctCountOffset := encoded.Len() - reader.Len()
	_, err = types.ReadInt64(reader)
	require.NoError(t, err)
	groupCountOffset := encoded.Len() - reader.Len()
	_, err = types.ReadInt64(reader)
	require.NoError(t, err)
	firstGroupOffset := encoded.Len() - reader.Len()

	secondGroupOffset := firstGroupOffset
	for range 2 {
		groupSize, readErr := types.ReadInt32AsInt(reader)
		require.NoError(t, readErr)
		require.GreaterOrEqual(t, groupSize, 0)
		_, readErr = reader.Seek(int64(groupSize), io.SeekCurrent)
		require.NoError(t, readErr)
		if secondGroupOffset == firstGroupOffset {
			secondGroupOffset = encoded.Len() - reader.Len()
		}
	}
	extraCountOffset := encoded.Len() - reader.Len()

	mutations := []struct {
		name   string
		offset int
		value  []byte
	}{
		{name: "negative-rows", offset: 0, value: types.EncodeInt64(ptr(int64(-1)))},
		{name: "result-row-mismatch", offset: 0, value: types.EncodeInt64(ptr(int64(1)))},
		{name: "empty-count", offset: emptyCountOffset, value: types.EncodeInt64(ptr(int64(2)))},
		{name: "distinct-sidecar", offset: distinctCountOffset, value: types.EncodeInt64(ptr(int64(1)))},
		{name: "group-count", offset: groupCountOffset, value: types.EncodeInt64(ptr(int64(1)))},
		{name: "negative-frame", offset: firstGroupOffset, value: types.EncodeInt32(ptr(int32(-1)))},
		{name: "zero-vector-count", offset: firstGroupOffset + 4, value: types.EncodeInt64(ptr(int64(0)))},
		{name: "oversized-vector", offset: firstGroupOffset + 12, value: types.EncodeUint32(ptr(uint32(^uint32(0))))},
		{name: "truncated-second-frame", offset: secondGroupOffset, value: types.EncodeInt32(ptr(int32(1)))},
		{name: "extra-state", offset: extraCountOffset, value: types.EncodeInt64(ptr(int64(1)))},
	}
	baseline := account.Snapshot().Used
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			payload := append([]byte(nil), encoded.Bytes()...)
			copy(payload[tc.offset:], tc.value)
			target, makeErr := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
			require.NoError(t, makeErr)
			owner := target.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			SyncAggregatorsToChunkSize([]AggFuncExec{target}, AggBatchSize)
			require.Error(t, target.UnmarshalFromReader(bytes.NewReader(payload), mp))
			target.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			require.Equal(t, baseline, account.Snapshot().Used)
		})
	}

	input.Free(mp)
	source.Free()
	require.NoError(t, sourceOwner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedMedianIntermediateWriterFailuresAreAtomic(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
	require.NoError(t, exec.GroupGrow(1))
	input := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9, 1, 5})
	require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
		0, []uint64{1, 1, 1}, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1}, []*vector.Vector{input}))
	var encoded bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &encoded))
	baseline := account.Snapshot().Used

	for cut := 0; cut < encoded.Len(); cut++ {
		err = exec.SaveIntermediateResult(1, [][]uint8{{1}}, &medianFailAfterWriter{remaining: cut})
		require.Error(t, err, "cut=%d", cut)
		require.Equal(t, baseline, account.Snapshot().Used, "cut=%d", cut)
	}
	shortWriteObserved := false
	for cut := 0; cut < encoded.Len(); cut++ {
		err = exec.SaveIntermediateResult(1, [][]uint8{{1}}, &medianFailAfterWriter{
			remaining: cut,
			short:     true,
		})
		if errors.Is(err, io.ErrShortWrite) {
			shortWriteObserved = true
		}
		require.Error(t, err, "cut=%d", cut)
		require.Equal(t, baseline, account.Snapshot().Used, "cut=%d", cut)
	}
	require.True(t, shortWriteObserved)

	input.Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestMedianAccountedPolicyAndLegacyBranches(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	legacy, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	accounted, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	owner := accounted.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{accounted}, AggBatchSize)
	accountedGroup := accounted.(GroupAggFuncExec)

	require.Zero(t, accountedGroup.PrepareParamKindChunkCount())
	require.Nil(t, accountedGroup.PrepareParamKindVectorForChunk(0))
	accountedGroup.SetPrepareParamKind(vector.PrepareParamNone)
	require.Zero(t, accountedGroup.AdditionalMemorySize())
	require.Zero(t, accountedGroup.GetNumGroups())
	require.Error(t, accounted.SaveIntermediateResult(-1, nil, io.Discard))
	require.Error(t, accounted.SaveIntermediateResult(0, nil, nil))
	require.Error(t, accounted.SaveIntermediateResultOfChunk(-1, io.Discard))
	require.Error(t, accounted.SaveIntermediateResultOfChunk(0, nil))
	require.ErrorIs(t, accounted.BulkFill(0, nil), mpool.ErrAllocationAccountInvalid)
	require.Error(t, accounted.(BatchCapacityPreflight).PreflightBatchMerge(legacy, 0, nil))
	require.NoError(t, legacy.(BatchCapacityPreflight).PreflightBatchMerge(accounted, 0, nil))
	require.Error(t, legacy.(SpillStateCodec).SaveSpillIntermediateResult(0, 0, nil, io.Discard))
	require.Error(t, legacy.(SpillStateCodec).UnmarshalSpillFromReader(bytes.NewReader(nil), mp))

	require.NoError(t, accounted.GroupGrow(1))
	require.Equal(t, 1, accountedGroup.GetNumGroups())
	require.Error(t, accounted.SaveIntermediateResult(1, [][]uint8{{0}}, io.Discard))
	require.Error(t, accounted.SaveIntermediateResult(0, [][]uint8{{}, {1}}, io.Discard))

	constNull := vector.NewConstNull(types.T_int64.ToType(), 3, mp)
	require.NoError(t, legacy.GroupGrow(1))
	require.NoError(t, legacy.BulkFill(0, []*vector.Vector{constNull}))
	constant, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 3, mp)
	require.NoError(t, err)
	require.NoError(t, legacy.BulkFill(0, []*vector.Vector{constant}))
	require.NoError(t, legacy.BatchFill(0, []uint64{1, GroupNotMatched, 1}, []*vector.Vector{constant}))
	require.Positive(t, legacy.Size())

	constant.Free(mp)
	constNull.Free(mp)
	legacy.Free()
	accounted.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}
