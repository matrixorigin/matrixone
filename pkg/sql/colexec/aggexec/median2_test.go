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
	"bytes"
	"io"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMedianExecAcrossSupportedTypes(t *testing.T) {
	mp := mpool.MustNewZero()

	cases := []struct {
		name   string
		typ    types.Type
		values any
		want   any
	}{
		{name: "bit", typ: types.T_bit.ToType(), values: []uint64{1, 3, 2}, want: 2.0},
		{name: "int8", typ: types.T_int8.ToType(), values: []int8{1, 3, 2}, want: 2.0},
		{name: "int16", typ: types.T_int16.ToType(), values: []int16{1, 3, 2}, want: 2.0},
		{name: "int32", typ: types.T_int32.ToType(), values: []int32{1, 3, 2}, want: 2.0},
		{name: "int64", typ: types.T_int64.ToType(), values: []int64{1, 3, 2}, want: 2.0},
		{name: "uint8", typ: types.T_uint8.ToType(), values: []uint8{1, 3, 2}, want: 2.0},
		{name: "uint16", typ: types.T_uint16.ToType(), values: []uint16{1, 3, 2}, want: 2.0},
		{name: "uint32", typ: types.T_uint32.ToType(), values: []uint32{1, 3, 2}, want: 2.0},
		{name: "uint64", typ: types.T_uint64.ToType(), values: []uint64{1, 3, 2}, want: 2.0},
		{name: "float32", typ: types.T_float32.ToType(), values: []float32{1, 3, 2}, want: 2.0},
		{name: "float64", typ: types.T_float64.ToType(), values: []float64{1, 3, 2}, want: 2.0},
		{name: "decimal64", typ: types.New(types.T_decimal64, 10, 2), values: mustDecimal64s(t, "1.00", "2.00", "3.00"), want: "2.000"},
		{name: "decimal128", typ: types.New(types.T_decimal128, 20, 2), values: mustDecimal128s(t, "1.00", "2.00", "3.00"), want: "2.000"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			exec, err := makeMedian(mp, 1, false, tc.typ)
			require.NoError(t, err)
			require.NoError(t, exec.GroupGrow(1))

			vec := medianTestVector(t, mp, tc.typ, tc.values)
			require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
			require.NoError(t, exec.SetExtraInformation(nil, 0))
			require.GreaterOrEqual(t, exec.Size(), int64(0))

			ret, err := exec.Flush()
			require.NoError(t, err)
			require.Len(t, ret, 1)

			switch want := tc.want.(type) {
			case float64:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
			case string:
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
			}

			vec.Free(mp)
			ret[0].Free(mp)
			exec.Free()
		})
	}
}

func TestMedianDistinctAndErrorPaths(t *testing.T) {
	mp := mpool.MustNewZero()

	exec, err := makeMedian(mp, 2, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))
	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 3})
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
	other, err := makeMedian(mp, 2, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, other.GroupGrow(1))
	require.NoError(t, other.Fill(0, 0, []*vector.Vector{vec}))
	require.NoError(t, exec.Merge(other, 0, 0))
	require.NoError(t, exec.BatchMerge(other, 0, []uint64{1}))
	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, 2.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))

	_, err = makeMedian(mp, 3, false, types.T_varchar.ToType())
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type for median")

	ret[0].Free(mp)
	vec.Free(mp)
	exec.Free()
	other.Free()

	info := aggInfo{argTypes: []types.Type{types.New(types.T_decimal64, 10, 2)}}
	state := aggState{}
	require.NoError(t, state.init(mp, 1, 1, &aggInfo{argTypes: info.argTypes, saveArg: true}, false))
	require.NoError(t, state.fillArg(mp, 0, types.EncodeDecimal64(ptr(mustDecimal64s(t, "-1.00")[0])), false))
	require.NoError(t, state.fillArg(mp, 0, types.EncodeDecimal64(ptr(mustDecimal64s(t, "-2.00")[0])), false))
	v64, err := medianDecimal64FromState(state, 0, &info)
	require.NoError(t, err)
	require.Equal(t, "-1.500", v64.Format(3))
	state.free(mp)

	info128 := aggInfo{argTypes: []types.Type{types.New(types.T_decimal128, 20, 2)}}
	state128 := aggState{}
	require.NoError(t, state128.init(mp, 1, 1, &aggInfo{argTypes: info128.argTypes, saveArg: true}, false))
	vals128 := mustDecimal128s(t, "1.00", "3.00")
	require.NoError(t, state128.fillArg(mp, 0, types.EncodeDecimal128(&vals128[0]), false))
	require.NoError(t, state128.fillArg(mp, 0, types.EncodeDecimal128(&vals128[1]), false))
	v128, err := medianDecimal128FromState(state128, 0, &info128)
	require.NoError(t, err)
	require.Equal(t, "2.000", v128.Format(3))
	state128.free(mp)
}

func TestMedianMultipleGroupsAndNullHandling(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(3))

	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(vec, []int64{1, 2, 10, 20, 0, 0, 100}, []bool{false, false, false, false, true, true, false}, mp))
	defer vec.Free(mp)

	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 2, 2, 2, GroupNotMatched, 3}, []*vector.Vector{vec}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 1.5, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 15.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	require.Equal(t, 100.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 2))
	ret[0].Free(mp)
	exec.Free()
}

func TestMedianDistinctConstAndBatchFill(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(2))

	constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(7), 4, mp)
	require.NoError(t, err)
	defer constVec.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{constVec}))
	require.NoError(t, exec.BatchFill(0, []uint64{2, 2, 2, 2}, []*vector.Vector{constVec}))

	nonConst := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{7, 8, 8, 9})
	defer nonConst.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 1}, []*vector.Vector{nonConst}))

	ret, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 8.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	ret[0].Free(mp)
	exec.Free()
}

func TestMedianBatchMergeAcrossGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(2))
	require.NoError(t, right.GroupGrow(2))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 9, 3, 11})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{5, 13, 7, 15})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BatchFill(0, []uint64{1, 1, 2, 2}, []*vector.Vector{vecRight}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1, 2}))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 9.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestMedianDistinctBatchMergeDeduplicates(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	left, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	right, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))

	vecLeft := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 5})
	vecRight := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{3, 5, 7})
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 4.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestAccountedMedianResidentAndSpillRoundTrip(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "distinct"}[distinct], func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeAgg(
				mp, AggIdOfMedian, distinct, types.T_int64.ToType())
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
			require.NoError(t, exec.GroupGrow(3))

			input := buildFixedVec(t, mp, types.T_int64.ToType(),
				[]int64{9, 1, 5, 5, 2, 8, 4})
			require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
				0, []uint64{1, 1, 1, 1, 2, 2, GroupNotMatched},
				[]*vector.Vector{input}))
			require.NoError(t, exec.BatchFill(
				0, []uint64{1, 1, 1, 1, 2, 2, GroupNotMatched},
				[]*vector.Vector{input}))

			var encoded bytes.Buffer
			codec := exec.(SpillStateCodec)
			require.NoError(t, codec.SaveSpillIntermediateRows(
				0, []int32{0, 1, 2}, &encoded))

			restored, err := MakeAgg(
				mp, AggIdOfMedian, distinct, types.T_int64.ToType())
			require.NoError(t, err)
			restoredOwner := restored.(AllocationAccountOwner)
			require.NoError(t, restoredOwner.SetAllocationAccount(allocation))
			SyncAggregatorsToChunkSize([]AggFuncExec{restored}, AggBatchSize)
			require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
				bytes.NewReader(encoded.Bytes()), mp))

			resident, err := exec.Flush()
			require.NoError(t, err)
			spilled, err := restored.Flush()
			require.NoError(t, err)
			require.Len(t, resident, 1)
			require.Len(t, spilled, 1)
			require.True(t, vector.AllocationAccountSelectionsEqual(
				allocation.vectorSelection(), resident[0].AllocationAccountSelection()))
			require.True(t, vector.AllocationAccountSelectionsEqual(
				allocation.vectorSelection(), spilled[0].AllocationAccountSelection()))
			for row, want := range []float64{5, 5, 0} {
				if row == 2 {
					require.True(t, resident[0].IsNull(uint64(row)))
					require.True(t, spilled[0].IsNull(uint64(row)))
					continue
				}
				if distinct && row == 0 {
					want = 5
				}
				require.Equal(t, want,
					vector.GetFixedAtNoTypeCheck[float64](resident[0], row))
				require.Equal(t, want,
					vector.GetFixedAtNoTypeCheck[float64](spilled[0], row))
			}

			resident[0].Free(mp)
			spilled[0].Free(mp)
			input.Free(mp)
			exec.Free()
			restored.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedMedianRetainsOrdinaryInputWithoutSavedArgumentIndex(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeSingleGroupAgg(
		mp, AggIdOfMedian, false, nil, nil, types.T_int64.ToType())
	require.NoError(t, err)
	median := exec.(*medianColumnNumericExec[int64])
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
	require.True(t, median.usesDenseAccountedState())
	require.False(t, median.accounted.saveArg)
	require.ErrorIs(t, exec.PreAllocateGroups(2),
		mpool.ErrAllocationAccountInvariant)
	require.ErrorIs(t, exec.GroupGrow(2),
		mpool.ErrAllocationAccountInvariant)
	require.NoError(t, exec.GroupGrow(1))
	require.Nil(t, median.accounted.state[0].argSkl)
	require.Empty(t, median.accounted.state[0].argbuf)

	const rows = MaxVectorLength + hashmap.UnitLimit
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(rows - i)
	}
	input := buildFixedVec(t, mp, types.T_int64.ToType(), values)
	groups := make([]uint64, hashmap.UnitLimit)
	for i := range groups {
		groups[i] = 1
	}
	for offset := 0; offset < rows; offset += hashmap.UnitLimit {
		require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
			offset, groups, []*vector.Vector{input}))
		require.NoError(t, exec.BatchFill(
			offset, groups, []*vector.Vector{input}))
	}
	require.Equal(t, rows, median.groups[0].Length())
	require.Len(t, median.groups[0].vecs, 2)
	require.Equal(t, MaxVectorLength, median.groups[0].vecs[0].Length())
	require.Equal(t, hashmap.UnitLimit, median.groups[0].vecs[1].Length())
	require.Nil(t, median.accounted.state[0].argSkl)
	require.Empty(t, median.accounted.state[0].argbuf)
	require.Positive(t, account.Snapshot().Used)

	var spill bytes.Buffer
	require.Error(t, exec.(SpillStateCodec).SaveSpillIntermediateRows(
		0, []int32{0, 0}, io.Discard))
	require.NoError(t, exec.(SpillStateCodec).SaveSpillIntermediateRows(
		0, []int32{0}, &spill))
	retainedBytes := 0
	for _, vec := range median.groups[0].vecs {
		require.Equal(t, vec.Length()*types.T_int64.ToType().TypeSize(),
			len(vec.UnsafeGetRawData()))
		retainedBytes += len(vec.UnsafeGetRawData())
	}
	require.Equal(t, rows*types.T_int64.ToType().TypeSize(), retainedBytes)
	require.Equal(t, 8+4+8+retainedBytes+8, spill.Len())
	restored, err := MakeSingleGroupAgg(
		mp, AggIdOfMedian, false, nil, nil, types.T_int64.ToType())
	require.NoError(t, err)
	restoredMedian := restored.(*medianColumnNumericExec[int64])
	restoredOwner := restored.(AllocationAccountOwner)
	require.NoError(t, restoredOwner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{restored}, AggBatchSize)
	var invalidIntermediate bytes.Buffer
	require.NoError(t, types.WriteInt64(&invalidIntermediate, 2))
	require.Error(t, restored.UnmarshalFromReader(
		bytes.NewReader(invalidIntermediate.Bytes()), mp))
	var invalidSpill bytes.Buffer
	require.NoError(t, types.WriteUint64(&invalidSpill, spillMagicNumber))
	require.NoError(t, types.WriteInt32(&invalidSpill, 2))
	require.Error(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
		bytes.NewReader(invalidSpill.Bytes()), mp))
	require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
		bytes.NewReader(spill.Bytes()), mp))
	require.Equal(t, rows, restoredMedian.groups[0].Length())
	require.Len(t, restoredMedian.groups[0].vecs, 2)

	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, float64(rows+1)/2,
		vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	restoredResult, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, float64(rows+1)/2,
		vector.GetFixedAtNoTypeCheck[float64](restoredResult[0], 0))
	result[0].Free(mp)
	restoredResult[0].Free(mp)
	input.Free(mp)
	exec.Free()
	restored.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestGroupedAccountedMedianDoesNotMaterializePerGroupVectors(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeGroupAgg(
		mp, AggIdOfMedian, false, allocation, nil, types.T_int64.ToType())
	require.NoError(t, err)
	median := exec.(*medianColumnNumericExec[int64])
	owner := exec.(AllocationAccountOwner)
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
	require.False(t, median.usesDenseAccountedState())
	require.True(t, median.accounted.saveArg)

	// Prospective high-cardinality GROUP BY state must stay in the accounted
	// chunk representation. In particular, it must not create one Go-heap
	// Vectors/vector holder pair for every unpublished group id.
	const prospectiveGroups = 100_000
	require.NoError(t, exec.PreAllocateGroups(prospectiveGroups))
	firstReservation := account.Snapshot().Used
	require.Positive(t, firstReservation)
	require.Positive(t, mp.CurrNB())
	require.NoError(t, exec.PreAllocateGroups(prospectiveGroups))
	require.Equal(t, firstReservation, account.Snapshot().Used)
	require.Zero(t, median.GetNumGroups())
	require.Empty(t, median.groups)

	input := buildFixedVec(t, mp, types.T_int64.ToType(),
		[]int64{1, 3, 5, 7})
	groups := []uint64{1, 1, 2, 2}
	require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
		0, groups, []*vector.Vector{input}))
	require.NoError(t, exec.GroupGrow(2))
	require.NoError(t, exec.BatchFill(
		0, groups, []*vector.Vector{input}))

	results, err := exec.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, []float64{2, 6},
		vector.MustFixedColNoTypeCheck[float64](results[0]))

	results[0].Free(mp)
	input.Free(mp)
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestLegacyMedianConstructorRespectsStaticGroupBound(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	grouped, err := MakeGroupAggWithLegacyTextMinMax(
		mp, AggIdOfMedian, false, allocation, nil, types.T_int64.ToType())
	require.NoError(t, err)
	single, err := MakeSingleGroupAggWithLegacyTextMinMax(
		mp, AggIdOfMedian, false, allocation, nil, types.T_int64.ToType())
	require.NoError(t, err)
	require.False(t,
		grouped.(*medianColumnNumericExec[int64]).usesDenseAccountedState())
	require.True(t,
		single.(*medianColumnNumericExec[int64]).usesDenseAccountedState())

	grouped.Free()
	single.Free()
	require.NoError(t, grouped.ClearAllocationAccount(allocation))
	require.NoError(t, single.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedMedianDirectFillAndMergeMethods(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	leftExec, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	rightExec, err := MakeAgg(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	leftOwner := leftExec.(AllocationAccountOwner)
	rightOwner := rightExec.(AllocationAccountOwner)
	require.NoError(t, leftOwner.SetAllocationAccount(allocation))
	require.NoError(t, rightOwner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{leftExec, rightExec}, AggBatchSize)
	require.NoError(t, leftExec.GroupGrow(2))
	require.NoError(t, rightExec.GroupGrow(3))

	// BulkFill must split a large direct input into bounded work units while
	// preserving one logical group. Fill must retain scalar-broadcast semantics
	// when the requested logical row is beyond the const vector's physical row.
	values := make([]int64, hashmap.UnitLimit+1)
	for i := range values {
		values[i] = int64(i + 1)
	}
	bulk := buildFixedVec(t, mp, types.T_int64.ToType(), values)
	require.NoError(t, leftExec.BulkFill(0, []*vector.Vector{bulk}))
	constant, err := vector.NewConstFixed(
		types.T_int64.ToType(), int64(99), 4, mp)
	require.NoError(t, err)
	require.NoError(t, rightExec.(BatchCapacityPreflight).PreflightBatchFill(
		0, []uint64{1, 2}, []*vector.Vector{constant}))
	require.NoError(t, rightExec.BatchFill(
		0, []uint64{1, 2}, []*vector.Vector{constant}))
	require.NoError(t, rightExec.Fill(1, 3, []*vector.Vector{constant}))
	nullable := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		nullable, []int64{7, 0}, []bool{false, true}, mp))
	require.NoError(t, leftExec.(BatchCapacityPreflight).PreflightBatchFill(
		0, []uint64{1, 2}, []*vector.Vector{nullable}))
	require.NoError(t, leftExec.BatchFill(
		0, []uint64{1, 2}, []*vector.Vector{nullable}))
	require.ErrorIs(t, leftExec.BatchFill(
		-1, []uint64{1}, []*vector.Vector{nullable}),
		mpool.ErrAllocationAccountInvalid)

	// Exercise both direct Merge and the scheduler's preflight + BatchMerge
	// pair. The not-matched entry must not publish or copy a group.
	require.NoError(t, leftExec.Merge(rightExec, 1, 1))
	require.NoError(t, leftExec.(BatchCapacityPreflight).PreflightBatchMerge(
		rightExec, 0, []uint64{1, 2}))
	require.NoError(t, leftExec.BatchMerge(
		rightExec, 0, []uint64{1, 2}))
	require.NoError(t, leftExec.(BatchCapacityPreflight).PreflightBatchMerge(
		rightExec, 0, []uint64{GroupNotMatched}))
	require.NoError(t, leftExec.BatchMerge(
		rightExec, 0, []uint64{GroupNotMatched}))
	require.NoError(t, leftExec.(BatchCapacityPreflight).PreflightBatchMerge(
		rightExec, 2, []uint64{1}))
	require.NoError(t, leftExec.BatchMerge(rightExec, 2, []uint64{1}))
	require.Positive(t, leftExec.Size())

	result, err := leftExec.Flush()
	require.NoError(t, err)
	require.Equal(t, 128.0,
		vector.GetFixedAtNoTypeCheck[float64](result[0], 0))
	require.Equal(t, 99.0,
		vector.GetFixedAtNoTypeCheck[float64](result[0], 1))

	result[0].Free(mp)
	bulk.Free(mp)
	constant.Free(mp)
	nullable.Free(mp)
	leftExec.Free()
	rightExec.Free()
	require.NoError(t, leftOwner.ClearAllocationAccount(allocation))
	require.NoError(t, rightOwner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestVectorsPreExtendBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	_, err := newAccountedVectors[int64](types.T_int64.ToType(), nil)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, (*Vectors[int64])(nil).PreExtend(1, mp),
		mpool.ErrAllocationAccountInvalid)

	preextended := NewVectors[int64](types.T_int64.ToType())
	require.NoError(t, preextended.PreExtend(0, mp))
	require.NoError(t, preextended.vecs[0].PreExtend(MaxVectorLength, mp))
	preextended.vecs[0].SetLength(MaxVectorLength)
	require.NoError(t, preextended.PreExtend(1, mp))
	require.Len(t, preextended.vecs, 2)
	preextended.Free(mp)

	appended := NewVectors[int64](types.T_int64.ToType())
	require.NoError(t, appended.vecs[0].PreExtend(MaxVectorLength, mp))
	appended.vecs[0].SetLength(MaxVectorLength)
	require.NoError(t, AppendMultiFixed(appended, int64(7), false, 1, mp))
	require.Len(t, appended.vecs, 2)
	require.Equal(t, int64(7),
		vector.GetFixedAtNoTypeCheck[int64](appended.vecs[1], 0))
	appended.Free(mp)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedMedianKeepsIndexedStateForDistinctAndOrderedPercentile(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int64
	}{
		{name: "distinct-median", id: AggIdOfMedian},
		{name: "ordered-percentile", id: AggIdOfPercentileCont},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeAgg(
				mp, tc.id, tc.id == AggIdOfMedian, types.T_int64.ToType())
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			require.NoError(t, exec.GroupGrow(1))

			switch typed := exec.(type) {
			case *medianColumnNumericExec[int64]:
				require.False(t, typed.usesDenseAccountedState())
				require.True(t, typed.accounted.saveArg)
				require.NotNil(t, typed.accounted.state[0].argSkl)
			case *orderedPercentileExec[int64, float64]:
				require.False(t, typed.usesDenseAccountedState())
				require.True(t, typed.accounted.saveArg)
				require.NotNil(t, typed.accounted.state[0].argSkl)
			default:
				t.Fatalf("unexpected aggregate type %T", exec)
			}

			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedMedianDenseDecimalTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		typ    types.Type
		values any
	}{
		{
			name:   "decimal64",
			typ:    types.New(types.T_decimal64, 10, 2),
			values: mustDecimal64s(t, "1.00", "3.00", "2.00", "4.00", "6.00"),
		},
		{
			name:   "decimal128",
			typ:    types.New(types.T_decimal128, 20, 2),
			values: mustDecimal128s(t, "1.00", "3.00", "2.00", "4.00", "6.00"),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeSingleGroupAgg(
				mp, AggIdOfMedian, false, nil, nil, tc.typ)
			require.NoError(t, err)
			owner := exec.(AllocationAccountOwner)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
			switch typed := exec.(type) {
			case *medianColumnDecimalExec[types.Decimal64]:
				require.True(t, typed.usesDenseAccountedState())
			case *medianColumnDecimalExec[types.Decimal128]:
				require.True(t, typed.usesDenseAccountedState())
			default:
				t.Fatalf("unexpected median type %T", exec)
			}
			require.NoError(t, exec.GroupGrow(1))

			input := medianTestVector(t, mp, tc.typ, tc.values)
			groups := []uint64{1, 1, 1, 1, 1}
			require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
				0, groups, []*vector.Vector{input}))
			require.NoError(t, exec.BatchFill(
				0, groups, []*vector.Vector{input}))

			results, err := exec.Flush()
			require.NoError(t, err)
			require.Equal(t, "3.000",
				vector.GetFixedAtNoTypeCheck[types.Decimal128](results[0], 0).
					Format(results[0].GetType().Scale))

			results[0].Free(mp)
			input.Free(mp)
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedMedianPreservesFloatNaNSemantics(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	legacy, err := MakeAgg(mp, AggIdOfMedian, false, types.T_float64.ToType())
	require.NoError(t, err)
	accounted, err := MakeSingleGroupAgg(
		mp, AggIdOfMedian, false, nil, nil, types.T_float64.ToType())
	require.NoError(t, err)
	owner := accounted.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{legacy, accounted}, AggBatchSize)
	require.NoError(t, legacy.GroupGrow(1))
	require.NoError(t, accounted.GroupGrow(1))

	input := buildFixedVec(t, mp, types.T_float64.ToType(),
		[]float64{1, math.NaN(), 2, 3})
	groups := []uint64{1, 1, 1, 1}
	require.NoError(t, legacy.BatchFill(
		0, groups, []*vector.Vector{input}))
	require.NoError(t, accounted.(BatchCapacityPreflight).PreflightBatchFill(
		0, groups, []*vector.Vector{input}))
	require.NoError(t, accounted.BatchFill(
		0, groups, []*vector.Vector{input}))

	legacyResult, err := legacy.Flush()
	require.NoError(t, err)
	accountedResult, err := accounted.Flush()
	require.NoError(t, err)
	require.Equal(t, 2.5,
		vector.GetFixedAtNoTypeCheck[float64](legacyResult[0], 0))
	require.Equal(t,
		math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](legacyResult[0], 0)),
		math.Float64bits(vector.GetFixedAtNoTypeCheck[float64](accountedResult[0], 0)))

	legacyResult[0].Free(mp)
	accountedResult[0].Free(mp)
	input.Free(mp)
	legacy.Free()
	accounted.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestDenseMedianSelectorAcrossSegments(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	ints := accountedMedianTestVectors(t, mp, allocation,
		types.T_int64.ToType(), [][]int64{{9, 1}, {4, 4, -2}, {8, 3}})
	gotInt, err := denseMedianNumeric(ints, allocation, mp)
	require.NoError(t, err)
	require.Equal(t, 4.0, gotInt)
	ints.Free(mp)

	floats := accountedMedianTestVectors(t, mp, allocation,
		types.T_float64.ToType(), [][]float64{{math.NaN(), 0}, {math.NaN()}})
	gotFloat, err := denseMedianNumeric(floats, allocation, mp)
	require.NoError(t, err)
	require.True(t, math.IsNaN(gotFloat))
	floats.Free(mp)

	decimal64s := accountedMedianTestVectors(t, mp, allocation,
		types.New(types.T_decimal64, 10, 2), [][]types.Decimal64{
			mustDecimal64s(t, "1.00", "3.00"),
			mustDecimal64s(t, "2.00", "4.00"),
		})
	gotDecimal64, err := denseMedianDecimal64(decimal64s, allocation, mp)
	require.NoError(t, err)
	require.Equal(t, "2.500", gotDecimal64.Format(3))
	decimal64s.Free(mp)

	decimal128s := accountedMedianTestVectors(t, mp, allocation,
		types.New(types.T_decimal128, 20, 2), [][]types.Decimal128{
			mustDecimal128s(t, "1.00", "3.00"),
			mustDecimal128s(t, "2.00", "4.00"),
		})
	gotDecimal128, err := denseMedianDecimal128(decimal128s, allocation, mp)
	require.NoError(t, err)
	require.Equal(t, "2.500", gotDecimal128.Format(3))
	decimal128s.Free(mp)

	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedMedianFinalizationExactCap(t *testing.T) {
	run := func(limit uint64) (uint64, error) {
		mp := mpool.MustNewZero()
		registry, err := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		allocation, err := NewAllocationAccount(
			account, mpool.AllocationOwnerGroup, AllocationAccountSites{
				VectorData: 1, VectorArea: 2, VectorNulls: 3,
				VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
			})
		require.NoError(t, err)

		exec, err := MakeSingleGroupAgg(
			mp, AggIdOfMedian, false, nil, nil, types.T_int64.ToType())
		require.NoError(t, err)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
		input := buildFixedVec(t, mp, types.T_int64.ToType(),
			[]int64{9, 1, 7, 3, 5})
		groups := []uint64{1, 1, 1, 1, 1}
		if err = exec.GroupGrow(1); err == nil {
			err = exec.(BatchCapacityPreflight).PreflightBatchFill(
				0, groups, []*vector.Vector{input})
		}
		if err == nil {
			err = exec.BatchFill(0, groups, []*vector.Vector{input})
		}
		var results []*vector.Vector
		if err == nil {
			results, err = exec.Flush()
			if err == nil {
				require.Equal(t, 5.0,
					vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
			}
		}
		peak := account.Snapshot().Peak
		for _, result := range results {
			result.Free(mp)
		}
		input.Free(mp)
		exec.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
		return peak, err
	}

	peak, err := run(128 << 20)
	require.NoError(t, err)
	require.Positive(t, peak)
	exactPeak, err := run(peak)
	require.NoError(t, err)
	require.Equal(t, peak, exactPeak)
	_, err = run(peak - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
}

func TestMedianStableIntermediateCompatibilityAcrossAllocationModes(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(map[bool]string{false: "ordinary", true: "distinct"}[distinct], func(t *testing.T) {
			for _, direction := range []struct {
				name            string
				accountedSource bool
			}{
				{name: "accounted-to-legacy", accountedSource: true},
				{name: "legacy-to-accounted", accountedSource: false},
			} {
				t.Run(direction.name, func(t *testing.T) {
					mp := mpool.MustNewZero()
					registry, account, allocation := newTestAggregateAllocation(t)
					source, err := MakeAgg(
						mp, AggIdOfMedian, distinct, types.T_int64.ToType())
					require.NoError(t, err)
					target, err := MakeAgg(
						mp, AggIdOfMedian, distinct, types.T_int64.ToType())
					require.NoError(t, err)
					var accounted AggFuncExec
					if direction.accountedSource {
						accounted = source
					} else {
						accounted = target
					}
					owner := accounted.(AllocationAccountOwner)
					require.NoError(t, owner.SetAllocationAccount(allocation))
					SyncAggregatorsToChunkSize([]AggFuncExec{source, target}, AggBatchSize)

					require.NoError(t, source.GroupGrow(2))
					input := buildFixedVec(t, mp, types.T_int64.ToType(),
						[]int64{9, 1, 5, 5, 2, 8})
					groups := []uint64{1, 1, 1, 1, 2, 2}
					if direction.accountedSource {
						require.NoError(t, source.(BatchCapacityPreflight).PreflightBatchFill(
							0, groups, []*vector.Vector{input}))
					}
					require.NoError(t, source.BatchFill(
						0, groups, []*vector.Vector{input}))
					var encoded bytes.Buffer
					require.NoError(t, source.SaveIntermediateResult(
						2, [][]uint8{{1, 1}}, &encoded))
					require.NoError(t, target.UnmarshalFromReader(
						bytes.NewReader(encoded.Bytes()), mp))
					results, err := target.Flush()
					require.NoError(t, err)
					require.Equal(t, 5.0,
						vector.GetFixedAtNoTypeCheck[float64](results[0], 0))
					require.Equal(t, 5.0,
						vector.GetFixedAtNoTypeCheck[float64](results[0], 1))

					for _, result := range results {
						result.Free(mp)
					}
					input.Free(mp)
					source.Free()
					target.Free()
					require.NoError(t, owner.ClearAllocationAccount(allocation))
					finishTestAggregateAllocation(t, registry, account)
					require.Zero(t, mp.CurrNB())
				})
			}
		})
	}
}

func TestAccountedMedianChunkIntermediateCompatibility(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	source, err := MakeAgg(
		mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	owner := source.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	SyncAggregatorsToChunkSize([]AggFuncExec{source}, AggBatchSize)
	require.NoError(t, source.GroupGrow(AggBatchSize+1))
	input := buildFixedVec(t, mp, types.T_int64.ToType(),
		[]int64{9, 1, 2, 8})
	require.NoError(t, source.(BatchCapacityPreflight).PreflightBatchFill(
		0, []uint64{1, 1, AggBatchSize + 1, AggBatchSize + 1}, []*vector.Vector{input}))
	require.NoError(t, source.BatchFill(
		0, []uint64{1, 1, AggBatchSize + 1, AggBatchSize + 1}, []*vector.Vector{input}))

	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResultOfChunk(1, &encoded))
	target, err := MakeAgg(
		mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	results, err := target.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 5.0,
		vector.GetFixedAtNoTypeCheck[float64](results[0], 0))

	results[0].Free(mp)
	input.Free(mp)
	source.Free()
	target.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestMedianIntermediateRoundTripMultipleGroups(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(3))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 2, 4, 6, 8, 10})
	defer vec.Free(mp)
	require.NoError(t, exec.BatchFill(0, []uint64{1, 1, 1, 2, 2, 3, 3}, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(3, [][]uint8{{1, 1, 1}}, &buf))

	restored, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	ret, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 1.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	require.Equal(t, 5.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 1))
	require.Equal(t, 9.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 2))
	ret[0].Free(mp)
	exec.Free()
	restored.Free()
}

func TestMedianDecimalDistinctMerge(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	typ := types.New(types.T_decimal64, 10, 2)
	left, err := makeMedian(mp, AggIdOfMedian, true, typ)
	require.NoError(t, err)
	right, err := makeMedian(mp, AggIdOfMedian, true, typ)
	require.NoError(t, err)
	require.NoError(t, left.GroupGrow(1))
	require.NoError(t, right.GroupGrow(1))

	vecLeft := medianTestVector(t, mp, typ, mustDecimal64s(t, "1.00", "3.00", "5.00"))
	vecRight := medianTestVector(t, mp, typ, mustDecimal64s(t, "3.00", "7.00", "9.00"))
	defer vecLeft.Free(mp)
	defer vecRight.Free(mp)

	require.NoError(t, left.BulkFill(0, []*vector.Vector{vecLeft}))
	require.NoError(t, right.BulkFill(0, []*vector.Vector{vecRight}))
	require.NoError(t, left.Merge(right, 0, 0))

	ret, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, "5.000", vector.GetFixedAtNoTypeCheck[types.Decimal128](ret[0], 0).Format(ret[0].GetType().Scale))
	ret[0].Free(mp)
	left.Free()
	right.Free()
}

func TestMedianNumericValsAvoidsInt64Overflow(t *testing.T) {
	vals := []int64{math.MaxInt64, math.MaxInt64}
	require.Equal(t, float64(math.MaxInt64), medianNumericVals(vals))
}

func TestMedianIntermediateRoundTripRejectsInvalidGroupCount(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3})
	defer vec.Free(mp)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	broken := append([]byte(nil), buf.Bytes()...)
	reader := bytes.NewReader(broken)
	probe, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	probeExec := probe.(*medianColumnNumericExec[int64])
	_, err = unmarshalFromReaderNoGroup(reader, &probeExec.ret.optSplitResult)
	require.NoError(t, err)
	offset := len(broken) - reader.Len()
	copy(broken[offset:offset+8], types.EncodeInt64(ptr(int64(1<<30))))
	probe.Free()

	restored, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	err = restored.UnmarshalFromReader(bytes.NewReader(broken), mp)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid group count")
	restored.Free()
	exec.Free()
}

func TestMedianDistinctRoundTripRebuildsDistinctState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	exec, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, exec.GroupGrow(1))

	vec := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 3, 3})
	defer vec.Free(mp)
	require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))

	var buf bytes.Buffer
	require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))

	restored, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(buf.Bytes()), mp))

	more := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{3, 5})
	defer more.Free(mp)
	require.NoError(t, restored.BulkFill(0, []*vector.Vector{more}))

	ret, err := restored.Flush()
	require.NoError(t, err)
	require.Len(t, ret, 1)
	require.Equal(t, 3.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))
	ret[0].Free(mp)
	restored.Free()
	exec.Free()
}

func TestMedianRepeatedUnmarshalReplacesOwnedState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Equal(t, int64(0), mp.CurrNB())
	}()

	encode := func(values []int64) []byte {
		exec, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
		require.NoError(t, err)
		require.NoError(t, exec.GroupGrow(1))
		vec := buildFixedVec(t, mp, types.T_int64.ToType(), values)
		require.NoError(t, exec.BulkFill(0, []*vector.Vector{vec}))
		var buf bytes.Buffer
		require.NoError(t, exec.SaveIntermediateResult(1, [][]uint8{{1}}, &buf))
		vec.Free(mp)
		exec.Free()
		return append([]byte(nil), buf.Bytes()...)
	}

	first := encode([]int64{1, 3})
	second := encode([]int64{10, 20})
	restored, err := makeMedian(mp, AggIdOfMedian, true, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(first), mp))
	require.NoError(t, restored.UnmarshalFromReader(bytes.NewReader(second), mp))

	// The first record's DISTINCT map must not affect the replacement record.
	// Value 1 appears only in the first record and therefore remains admissible.
	more := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1})
	require.NoError(t, restored.BulkFill(0, []*vector.Vector{more}))
	ret, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, 10.0, vector.GetFixedAtNoTypeCheck[float64](ret[0], 0))

	ret[0].Free(mp)
	more.Free(mp)
	restored.Free()
}

func TestMedianFailedUnmarshalPreservesOwnedState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	target, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, target.GroupGrow(1))
	old := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{99})
	require.NoError(t, target.BulkFill(0, []*vector.Vector{old}))

	source, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, source.GroupGrow(1))
	fresh := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2})
	require.NoError(t, source.BulkFill(0, []*vector.Vector{fresh}))
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(
		1, [][]uint8{{1}}, &encoded))
	broken := encoded.Bytes()[:encoded.Len()-1]

	require.Error(t, target.UnmarshalFromReader(bytes.NewReader(broken), mp))
	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, 99.0,
		vector.GetFixedAtNoTypeCheck[float64](result[0], 0))

	result[0].Free(mp)
	fresh.Free(mp)
	old.Free(mp)
	source.Free()
	target.Free()
}

func TestMedianEmptyIntermediateRoundTripReplacesState(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	empty, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, empty.SaveIntermediateResult(0, nil, &encoded))
	empty.Free()

	target, err := makeMedian(mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, target.GroupGrow(1))
	old := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{99})
	require.NoError(t, target.BulkFill(0, []*vector.Vector{old}))
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))

	// An empty replacement remains a reusable zero-group executor.
	require.NoError(t, target.GroupGrow(1))
	fresh := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{7})
	require.NoError(t, target.BulkFill(0, []*vector.Vector{fresh}))
	result, err := target.Flush()
	require.NoError(t, err)
	require.Equal(t, 7.0, vector.GetFixedAtNoTypeCheck[float64](result[0], 0))

	result[0].Free(mp)
	fresh.Free(mp)
	old.Free(mp)
	target.Free()
}

func TestSingleGroupMedianContractSurvivesEmptyIntermediate(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)

	source, err := MakeAgg(
		mp, AggIdOfMedian, false, types.T_int64.ToType())
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(0, nil, &encoded))
	source.Free()

	target, err := MakeSingleGroupAgg(
		mp, AggIdOfMedian, false, nil, nil, types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	owner := target.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.True(t,
		target.(*medianColumnNumericExec[int64]).usesDenseAccountedState())
	require.NoError(t, target.GroupGrow(1))

	target.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestSelectKthFuncHandlesDuplicateHeavyInput(t *testing.T) {
	vals := make([]int, 4096)
	comparisons := 0

	got := selectKthFunc(vals, len(vals)/2, func(a, b int) int {
		comparisons++
		switch {
		case a < b:
			return -1
		case a > b:
			return 1
		default:
			return 0
		}
	})

	require.Equal(t, 0, got)
	require.LessOrEqual(t, comparisons, len(vals)*2)
}

func medianTestVector(t *testing.T, mp *mpool.MPool, typ types.Type, values any) *vector.Vector {
	t.Helper()
	v := vector.NewVec(typ)
	switch typ.Oid {
	case types.T_bit, types.T_uint64:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint64), nil, mp))
	case types.T_int8:
		require.NoError(t, vector.AppendFixedList(v, values.([]int8), nil, mp))
	case types.T_int16:
		require.NoError(t, vector.AppendFixedList(v, values.([]int16), nil, mp))
	case types.T_int32:
		require.NoError(t, vector.AppendFixedList(v, values.([]int32), nil, mp))
	case types.T_int64:
		require.NoError(t, vector.AppendFixedList(v, values.([]int64), nil, mp))
	case types.T_uint8:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint8), nil, mp))
	case types.T_uint16:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint16), nil, mp))
	case types.T_uint32:
		require.NoError(t, vector.AppendFixedList(v, values.([]uint32), nil, mp))
	case types.T_float32:
		require.NoError(t, vector.AppendFixedList(v, values.([]float32), nil, mp))
	case types.T_float64:
		require.NoError(t, vector.AppendFixedList(v, values.([]float64), nil, mp))
	case types.T_decimal64:
		require.NoError(t, vector.AppendFixedList(v, values.([]types.Decimal64), nil, mp))
	case types.T_decimal128:
		require.NoError(t, vector.AppendFixedList(v, values.([]types.Decimal128), nil, mp))
	default:
		t.Fatalf("unsupported test type %v", typ.Oid)
	}
	return v
}

func accountedMedianTestVectors[T numeric | types.Decimal64 | types.Decimal128](
	t *testing.T,
	mp *mpool.MPool,
	allocation *AllocationAccount,
	typ types.Type,
	segments [][]T,
) *Vectors[T] {
	t.Helper()
	ret := &Vectors[T]{allocation: allocation.vectorSelection()}
	for _, values := range segments {
		vec, err := allocation.newVector(typ)
		require.NoError(t, err)
		require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
		ret.vecs = append(ret.vecs, vec)
	}
	ret.appendAt = len(ret.vecs) - 1
	return ret
}

func mustDecimal64s(t *testing.T, vals ...string) []types.Decimal64 {
	t.Helper()
	ret := make([]types.Decimal64, len(vals))
	for i, v := range vals {
		d, err := types.ParseDecimal64(v, 10, 2)
		require.NoError(t, err)
		ret[i] = d
	}
	return ret
}

func mustDecimal128s(t *testing.T, vals ...string) []types.Decimal128 {
	t.Helper()
	ret := make([]types.Decimal128, len(vals))
	for i, v := range vals {
		d, err := types.ParseDecimal128(v, 20, 2)
		require.NoError(t, err)
		ret[i] = d
	}
	return ret
}
