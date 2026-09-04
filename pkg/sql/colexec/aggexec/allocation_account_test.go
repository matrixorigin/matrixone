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
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type aggregateAllocationTestCase struct {
	name     string
	id       int64
	distinct bool
	params   []types.Type
}

func newTestAggregateAllocation(
	t *testing.T,
) (*mpool.AllocationAccountRegistry, *mpool.AllocationAccount, *AllocationAccount) {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err := registry.Open(128 << 20)
	require.NoError(t, err)
	allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData:     1,
		VectorArea:     2,
		VectorNulls:    3,
		VectorGrouping: 4,
		ArgumentCount:  5,
		ArgumentArena:  6,
	})
	require.NoError(t, err)
	return registry, account, allocation
}

func finishTestAggregateAllocation(
	t *testing.T,
	registry *mpool.AllocationAccountRegistry,
	account *mpool.AllocationAccount,
) {
	t.Helper()
	require.Zero(t, account.Snapshot().Used)
	account.Seal()
	_, err := registry.Finalize(account)
	require.NoError(t, err)
}

// This table is the executable counterpart of the M3 aggregate capability
// matrix. Every aggregate ID accepted by makeSpecialAggExec is represented in
// either this supported table or the controlled-rejection table below;
// repeated IDs exercise distinct implementation-family branches.
func TestAllocationAccountSupportedAggregateFamilies(t *testing.T) {
	tests := []aggregateAllocationTestCase{
		{name: "bit-and", id: AggIdOfBitAnd, params: []types.Type{types.T_int64.ToType()}},
		{name: "bit-or", id: AggIdOfBitOr, params: []types.Type{types.T_int64.ToType()}},
		{name: "bit-xor", id: AggIdOfBitXor, params: []types.Type{types.T_int64.ToType()}},
		{name: "var-pop", id: AggIdOfVarPop, params: []types.Type{types.T_int64.ToType()}},
		{name: "stddev-pop-distinct", id: AggIdOfStdDevPop, distinct: true, params: []types.Type{types.T_int64.ToType()}},
		{name: "var-sample", id: AggIdOfVarSample, params: []types.Type{types.T_int64.ToType()}},
		{name: "stddev-sample", id: AggIdOfStdDevSample, params: []types.Type{types.T_int64.ToType()}},
		{name: "any", id: AggIdOfAny, params: []types.Type{types.T_varchar.ToType()}},
		{name: "min", id: AggIdOfMin, params: []types.Type{types.T_varchar.ToType()}},
		{name: "max", id: AggIdOfMax, params: []types.Type{types.T_int64.ToType()}},
		{name: "max-by", id: AggIdOfMaxBy, params: []types.Type{
			types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
		}},
		{name: "max-by-non-null", id: AggIdOfMaxByNonNull, params: []types.Type{
			types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
		}},
		{name: "sum", id: AggIdOfSum, params: []types.Type{types.T_int64.ToType()}},
		{name: "sum-distinct-decimal", id: AggIdOfSum, distinct: true, params: []types.Type{types.T_decimal64.ToType()}},
		{name: "avg", id: AggIdOfAvg, params: []types.Type{types.T_int64.ToType()}},
		{name: "avg-distinct", id: AggIdOfAvg, distinct: true, params: []types.Type{types.T_float64.ToType()}},
		{name: "count-column", id: AggIdOfCountColumn, params: []types.Type{types.T_int64.ToType()}},
		{name: "count-column-distinct", id: AggIdOfCountColumn, distinct: true, params: []types.Type{types.T_int64.ToType()}},
		{name: "count-star", id: AggIdOfCountStar},
		{name: "median", id: AggIdOfMedian, params: []types.Type{types.T_int64.ToType()}},
		{name: "percentile-cont", id: AggIdOfPercentileCont, params: []types.Type{types.T_int64.ToType()}},
		{name: "percentile-disc", id: AggIdOfPercentileDisc, params: []types.Type{types.T_int64.ToType()}},
		{name: "approx-percentile", id: AggIdOfApproxPercentile, params: []types.Type{types.T_int64.ToType()}},
		{name: "approx-count", id: AggIdOfApproxCount, params: []types.Type{types.T_int64.ToType()}},
		{name: "approx-count-distinct", id: AggIdOfApproxCountDistinct, params: []types.Type{types.T_int64.ToType()}},
		{name: "hll-add", id: AggIdOfHllAdd, params: []types.Type{types.T_int64.ToType()}},
		{name: "hll-merge", id: AggIdOfHllMerge, params: []types.Type{types.T_varbinary.ToType()}},
		{name: "bitmap-construct", id: AggIdOfBitmapConstruct, params: []types.Type{types.T_uint64.ToType()}},
		{name: "bitmap-or", id: AggIdOfBitmapOr, params: []types.Type{types.T_varbinary.ToType()}},
		{name: "json-array", id: AggIdOfJsonArrayAgg, params: []types.Type{types.T_json.ToType()}},
		{name: "json-object", id: AggIdOfJsonObjectAgg, params: []types.Type{types.T_varchar.ToType(), types.T_json.ToType()}},
		{name: "group-concat", id: AggIdOfGroupConcat, params: []types.Type{types.T_varchar.ToType()}},
		{name: "group-concat-distinct", id: AggIdOfGroupConcat, distinct: true, params: []types.Type{types.T_varchar.ToType()}},
		{name: "avg-tw-cache", id: AggIdOfAvgTwCache, params: []types.Type{types.T_int64.ToType()}},
		{name: "avg-tw-result", id: AggIdOfAvgTwResult, params: []types.Type{types.T_char.ToType()}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeGroupAgg(
				mp, tc.id, tc.distinct, allocation, nil, tc.params...)
			require.NoError(t, err)
			if tc.id == AggIdOfApproxPercentile ||
				tc.id == AggIdOfPercentileCont || tc.id == AggIdOfPercentileDisc {
				require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))
			}

			require.NoError(t, exec.GroupGrow(1))
			results, err := exec.Flush()
			require.NoError(t, err)
			require.NotEmpty(t, results)
			for _, result := range results {
				require.True(t, vector.AllocationAccountSelectionsEqual(
					allocation.vectorSelection(), result.AllocationAccountSelection()))
				result.Free(mp)
			}
			exec.Free()
			require.NoError(t, exec.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestMakeGroupAggRejectsNonGroupFamilies(t *testing.T) {
	tests := []aggregateAllocationTestCase{
		{name: "bit-and-distinct", id: AggIdOfBitAnd, distinct: true,
			params: []types.Type{types.T_int64.ToType()}},
		{name: "row-number", id: WinIdOfRowNumber},
		{name: "rank", id: WinIdOfRank},
		{name: "dense-rank", id: WinIdOfDenseRank},
		{name: "percent-rank", id: WinIdOfPercentRank},
		{name: "ntile", id: WinIdOfNtile, params: []types.Type{types.T_int64.ToType()}},
		{name: "cume-dist", id: WinIdOfCumeDist},
		{name: "lag", id: WinIdOfLag, params: []types.Type{types.T_int64.ToType()}},
		{name: "lead", id: WinIdOfLead, params: []types.Type{types.T_int64.ToType()}},
		{name: "first-value", id: WinIdOfFirstValue, params: []types.Type{types.T_int64.ToType()}},
		{name: "last-value", id: WinIdOfLastValue, params: []types.Type{types.T_int64.ToType()}},
		{name: "nth-value", id: WinIdOfNthValue, params: []types.Type{types.T_int64.ToType()}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			exec, err := MakeGroupAgg(
				mp, tc.id, tc.distinct, nil, nil, tc.params...)
			require.Error(t, err)
			require.Nil(t, exec)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedGroupConcatFinalizationExactCap(t *testing.T) {
	const value = "accounted-group-concat-payload-"
	payload := strings.Repeat(value, 256)
	run := func(limit uint64) (uint64, error) {
		mp := mpool.MustNewZero()
		registry, err := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		allocation, err := NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
			VectorData:     1,
			VectorArea:     2,
			VectorNulls:    3,
			VectorGrouping: 4,
			ArgumentCount:  5,
			ArgumentArena:  6,
		})
		require.NoError(t, err)

		exec, err := MakeAgg(mp, AggIdOfGroupConcat, false, types.T_varchar.ToType())
		require.NoError(t, err)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		input := vector.NewVec(types.T_varchar.ToType())
		require.NoError(t, vector.AppendBytes(input, []byte(payload), false, mp))

		if err = exec.GroupGrow(1); err == nil {
			err = exec.Fill(0, 0, []*vector.Vector{input})
		}
		var results []*vector.Vector
		if err == nil {
			results, err = exec.Flush()
			if err == nil {
				require.Len(t, results, 1)
				require.Equal(t, payload, string(results[0].GetBytesAt(0)))
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
	require.Error(t, err)
	require.True(t, errors.Is(err, mpool.ErrAllocationAccountCapacity), err)
}

func TestAccountedDistinctGroupConcatDeduplicatesAndMerges(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	makeExec := func() AggFuncExec {
		exec := newGroupConcatExec(mp, multiAggInfo{
			aggID:    AggIdOfGroupConcat,
			distinct: true,
			argTypes: []types.Type{
				types.T_varchar.ToType(), types.T_varchar.ToType(),
			},
			retType:   types.T_text.ToType(),
			emptyNull: true,
		}, "|")
		require.NoError(t,
			exec.(AllocationAccountOwner).SetAllocationAccount(allocation))
		require.NoError(t, exec.GroupGrow(1))
		return exec
	}
	left, right := makeExec(), makeExec()
	first := buildVarlenVec(t, mp, types.T_varchar.ToType(),
		[]string{"a", "ab", "aa", "aa"})
	second := buildVarlenVec(t, mp, types.T_varchar.ToType(),
		[]string{"bc", "c", "bb", "bb"})
	vectors := []*vector.Vector{first, second}
	groups := []uint64{1, 1}
	require.NoError(t, left.(BatchCapacityPreflight).
		PreflightBatchFill(0, groups, vectors))
	require.NoError(t, left.BatchFill(0, groups, vectors))
	require.NoError(t, right.(BatchCapacityPreflight).
		PreflightBatchFill(2, groups, vectors))
	require.NoError(t, right.BatchFill(2, groups, vectors))
	require.NoError(t, left.(BatchCapacityPreflight).
		PreflightBatchMerge(right, 0, []uint64{1}))
	require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))
	var encoded bytes.Buffer
	require.NoError(t, left.(SpillStateCodec).SaveSpillIntermediateRows(
		0, []int32{0}, &encoded))
	restored := makeExec()
	require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	restoredResults, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, "abc|abc|aabb", string(restoredResults[0].GetBytesAt(0)))
	restoredResults[0].Free(mp)

	results, err := left.Flush()
	require.NoError(t, err)
	require.Equal(t, "abc|abc|aabb", string(results[0].GetBytesAt(0)))
	results[0].Free(mp)
	first.Free(mp)
	second.Free(mp)
	for _, exec := range []AggFuncExec{left, right, restored} {
		owner := exec.(AllocationAccountOwner)
		exec.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
	}
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedGroupConcatUnmarshalPreservesOrderAcrossArenaGrowth(t *testing.T) {
	const rows = 4096

	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	makeExec := func() *groupConcatExec {
		exec := newGroupConcatExec(mp, multiAggInfo{
			aggID:     AggIdOfGroupConcat,
			argTypes:  []types.Type{types.T_varchar.ToType()},
			retType:   types.T_text.ToType(),
			emptyNull: true,
		}, "|").(*groupConcatExec)
		require.NoError(t, exec.SetAllocationAccount(allocation))
		return exec
	}

	source, target := makeExec(), makeExec()
	defer func() {
		for _, exec := range []*groupConcatExec{source, target} {
			exec.Free()
			require.NoError(t, exec.ClearAllocationAccount(allocation))
		}
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	}()
	require.NoError(t, source.GroupGrow(2))

	input := vector.NewVec(types.T_varchar.ToType())
	defer input.Free(mp)
	groups := make([]uint64, hashmap.UnitLimit)
	for row := range groups {
		groups[row] = uint64(row%2 + 1)
	}
	payload := strings.Repeat("x", 512)
	var expected [2]strings.Builder
	for row := range rows {
		value := fmt.Sprintf("%04d-%s", row, payload)
		require.NoError(t, vector.AppendBytes(input, []byte(value), false, mp))
		group := row % 2
		if expected[group].Len() > 0 {
			expected[group].WriteByte('|')
		}
		expected[group].WriteString(value)
	}
	for offset := 0; offset < rows; offset += hashmap.UnitLimit {
		workGroups := groups[:min(hashmap.UnitLimit, rows-offset)]
		require.NoError(t, source.PreflightBatchFill(
			offset, workGroups, []*vector.Vector{input}))
		require.NoError(t, source.BatchFill(
			offset, workGroups, []*vector.Vector{input}))
	}

	var encoded bytes.Buffer
	require.NoError(t, source.SaveIntermediateResult(
		2, [][]uint8{{1, 1}}, &encoded))
	require.NoError(t, target.UnmarshalFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	targetBase := target.aggregateBase()
	require.Greater(t, len(targetBase.state[0].argbuf), 4*kAggArgArenaSize,
		"the ordered saved arguments must cross several arena relocations")

	followup := buildVarlenVec(t, mp, types.T_varchar.ToType(),
		[]string{"tail-even", "tail-odd"})
	defer followup.Free(mp)
	require.NoError(t, target.PreflightBatchFill(
		0, []uint64{1, 2}, []*vector.Vector{followup}))
	require.NoError(t, target.BatchFill(
		0, []uint64{1, 2}, []*vector.Vector{followup}))
	expected[0].WriteString("|tail-even")
	expected[1].WriteString("|tail-odd")

	results, err := target.Flush()
	require.NoError(t, err)
	require.Len(t, results, 1)
	defer results[0].Free(mp)
	for group := range 2 {
		require.Equal(t, expected[group].String(),
			string(results[0].GetBytesAt(group)))
	}
}

func TestAccountedOrderedGroupConcatSortsDeduplicatesAndMerges(t *testing.T) {
	for _, distinct := range []bool{false, true} {
		t.Run(fmt.Sprintf("distinct=%t", distinct), func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			makeExec := func() *groupConcatExec {
				info := multiAggInfo{
					aggID:     AggIdOfGroupConcat,
					distinct:  distinct,
					argTypes:  []types.Type{types.T_varchar.ToType(), types.T_int64.ToType()},
					retType:   types.T_text.ToType(),
					emptyNull: true,
				}
				exec := newGroupConcatExec(mp, info, ",").(*groupConcatExec)
				require.NoError(t, exec.SetExtraInformation(
					testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|"), 0))
				require.NoError(t, exec.SetAllocationAccount(allocation))
				require.NoError(t, exec.GroupGrow(1))
				return exec
			}
			left, right := makeExec(), makeExec()
			leftValues := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"b", "a", "a"})
			leftOrder := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(leftOrder, []int64{3, 2, 4}, nil, mp))
			rightValues := buildVarlenVec(t, mp, types.T_varchar.ToType(), []string{"c", "a"})
			rightOrder := vector.NewVec(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixedList(rightOrder, []int64{5, 1}, nil, mp))

			leftGroups := []uint64{1, 1, 1}
			rightGroups := []uint64{1, 1}
			require.NoError(t, left.PreflightBatchFill(
				0, leftGroups, []*vector.Vector{leftValues, leftOrder}))
			require.NoError(t, left.BatchFill(
				0, leftGroups, []*vector.Vector{leftValues, leftOrder}))
			require.NoError(t, right.PreflightBatchFill(
				0, rightGroups, []*vector.Vector{rightValues, rightOrder}))
			require.NoError(t, right.BatchFill(
				0, rightGroups, []*vector.Vector{rightValues, rightOrder}))
			require.NoError(t, left.PreflightBatchMerge(right, 0, []uint64{1}))
			require.NoError(t, left.BatchMerge(right, 0, []uint64{1}))
			results, err := left.Flush()
			require.NoError(t, err)
			want := "a|a|b|a|c"
			if distinct {
				want = "a|b|c"
			}
			require.Equal(t, want, string(results[0].GetBytesAt(0)))

			results[0].Free(mp)
			leftValues.Free(mp)
			leftOrder.Free(mp)
			rightValues.Free(mp)
			rightOrder.Free(mp)
			for _, exec := range []*groupConcatExec{left, right} {
				exec.Free()
				require.NoError(t, exec.ClearAllocationAccount(allocation))
			}
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedOrderedGroupConcatPreflightsNullOrderStorage(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec := newGroupConcatExec(mp, multiAggInfo{
		aggID: AggIdOfGroupConcat,
		argTypes: []types.Type{
			types.T_varchar.ToType(), types.T_int64.ToType(),
		},
		retType:   types.T_text.ToType(),
		emptyNull: true,
	}, ",").(*groupConcatExec)
	require.NoError(t, exec.SetExtraInformation(
		testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|"), 0))
	require.NoError(t, exec.SetAllocationAccount(allocation))
	require.NoError(t, exec.GroupGrow(1))

	values := buildVarlenVec(
		t, mp, types.T_varchar.ToType(), []string{"null-key", "one"})
	order := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		order, []int64{0, 1}, []bool{true, false}, mp))
	groups := []uint64{1, 1}
	require.NoError(t, exec.PreflightBatchFill(
		0, groups, []*vector.Vector{values, order}))
	require.NoError(t, exec.BatchFill(
		0, groups, []*vector.Vector{values, order}))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, "null-key|one", string(result[0].GetBytesAt(0)))

	result[0].Free(mp)
	values.Free(mp)
	order.Free(mp)
	exec.Free()
	require.NoError(t, exec.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedOrderedGroupConcatFinalizationExactCap(t *testing.T) {
	const rows = 128
	values := make([]string, rows)
	order := make([]int64, rows)
	expected := make([]string, rows)
	for i := range rows {
		values[i] = fmt.Sprintf("value-%03d-%s", i, strings.Repeat("x", 48))
		order[i] = int64(rows - i)
		expected[rows-1-i] = values[i]
	}

	run := func(limit uint64) (uint64, error) {
		mp := mpool.MustNewZero()
		registry, err := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, err)
		account, err := registry.Open(limit)
		require.NoError(t, err)
		allocation, err := NewAllocationAccount(
			account,
			mpool.AllocationOwnerGroup,
			AllocationAccountSites{
				VectorData: 1, VectorArea: 2, VectorNulls: 3,
				VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
			},
		)
		require.NoError(t, err)
		exec := newGroupConcatExec(mp, multiAggInfo{
			aggID: AggIdOfGroupConcat,
			argTypes: []types.Type{
				types.T_varchar.ToType(), types.T_int64.ToType(),
			},
			retType:   types.T_text.ToType(),
			emptyNull: true,
		}, ",").(*groupConcatExec)
		require.NoError(t, exec.SetExtraInformation(
			testGroupConcatOrderConfig(1, []byte{groupConcatOrderAsc}, "|"), 0))
		require.NoError(t, exec.SetAllocationAccount(allocation))

		valueVector := buildVarlenVec(
			t, mp, types.T_varchar.ToType(), values)
		orderVector := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(orderVector, order, nil, mp))
		groups := slices.Repeat([]uint64{1}, rows)
		if err = exec.GroupGrow(1); err == nil {
			err = exec.PreflightBatchFill(
				0, groups, []*vector.Vector{valueVector, orderVector})
		}
		if err == nil {
			err = exec.BatchFill(
				0, groups, []*vector.Vector{valueVector, orderVector})
		}
		var results []*vector.Vector
		if err == nil {
			results, err = exec.Flush()
			if err == nil {
				require.Len(t, results, 1)
				require.Equal(t,
					strings.Join(expected, "|"), string(results[0].GetBytesAt(0)))
			}
		}
		peak := account.Snapshot().Peak
		for _, result := range results {
			result.Free(mp)
		}
		valueVector.Free(mp)
		orderVector.Free(mp)
		exec.Free()
		require.NoError(t, exec.ClearAllocationAccount(allocation))
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

func TestAccountedHLLPreflightRollbackAndSpillRoundTrip(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		input, []int64{11, 22}, nil, inputMp))
	defer func() {
		input.Free(inputMp)
		require.Zero(t, inputMp.CurrNB())
	}()

	run := func(limit uint64, invalid bool) (before, after uint64, err error) {
		mp := mpool.MustNewZero()
		registry, openErr := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, openErr)
		account, openErr := registry.Open(limit)
		require.NoError(t, openErr)
		allocation, openErr := NewAllocationAccount(
			account, mpool.AllocationOwnerGroup, AllocationAccountSites{
				VectorData: 1, VectorArea: 2, VectorNulls: 3,
				VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
			})
		require.NoError(t, openErr)
		exec, openErr := MakeAgg(
			mp, AggIdOfApproxCount, false, types.T_int64.ToType())
		require.NoError(t, openErr)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		defer func() {
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		}()
		require.NoError(t, exec.PreAllocateGroups(2))
		before = account.Snapshot().Used
		groups := []uint64{1, 2}
		if invalid {
			groups[1] = uint64(AggBatchSize + 1)
		}
		err = exec.(BatchCapacityPreflight).PreflightBatchFill(
			0, groups, []*vector.Vector{input})
		after = account.Snapshot().Used
		return before, after, err
	}

	baselineBefore, admitted, err := run(128<<20, false)
	require.NoError(t, err)
	require.Equal(t, uint64(2*hllRegisterCnt), admitted-baselineBefore)
	failedBefore, failedAfter, err := run(admitted-1, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, failedBefore, failedAfter)
	invalidBefore, invalidAfter, err := run(128<<20, true)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvariant)
	require.Equal(t, invalidBefore, invalidAfter)

	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeAgg(mp, AggIdOfApproxCount, false, types.T_int64.ToType())
	require.NoError(t, err)
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.PreAllocateGroups(2))
	preflight := exec.(BatchCapacityPreflight)
	require.NoError(t, preflight.PreflightBatchFill(
		0, []uint64{1, 2}, []*vector.Vector{input}))
	usedBeforeGrow := account.Snapshot().Used
	require.NoError(t, exec.GroupGrow(2))
	require.Equal(t, usedBeforeGrow, account.Snapshot().Used)
	require.NoError(t, exec.BatchFill(
		0, []uint64{1, 2}, []*vector.Vector{input}))

	var encoded bytes.Buffer
	codec := exec.(SpillStateCodec)
	require.NoError(t, codec.SaveSpillIntermediateRows(
		0, []int32{0, 1}, &encoded))
	restored, err := MakeAgg(
		mp, AggIdOfApproxCount, false, types.T_int64.ToType())
	require.NoError(t, err)
	restoredOwner := restored.(AllocationAccountOwner)
	require.NoError(t, restoredOwner.SetAllocationAccount(allocation))
	require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	results, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](results[0], 0))
	require.Equal(t, uint64(1), vector.GetFixedAtNoTypeCheck[uint64](results[0], 1))
	results[0].Free(mp)
	exec.Free()
	restored.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAccountedApproxPercentilePreflightAndSpillRoundTrip(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	values := make([]int64, 256)
	groups := make([]uint64, len(values))
	for i := range values {
		values[i] = int64(i)
		groups[i] = 1
	}
	require.NoError(t, vector.AppendFixedList(input, values, nil, inputMp))
	defer func() {
		input.Free(inputMp)
		require.Zero(t, inputMp.CurrNB())
	}()

	runPreflight := func(limit uint64) (before, after uint64, err error) {
		mp := mpool.MustNewZero()
		registry, openErr := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, openErr)
		account, openErr := registry.Open(limit)
		require.NoError(t, openErr)
		allocation, openErr := NewAllocationAccount(
			account, mpool.AllocationOwnerGroup, AllocationAccountSites{
				VectorData: 1, VectorArea: 2, VectorNulls: 3,
				VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
			})
		require.NoError(t, openErr)
		exec, openErr := MakeAgg(
			mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
		require.NoError(t, openErr)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		defer func() {
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		}()
		require.NoError(t, exec.PreAllocateGroups(1))
		before = account.Snapshot().Used
		err = exec.(BatchCapacityPreflight).PreflightBatchFill(
			0, groups, []*vector.Vector{input})
		after = account.Snapshot().Used
		return before, after, err
	}

	base, admitted, err := runPreflight(128 << 20)
	require.NoError(t, err)
	require.Greater(t, admitted, base)
	failedBefore, failedAfter, err := runPreflight(admitted - 1)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Equal(t, failedBefore, failedAfter)

	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	exec, err := MakeAgg(
		mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, exec.PreAllocateGroups(1))
	require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
		0, groups, []*vector.Vector{input}))
	usedBeforeGrow := account.Snapshot().Used
	require.NoError(t, exec.GroupGrow(1))
	require.Equal(t, usedBeforeGrow, account.Snapshot().Used)
	require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))

	var encoded bytes.Buffer
	codec := exec.(SpillStateCodec)
	require.NoError(t, codec.SaveSpillIntermediateRows(
		0, []int32{0}, &encoded))
	restored, err := MakeAgg(
		mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	restoredOwner := restored.(AllocationAccountOwner)
	require.NoError(t, restoredOwner.SetAllocationAccount(allocation))
	require.NoError(t, restored.SetExtraInformation([]byte("0.5"), 0))
	require.NoError(t, restored.(SpillStateCodec).UnmarshalSpillFromReader(
		bytes.NewReader(encoded.Bytes()), mp))
	resident, err := exec.Flush()
	require.NoError(t, err)
	spilled, err := restored.Flush()
	require.NoError(t, err)
	require.Equal(t,
		vector.GetFixedAtNoTypeCheck[float64](resident[0], 0),
		vector.GetFixedAtNoTypeCheck[float64](spilled[0], 0))
	require.True(t, vector.AllocationAccountSelectionsEqual(
		allocation.vectorSelection(), resident[0].AllocationAccountSelection()))
	require.True(t, vector.AllocationAccountSelectionsEqual(
		allocation.vectorSelection(), spilled[0].AllocationAccountSelection()))
	resident[0].Free(mp)
	spilled[0].Free(mp)
	exec.Free()
	restored.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	require.NoError(t, restoredOwner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestAggregatePreflightRejectsConcreteMergeMismatch(t *testing.T) {
	t.Run("hll-family", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry, account, allocation := newTestAggregateAllocation(t)
		target, err := MakeAgg(
			mp, AggIdOfApproxCount, false, types.T_int64.ToType())
		require.NoError(t, err)
		owner := target.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		require.NoError(t, target.PreAllocateGroups(1))
		before := account.Snapshot().Used
		for _, source := range []AggFuncExec{
			makeHllAdd(mp, AggIdOfHllAdd, types.T_int64.ToType()),
			makeHllMerge(mp, AggIdOfHllMerge, types.T_varbinary.ToType()),
		} {
			require.ErrorIs(t,
				target.(BatchCapacityPreflight).PreflightBatchMerge(
					source, 0, []uint64{1}),
				mpool.ErrAllocationAccountInvalid)
			require.Equal(t, before, account.Snapshot().Used)
			source.Free()
		}
		target.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	})

	t.Run("median-type", func(t *testing.T) {
		mp := mpool.MustNewZero()
		registry, account, allocation := newTestAggregateAllocation(t)
		target, err := MakeAgg(
			mp, AggIdOfMedian, false, types.T_int64.ToType())
		require.NoError(t, err)
		source, err := MakeAgg(
			mp, AggIdOfMedian, false, types.T_float64.ToType())
		require.NoError(t, err)
		owners := []AllocationAccountOwner{
			target.(AllocationAccountOwner), source.(AllocationAccountOwner)}
		for _, owner := range owners {
			require.NoError(t, owner.SetAllocationAccount(allocation))
		}
		require.NoError(t, target.PreAllocateGroups(1))
		require.NoError(t, source.PreAllocateGroups(1))
		before := account.Snapshot().Used
		require.ErrorIs(t,
			target.(BatchCapacityPreflight).PreflightBatchMerge(
				source, 0, []uint64{1}),
			mpool.ErrAllocationAccountMismatch)
		require.Equal(t, before, account.Snapshot().Used)
		target.Free()
		source.Free()
		for _, owner := range owners {
			require.NoError(t, owner.ClearAllocationAccount(allocation))
		}
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
	})
}

func TestBoundedAggregateMergePreflightRejectsOversizedWorkUnit(t *testing.T) {
	tests := []struct {
		name string
		id   int64
	}{
		{name: "hll", id: AggIdOfApproxCount},
		{name: "approx-percentile", id: AggIdOfApproxPercentile},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			target, err := MakeAgg(mp, test.id, false, types.T_int64.ToType())
			require.NoError(t, err)
			source, err := MakeAgg(mp, test.id, false, types.T_int64.ToType())
			require.NoError(t, err)
			owners := []AllocationAccountOwner{
				target.(AllocationAccountOwner), source.(AllocationAccountOwner)}
			for _, owner := range owners {
				require.NoError(t, owner.SetAllocationAccount(allocation))
			}
			groups := make([]uint64, hashmap.UnitLimit+1)
			require.NoError(t, source.PreAllocateGroups(len(groups)))
			before := account.Snapshot().Used
			require.ErrorIs(t,
				target.(BatchCapacityPreflight).PreflightBatchMerge(
					source, 0, groups),
				mpool.ErrAllocationAccountInvalid)
			require.Equal(t, before, account.Snapshot().Used)
			target.Free()
			source.Free()
			for _, owner := range owners {
				require.NoError(t, owner.ClearAllocationAccount(allocation))
			}
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAccountedApproxPercentileDuplicateTargetMergePreflight(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, account, allocation := newTestAggregateAllocation(t)
	target, err := MakeAgg(
		mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	source, err := MakeAgg(
		mp, AggIdOfApproxPercentile, false, types.T_int64.ToType())
	require.NoError(t, err)
	owners := []AllocationAccountOwner{
		target.(AllocationAccountOwner), source.(AllocationAccountOwner)}
	for _, owner := range owners {
		require.NoError(t, owner.SetAllocationAccount(allocation))
	}
	for _, exec := range []AggFuncExec{target, source} {
		require.NoError(t, exec.SetExtraInformation([]byte("0.5"), 0))
	}
	require.NoError(t, target.PreAllocateGroups(1))
	require.NoError(t, source.PreAllocateGroups(2))
	require.NoError(t, target.GroupGrow(1))
	require.NoError(t, source.GroupGrow(2))
	input := vector.NewVec(types.T_int64.ToType())
	values := make([]int64, 512)
	groups := make([]uint64, len(values))
	for i := range values {
		values[i] = int64(i)
		if i < len(values)/2 {
			groups[i] = 1
		} else {
			groups[i] = 2
		}
	}
	require.NoError(t, vector.AppendFixedList(input, values, nil, mp))
	for offset := 0; offset < len(values); offset += hashmap.UnitLimit {
		end := min(offset+hashmap.UnitLimit, len(values))
		require.NoError(t, source.(BatchCapacityPreflight).PreflightBatchFill(
			offset, groups[offset:end], []*vector.Vector{input}))
		require.NoError(t, source.BatchFill(
			offset, groups[offset:end], []*vector.Vector{input}))
	}
	require.NoError(t, target.(BatchCapacityPreflight).PreflightBatchMerge(
		source, 0, []uint64{1, 1}))
	usedBeforePublication := account.Snapshot().Used
	require.NoError(t, target.BatchMerge(source, 0, []uint64{1, 1}))
	require.Equal(t, usedBeforePublication, account.Snapshot().Used,
		"duplicate-target merge publication must not allocate after preflight")
	results, err := target.Flush()
	require.NoError(t, err)
	require.InDelta(t, 255.5,
		vector.GetFixedAtNoTypeCheck[float64](results[0], 0), 1.0)
	results[0].Free(mp)
	input.Free(mp)
	target.Free()
	source.Free()
	for _, owner := range owners {
		require.NoError(t, owner.ClearAllocationAccount(allocation))
	}
	finishTestAggregateAllocation(t, registry, account)
	require.Zero(t, mp.CurrNB())
}

func TestFixedMinMaxPreflightsMixedFuturePrepareParamKinds(t *testing.T) {
	inputMp := mpool.MustNewZero()
	input := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(
		input, []int64{7, 9}, nil, inputMp))
	require.NoError(t, input.SetPrepareParamKindsWithMP(
		[]vector.PrepareParamKind{
			vector.PrepareParamInteger,
			vector.PrepareParamFloat,
		}, inputMp))
	defer func() {
		input.Free(inputMp)
		require.Zero(t, inputMp.CurrNB())
	}()

	run := func(limit uint64, publish bool) (before, after uint64, err error) {
		mp := mpool.MustNewZero()
		registry, registryErr := mpool.NewAllocationAccountRegistry(1, 512)
		require.NoError(t, registryErr)
		account, registryErr := registry.Open(limit)
		require.NoError(t, registryErr)
		allocation, registryErr := NewAllocationAccount(
			account, mpool.AllocationOwnerGroup, AllocationAccountSites{
				VectorData: 1, VectorArea: 2, VectorNulls: 3,
				VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
			})
		require.NoError(t, registryErr)
		exec := makeMinMaxExec(
			mp, AggIdOfMin, true, types.T_int64.ToType())
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		defer func() {
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		}()

		if err = exec.PreAllocateGroups(2); err != nil {
			return 0, account.Snapshot().Used, err
		}
		before = account.Snapshot().Used
		preflight := exec.(BatchCapacityPreflight)
		err = preflight.PreflightBatchFill(
			0, []uint64{1, 2}, []*vector.Vector{input})
		after = account.Snapshot().Used
		if err != nil || !publish {
			return before, after, err
		}
		require.NoError(t, exec.GroupGrow(2))
		admitted := account.Snapshot().Used
		require.NoError(t, exec.BatchFill(
			0, []uint64{1, 2}, []*vector.Vector{input}))
		require.Equal(t, admitted, account.Snapshot().Used,
			"winner publication must not allocate after preflight")
		results, flushErr := exec.Flush()
		if flushErr != nil {
			return before, account.Snapshot().Used, flushErr
		}
		require.Len(t, results, 1)
		require.Equal(t, vector.PrepareParamInteger,
			results[0].GetPrepareParamKindAt(0))
		require.Equal(t, vector.PrepareParamFloat,
			results[0].GetPrepareParamKindAt(1))
		results[0].Free(mp)
		return before, after, nil
	}

	before, exact, err := run(128<<20, true)
	require.NoError(t, err)
	require.Greater(t, exact, before)
	_, exactAgain, err := run(exact, true)
	require.NoError(t, err)
	require.Equal(t, exact, exactAgain)
	failedBefore, _, err := run(exact-1, false)
	require.Equal(t, before, failedBefore)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
}

func TestDistinctMergePreflightSkipsExistingAndWorkUnitDuplicates(t *testing.T) {
	const sourceGroups = 16
	payload := []byte(strings.Repeat("distinct-merge-", 4096))

	run := func(preload bool, groups int) uint64 {
		mp := mpool.MustNewZero()
		registry, account, allocation := newTestAggregateAllocation(t)
		destination, err := MakeAgg(
			mp, AggIdOfCountColumn, true, types.T_varchar.ToType())
		require.NoError(t, err)
		owner := destination.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		require.NoError(t, destination.GroupGrow(1))

		input := vector.NewVec(types.T_varchar.ToType())
		for range groups {
			require.NoError(t, vector.AppendBytes(input, payload, false, mp))
		}
		if preload {
			require.NoError(t, destination.Fill(
				0, 0, []*vector.Vector{input}))
		}

		source, err := MakeAgg(
			mp, AggIdOfCountColumn, true, types.T_varchar.ToType())
		require.NoError(t, err)
		require.NoError(t, source.GroupGrow(groups))
		sourceMapping := make([]uint64, groups)
		mergeMapping := make([]uint64, groups)
		for i := range groups {
			sourceMapping[i] = uint64(i + 1)
			mergeMapping[i] = 1
		}
		require.NoError(t, source.BatchFill(
			0, sourceMapping, []*vector.Vector{input}))

		before := account.Snapshot().Used
		preflight := destination.(BatchCapacityPreflight)
		require.NoError(t, preflight.PreflightBatchMerge(
			source, 0, mergeMapping))
		after := account.Snapshot().Used
		require.NoError(t, destination.BatchMerge(
			source, 0, mergeMapping))
		require.Equal(t, after, account.Snapshot().Used,
			"admitted distinct merge must not allocate while publishing")

		results, err := destination.Flush()
		require.NoError(t, err)
		require.Len(t, results, 1)
		require.Equal(t, int64(1),
			vector.GetFixedAtNoTypeCheck[int64](results[0], 0))
		results[0].Free(mp)
		input.Free(mp)
		source.Free()
		destination.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
		return after - before
	}

	require.Zero(t, run(true, sourceGroups),
		"values already retained by the target need no additional arena capacity")
	one := run(false, 1)
	many := run(false, sourceGroups)
	require.Positive(t, one)
	require.Equal(t, one, many,
		"same-work-unit duplicates need capacity for only one retained value")
}

func TestDistinctFillPreflightUsesPublishedNodeFootprints(t *testing.T) {
	const rows = 256
	run := func(limit uint64, publish bool) (uint64, uint32, error) {
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

		exec, err := MakeAgg(
			mp, AggIdOfCountColumn, true, types.T_int64.ToType())
		require.NoError(t, err)
		owner := exec.(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))
		input := vector.NewVec(types.T_int64.ToType())
		groups := make([]uint64, rows)
		for i := range rows {
			groups[i] = 1
			require.NoError(t, vector.AppendFixed(
				input, int64(i), false, mp))
		}
		require.NoError(t, exec.GroupGrow(1))
		state := &exec.(*countColumnExec).state[0]
		initialArenaCapacity := state.argSkl.Arena().Capacity()
		err = exec.(BatchCapacityPreflight).PreflightBatchFill(
			0, groups, []*vector.Vector{input})
		admitted := account.Snapshot().Used
		if err == nil {
			require.Equal(t, initialArenaCapacity, state.argSkl.Arena().Capacity(),
				"short planned nodes must fit the initial arena without a max-height reservation per row")
		}
		if err == nil && publish {
			err = exec.BatchFill(0, groups, []*vector.Vector{input})
			require.Equal(t, admitted, account.Snapshot().Used,
				"publication must use the same tower plans admitted by preflight")
		}
		count := state.argCnt[0]
		input.Free(mp)
		exec.Free()
		require.NoError(t, owner.ClearAllocationAccount(allocation))
		finishTestAggregateAllocation(t, registry, account)
		require.Zero(t, mp.CurrNB())
		return admitted, count, err
	}

	exact, count, err := run(128<<20, true)
	require.NoError(t, err)
	require.Equal(t, uint32(rows), count)
	exactAgain, count, err := run(exact, true)
	require.NoError(t, err)
	require.Equal(t, exact, exactAgain)
	require.Equal(t, uint32(rows), count)
	_, count, err = run(exact-1, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Zero(t, count, "failed admission must not publish distinct keys")
}

func TestAccountedSavedArgumentsPreserveConstLogicalRows(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	registry, account, allocation := newTestAggregateAllocation(t)
	exec := newCountColumnExec(
		mp, AggIdOfCountColumn, true, []types.Type{types.T_varchar.ToType()})
	owner := exec.(AllocationAccountOwner)
	require.NoError(t, owner.SetAllocationAccount(allocation))
	require.NoError(t, exec.PreAllocateGroups(1))
	require.NoError(t, exec.GroupGrow(1))

	input, err := vector.NewConstBytes(
		types.T_varchar.ToType(), []byte("constant"), 1, mp)
	require.NoError(t, err)
	defer input.Free(mp)

	groups := []uint64{1, 1}
	require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(
		2, groups, []*vector.Vector{input}))
	require.NoError(t, exec.BatchFill(2, groups, []*vector.Vector{input}))
	result, err := exec.Flush()
	require.NoError(t, err)
	require.Equal(t, int64(1), vector.MustFixedColNoTypeCheck[int64](result[0])[0])
	for _, vec := range result {
		vec.Free(mp)
	}
	exec.Free()
	require.NoError(t, owner.ClearAllocationAccount(allocation))
	finishTestAggregateAllocation(t, registry, account)
}

func TestMaxByNullWinnerRetainsPreflightedPrepareParamCapacity(t *testing.T) {
	run := func(limit uint64, publish bool) (uint64, error) {
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

		exec := makeMaxByExec(mp, AggIdOfMaxBy, false, []types.Type{
			types.T_text.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
		}).(*maxByExec)
		owner := any(exec).(AllocationAccountOwner)
		require.NoError(t, owner.SetAllocationAccount(allocation))

		initialValue := vector.NewVec(types.T_text.ToType())
		initialOrder := vector.NewVec(types.T_int64.ToType())
		initialTie := vector.NewVec(types.T_int64.ToType())
		batchValue := vector.NewVec(types.T_text.ToType())
		batchOrder := vector.NewVec(types.T_int64.ToType())
		batchTie := vector.NewVec(types.T_int64.ToType())
		inputs := []*vector.Vector{
			initialValue, initialOrder, initialTie,
			batchValue, batchOrder, batchTie,
		}
		defer func() {
			for _, input := range inputs {
				input.Free(mp)
			}
			exec.Free()
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		}()

		require.NoError(t, vector.AppendBytes(
			initialValue, []byte("initial"), false, mp))
		initialValue.SetPrepareParamKind(vector.PrepareParamInteger)
		require.NoError(t, vector.AppendFixed(initialOrder, int64(1), false, mp))
		require.NoError(t, vector.AppendFixed(initialTie, int64(1), false, mp))
		require.NoError(t, exec.GroupGrow(1))
		require.NoError(t, exec.Fill(0, 0, []*vector.Vector{
			initialValue, initialOrder, initialTie,
		}))

		require.NoError(t, vector.AppendBytes(batchValue, nil, true, mp))
		require.NoError(t, vector.AppendBytes(batchValue, []byte("float"), false, mp))
		require.NoError(t, vector.AppendBytes(batchValue, []byte("decimal"), false, mp))
		require.NoError(t, batchValue.SetPrepareParamKindsWithMP(
			[]vector.PrepareParamKind{
				vector.PrepareParamNone,
				vector.PrepareParamFloat,
				vector.PrepareParamDecimal,
			}, mp))
		for _, order := range []int64{2, 3, 4} {
			require.NoError(t, vector.AppendFixed(batchOrder, order, false, mp))
			require.NoError(t, vector.AppendFixed(batchTie, order, false, mp))
		}

		preflight := any(exec).(BatchCapacityPreflight)
		err = preflight.PreflightBatchFill(
			0, []uint64{1, 1, 1},
			[]*vector.Vector{batchValue, batchOrder, batchTie})
		admitted := account.Snapshot().Used
		if err != nil || !publish {
			return admitted, err
		}
		require.NoError(t, exec.BatchFill(
			0, []uint64{1, 1, 1},
			[]*vector.Vector{batchValue, batchOrder, batchTie}))
		require.LessOrEqual(t, account.Snapshot().Used, admitted)
		require.Equal(t, admitted, account.Snapshot().Peak,
			"winner publication must not allocate beyond admitted capacity")

		results, flushErr := exec.Flush()
		if flushErr != nil {
			return admitted, flushErr
		}
		require.Len(t, results, 1)
		require.Equal(t, "decimal", string(results[0].GetBytesAt(0)))
		require.Equal(t, vector.PrepareParamDecimal,
			results[0].GetPrepareParamKindAt(0))
		results[0].Free(mp)
		return admitted, nil
	}

	exact, err := run(128<<20, true)
	require.NoError(t, err)
	require.Positive(t, exact)
	exactAgain, err := run(exact, true)
	require.NoError(t, err)
	require.Equal(t, exact, exactAgain)
	_, err = run(exact-1, false)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
}
