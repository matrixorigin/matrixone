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
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type aggregateAllocationTestCase struct {
	name          string
	id            int64
	distinct      bool
	params        []types.Type
	factoryReject bool
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
		{name: "group-concat", id: AggIdOfGroupConcat, params: []types.Type{types.T_varchar.ToType()}},
		{name: "avg-tw-cache", id: AggIdOfAvgTwCache, params: []types.Type{types.T_int64.ToType()}},
		{name: "avg-tw-result", id: AggIdOfAvgTwResult, params: []types.Type{types.T_char.ToType()}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeAgg(mp, tc.id, tc.distinct, tc.params...)
			require.NoError(t, err)
			owner, ok := exec.(AllocationAccountOwner)
			require.True(t, ok)
			require.NoError(t, owner.SetAllocationAccount(allocation))
			codec, ok := exec.(SpillStateCodec)
			require.True(t, ok)
			require.True(t, codec.SupportsBoundedSpillState())

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
			require.NoError(t, owner.ClearAllocationAccount(allocation))
			finishTestAggregateAllocation(t, registry, account)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestAllocationAccountRejectsOpaqueAggregateFamilies(t *testing.T) {
	tests := []aggregateAllocationTestCase{
		{name: "bit-and-distinct", id: AggIdOfBitAnd, distinct: true,
			params: []types.Type{types.T_int64.ToType()}, factoryReject: true},
		{name: "bitmap-construct", id: AggIdOfBitmapConstruct, params: []types.Type{types.T_uint64.ToType()}},
		{name: "bitmap-or", id: AggIdOfBitmapOr, params: []types.Type{types.T_varbinary.ToType()}},
		{name: "median", id: AggIdOfMedian, params: []types.Type{types.T_int64.ToType()}},
		{name: "group-concat-distinct", id: AggIdOfGroupConcat, distinct: true, params: []types.Type{types.T_varchar.ToType()}},
		{name: "approx-count", id: AggIdOfApproxCount, params: []types.Type{types.T_int64.ToType()}},
		{name: "approx-count-distinct", id: AggIdOfApproxCountDistinct, params: []types.Type{types.T_int64.ToType()}},
		{name: "hll-add", id: AggIdOfHllAdd, params: []types.Type{types.T_int64.ToType()}},
		{name: "hll-merge", id: AggIdOfHllMerge, params: []types.Type{types.T_varbinary.ToType()}},
		{name: "approx-percentile", id: AggIdOfApproxPercentile, params: []types.Type{types.T_int64.ToType()}},
		{name: "json-array", id: AggIdOfJsonArrayAgg, params: []types.Type{types.T_json.ToType()}},
		{name: "json-object", id: AggIdOfJsonObjectAgg, params: []types.Type{types.T_varchar.ToType(), types.T_json.ToType()}},
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
			registry, account, allocation := newTestAggregateAllocation(t)
			exec, err := MakeAgg(mp, tc.id, tc.distinct, tc.params...)
			if tc.factoryReject {
				require.Error(t, err)
				finishTestAggregateAllocation(t, registry, account)
				require.Zero(t, mp.CurrNB())
				return
			}
			require.NoError(t, err)
			owner, ownsAllocation := exec.(AllocationAccountOwner)
			if ownsAllocation {
				require.Error(t, owner.SetAllocationAccount(allocation))
			}
			codec, hasCodec := exec.(SpillStateCodec)
			require.False(t, hasCodec && codec.SupportsBoundedSpillState())
			exec.Free()
			finishTestAggregateAllocation(t, registry, account)
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
