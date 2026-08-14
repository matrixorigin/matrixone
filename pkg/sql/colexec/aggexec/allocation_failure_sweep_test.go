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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

type rejectNthAggregateAllocation struct {
	failAt   int
	calls    int
	rejected bool
	used     uint64
}

func (c *rejectNthAggregateAllocation) AcquireAllocationCapacity(size uint64) error {
	c.calls++
	if c.calls == c.failAt {
		c.rejected = true
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += size
	return nil
}

func (c *rejectNthAggregateAllocation) ReleaseAllocationCapacity(size uint64) {
	if c.used < size {
		panic("aggregate allocation controller release underflow")
	}
	c.used -= size
}

func TestAccountedAggregatesRollbackEveryPhysicalAllocationFailure(t *testing.T) {
	long := "physical-allocation-failure-sweep-varlen-payload"
	jsonValue := func(t *testing.T, value any) []byte {
		t.Helper()
		bj, err := bytejson.CreateByteJSONWithCheck(value)
		require.NoError(t, err)
		encoded, err := bj.Marshal()
		require.NoError(t, err)
		return encoded
	}
	tests := []struct {
		name   string
		id     int64
		extra  any
		params []types.Type
		build  func(*testing.T, *mpool.MPool) []*vector.Vector
	}{
		{
			name: "any-varlen", id: AggIdOfAny,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-a", long + "-b", long + "-c", long + "-d"})}
			},
		},
		{
			name: "min-varlen", id: AggIdOfMin,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-d", long + "-a", long + "-c", long + "-b"})}
			},
		},
		{
			name: "max-by", id: AggIdOfMaxBy,
			params: []types.Type{
				types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
			},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{
					buildVarlenVec(t, mp, types.T_varchar.ToType(),
						[]string{long + "-a", long + "-b", long + "-c", long + "-d"}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 1, 1}),
				}
			},
		},
		{
			name: "group-concat", id: AggIdOfGroupConcat,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-a", long + "-b", long + "-c", long + "-d"})}
			},
		},
		{
			name: "bitmap", id: AggIdOfBitmapConstruct,
			params: []types.Type{types.T_uint64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_uint64.ToType(), []uint64{1, 2, 3, 4})}
			},
		},
		{
			name: "json-array", id: AggIdOfJsonArrayAgg,
			params: []types.Type{types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				vec := vector.NewVec(types.T_json.ToType())
				for _, value := range []any{long + "-a", int64(2), true, nil} {
					require.NoError(t, vector.AppendBytes(vec, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{vec}
			},
		},
		{
			name: "json-object", id: AggIdOfJsonObjectAgg,
			params: []types.Type{types.T_varchar.ToType(), types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				keys := buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{"a", "b", "c", "d"})
				values := vector.NewVec(types.T_json.ToType())
				for _, value := range []any{long + "-a", int64(2), true, nil} {
					require.NoError(t, vector.AppendBytes(values, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{keys, values}
			},
		},
		{
			name: "median", id: AggIdOfMedian,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "median-decimal64", id: AggIdOfMedian,
			params: []types.Type{types.New(types.T_decimal64, 10, 2)},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.New(types.T_decimal64, 10, 2),
					mustDecimal64s(t, "9.00", "1.00", "5.00", "8.00"))}
			},
		},
		{
			name: "percentile-cont", id: AggIdOfPercentileCont, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "percentile-disc", id: AggIdOfPercentileDisc, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "approx-count", id: AggIdOfApproxCount,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "hll-add", id: AggIdOfHllAdd,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "approx-percentile", id: AggIdOfApproxPercentile, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			completed := false
			for failAt := 1; failAt <= 128; failAt++ {
				mp := mpool.MustNewZero()
				controller := &rejectNthAggregateAllocation{failAt: failAt}
				registry, err := mpool.NewAllocationAccountRegistry(1, 512)
				require.NoError(t, err)
				account, err := registry.OpenWithController(128<<20, controller)
				require.NoError(t, err)
				allocation, err := NewAllocationAccount(
					account, mpool.AllocationOwnerGroup, AllocationAccountSites{
						VectorData: 1, VectorArea: 2, VectorNulls: 3,
						VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
					})
				require.NoError(t, err)
				exec, err := MakeGroupAgg(
					mp, tc.id, false, allocation, tc.extra, tc.params...)
				require.NoError(t, err)
				SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
				vectors := tc.build(t, mp)
				groups := []uint64{1, 1, 2, 2}
				if err = exec.GroupGrow(2); err == nil {
					err = exec.PreflightBatchFill(0, groups, vectors)
				}
				if err == nil {
					err = exec.BatchFill(0, groups, vectors)
				}
				var results []*vector.Vector
				if err == nil {
					results, err = exec.Flush()
				}
				for _, result := range results {
					if result != nil {
						result.Free(mp)
					}
				}
				for _, vec := range vectors {
					vec.Free(mp)
				}
				exec.Free()
				require.NoError(t, exec.ClearAllocationAccount(allocation))
				require.Zero(t, account.Snapshot().Used, "failAt=%d", failAt)
				require.Zero(t, controller.used, "failAt=%d", failAt)
				account.Seal()
				_, finalizeErr := registry.Finalize(account)
				require.NoError(t, finalizeErr)
				require.Zero(t, mp.CurrNB(), "failAt=%d", failAt)

				if controller.rejected {
					require.Error(t, err, "failAt=%d", failAt)
					require.True(t, mpool.IsRetryableAllocationCapacity(err),
						"failAt=%d err=%v", failAt, err)
					continue
				}
				require.NoError(t, err)
				completed = true
				break
			}
			require.True(t, completed, "allocation sweep did not reach success")
		})
	}
}

func TestAccountedAggregateMergesRollbackEveryPhysicalAllocationFailure(t *testing.T) {
	long := "physical-allocation-merge-sweep-varlen-payload"
	tests := []struct {
		name   string
		id     int64
		extra  any
		params []types.Type
		build  func(*testing.T, *mpool.MPool) []*vector.Vector
	}{
		{
			name: "any", id: AggIdOfAny, params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-a", long + "-b", long + "-c", long + "-d"})}
			},
		},
		{
			name: "min", id: AggIdOfMin, params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-d", long + "-a", long + "-c", long + "-b"})}
			},
		},
		{
			name: "max-by", id: AggIdOfMaxBy,
			params: []types.Type{
				types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
			},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{
					buildVarlenVec(t, mp, types.T_varchar.ToType(),
						[]string{long + "-a", long + "-b", long + "-c", long + "-d"}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 1, 1}),
				}
			},
		},
		{
			name: "bitmap", id: AggIdOfBitmapConstruct,
			params: []types.Type{types.T_uint64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_uint64.ToType(), []uint64{1, 2, 3, 4})}
			},
		},
		{
			name: "median", id: AggIdOfMedian,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "percentile-cont", id: AggIdOfPercentileCont,
			extra: []byte("0.5"), params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "percentile-disc", id: AggIdOfPercentileDisc,
			extra: []byte("0.5"), params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
		{
			name: "approx-percentile", id: AggIdOfApproxPercentile,
			extra: []byte("0.5"), params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8})}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			completed := false
			for failAt := 1; failAt <= 128; failAt++ {
				mp := mpool.MustNewZero()
				controller := &rejectNthAggregateAllocation{failAt: failAt}
				registry, err := mpool.NewAllocationAccountRegistry(1, 512)
				require.NoError(t, err)
				account, err := registry.OpenWithController(128<<20, controller)
				require.NoError(t, err)
				allocation, err := NewAllocationAccount(
					account, mpool.AllocationOwnerGroup, AllocationAccountSites{
						VectorData: 1, VectorArea: 2, VectorNulls: 3,
						VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
					})
				require.NoError(t, err)
				makeExec := func() GroupAggFuncExec {
					exec, makeErr := MakeGroupAgg(
						mp, tc.id, false, allocation, tc.extra, tc.params...)
					require.NoError(t, makeErr)
					SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
					return exec
				}
				left, right := makeExec(), makeExec()
				vectors := tc.build(t, mp)
				groups := []uint64{1, 1, 2, 2}
				if err = left.GroupGrow(2); err == nil {
					err = right.GroupGrow(2)
				}
				if err == nil {
					err = right.PreflightBatchFill(0, groups, vectors)
				}
				if err == nil {
					err = right.BatchFill(0, groups, vectors)
				}
				if err == nil {
					err = left.PreflightBatchMerge(right, 0, []uint64{1, 2})
				}
				if err == nil {
					err = left.BatchMerge(right, 0, []uint64{1, 2})
				}
				var results []*vector.Vector
				if err == nil {
					results, err = left.Flush()
				}
				for _, result := range results {
					if result != nil {
						result.Free(mp)
					}
				}
				for _, vec := range vectors {
					vec.Free(mp)
				}
				for _, exec := range []GroupAggFuncExec{left, right} {
					exec.Free()
					require.NoError(t, exec.ClearAllocationAccount(allocation))
				}
				require.Zero(t, account.Snapshot().Used, "failAt=%d", failAt)
				require.Zero(t, controller.used, "failAt=%d", failAt)
				account.Seal()
				_, finalizeErr := registry.Finalize(account)
				require.NoError(t, finalizeErr)
				require.Zero(t, mp.CurrNB(), "failAt=%d", failAt)
				if controller.rejected {
					require.Error(t, err, "failAt=%d", failAt)
					require.True(t, mpool.IsRetryableAllocationCapacity(err),
						"failAt=%d err=%v", failAt, err)
					continue
				}
				require.NoError(t, err)
				completed = true
				break
			}
			require.True(t, completed, "merge allocation sweep did not reach success")
		})
	}
}
