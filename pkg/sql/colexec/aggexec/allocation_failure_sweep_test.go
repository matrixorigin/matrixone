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
	"github.com/stretchr/testify/assert"
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

type allocationFailureCase struct {
	groupCount      int
	name, mergeName string
	id              int64
	extra           any
	distinct        bool
	params          []types.Type
	build           func(*testing.T, *mpool.MPool, func(*vector.Vector)) []*vector.Vector
}

func allocationFailureCases(long string) []allocationFailureCase {
	jsonValue := func(t *testing.T, value any) []byte {
		t.Helper()
		bj, err := bytejson.CreateByteJSONWithCheck(value)
		require.NoError(t, err)
		encoded, err := bj.Marshal()
		require.NoError(t, err)
		return encoded
	}
	return []allocationFailureCase{
		{
			name: "any-varlen", groupCount: 2, mergeName: "any", id: AggIdOfAny,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-a", long + "-b", long + "-c", long + "-d"}, own)}
			},
		},
		{
			name: "min-varlen", groupCount: 2, mergeName: "min", id: AggIdOfMin,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-d", long + "-a", long + "-c", long + "-b"}, own)}
			},
		},
		{
			name: "max-by", groupCount: 2, mergeName: "max-by", id: AggIdOfMaxBy,
			params: []types.Type{
				types.T_varchar.ToType(), types.T_int64.ToType(), types.T_int64.ToType(),
			},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{
					buildVarlenVec(t, mp, types.T_varchar.ToType(),
						[]string{long + "-a", long + "-b", long + "-c", long + "-d"}, own),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 2, 3, 4}, own),
					buildFixedVec(t, mp, types.T_int64.ToType(), []int64{1, 1, 1, 1}, own),
				}
			},
		},
		{
			name: "group-concat", groupCount: 2, id: AggIdOfGroupConcat,
			params: []types.Type{types.T_varchar.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{long + "-a", long + "-b", long + "-c", long + "-d"}, own)}
			},
		},
		{
			name: "bitmap", groupCount: 2, mergeName: "bitmap", id: AggIdOfBitmapConstruct,
			params: []types.Type{types.T_uint64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_uint64.ToType(), []uint64{1, 2, 3, 4}, own)}
			},
		},
		{
			name: "json-array", groupCount: 2, id: AggIdOfJsonArrayAgg,
			params: []types.Type{types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				vec := vector.NewVec(types.T_json.ToType())
				own(vec)
				for _, value := range []any{long + "-a", int64(2), true, nil} {
					require.NoError(t, vector.AppendBytes(vec, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{vec}
			},
		},
		{
			name: "json-object", groupCount: 2, id: AggIdOfJsonObjectAgg,
			params: []types.Type{types.T_varchar.ToType(), types.T_json.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				keys := buildVarlenVec(t, mp, types.T_varchar.ToType(),
					[]string{"a", "b", "c", "d"}, own)
				values := vector.NewVec(types.T_json.ToType())
				own(values)
				for _, value := range []any{long + "-a", int64(2), true, nil} {
					require.NoError(t, vector.AppendBytes(values, jsonValue(t, value), false, mp))
				}
				return []*vector.Vector{keys, values}
			},
		},
		{
			name: "median", groupCount: 2, mergeName: "median", id: AggIdOfMedian,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8}, own)}
			},
		},
		{
			name: "median-decimal64", groupCount: 2, id: AggIdOfMedian,
			params: []types.Type{types.New(types.T_decimal64, 10, 2)},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.New(types.T_decimal64, 10, 2),
					mustDecimal64s(t, "9.00", "1.00", "5.00", "8.00"), own)}
			},
		},
		{
			name: "percentile-cont", groupCount: 2, mergeName: "percentile-cont", id: AggIdOfPercentileCont, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8}, own)}
			},
		},
		{
			name: "percentile-disc", groupCount: 2, mergeName: "percentile-disc", id: AggIdOfPercentileDisc, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8}, own)}
			},
		},
		{
			name: "approx-count", groupCount: 2, id: AggIdOfApproxCount,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8}, own)}
			},
		},
		{
			name: "hll-add", groupCount: 2, id: AggIdOfHllAdd,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8}, own)}
			},
		},
		{
			name: "approx-percentile", groupCount: 2, mergeName: "approx-percentile", id: AggIdOfApproxPercentile, extra: []byte("0.5"),
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp,
					types.T_int64.ToType(), []int64{9, 1, 5, 8}, own)}
			},
		},
		{
			name: "var-pop-int64-origin", groupCount: 3, id: AggIdOfVarPop,
			params: []types.Type{types.T_int64.ToType()},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp, types.T_int64.ToType(), []int64{9223372036854775804, 9223372036854775805, 9223372036854775806, 9223372036854775807}, own)}
			},
		},
		{
			name: "var-pop-distinct-decimal128", groupCount: 3, id: AggIdOfVarPop, distinct: true,
			params: []types.Type{types.New(types.T_decimal128, 20, 2)},
			build: func(t *testing.T, mp *mpool.MPool, own func(*vector.Vector)) []*vector.Vector {
				return []*vector.Vector{buildFixedVec(t, mp, types.New(types.T_decimal128, 20, 2), mustDecimal128s(t, "9.00", "1.00", "5.00", "8.00"), own)}
			},
		},
	}
}

func TestAccountedAggregatesRollbackEveryPhysicalAllocationFailure(t *testing.T) {
	for _, tc := range allocationFailureCases("physical-allocation-failure-sweep-varlen-payload") {
		t.Run(tc.name, func(t *testing.T) { sweepAggregateAllocations(t, tc, false) })
	}
}

func TestAccountedAggregateMergesRollbackEveryPhysicalAllocationFailure(t *testing.T) {
	for _, tc := range allocationFailureCases("physical-allocation-merge-sweep-varlen-payload") {
		if tc.mergeName != "" {
			t.Run(tc.mergeName, func(t *testing.T) { sweepAggregateAllocations(t, tc, true) })
		}
	}
}

// Inputs are borrowed and immutable; each denial attempt owns fresh aggregate state.
func sweepAggregateAllocations(t *testing.T, tc allocationFailureCase, merge bool) {
	t.Helper()
	inputPool := newAggExecTestPool(t)
	vectors := tc.build(t, inputPool, func(v *vector.Vector) {
		t.Cleanup(func() { v.Free(inputPool) })
	})
	for failAt := 1; failAt <= 128; failAt++ {
		controller := &rejectNthAggregateAllocation{failAt: failAt}
		err := attemptAggregateAllocation(t, tc, vectors, controller, merge)
		if t.Failed() {
			return
		}
		require.Positive(t, controller.calls, "no accounted physical allocation")
		require.Equal(t, controller.calls >= failAt, controller.rejected,
			"physical denial witness failAt=%d calls=%d", failAt, controller.calls)
		if controller.rejected {
			require.Error(t, err, "failAt=%d", failAt)
			require.True(t, mpool.IsRetryableAllocationCapacity(err), "failAt=%d err=%v", failAt, err)
			continue
		}
		require.NoError(t, err)
		return
	}
	t.Fatal("allocation sweep did not reach success")
}

func attemptAggregateAllocation(t *testing.T, tc allocationFailureCase, vectors []*vector.Vector, controller *rejectNthAggregateAllocation, merge bool) error {
	t.Helper()
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	var account *mpool.AllocationAccount
	var registry *mpool.AllocationAccountRegistry
	var allocation *AllocationAccount
	var execs []GroupAggFuncExec
	var results []*vector.Vector
	// Registered before initialization: FailNow also releases partially built attempts.
	defer func() {
		for _, result := range results {
			if result != nil {
				result.Free(mp)
			}
		}
		for _, exec := range execs {
			exec.Free()
		}
		for _, exec := range execs {
			assert.NoError(t, exec.ClearAllocationAccount(allocation))
		}
		if account != nil {
			assert.Zero(t, account.Snapshot().Used, "failAt=%d", controller.failAt)
			assert.Zero(t, controller.used, "failAt=%d", controller.failAt)
			account.Seal()
			_, err := registry.Finalize(account)
			assert.NoError(t, err)
		}
		assert.Zero(t, mp.CurrNB(), "failAt=%d", controller.failAt)
		bytes, objects := mp.OnHeapOutstanding()
		assert.Zero(t, bytes)
		assert.Zero(t, objects)
	}()
	var err error
	registry, err = mpool.NewAllocationAccountRegistry(1, 512)
	require.NoError(t, err)
	account, err = registry.OpenWithController(128<<20, controller)
	require.NoError(t, err)
	allocation, err = NewAllocationAccount(account, mpool.AllocationOwnerGroup, AllocationAccountSites{
		VectorData: 1, VectorArea: 2, VectorNulls: 3, VectorGrouping: 4, ArgumentCount: 5, ArgumentArena: 6,
	})
	require.NoError(t, err)
	makeExec := func() GroupAggFuncExec {
		exec, err := MakeGroupAgg(mp, tc.id, tc.distinct, allocation, tc.extra, tc.params...)
		require.NoError(t, err)
		execs = append(execs, exec)
		SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
		return exec
	}
	left := makeExec()
	groups := []uint64{1, 1, 2, 2}
	if merge {
		right := makeExec()
		if err = left.GroupGrow(tc.groupCount); err == nil {
			err = right.GroupGrow(tc.groupCount)
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
	} else {
		if err = left.GroupGrow(tc.groupCount); err == nil {
			err = left.PreflightBatchFill(0, groups, vectors)
		}
		if err == nil {
			err = left.BatchFill(0, groups, vectors)
		}
	}
	if err == nil {
		results, err = left.Flush()
	}
	return err
}
