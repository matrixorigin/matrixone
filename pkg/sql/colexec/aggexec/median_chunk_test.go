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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestMedianFlushAcrossResultChunks(t *testing.T) {
	for _, typ := range []types.Type{
		types.T_int64.ToType(), types.New(types.T_decimal64, 10, 2), types.New(types.T_decimal128, 20, 2),
	} {
		for _, count := range []int{0, 1, AggBatchSize - 1, AggBatchSize, AggBatchSize + 1, 2*AggBatchSize + 1} {
			for _, accounted := range []bool{false, true} {
				if accounted && count != AggBatchSize+1 {
					continue
				}
				t.Run(fmt.Sprintf("%s/groups=%d/accounted=%t", typ.Oid, count, accounted), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
					exec, err := MakeAgg(mp, AggIdOfMedian, false, typ)
					require.NoError(t, err)
					if accounted {
						registry, account, allocation := newTestAggregateAllocation(t)
						owner := exec.(AllocationAccountOwner)
						require.NoError(t, owner.SetAllocationAccount(allocation))
						t.Cleanup(func() {
							require.NoError(t, owner.ClearAllocationAccount(allocation))
							finishTestAggregateAllocation(t, registry, account)
						})
					}
					t.Cleanup(exec.Free)
					SyncAggregatorsToChunkSize([]AggFuncExec{exec}, AggBatchSize)
					require.NoError(t, exec.GroupGrow(count))

					samples := []struct {
						group int
						value int64
						null  bool
					}{
						{0, 10, true}, {1, 20, false}, {AggBatchSize - 2, 25, false}, {AggBatchSize - 1, 30, false},
						{AggBatchSize, 99, false}, {AggBatchSize + 1, 0, true},
						{2*AggBatchSize - 1, 42, false}, {2 * AggBatchSize, 123, false},
					}
					var raw []int64
					var groups []uint64
					var nullRows []int
					want := make(map[int]int64)
					for _, sample := range samples {
						if sample.group >= count {
							continue
						}
						groups = append(groups, uint64(sample.group+1))
						raw = append(raw, sample.value)
						if sample.null {
							nullRows = append(nullRows, len(raw)-1)
						} else {
							want[sample.group] = sample.value
						}
					}
					var values any = raw
					switch typ.Oid {
					case types.T_decimal64:
						decimal := make([]types.Decimal64, len(raw))
						for i, value := range raw {
							decimal[i] = types.Decimal64(value * 100)
						}
						values = decimal
					case types.T_decimal128:
						decimal := make([]types.Decimal128, len(raw))
						for i, value := range raw {
							decimal[i] = types.Decimal128{B0_63: uint64(value * 100)}
						}
						values = decimal
					}
					input := medianTestVector(t, mp, typ, values)
					t.Cleanup(func() { input.Free(mp) })
					for _, row := range nullRows {
						input.SetNull(uint64(row))
					}
					if accounted {
						require.NoError(t, exec.(BatchCapacityPreflight).PreflightBatchFill(0, groups, []*vector.Vector{input}))
					}
					require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
					result, err := exec.Flush()
					t.Cleanup(func() {
						for _, v := range result {
							v.Free(mp)
						}
					})
					require.NoError(t, err)
					if count == 0 {
						require.Len(t, result, 1)
						require.Zero(t, result[0].Length())
						return
					}
					require.Len(t, result, (count+AggBatchSize-1)/AggBatchSize)
					for i, chunk := range result {
						require.Equal(t, min(AggBatchSize, count-i*AggBatchSize), chunk.Length())
					}
					for group := 0; group < count; group++ {
						chunk, row := result[group/AggBatchSize], group%AggBatchSize
						expected, present := want[group]
						if chunk.IsNull(uint64(row)) != !present {
							t.Fatalf("group %d: NULL=%t, want %t", group, chunk.IsNull(uint64(row)), !present)
						}
						if !present {
							continue
						}
						if typ.Oid == types.T_int64 {
							if got := vector.GetFixedAtNoTypeCheck[float64](chunk, row); got != float64(expected) {
								t.Fatalf("group %d: got %v, want %d", group, got, expected)
							}
						} else if got := vector.GetFixedAtNoTypeCheck[types.Decimal128](chunk, row); got != (types.Decimal128{B0_63: uint64(expected * 1000)}) {
							t.Fatalf("group %d: got %s, want %d.000", group, got.Format(chunk.GetType().Scale), expected)
						}
					}
				})
			}
		}
	}
}

func TestPreallocatedMedianAndPercentileChunks(t *testing.T) {
	for _, tc := range []struct {
		name     string
		distinct bool
		ordered  bool
	}{
		{"median", false, false}, {"median_distinct", true, false}, {"percentile", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
			var exec AggFuncExec
			var err error
			if tc.ordered {
				exec, err = makeOrderedPercentileExec(mp, AggIdOfPercentileCont, false, types.T_int64.ToType(), orderedPercentileContinuous)
			} else {
				exec, err = MakeAgg(mp, AggIdOfMedian, tc.distinct, types.T_int64.ToType())
			}
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			if tc.ordered {
				require.NoError(t, exec.SetExtraInformation(EncodeOrderedPercentileConfig([]byte("0.5"), false), 0))
			}
			SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 4)
			require.NoError(t, exec.PreAllocateGroups(12))
			require.NoError(t, exec.GroupGrow(12))
			var values []int64
			var groups []uint64
			for group := 0; group < 12; group++ {
				values = append(values, int64(group+1), int64(group+3), int64(group+3))
				groups = append(groups, uint64(group+1), uint64(group+1), uint64(group+1))
			}
			input := buildFixedVec(t, mp, types.T_int64.ToType(), values)
			t.Cleanup(func() { input.Free(mp) })
			for row := uint64(0); row < 3; row++ {
				input.SetNull(row)
			}
			require.NoError(t, exec.BatchFill(0, groups, []*vector.Vector{input}))
			result, err := exec.Flush()
			t.Cleanup(func() {
				for _, v := range result {
					v.Free(mp)
				}
			})
			require.NoError(t, err)
			require.Len(t, result, 3)
			for group := 0; group < 12; group++ {
				chunk, row := result[group/4], group%4
				if group == 0 {
					require.True(t, chunk.IsNull(uint64(row)))
					continue
				}
				want := float64(group + 3)
				if tc.distinct {
					want = float64(group + 2)
				}
				require.False(t, chunk.IsNull(uint64(row)))
				require.Equal(t, want, vector.GetFixedAtNoTypeCheck[float64](chunk, row))
			}
		})
	}
}

func TestMedianFlushErrorKeepsResultsOwned(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	typ := types.New(types.T_decimal128, 38, 0)
	exec, err := MakeAgg(mp, AggIdOfMedian, false, typ)
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 2)
	require.NoError(t, exec.GroupGrow(3))
	large, err := types.ParseDecimal128("99999999999999999999999999999999999999", 38, 0)
	require.NoError(t, err)
	input := medianTestVector(t, mp, typ, []types.Decimal128{{B0_63: 1}, large})
	t.Cleanup(func() { input.Free(mp) })
	require.NoError(t, exec.BatchFill(0, []uint64{1, 3}, []*vector.Vector{input}))
	result, err := exec.Flush()
	require.Nil(t, result)
	require.Truef(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), "error: %v", err)
	require.ErrorContains(t, err, "Decimal128 scale overflow")
}

func TestPreallocatedRowNumberChunks(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()) })
	exec, err := makeWindowExec(mp, WinIdOfRowNumber, false)
	require.NoError(t, err)
	t.Cleanup(exec.Free)
	SyncAggregatorsToChunkSize([]AggFuncExec{exec}, 4)
	require.NoError(t, exec.PreAllocateGroups(12))
	require.NoError(t, exec.GroupGrow(12))
	boundaries := buildFixedVec(t, mp, types.T_int64.ToType(), []int64{0, 12})
	t.Cleanup(func() { boundaries.Free(mp) })
	for row := 0; row < 2; row++ {
		require.NoError(t, exec.Fill(0, row, []*vector.Vector{boundaries}))
	}
	result, err := exec.Flush()
	t.Cleanup(func() {
		for _, v := range result {
			v.Free(mp)
		}
	})
	require.NoError(t, err)
	require.Len(t, result, 3)
	for group := 0; group < 12; group++ {
		require.Equal(t, uint64(group+1), vector.GetFixedAtNoTypeCheck[uint64](result[group/4], group%4))
	}
}
