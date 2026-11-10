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
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestWindowPrefixSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int64
	}{
		{"count_star", AggIdOfCountStar}, {"count_column", AggIdOfCountColumn},
		{"sum", AggIdOfSum}, {"avg", AggIdOfAvg}, {"min", AggIdOfMin}, {"max", AggIdOfMax},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := tc.id
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			input := vector.NewOffHeapVecWithType(types.T_float64.ToType())
			t.Cleanup(func() { input.Free(mp) })
			values := []float64{0, math.Copysign(0, -1), 1e16, 1, -1e16, math.Float64frombits(0x7ff8000000000001), math.Inf(1)}
			for i, value := range values {
				require.NoError(t, vector.AppendFixed(input, value, i == 0, mp))
			}
			running, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			t.Cleanup(running.Free)
			output, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			t.Cleanup(output.Free)
			oracle, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			t.Cleanup(oracle.Free)
			require.True(t, SupportsWindowPrefixSnapshot(running))
			require.NoError(t, GrowWindowGroups(running, 1))
			require.NoError(t, GrowWindowGroups(output, len(values)))
			require.NoError(t, oracle.GroupGrow(len(values)))
			for row := range values {
				require.NoError(t, running.Fill(0, row, []*vector.Vector{input}))
				require.NoError(t, SnapshotWindowPrefix(output, running, row, 0))
				for previous := 0; previous <= row; previous++ {
					require.NoError(t, oracle.Fill(row, previous, []*vector.Vector{input}))
				}
			}
			got, err := output.Flush()
			t.Cleanup(func() {
				for _, vec := range got {
					vec.Free(mp)
				}
			})
			require.NoError(t, err)
			want, err := oracle.Flush()
			t.Cleanup(func() {
				for _, vec := range want {
					vec.Free(mp)
				}
			})
			require.NoError(t, err)
			require.Len(t, got, 1)
			require.Len(t, want, 1)
			for row := range values {
				require.Equal(t, want[0].IsNull(uint64(row)), got[0].IsNull(uint64(row)))
				if !want[0].IsNull(uint64(row)) {
					require.Equal(t, want[0].GetRawBytesAt(row), got[0].GetRawBytesAt(row), "row %d", row)
				}
			}
		})
	}
}

func TestWindowGroupCapacity(t *testing.T) {
	for _, id := range []int64{AggIdOfCountStar, AggIdOfCountColumn, AggIdOfSum, AggIdOfAvg, AggIdOfMin, AggIdOfMax} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			mp := mpool.MustNewZero()
			t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
			input := vector.NewVec(types.T_float64.ToType())
			t.Cleanup(func() { input.Free(mp) })
			require.NoError(t, vector.AppendFixed(input, float64(7), false, mp))
			exec, err := MakeAgg(mp, id, false, *input.GetType())
			require.NoError(t, err)
			t.Cleanup(exec.Free)
			require.NoError(t, GrowWindowGroups(exec, 3))
			state := exec.(windowPrefixState).windowPrefixState()
			require.Equal(t, 3, state.GetNumGroups())
			require.Less(t, state.state[0].vecs[0].Capacity(), AggBatchSize)
			require.NoError(t, exec.Fill(0, 0, []*vector.Vector{input}))
			// Compact physical storage must not change the logical chunk map,
			// initial NULLs or values when normal growth crosses a chunk boundary.
			require.NoError(t, exec.GroupGrow(AggBatchSize))
			require.Equal(t, AggBatchSize+3, state.GetNumGroups())
			require.NoError(t, exec.Fill(AggBatchSize+2, 0, []*vector.Vector{input}))
			result, err := exec.Flush()
			t.Cleanup(func() {
				for _, vec := range result {
					vec.Free(mp)
				}
			})
			require.NoError(t, err)
			require.Len(t, result, 2)
			for chunk, vec := range result {
				for row := 0; row < vec.Length(); row++ {
					filled := (chunk == 0 && row == 0) || (chunk == 1 && row == 2)
					if id == AggIdOfCountStar || id == AggIdOfCountColumn {
						want := int64(0)
						if filled {
							want = 1
						}
						require.Equal(t, want, vector.GetAny(vec, row, false))
					} else {
						require.Equal(t, !filled, vec.IsNull(uint64(row)))
						if filled {
							require.Equal(t, float64(7), vector.GetAny(vec, row, false))
						}
					}
				}
			}
		})
	}
}

func BenchmarkWindowGroupCapacity(b *testing.B) {
	for _, groups := range []int{16, 256, AggBatchSize} {
		for _, compact := range []bool{false, true} {
			b.Run(fmt.Sprintf("groups_%d/compact_%t", groups, compact), func(b *testing.B) {
				mp := mpool.MustNewZero()
				b.Cleanup(func() {
					if mp.CurrNB() != 0 {
						b.Fatal("leaked aggregate memory")
					}
					mpool.DeleteMPool(mp)
				})
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					exec := makeSumAvgExec(mp, true, AggIdOfSum, false, types.New(types.T_decimal128, 38, 2))
					var err error
					if compact {
						err = GrowWindowGroups(exec, groups)
					} else {
						err = exec.GroupGrow(groups)
					}
					exec.Free()
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestWindowGroupCapacityAllocationFailure(t *testing.T) {
	for _, id := range []int64{AggIdOfSum, AggIdOfAvg} {
		allocations := 0
		t.Run(fmt.Sprintf("%d/success", id), func(t *testing.T) {
			f := newFixedMetadataPreflightFixture(t, id, types.T_float64.ToType())
			require.NoError(t, GrowWindowGroups(f.exec, 3))
			allocations = f.controller.calls
			require.Positive(t, allocations)
		})
		for failAt := 1; failAt <= allocations; failAt++ {
			t.Run(fmt.Sprintf("%d/fail_%d", id, failAt), func(t *testing.T) {
				f := newFixedMetadataPreflightFixture(t, id, types.T_float64.ToType())
				f.controller.failAt = failAt
				require.ErrorIs(t, GrowWindowGroups(f.exec, 3), mpool.ErrAllocationAccountCapacity)
				require.True(t, f.controller.rejected)
				require.Empty(t, f.base.state)
				require.Zero(t, f.controller.used)
				f.controller.failAt = 0
				require.NoError(t, GrowWindowGroups(f.exec, 3))
				require.Equal(t, 3, f.base.GetNumGroups())
			})
		}
	}
}

func TestWindowPrefixSnapshotRejectsUnboundedStates(t *testing.T) {
	mp := mpool.MustNewZero()
	for _, tc := range []struct {
		id       int64
		distinct bool
		typ      types.Type
	}{
		{AggIdOfSum, true, types.T_int32.ToType()},
		{AggIdOfMin, false, types.T_varchar.ToType()},
		{AggIdOfVarPop, false, types.T_int32.ToType()},
	} {
		exec, err := MakeAgg(mp, tc.id, tc.distinct, tc.typ)
		require.NoError(t, err)
		require.False(t, SupportsWindowPrefixSnapshot(exec))
		require.NoError(t, GrowWindowGroups(exec, 1))
		require.Equal(t, int32(AggBatchSize), exec.(aggregateBaseCarrier).aggregateBase().state[0].capacity)
		exec.Free()
	}
	require.Zero(t, mp.CurrNB())
}
