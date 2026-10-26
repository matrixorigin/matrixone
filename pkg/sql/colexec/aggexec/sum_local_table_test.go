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

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/stretchr/testify/require"
)

func TestSumFullLocalTablePreservesGroups(t *testing.T) {
	for _, distinctGroups := range []int{254, 255, 256} {
		for _, constant := range []bool{false, true} {
			for _, chunk := range []int{hashmap.UnitLimit, 1024} {
				t.Run(fmt.Sprintf("groups_%d/constant_%t/chunk_%d", distinctGroups, constant, chunk), func(t *testing.T) {
					mp := mpool.MustNewZero()
					t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
					param := types.New(types.T_decimal128, 38, 0)
					exec := makeSumAvgExec(mp, true, AggIdOfSum, false, param)
					t.Cleanup(exec.Free)
					require.IsType(t, &sumAvgDecExec[types.Decimal128, types.Decimal256]{}, exec)
					require.NoError(t, exec.GroupGrow(901))
					groups := make([]uint64, distinctGroups, distinctGroups+8)
					for i := range groups {
						groups[i] = uint64(100 + i*2)
					}
					// Both outside-range directions, an in-range hole, both endpoints,
					// and a repeated existing group after the table became full.
					groups = append(groups, 50, 900, 101, 100, 608, 100, GroupNotMatched, 899)
					const offset = 2
					values := make([]types.Decimal128, len(groups)+offset)
					want := make([]int64, 901)
					for i, group := range groups {
						n := int64(i%7 - 3)
						if constant {
							n = 5
						}
						values[i+offset] = types.Decimal128FromInt64(n)
						if group != GroupNotMatched && (constant || i != len(groups)-1) {
							want[group-1] += n
						}
					}
					var input *vector.Vector
					if constant {
						var err error
						input, err = vector.NewConstFixed(param, types.Decimal128FromInt64(5), len(values), mp)
						require.NoError(t, err)
					} else {
						input = vector.NewVec(param)
					}
					t.Cleanup(func() { input.Free(mp) })
					if !constant {
						for i, value := range values {
							require.NoError(t, vector.AppendFixed(input, value, i == len(values)-1, mp))
						}
					}
					for start := 0; start < len(groups); start += chunk {
						require.NoError(t, exec.BatchFill(offset+start, groups[start:min(start+chunk, len(groups))], []*vector.Vector{input}))
					}
					results, err := exec.Flush()
					t.Cleanup(func() {
						for _, result := range results {
							result.Free(mp)
						}
					})
					require.NoError(t, err)
					require.Len(t, results, 1)
					got := vector.MustFixedColNoTypeCheck[types.Decimal256](results[0])
					seen := make(map[uint64]bool)
					for i, g := range groups {
						if g != 0 && (constant || i != len(groups)-1) {
							seen[g] = true
						}
					}
					for i := range want {
						if !seen[uint64(i+1)] {
							require.True(t, results[0].IsNull(uint64(i)))
							continue
						}
						require.False(t, results[0].IsNull(uint64(i)))
						require.Equal(t, fmt.Sprint(want[i]), got[i].Format(0), "group %d", i+1)
					}
				})
			}
		}
	}
}

// Mirror group.buildOneBatch: state grows once, BatchFill receives at most
// hashmap.UnitLimit rows, with offsets into the original input vector.
func BenchmarkSumFullLocalTable(b *testing.B) {
	for _, tc := range []struct {
		name               string
		cardinality, chunk int
		order              string
	}{
		{"groups_4", 4, hashmap.UnitLimit, "sequential"},
		{"groups_254", 254, hashmap.UnitLimit, "sequential"},
		{"groups_255", 255, hashmap.UnitLimit, "sequential"},
		{"groups_256", 256, hashmap.UnitLimit, "sequential"},
		{"sequential", 8192, hashmap.UnitLimit, "sequential"},
		{"permuted", 8192, hashmap.UnitLimit, "permuted"},
		{"one_endpoint", 8192, hashmap.UnitLimit, "endpoint"},
		{"large_call_sequential", 8192, 8192, "sequential"},
		{"large_call_permuted", 8192, 8192, "permuted"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			mp := mpool.MustNewZero()
			defer mpool.DeleteMPool(mp)
			param := types.New(types.T_decimal128, 38, 0)
			input := vector.NewVec(param)
			defer input.Free(mp)
			groups := make([]uint64, 8192)
			for i := range groups {
				group := i%tc.cardinality + 1
				switch tc.order {
				case "permuted":
					group = (1999*i)%tc.cardinality + 1
				case "endpoint":
					group = i%hashmap.UnitLimit + 1
					if group == hashmap.UnitLimit {
						group = tc.cardinality
					}
				}
				groups[i] = uint64(group)
				if err := vector.AppendFixed(input, types.Decimal128FromInt64(1), false, mp); err != nil {
					b.Fatal(err)
				}
			}
			exec := makeSumAvgExec(mp, true, AggIdOfSum, false, param)
			defer exec.Free()
			if err := exec.GroupGrow(tc.cardinality); err != nil {
				b.Fatal(err)
			}
			vectors := []*vector.Vector{input}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for offset := 0; offset < len(groups); offset += tc.chunk {
					if err := exec.BatchFill(offset, groups[offset:min(offset+tc.chunk, len(groups))], vectors); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestSumFullLocalTablePreservesDeferredOverflowOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	t.Cleanup(func() { require.Zero(t, mp.CurrNB()); mpool.DeleteMPool(mp) })
	param := types.New(types.T_decimal256, 65, 0)
	exec := makeSumAvgExec(mp, true, AggIdOfSum, false, param).(*sumAvgDecExec[types.Decimal256, types.Decimal256])
	t.Cleanup(exec.Free)
	require.NoError(t, exec.GroupGrow(901))
	// Inject a physically reachable wide partial accumulator without building
	// the enormous input needed to reach the physical overflow boundary.
	maximum := types.Decimal256{B0_63: ^uint64(0), B64_127: ^uint64(0), B128_191: ^uint64(0), B192_255: ^uint64(0) >> 1}
	sums := chunkArr[types.Decimal256](exec.state[0].vecs[0])
	for _, group := range []int{99, 899} {
		sums[group] = maximum
		exec.state[0].vecs[0].UnsetNull(uint64(group))
	}
	input := vector.NewVec(param)
	t.Cleanup(func() { input.Free(mp) })
	groups := make([]uint64, 255, 258)
	for i := range groups {
		groups[i] = uint64(100 + i*2)
		require.NoError(t, vector.AppendFixed(input, types.Decimal256FromInt64(1), false, mp))
	}
	groups = append(groups, 50, 100, 900)
	for _, value := range []int64{1, -1, 1} {
		require.NoError(t, vector.AppendFixed(input, types.Decimal256FromInt64(value), false, mp))
	}
	err := exec.BatchFill(0, groups, []*vector.Vector{input})
	require.ErrorContains(t, err, "Decimal256 Add overflow")
	// An early scatter would overflow group 100 before the successful direct
	// update of group 50. The original deferred scatter must remain deferred.
	require.False(t, exec.state[0].vecs[0].IsNull(49))
	require.Equal(t, types.Decimal256FromInt64(1), sums[49])
	require.Equal(t, maximum, sums[99])
}
