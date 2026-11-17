// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashjoin

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func BenchmarkHashJoinIdentityProjection(b *testing.B) {
	for _, rows := range []int{1, 256, colexec.DefaultBatchSize} {
		mismatches := []int{-1, 0}
		if rows > 1 {
			mismatches = append(mismatches, rows/2, rows-1)
		}
		for _, mismatch := range mismatches {
			for _, forward := range []bool{false, true} {
				b.Run(fmt.Sprintf("rows=%d/mismatch=%d/forward=%t", rows, mismatch, forward), func(b *testing.B) {
					proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
					input := batch.NewOffHeapWithSize(8)
					arg := &HashJoin{}
					installTestAllocation(b, arg)
					for column := range input.Vecs {
						typ := types.T_decimal128.ToType()
						var err error
						input.Vecs[column], err = vector.NewOffHeapVecWithTypeAndAllocation(typ, arg.resultAllocation)
						require.NoError(b, err)
						arg.LeftTypes = append(arg.LeftTypes, typ)
						arg.ResultCols = append(arg.ResultCols, colexec.ResultPos{Rel: 0, Pos: int32(column)})
						values := make([]types.Decimal128, rows)
						for row := range values {
							values[row] = types.Decimal128{B0_63: uint64(row + column)}
						}
						require.NoError(b, vector.AppendFixedList(input.Vecs[column], values, nil, proc.Mp()))
					}
					input.SetRowCount(rows)
					b.Cleanup(func() {
						arg.Free(proc, false, nil)
						input.Clean(proc.Mp())
						proc.Free()
						require.Zero(b, proc.Mp().CurrNB())
					})
					var values [hashmap.UnitLimit]uint64
					var nonnull [hashmap.UnitLimit]int64
					for i := range nonnull {
						nonnull[i] = 1
					}
					run := func() *batch.Batch {
						if output := arg.ctr.probeOutput; output != nil {
							output.retire()
						}
						if err := arg.resetResultBat(); err != nil {
							b.Fatal(err)
						}
						arg.ctr.leftBat = input
						if forward {
							if err := arg.beginProbeOutput(); err != nil {
								b.Fatal(err)
							}
						}
						selected := 0
						for offset := 0; offset < rows; offset += hashmap.UnitLimit {
							count := min(hashmap.UnitLimit, rows-offset)
							for i := range count {
								values[i] = 1
								if offset+i == mismatch {
									values[i] = 0
								}
							}
							arg.ctr.lastIdx, arg.ctr.vsIdx = offset, 0
							arg.ctr.vs, arg.ctr.zvs = values[:count], nonnull[:count]
							n, err := arg.ctr.appendUniqueMatches(arg, proc, count)
							if err != nil {
								b.Fatal(err)
							}
							selected += n
						}
						arg.ctr.resBat.SetRowCount(selected)
						if forward {
							return arg.ctr.probeOutput.publish(arg)
						}
						return arg.ctr.resBat
					}
					output := run()
					want := rows
					if mismatch >= 0 {
						want--
					}
					require.Equal(b, want, output.RowCount())
					if want > 0 {
						require.Equal(b, want, output.Vecs[0].Length())
					}
					if forward && mismatch < 0 {
						require.Same(b, input.Vecs[0], output.Vecs[0])
					}
					b.ReportAllocs()
					b.SetBytes(int64(rows * 8 * types.T_decimal128.TypeLen()))
					b.ResetTimer()
					for range b.N {
						run()
					}
				})
			}
		}
	}
}
