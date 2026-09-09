// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hashjoin

import (
	"fmt"
	"testing"

	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestRightExistentialDuplicateGroupAcrossBatchesAndReset(t *testing.T) {
	typ := types.T_int32.ToType()
	for _, join := range []plan.Node_JoinType{plan.Node_SEMI, plan.Node_ANTI} {
		t.Run(join.String(), func(t *testing.T) {
			tc := newTestCase(t, []bool{true}, []types.Type{typ}, []colexec.ResultPos{colexec.NewResultPos(1, 0)}, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			tc.arg.JoinType, tc.arg.IsRightJoin, tc.arg.NonEqCond = join, true, nil
			defer func() {
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				tc.proc.Free()
				tc.cancel()
				require.Zero(t, tc.proc.Mp().CurrNB())
			}()
			const duplicates = colexec.DefaultBatchSize*2 + 17
			for generation := 0; generation < 2; generation++ {
				values := make([]int32, duplicates+8)
				for i := range duplicates {
					values[i] = 1
				}
				for i := duplicates; i < len(values); i++ {
					values[i] = 2
				}
				build := makeInt32Batch(tc.proc, values)
				build.Vecs[0].GetNulls().Add(uint64(len(values) - 1))
				probeKey := int32(generation + 1)
				probe := makeInt32Batch(tc.proc, []int32{probeKey, probeKey, probeKey, 3, 0})
				probe.Vecs[0].GetNulls().Add(4)
				resetHashBuildChildrenWithBatch(tc.barg, build)
				resetChildrenWithBatch(tc.arg, probe)
				require.NoError(t, tc.arg.Prepare(tc.proc))
				require.NoError(t, tc.barg.Prepare(tc.proc))
				_, err := vm.Exec(tc.barg, tc.proc)
				require.NoError(t, err)
				counts := map[int32]int{}
				nulls := 0
				for {
					result, err := vm.Exec(tc.arg, tc.proc)
					require.NoError(t, err)
					if result.Batch == nil {
						break
					}
					v := result.Batch.Vecs[0]
					for i, value := range vector.MustFixedColWithTypeCheck[int32](v) {
						if v.GetNulls().Contains(uint64(i)) {
							nulls++
						} else {
							counts[value]++
						}
					}
				}
				want := map[int32]int{}
				wantNulls := 0
				if join == plan.Node_SEMI {
					if generation == 0 {
						want[1] = duplicates
					} else {
						want[2] = 7
					}
				} else {
					wantNulls = 1
					if generation == 0 {
						want[2] = 7
					} else {
						want[1] = duplicates
					}
				}
				require.Equal(t, want, counts)
				require.Equal(t, wantNulls, nulls)
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.proc.GetMessageBoard().Reset()
			}
		})
	}
}

func TestRightExistentialResidualMatchesDifferentRows(t *testing.T) {
	typ := types.T_int32.ToType()
	for _, join := range []plan.Node_JoinType{plan.Node_SEMI, plan.Node_ANTI} {
		t.Run(join.String(), func(t *testing.T) {
			// Hash on the constant key in column 1; the existing equality residual
			// compares payload column 0. First probe marks only the first build row,
			// so its bitmap bit must not certify the entire group on the second probe.
			tc := newTestCase(t, []bool{false, false}, []types.Type{typ, typ}, []colexec.ResultPos{colexec.NewResultPos(1, 0)}, [][]*plan.Expr{{newExpr(1, typ)}, {newExpr(1, typ)}})
			tc.arg.JoinType, tc.arg.IsRightJoin = join, true
			defer func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				tc.proc.Free()
				tc.cancel()
				require.Zero(t, tc.proc.Mp().CurrNB())
			}()
			makeBatch := func(values []int32) *batch.Batch {
				bat := batch.NewWithSize(2)
				for i := range 2 {
					bat.Vecs[i] = vector.NewVec(typ)
				}
				require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, tc.proc.Mp()))
				keys := make([]int32, len(values))
				require.NoError(t, vector.AppendFixedList(bat.Vecs[1], keys, nil, tc.proc.Mp()))
				bat.SetRowCount(len(values))
				return bat
			}
			resetHashBuildChildrenWithBatch(tc.barg, makeBatch([]int32{10, 20, 30}))
			resetChildrenWithBatch(tc.arg, makeBatch([]int32{10, 20, 20}))
			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			var values []int32
			for {
				r, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				if r.Batch == nil {
					break
				}
				values = append(values, vector.MustFixedColWithTypeCheck[int32](r.Batch.Vecs[0])...)
			}
			if join == plan.Node_SEMI {
				require.Equal(t, []int32{10, 20}, values)
			} else {
				require.Equal(t, []int32{30}, values)
			}
		})
	}
}

// Full build/probe/reset measurement, reusable unchanged on the parent. At low
// NDV the old implementation revisited all build duplicates for every probe.
func BenchmarkRightExistentialRepeatedKeys(b *testing.B) {
	for _, n := range []int{2048, 8192} {
		for _, join := range []plan.Node_JoinType{plan.Node_SEMI, plan.Node_ANTI} {
			b.Run(fmt.Sprintf("%s/%d", join, n), func(b *testing.B) {
				typ := types.T_int32.ToType()
				tc := newTestCase(b, []bool{false}, []types.Type{typ}, []colexec.ResultPos{colexec.NewResultPos(1, 0)}, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
				tc.arg.JoinType, tc.arg.IsRightJoin, tc.arg.NonEqCond = join, true, nil
				values := make([]int32, n)
				for i := range values {
					values[i] = int32(i % 2)
				}
				defer func() {
					tc.arg.Free(tc.proc, false, nil)
					tc.barg.Free(tc.proc, false, nil)
					tc.proc.Free()
					tc.cancel()
				}()
				b.ReportAllocs()
				b.ResetTimer()
				for k := 0; k < b.N; k++ {
					resetHashBuildChildrenWithBatch(tc.barg, makeInt32Batch(tc.proc, values))
					resetChildrenWithBatch(tc.arg, makeInt32Batch(tc.proc, values))
					require.NoError(b, tc.arg.Prepare(tc.proc))
					require.NoError(b, tc.barg.Prepare(tc.proc))
					_, err := vm.Exec(tc.barg, tc.proc)
					require.NoError(b, err)
					rows := 0
					for {
						r, err := vm.Exec(tc.arg, tc.proc)
						require.NoError(b, err)
						if r.Batch == nil {
							break
						}
						rows += r.Batch.RowCount()
					}
					if join == plan.Node_SEMI {
						require.Equal(b, n, rows)
					} else {
						require.Zero(b, rows)
					}
					tc.arg.Reset(tc.proc, false, nil)
					tc.barg.Reset(tc.proc, false, nil)
					tc.proc.GetMessageBoard().Reset()
				}
			})
		}
	}
}

func TestRightExistentialGroupsAcrossSpillBuckets(t *testing.T) {
	typ := types.T_int32.ToType()
	for _, join := range []plan.Node_JoinType{plan.Node_SEMI, plan.Node_ANTI} {
		t.Run(join.String(), func(t *testing.T) {
			tc := newTestCase(t, []bool{false}, []types.Type{typ}, []colexec.ResultPos{colexec.NewResultPos(1, 0)}, [][]*plan.Expr{{newExpr(0, typ)}, {newExpr(0, typ)}})
			defer func() {
				tc.arg.Reset(tc.proc, false, nil)
				tc.barg.Reset(tc.proc, false, nil)
				tc.arg.Free(tc.proc, false, nil)
				tc.barg.Free(tc.proc, false, nil)
				budget, err := tc.proc.GetExecutionResourceBudget()
				require.NoError(t, err)
				require.Zero(t, budget.Used())
				require.Zero(t, budget.SpillDiskUsed())
				require.Zero(t, budget.SpillFDUsed())
				tc.proc.Free()
				tc.cancel()
				require.Zero(t, tc.proc.Mp().CurrNB())
			}()
			tc.proc.Base.Lim.Size = 8 << 20
			tc.proc.Base.Lim.SpillSize = 64 << 20
			tc.arg.JoinType, tc.arg.IsRightJoin, tc.arg.NonEqCond = join, true, nil
			tc.arg.IsShuffle, tc.arg.ShuffleIdx, tc.arg.SpillThreshold = true, 0, 50
			tc.barg.IsShuffle, tc.barg.ShuffleIdx, tc.barg.SpillThreshold = true, 0, 50
			tc.barg.RuntimeFilterSpec = &plan.RuntimeFilterSpec{Tag: tc.arg.JoinMapTag + 1200}
			const rows = 8192
			build, probe := make([]int32, rows), make([]int32, rows)
			for i := range rows {
				build[i] = int32(i % 1024)
				probe[i] = int32(i % 512)
			}
			resetHashBuildChildrenWithBatch(tc.barg, makeInt32Batch(tc.proc, build))
			resetChildrenWithBatch(tc.arg, makeInt32Batch(tc.proc, probe))
			before := promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1"))
			require.NoError(t, tc.arg.Prepare(tc.proc))
			require.NoError(t, tc.barg.Prepare(tc.proc))
			_, err := vm.Exec(tc.barg, tc.proc)
			require.NoError(t, err)
			count := 0
			for {
				r, err := vm.Exec(tc.arg, tc.proc)
				require.NoError(t, err)
				if r.Batch != nil {
					count += r.Batch.RowCount()
					for _, v := range vector.MustFixedColWithTypeCheck[int32](r.Batch.Vecs[0]) {
						if join == plan.Node_SEMI {
							require.Less(t, v, int32(512))
						} else {
							require.GreaterOrEqual(t, v, int32(512))
						}
					}
				}
				if r.Status == vm.ExecStop {
					break
				}
			}
			require.Equal(t, rows/2, count)
			require.Greater(t, promtestutil.ToFloat64(metricv2.HashBuildSpillDepthCounter.WithLabelValues("spill", "1")), before)
		})
	}
}
