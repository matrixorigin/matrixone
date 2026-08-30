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

package top

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	groupop "github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func BenchmarkGroupedAggregateTopFinalization(b *testing.B) {
	for _, groups := range []int{128, aggexec.AggBatchSize + 1} {
		for _, directEdge := range []bool{false, true} {
			name := fmt.Sprintf("groups=%d/direct-edge=%t", groups, directEdge)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()
				for range b.N {
					b.StopTimer()
					mp := mpool.MustNewZero()
					proc := testutil.NewProcessWithMPool(b, "", mp)
					input := batch.NewWithSize(1)
					input.Vecs[0] = vector.NewVec(types.T_int32.ToType())
					keys := make([]int32, groups)
					for i := range keys {
						keys[i] = int32(i)
					}
					require.NoError(b, vector.AppendFixedList(
						input.Vecs[0], keys, nil, mp))
					input.SetRowCount(groups)
					child := colexec.NewMockOperator().WithBatchs(
						[]*batch.Batch{input})
					group := groupop.NewArgument()
					group.NeedEval = true
					group.GroupBy = []*plan.Expr{{
						Typ: plan.Type{
							Id: int32(types.T_int32), NotNullable: true,
						},
						Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
					}}
					group.Aggs = []aggexec.AggFuncExecExpression{
						aggexec.MakeAggFunctionExpression(
							aggexec.AggIdOfCountStar, false,
							[]*plan.Expr{{
								Typ: plan.Type{
									Id: int32(types.T_int32), NotNullable: true,
								},
								Expr: &plan.Expr_Col{
									Col: &plan.ColRef{ColPos: 0},
								},
							}}, nil),
					}
					group.AppendChild(child)
					top := &Top{
						Limit: plan2.MakePlan2Uint64ConstExprWithType(10),
						Fs: []*plan.OrderBySpec{{
							Expr: newExpression(1),
							Flag: plan.OrderBySpec_DESC,
						}},
					}
					var transparent *transparentOperator
					if directEdge {
						top.AppendChild(group)
					} else {
						transparent = &transparentOperator{}
						transparent.AppendChild(group)
						top.AppendChild(transparent)
					}

					b.StartTimer()
					require.NoError(b, vm.Prepare(top, proc))
					result, err := vm.Exec(top, proc)
					require.NoError(b, err)
					require.NotNil(b, result.Batch)
					b.StopTimer()

					b.ReportMetric(
						float64(top.OpAnalyzer.GetOpStats().InputRows),
						"top-input-rows/op")
					b.ReportMetric(
						float64(group.OpAnalyzer.GetOpStats().ExtraStats["GroupTopKFinalizedBytes"]),
						"fused-finalized-bytes/op")
					top.Free(proc, false, nil)
					if transparent != nil {
						transparent.Free(proc, false, nil)
					}
					group.Free(proc, false, nil)
					child.Free(proc, false, nil)
					group.Release()
					proc.Free()
					require.Zero(b, mp.CurrNB())
				}
			})
		}
	}
}
