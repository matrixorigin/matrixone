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

package window

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestFusedWindowIndependentStateAndReset(t *testing.T) {
	for _, tc := range []struct {
		name                            string
		stopEarly, fullPartition, spill bool
	}{
		{name: "cumulative"},
		{name: "cancel after first chunk", stopEarly: true},
		{name: "full partition", fullPartition: true},
		{name: "shared external sort", spill: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newWindowSpillProcess(t)
			arg := &Window{}
			if tc.spill {
				arg.SpillThreshold = 1
			}
			child := colexec.NewMockOperator()
			arg.AppendChild(child)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			for i, name := range []string{"max", "min"} {
				spec := makeAggWindowSpec(name)
				spec.GetW().Frame = makeCumulativeFrame()
				if tc.fullPartition {
					spec.GetW().Frame = makeFullFrame()
				}
				spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
				arg.WinSpecList = append(arg.WinSpecList, spec)
				fn, err := function.GetFunctionByName(context.Background(), name, []types.Type{types.T_int32.ToType()})
				require.NoError(t, err)
				arg.Aggs = append(arg.Aggs, aggexec.MakeAggFunctionExpression(
					fn.GetEncodedOverloadID(), false, []*plan.Expr{newColExpr(int32(i))}, nil))
			}
			for generation := range 2 {
				ctx, cancel := context.WithCancel(t.Context())
				proc.Ctx = ctx
				t.Cleanup(cancel)
				const rows = colexec.DefaultBatchSize + 3
				first, second := make([]int32, rows), make([]int32, rows)
				for i := range first {
					first[i], second[i] = int32(i+generation*rows), int32(2*rows-i)
				}
				input := batch.NewWithSize(2)
				child.WithBatchs([]*batch.Batch{input})
				input.Vecs[0] = testutil.MakeInt32Vector(first, []uint64{0}, proc.Mp())
				input.Vecs[1] = testutil.MakeInt32Vector(second, []uint64{0}, proc.Mp())
				input.SetRowCount(rows)
				require.NoError(t, arg.Prepare(proc))
				seen := 0
				for {
					result, err := vm.Exec(arg, proc)
					require.NoError(t, err)
					if result.Batch == nil {
						break
					}
					require.Len(t, result.Batch.Vecs, 4)
					left := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[2])
					right := vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[3])
					for i := range left {
						if tc.fullPartition {
							require.Equal(t, first[rows-1], left[i])
							require.Equal(t, second[rows-1], right[i])
							continue
						}
						if seen+i == 0 {
							require.True(t, result.Batch.Vecs[2].IsNull(uint64(i)))
							require.True(t, result.Batch.Vecs[3].IsNull(uint64(i)))
							continue
						}
						require.Equal(t, first[seen+i], left[i])
						require.Equal(t, second[seen+i], right[i])
					}
					seen += len(left)
					if tc.stopEarly && generation == 0 {
						cancel()
						_, err = vm.Exec(arg, proc)
						require.Error(t, err)
						break
					}
				}
				if !tc.stopEarly || generation == 1 {
					require.Equal(t, rows, seen)
				}
				if tc.spill {
					require.Positive(t, arg.OpAnalyzer.GetOpStats().SpillSize)
				}
				arg.Reset(proc, false, nil)
				require.Empty(t, arg.ctr.aggregateStates)
				child.Reset(proc, false, nil)
			}
		})
	}
}
