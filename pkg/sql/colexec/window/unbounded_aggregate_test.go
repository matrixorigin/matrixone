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

package window

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestUnboundedRangeCancellationReleasesState(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	const rows = cancellationCheckInterval * 2
	values := make([]int32, rows)
	for i := range values {
		values[i] = 1
	}
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeAggWindowSpec("sum")
	spec.GetW().Frame = makeCumulativeFrame()
	spec.GetW().Frame.Type = plan.FrameClause_RANGE
	spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
	arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
	ctr := &container{bat: bat, os: []int64{0}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	t.Cleanup(func() {
		ctr.freeAggFun()
		ctr.freeRunningAgg()
		bat.Clean(proc.Mp())
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	// Entry, first fill, then cancellation after 1024 rows of the long peer.
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 3)
	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, rows)
	require.Nil(t, result)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ctr.runningAgg)
	require.Nil(t, ctr.batAggs)
}

func TestUnboundedAggregateChunkAndReuse(t *testing.T) {
	for _, frame := range []string{"full", "range"} {
		t.Run(frame, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			spec := makeAggWindowSpec("sum")
			if frame == "range" {
				spec.GetW().Frame = makeCumulativeFrame()
				spec.GetW().Frame.Type = plan.FrameClause_RANGE
				spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
			}
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newAggExprAt(1)}}
			var child *colexec.MockOperator
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				if child != nil {
					child.Free(proc, false, nil)
				}
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			for generation := 0; generation < 2; generation++ {
				rows := colexec.DefaultBatchSize + 11 + generation*6
				keys, values := make([]int32, rows), make([]int32, rows)
				for i := range values {
					keys[i], values[i] = int32(i/3), int32(generation+1)
				}
				input := batch.NewWithSize(2)
				input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
				input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
				input.SetRowCount(rows)
				child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
				arg.Children = nil
				arg.AppendChild(child)
				require.NoError(t, arg.Prepare(proc))
				seen := 0
				for {
					result, err := vm.Exec(arg, proc)
					require.NoError(t, err)
					if result.Batch == nil {
						break
					}
					got := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2])
					for _, value := range got {
						end := rows
						if frame == "range" {
							end = min((seen/3+1)*3, rows)
						}
						require.Equal(t, int64(end*(generation+1)), value, "row %d", seen)
						seen++
					}
					// The first execution models LIMIT after one output chunk;
					// the second consumes all chunks with a different generation.
					if generation == 0 {
						break
					}
				}
				if generation == 0 {
					require.Equal(t, colexec.DefaultBatchSize, seen)
				} else {
					require.Equal(t, rows, seen)
				}
				arg.Reset(proc, false, nil)
				child.Free(proc, false, nil)
				child = nil
			}
		})
	}
}

func TestUnboundedTimestampRangeFoldAndMonotonic(t *testing.T) {
	for _, fold := range []bool{false, true} {
		name := "monotonic"
		if fold {
			name = "fold"
		}
		t.Run(name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithOwnedMPool(t, "", mp)
			location, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)
			proc.GetSessionInfo().TimeZone = location
			const rows = 160
			order := vector.NewVec(types.T_timestamp.ToType())
			values := make([]int32, rows)
			peers := make([]int64, 0, rows)
			for i := range values {
				value := "2024-11-03 04:00:00.000000"
				if fold {
					value = []string{"2024-11-03 05:00:00.000000", "2024-11-03 05:30:00.000000", "2024-11-03 06:00:00.000000", "2024-11-03 06:30:00.000000"}[i/40]
				}
				ts, parseErr := types.ParseTimestamp(time.UTC, value, 6)
				require.NoError(t, parseErr)
				if !fold {
					ts += types.Timestamp(i)
				}
				require.NoError(t, vector.AppendFixed(order, ts, false, mp))
				values[i] = 1 << (i / 40)
				if !fold || i%40 == 0 {
					peers = append(peers, int64(i))
				}
			}
			bat := makeInt32Batch(mp, values)
			spec := makeWindowSpec()
			spec.GetW().Frame = makeCumulativeFrame()
			spec.GetW().Frame.Type = plan.FrameClause_RANGE
			spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
			ctr := &container{bat: bat, os: peers, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{order}}}}
			t.Cleanup(func() {
				ctr.freeRunningAgg()
				ctr.clearCivilOrder()
				bat.Clean(mp)
				order.Free(mp)
				proc.Free()
				require.Zero(t, mp.CurrNB())
			})
			end := rows
			if !fold {
				end = rows / 2
			}
			result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, end)
			require.NoError(t, err)
			got := vector.MustFixedColWithTypeCheck[int64](result)
			var prefix int64
			for row, value := range got {
				prefix += int64(values[row])
				want := prefix
				if fold {
					want = []int64{5 * 40, 15 * 40, 5 * 40, 15 * 40}[row/40]
				}
				require.Equal(t, want, value, "row %d", row)
			}
			result.Free(mp)
			if fold {
				require.Nil(t, ctr.runningAgg)
				return
			}
			require.NotNil(t, ctr.runningAgg)
			result, err = ctr.processAggregateFuncRange(0, arg, proc, end, rows)
			require.NoError(t, err)
			for row, value := range vector.MustFixedColWithTypeCheck[int64](result) {
				prefix += int64(values[end+row])
				require.Equal(t, prefix, value)
			}
			result.Free(mp)
			require.Nil(t, ctr.runningAgg)
		})
	}
}

func TestUnboundedDistinctFullAndRange(t *testing.T) {
	for _, frame := range []string{"full", "range"} {
		t.Run(frame, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			const rows = 132
			keys, values := make([]int32, rows), make([]int32, rows)
			for i := range values {
				keys[i], values[i] = int32(i), int32(i%3+1)
			}
			input := batch.NewWithSize(2)
			input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
			input.SetRowCount(rows)
			spec := makeAggWindowSpec("sum")
			if frame == "range" {
				spec.GetW().Frame = makeCumulativeFrame()
				spec.GetW().Frame.Type = plan.FrameClause_RANGE
				spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
			}
			base := newAggExprAt(1)
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(base.GetAggID(), true, []*plan.Expr{newColExpr(1)}, nil)}}
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
			arg.AppendChild(child)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				child.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			for row, value := range vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]) {
				want := int64(6)
				if frame == "range" && row < 2 {
					want = []int64{1, 3}[row]
				}
				require.Equal(t, want, value, "row %d", row)
			}
		})
	}
}
