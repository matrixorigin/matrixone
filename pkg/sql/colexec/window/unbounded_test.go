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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

type observedWindowAgg struct {
	aggexec.AggFuncExec
	fills, merges, flushes, groups int
	failFill, failMerge, failFlush bool
	cancel, cancelMerge            context.CancelFunc
}

var errWindowAggregate = errors.New("window aggregate failure")

func (a *observedWindowAgg) GroupGrow(n int) error {
	a.groups += n
	return a.AggFuncExec.GroupGrow(n)
}

func (a *observedWindowAgg) Fill(group, row int, vecs []*vector.Vector) error {
	a.fills++
	if a.fills == 2 {
		if a.failFill {
			return errWindowAggregate
		}
		if a.cancel != nil {
			a.cancel()
		}
	}
	return a.AggFuncExec.Fill(group, row, vecs)
}

func (a *observedWindowAgg) Merge(other aggexec.AggFuncExec, target, source int) error {
	a.merges++
	if a.merges == 2 && a.cancelMerge != nil {
		a.cancelMerge()
	}
	if a.failMerge {
		return errWindowAggregate
	}
	return a.AggFuncExec.Merge(other.(*observedWindowAgg).AggFuncExec, target, source)
}

func (a *observedWindowAgg) Flush() ([]*vector.Vector, error) {
	a.flushes++
	if a.failFlush {
		return nil, errWindowAggregate
	}
	return a.AggFuncExec.Flush()
}

func TestWindowUnboundedWorkAndFailure(t *testing.T) {
	for _, frame := range []string{"full", "rows", "range"} {
		for _, failure := range []string{"none", "fill", "merge", "flush", "cancel", "cancel_merge"} {
			if frame == "full" && (failure == "merge" || failure == "cancel_merge") {
				continue
			}
			t.Run(frame+"/"+failure, func(t *testing.T) {
				proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
				ctx, cancel := context.WithCancel(proc.Ctx)
				defer cancel()
				proc.Ctx = ctx
				const rows = 2*cancellationCheckInterval + 1
				bat := batch.NewWithSize(1)
				values := make([]int32, rows)
				for i := range values {
					values[i] = 1
				}
				bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
				bat.SetRowCount(rows)
				spec := makeAggWindowSpec("sum")
				if frame != "full" {
					spec.GetW().Frame.End = &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}
				}
				if frame == "range" {
					spec.GetW().Frame.Type = plan.FrameClause_RANGE
					spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
				}
				arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newAggExpr()}}
				t.Cleanup(func() { arg.Free(proc, false, nil); proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
				require.NoError(t, arg.Prepare(proc))
				arg.ctr.bat = bat
				require.NoError(t, arg.ctr.evalAggVector(bat, proc))
				exec, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfSum, false, types.T_int32.ToType())
				require.NoError(t, err)
				observed := &observedWindowAgg{AggFuncExec: exec, failFill: failure == "fill", failMerge: failure == "merge", failFlush: failure == "flush"}
				if failure == "cancel" {
					observed.cancel = cancel
				}
				if failure == "cancel_merge" {
					observed.cancelMerge = cancel
				}
				arg.ctr.batAggs = []aggexec.AggFuncExec{observed}
				// All equal keys form one RANGE peer, including the no-boundary seam.
				err = arg.ctr.processFunc(0, arg, proc, arg.OpAnalyzer)
				switch failure {
				case "none":
					require.NoError(t, err)
					require.Equal(t, rows, observed.fills)
					require.Equal(t, 1, observed.flushes)
					if frame == "full" {
						require.Equal(t, 1, observed.groups)
						require.Zero(t, observed.merges)
					} else {
						require.Equal(t, rows, observed.groups)
						require.Equal(t, rows-1, observed.merges)
					}
					for i, v := range vector.MustFixedColWithTypeCheck[int64](arg.ctr.vec) {
						want := int64(rows)
						if frame == "rows" {
							want = int64(i + 1)
						}
						require.Equal(t, want, v)
					}
				case "cancel":
					require.ErrorIs(t, err, context.Canceled)
					require.GreaterOrEqual(t, observed.fills, 2)
					require.Less(t, observed.fills, rows)
				case "cancel_merge":
					require.ErrorIs(t, err, context.Canceled)
					require.GreaterOrEqual(t, observed.merges, 2)
					require.Less(t, observed.merges, rows-1)
				default:
					require.ErrorIs(t, err, errWindowAggregate)
				}
				arg.Reset(proc, err != nil, err)
				require.Nil(t, arg.ctr.batAggs)
				require.Nil(t, arg.ctr.vec)
			})
		}
	}
}

func TestWindowFullPartitionBroadcastFailure(t *testing.T) {
	for _, failure := range []string{"allocation", "cancel"} {
		t.Run(failure, func(t *testing.T) {
			limit := int64(0)
			if failure == "allocation" {
				limit = 1 << 20
			}
			mp, err := mpool.NewMPool("window-broadcast", limit, mpool.NoFixed)
			require.NoError(t, err)
			proc := testutil.NewProcessWithMPool(t, "", mp)
			arg := &Window{}
			t.Cleanup(func() { arg.Free(proc, true, nil); proc.Free(); require.Zero(t, mp.CurrNB()) })
			arg.ctr.vec = vector.NewOffHeapVecWithType(types.T_int64.ToType())
			require.NoError(t, vector.AppendFixed(arg.ctr.vec, int64(42), false, mp))
			if failure == "cancel" {
				// Cancel after the copy allocated its result, before publication.
				proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
			}
			rows := aggexec.AggBatchSize
			if failure == "allocation" {
				rows = 1 << 18
			}
			err = arg.ctr.broadcastAggregate(rows, proc)
			if failure == "allocation" {
				require.True(t, mpool.IsMPoolCapacityFailure(err), "%v", err)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Nil(t, arg.ctr.vec)
			require.Zero(t, mp.CurrNB())
		})
	}
}

func TestWindowCumulativeFloatAdditionOrder(t *testing.T) {
	for _, name := range []string{"sum", "avg"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeFloat64Vector([]float64{1e16, 1, -1e16, 3}, nil, proc.Mp())
			bat.SetRowCount(4)
			spec := makeAggWindowSpec(name)
			spec.GetW().Frame.End = &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}
			typ := types.T_float64.ToType()
			f, err := function.GetFunctionByName(context.Background(), name, []types.Type{typ})
			require.NoError(t, err)
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(f.GetEncodedOverloadID(), false, []*plan.Expr{newColExprWithType(0, typ)}, nil)}}
			op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
			arg.AppendChild(op)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				op.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			want := []float64{1e16, 1e16, 0, 3}
			if name == "avg" {
				want = []float64{1e16, 5e15, 0, 0.75}
			}
			require.Equal(t, want, vector.MustFixedColWithTypeCheck[float64](result.Batch.Vecs[1]))
		})
	}
}

func TestWindowUnboundedAggregates(t *testing.T) {
	// Explicit prefix answers distinguish NULL from zero and AVG state from its
	// finalized value. RANGE peers must include all rows with the same order key.
	cases := []struct {
		name string
		id   int64
		want []any
	}{
		{"count_star", aggexec.AggIdOfCountStar, []any{int64(1), int64(2), int64(3), int64(4), int64(5), int64(6)}},
		{"count", aggexec.AggIdOfCountColumn, []any{int64(0), int64(1), int64(2), int64(3), int64(3), int64(4)}},
		{"sum", aggexec.AggIdOfSum, []any{nil, int64(2), int64(6), int64(5), int64(5), int64(12)}},
		{"avg", aggexec.AggIdOfAvg, []any{nil, float64(2), float64(3), float64(5) / 3, float64(5) / 3, float64(3)}},
		{"min", aggexec.AggIdOfMin, []any{nil, int32(2), int32(2), int32(-1), int32(-1), int32(-1)}},
		{"max", aggexec.AggIdOfMax, []any{nil, int32(2), int32(4), int32(4), int32(4), int32(7)}},
	}
	for _, tc := range cases {
		for _, frame := range []string{"full", "rows", "range", "range_no_order"} {
			t.Run(tc.name+"/"+frame, func(t *testing.T) {
				proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
				spec := makeAggWindowSpec(tc.name)
				w := spec.Expr.(*plan.Expr_W).W
				if frame != "full" {
					w.Frame.End = &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}
				}
				if frame == "range" || frame == "range_no_order" {
					w.Frame.Type = plan.FrameClause_RANGE
				}
				if frame == "rows" || frame == "range" {
					w.OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(0)}}
				}
				args := []*plan.Expr{newColExpr(1)}
				if tc.id == aggexec.AggIdOfCountStar {
					args = nil
				}
				arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(tc.id, false, args, nil)}}
				var op *colexec.MockOperator
				t.Cleanup(func() {
					arg.Free(proc, false, nil)
					if op != nil {
						op.Free(proc, false, nil)
					}
					proc.Free()
					require.Zero(t, proc.Mp().CurrNB())
				})
				for run := 0; run < 2; run++ {
					bat := batch.NewWithSize(2)
					bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 2, 2, 3, 3, 4}, nil, proc.Mp())
					bat.Vecs[1] = testutil.MakeInt32Vector([]int32{0, 2, 4, -1, 0, 7}, []uint64{0, 4}, proc.Mp())
					bat.SetRowCount(6)
					op = colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
					arg.Children = nil
					arg.AppendChild(op)
					require.NoError(t, arg.Prepare(proc))
					result, err := vm.Exec(arg, proc)
					require.NoError(t, err)
					out := result.Batch.Vecs[2]
					require.Equal(t, 6, out.Length())
					for i := 0; i < 6; i++ {
						end := i
						if frame == "full" || frame == "range_no_order" {
							end = 5
						}
						if frame == "range" {
							end = []int{0, 2, 2, 4, 4, 5}[i]
						}
						want := tc.want[end]
						if want == nil {
							require.True(t, out.IsNull(uint64(i)))
							continue
						}
						require.False(t, out.IsNull(uint64(i)))
						switch want := want.(type) {
						case int64:
							require.Equal(t, want, vector.MustFixedColWithTypeCheck[int64](out)[i])
						case int32:
							require.Equal(t, want, vector.MustFixedColWithTypeCheck[int32](out)[i])
						case float64:
							require.Equal(t, want, vector.MustFixedColWithTypeCheck[float64](out)[i])
						}
					}
					arg.Reset(proc, false, nil)
					op.Free(proc, false, nil)
					op = nil
				}
			})
		}
	}
}

func TestWindowUnboundedAcrossAggregateChunks(t *testing.T) {
	for _, frame := range []string{"full", "rows", "range"} {
		t.Run(frame, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			const rows = aggexec.AggBatchSize + 17
			values := make([]int32, rows)
			for i := range values {
				values[i] = int32(i/3 + 1)
			}
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = testutil.MakeInt32Vector(make([]int32, rows), nil, proc.Mp())
			bat.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
			bat.SetRowCount(rows)
			spec := makeAggWindowSpec("sum")
			w := spec.Expr.(*plan.Expr_W).W
			w.PartitionBy = []*plan.Expr{newColExpr(0)} // mock child delivers one complete partition
			if frame != "full" {
				w.Frame.End = &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}
				w.OrderBy = []*plan.OrderBySpec{{Expr: newColExpr(1)}}
			}
			if frame == "range" {
				w.Frame.Type = plan.FrameClause_RANGE
			}
			arg := &Window{WinSpecList: []*plan.Expr{spec}, Aggs: []aggexec.AggFuncExecExpression{newAggExprAt(1)}}
			op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
			arg.AppendChild(op)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				op.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			got := vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2])
			require.Len(t, got, rows)
			prefix := make([]int64, rows+1)
			for i, v := range values {
				prefix[i+1] = prefix[i] + int64(v)
			}
			for i := range values {
				end := i + 1
				if frame == "full" {
					end = rows
				}
				if frame == "range" {
					end = min((i/3+1)*3, rows)
				}
				require.Equal(t, prefix[end], got[i], "row %d", i)
			}
		})
	}
}

func TestWindowFullPartitionVarlenAndDistinct(t *testing.T) {
	for _, tc := range []struct {
		name     string
		distinct bool
		vals     []string
		nulls    []uint64
		want     string
		wantNull bool
	}{
		{"min", false, []string{"long string retained beyond inline storage", "z"}, nil, "long string retained beyond inline storage", false},
		{"max", false, []string{"", ""}, []uint64{0, 1}, "", true},
		{"count", true, []string{"x", "x", "y"}, nil, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeVarcharVector(tc.vals, tc.nulls, proc.Mp())
			bat.SetRowCount(len(tc.vals))
			typ := types.T_varchar.ToType()
			f, err := function.GetFunctionByName(context.Background(), tc.name, []types.Type{typ})
			require.NoError(t, err)
			arg := &Window{WinSpecList: []*plan.Expr{makeAggWindowSpec(tc.name)}, Aggs: []aggexec.AggFuncExecExpression{aggexec.MakeAggFunctionExpression(f.GetEncodedOverloadID(), tc.distinct, []*plan.Expr{newColExprWithType(0, typ)}, nil)}}
			op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
			arg.AppendChild(op)
			t.Cleanup(func() {
				arg.Free(proc, false, nil)
				op.Free(proc, false, nil)
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			out := result.Batch.Vecs[1]
			require.Equal(t, len(tc.vals), out.Length())
			for i := range tc.vals {
				require.Equal(t, tc.wantNull, out.IsNull(uint64(i)))
				if tc.wantNull {
					continue
				}
				if tc.distinct {
					require.Equal(t, int64(2), vector.MustFixedColWithTypeCheck[int64](out)[i])
				} else {
					require.Equal(t, tc.want, out.GetStringAt(i))
				}
			}
		})
	}
}
