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

package window

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestWindowPrepareMaterializesIntervalBounds(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	raw := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}},
		plan2.MakePlan2StringConstExprWithType("second"),
	}}}}
	planned := &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: raw}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
	original := planned.String()
	arg := makeWindowWithFrame(planned)
	defer func() {
		arg.Free(proc, false, nil)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	}()
	for _, tc := range []struct {
		text *string
		want int64
		fail bool
	}{{stringPtr("1.5"), 1500000, false}, {nil, 0, true}, {stringPtr("-0.0000001"), 0, true}, {stringPtr("9223372036854.775808"), 0, true}, {stringPtr("0.000001"), 1, false}, {stringPtr("2"), 2000000, false}} {
		func() {
			params := setWindowPrepareParams(t, proc, tc.text)
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			err := arg.Prepare(proc) // no input batch is needed for validation
			if tc.fail {
				require.Error(t, err)
				require.Nil(t, arg.ctr.runtimeFrames)
			} else {
				require.NoError(t, err)
				got := arg.ctr.runtimeFrames[0].Start.Val
				require.NotSame(t, raw, got)
				require.Equal(t, tc.want, got.GetList().List[0].GetLit().GetI64Val())
				require.Equal(t, int64(types.MicroSecond), got.GetList().List[1].GetLit().GetI64Val())
			}
			require.Equal(t, original, planned.String())
			arg.Reset(proc, false, nil)
		}()
	}
}

func TestWindowIntervalBooleanMaterialization(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	for _, unit := range []struct {
		name string
		want int64
		typ  types.IntervalType
	}{{"second", 1000000, types.MicroSecond}, {"day", 1, types.Day}} {
		for _, tc := range []struct {
			name        string
			value, null bool
		}{{"true", true, false}, {"false", false, false}, {"NULL", false, true}} {
			t.Run(unit.name+"/"+tc.name, func(t *testing.T) {
				source := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{
					Isnull: tc.null, Value: &plan.Literal_Bval{Bval: tc.value},
				}}}
				raw := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
					source, plan2.MakePlan2StringConstExprWithType(unit.name),
				}}}}
				original := raw.String()
				got, err := materializeWindowInterval(proc, raw)
				require.Equal(t, original, raw.String())
				if tc.null {
					require.ErrorContains(t, err, "negative, NULL or of non-integral type")
					require.Nil(t, got)
					return
				}
				require.NoError(t, err)
				require.NotSame(t, raw, got)
				magnitude := got.GetList().List[0]
				require.NotSame(t, source, magnitude)
				require.False(t, magnitude.GetLit().Isnull)
				want := int64(0)
				if tc.value {
					want = unit.want
				}
				require.Equal(t, want, magnitude.GetLit().GetI64Val())
				require.Equal(t, int64(unit.typ), got.GetList().List[1].GetLit().GetI64Val())
			})
		}
	}
}

func TestNullIntervalRangeBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	magnitude := intervalInt64(0)
	magnitude.GetLit().Isnull = true
	offset := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{magnitude, intervalInt64(int64(types.Second))}}}}
	for _, typ := range []types.T{types.T_date, types.T_time, types.T_datetime, types.T_timestamp} {
		t.Run(typ.String(), func(t *testing.T) {
			// Zero-valued temporal keys suffice: the NULL arithmetic must be
			// decided before any date arithmetic or timestamp fold indexing.
			vec := vector.NewVec(typ.ToType())
			defer vec.Free(proc.Mp())
			for _, isNull := range []bool{false, true} {
				vec.CleanOnlyData()
				switch typ {
				case types.T_date:
					require.NoError(t, vector.AppendFixed(vec, types.Date(0), isNull, proc.Mp()))
				case types.T_time:
					require.NoError(t, vector.AppendFixed(vec, types.Time(0), isNull, proc.Mp()))
				case types.T_datetime:
					require.NoError(t, vector.AppendFixed(vec, types.Datetime(0), isNull, proc.Mp()))
				case types.T_timestamp:
					require.NoError(t, vector.AppendFixed(vec, types.Timestamp(0), isNull, proc.Mp()))
				}
				for _, desc := range []bool{false, true} {
					for _, startNull := range []bool{false, true} {
						frame := &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
						if startNull {
							frame.Start = &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: offset}
						} else {
							frame.End = &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: offset}
						}
						ctr := &container{desc: []bool{desc}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}}}
						left, right, selection, err := ctr.buildIntervalRows(proc, 0, 0, 1, frame)
						require.NoError(t, err)
						require.Nil(t, selection)
						require.Zero(t, left)
						if isNull {
							require.Equal(t, 1, right)
						} else {
							require.Zero(t, right)
						}
						require.True(t, boundedSlidingRangeFrame(frame, ctr.orderVecs))
						// Exercise the separate timestamp consumer directly too.
						selection, err = ctr.timestampRangeSelection(proc, time.UTC, 0, 0, 1, vec, frame)
						require.NoError(t, err)
						require.Nil(t, selection)
					}
				}
			}
		})
	}
}

// This benchmark isolates aggregate work from sorting and checks each result.
// The zero-bound control has the same NULL peer group as the NULL-bound case.
func BenchmarkNullIntervalRangePeers(b *testing.B) {
	for _, rows := range []int{1024, 4096} {
		for _, nullBound := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/null_bound=%t", rows, nullBound), func(b *testing.B) {
				proc := testutil.NewProcessWithOwnedMPool(b, "", mpool.MustNewZero())
				defer func() { proc.Free(); require.Zero(b, proc.Mp().CurrNB()) }()
				bat := batch.NewWithSize(2)
				defer bat.Clean(proc.Mp())
				bat.Vecs[0] = vector.NewVec(types.T_int32.ToType())
				bat.Vecs[1] = vector.NewConstNull(types.T_datetime.ToType(), rows, proc.Mp())
				for i := 0; i < rows; i++ {
					require.NoError(b, vector.AppendFixed(bat.Vecs[0], int32(1), false, proc.Mp()))
				}
				bat.SetRowCount(rows)
				magnitude := intervalInt64(0)
				magnitude.GetLit().Isnull = nullBound
				offset := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{magnitude, intervalInt64(int64(types.Second))}}}}
				frame := &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: offset}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
				arg := makeWindowWithFrame(frame)
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					func() {
						ctr := &container{bat: bat, os: []int64{0}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[1]}}}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
						defer ctr.freeRunningAgg()
						result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, rows)
						require.NoError(b, err)
						defer result.Free(proc.Mp())
						require.Equal(b, rows, result.Length())
						for row, value := range vector.MustFixedColWithTypeCheck[int64](result) {
							if result.IsNull(uint64(row)) || value != int64(rows) {
								b.Fatalf("row %d: sum %v (NULL=%t), want %d", row, value, result.IsNull(uint64(row)), rows)
							}
						}
					}()
				}
			})
		}
	}
}

func rawIntervalFrameForTest() *plan.FrameClause {
	return &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
		{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}, plan2.MakePlan2StringConstExprWithType("second"),
	}}}}}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
}

func TestValidateIntervalFrameBoundsReachabilityAndReuse(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	spec := makeWindowSpec()
	spec.GetW().Frame = rawIntervalFrameForTest()
	// A final LIMIT 0 can prune both its child and a separate CTE producer step.
	query := &plan.Query{Steps: []int32{0, 2}, Nodes: []*plan.Node{
		{Children: []int32{1}}, {WinSpecList: []*plan.Expr{spec}}, {Limit: plan2.MakePlan2Uint64ConstExprWithType(0)},
		{WinSpecList: []*plan.Expr{spec}}, // detached optimizer node
	}}
	pn := &plan.Plan{Plan: &plan.Plan_Query{Query: query}}
	original := pn.String()
	for _, value := range []*string{stringPtr("0"), stringPtr("-1"), nil, stringPtr("9223372036854.775808"), stringPtr("1.5"), stringPtr("-1"), stringPtr("0")} {
		func() {
			params := setWindowPrepareParams(t, proc, value)
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			err := ValidateIntervalFrameBounds(proc, pn)
			if value != nil && (*value == "0" || *value == "1.5") {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, original, pn.String())
		}()
	}
	// Removing the producer root leaves only detached invalid frames, which must
	// not be evaluated. The remaining root has no input and needs no parameters.
	query.Steps = []int32{2}
	require.NoError(t, ValidateIntervalFrameBounds(proc, pn))
	ddl := &plan.Plan{Plan: &plan.Plan_Ddl{Ddl: &plan.DataDefinition{Query: query}}}
	require.NoError(t, ValidateIntervalFrameBounds(proc, ddl))
	query.Steps = []int32{0, 2}
	require.Error(t, ValidateIntervalFrameBounds(proc, ddl))
}

func TestNullIntervalSlidingAggregates(t *testing.T) {
	for _, typ := range []types.T{types.T_date, types.T_time, types.T_datetime, types.T_timestamp} {
		for _, nullsLast := range []bool{false, true} {
			for _, startNull := range []bool{false, true} {
				for _, name := range []string{"sum", "avg"} {
					t.Run(fmt.Sprintf("%s/nulls_last=%t/start_null=%t/%s", typ, nullsLast, startNull, name), func(t *testing.T) {
						proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
						defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
						bat := makeInt32Batch(proc.Mp(), []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
						defer bat.Clean(proc.Mp())
						order := vector.NewVec(typ.ToType())
						defer order.Free(proc.Mp())
						for row := 0; row < 10; row++ {
							pos := row % 5
							isNull := pos < 2
							value := int64(pos/2 + 1)
							if nullsLast {
								isNull = pos >= 3
								value = int64(min(pos/2+1, 2))
							}
							switch typ {
							case types.T_date:
								require.NoError(t, vector.AppendFixed(order, types.Date(value), isNull, proc.Mp()))
							case types.T_time:
								require.NoError(t, vector.AppendFixed(order, types.Time(value), isNull, proc.Mp()))
							case types.T_datetime:
								require.NoError(t, vector.AppendFixed(order, types.Datetime(value), isNull, proc.Mp()))
							case types.T_timestamp:
								require.NoError(t, vector.AppendFixed(order, types.Timestamp(value), isNull, proc.Mp()))
							}
						}
						magnitude := intervalInt64(0)
						magnitude.GetLit().Isnull = true
						offset := &plan.Expr{Typ: plan.Type{Id: int32(types.T_interval)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{magnitude, intervalInt64(int64(types.Second))}}}}
						frame := &plan.FrameClause{Type: plan.FrameClause_RANGE, Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}, End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}}
						if startNull {
							frame.Start = &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: offset}
						} else {
							frame.End = &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: offset}
						}
						arg := makeWindowWithFrame(frame)
						if name == "avg" {
							arg.Aggs = []aggexec.AggFuncExecExpression{newTypedAvgAggExpr(t, 0, types.T_int32.ToType())}
						}
						peers := []int64{0, 2, 4, 5, 7, 9}
						if nullsLast {
							peers = []int64{0, 2, 3, 5, 7, 8}
						}
						ctr := &container{bat: bat, ps: []int64{0, 5}, os: peers, desc: []bool{nullsLast}, nullsLast: []bool{nullsLast}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{order}}}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
						defer ctr.freeRunningAgg()
						require.True(t, boundedSlidingRangeFrame(frame, ctr.orderVecs))
						// Cut inside both NULL peer groups and across the partition boundary.
						for _, output := range [][2]int{{0, 1}, {1, 4}, {4, 6}, {6, 9}, {9, 10}} {
							func() {
								result, err := ctr.processAggregateFuncRange(0, arg, proc, output[0], output[1])
								require.NoError(t, err)
								defer result.Free(proc.Mp())
								for i := 0; i < result.Length(); i++ {
									row := output[0] + i
									pos := row % 5
									wantNull := pos >= 2
									if nullsLast {
										wantNull = pos < 3
									}
									require.Equal(t, wantNull, result.IsNull(uint64(i)), "row %d", row)
									if !wantNull {
										want := int64(3 + 10*(row/5))
										if nullsLast {
											want += 6
										}
										if name == "sum" {
											require.Equal(t, want, vector.MustFixedColWithTypeCheck[int64](result)[i])
										} else {
											require.Equal(t, float64(want)/2, types.Decimal128ToFloat64(vector.MustFixedColWithTypeCheck[types.Decimal128](result)[i], result.GetType().Scale))
										}
									}
								}
							}()
							if output[1] < 10 {
								require.NotNil(t, ctr.runningAgg, "peer state must survive output chunks")
							} else {
								require.Nil(t, ctr.runningAgg)
							}
						}
					})
				}
			}
		}
	}
}

func TestNullIntervalSlidingCancellationAndReuse(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) }()
	bat := makeInt32Batch(proc.Mp(), []int32{1, 2, 3})
	defer bat.Clean(proc.Mp())
	order := vector.NewConstNull(types.T_datetime.ToType(), 3, proc.Mp())
	defer order.Free(proc.Mp())
	frame := rawIntervalFrameForTest()
	// Use an execution-owned NULL magnitude rather than a SQL NULL argument.
	magnitude := intervalInt64(0)
	magnitude.GetLit().Isnull = true
	frame.Start.Val.GetList().List = []*plan.Expr{magnitude, intervalInt64(int64(types.Second))}
	arg := makeWindowWithFrame(frame)
	ctr := &container{bat: bat, os: []int64{0}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{order}}}, aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}}
	defer ctr.freeRunningAgg()
	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.NoError(t, err)
	first.Free(proc.Mp())
	require.NotNil(t, ctr.runningAgg)
	originalCtx := proc.Ctx
	canceled, cancel := context.WithCancel(originalCtx)
	cancel()
	proc.Ctx = canceled
	_, err = ctr.processAggregateFuncRange(0, arg, proc, 1, 3)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, ctr.runningAgg)
	require.Nil(t, ctr.batAggs)
	proc.Ctx = originalCtx
	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 3)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	require.Equal(t, []int64{6, 6, 6}, vector.MustFixedColWithTypeCheck[int64](result))
	require.Nil(t, ctr.runningAgg)
}
