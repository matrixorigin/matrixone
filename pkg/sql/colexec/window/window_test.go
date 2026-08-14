// Copyright 2021 Matrix Origin
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
	"bytes"
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/vm"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// add unit tests for cases
type winTestCase struct {
	arg  *Window
	proc *process.Process
}

type cancelAfterDoneChecksContext struct {
	context.Context
	done      chan struct{}
	remaining atomic.Int32
}

func newCancelAfterDoneChecksContext(parent context.Context, checks int32) *cancelAfterDoneChecksContext {
	ctx := &cancelAfterDoneChecksContext{
		Context: parent,
		done:    make(chan struct{}),
	}
	ctx.remaining.Store(checks)
	return ctx
}

func (c *cancelAfterDoneChecksContext) Done() <-chan struct{} {
	if c.remaining.Add(-1) == 0 {
		close(c.done)
	}
	return c.done
}

func (c *cancelAfterDoneChecksContext) Err() error {
	select {
	case <-c.done:
		return context.Canceled
	default:
		return nil
	}
}

func makeTestCases(t *testing.T) []winTestCase {
	return []winTestCase{
		{
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Window{
				WinSpecList: []*plan.Expr{makeWindowSpec()},
				Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
		{
			// Multi-argument window aggregate (json_objectagg): the operator must
			// derive one argument type per argument. Guards against regressing the
			// fix for issue #25483 where only a single type was passed to MakeAgg.
			proc: testutil.NewProcessWithMPool(t, "", mpool.MustNewZero()),
			arg: &Window{
				WinSpecList: []*plan.Expr{makeAggWindowSpec("json_objectagg")},
				Aggs:        []aggexec.AggFuncExecExpression{newJsonObjectAggExpr(t)},
				OperatorBase: vm.OperatorBase{
					OperatorInfo: vm.OperatorInfo{
						Idx:     0,
						IsFirst: false,
						IsLast:  false,
					},
				},
			},
		},
	}
}

func TestString(t *testing.T) {
	buf := new(bytes.Buffer)
	for _, tc := range makeTestCases(t) {
		tc.arg.String(buf)
	}
}

func TestPrepare(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
	}
}

func TestWin(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		resetChildren(tc.arg, tc.proc.Mp())
		err := tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)

		tc.arg.Reset(tc.proc, false, nil)

		resetChildren(tc.arg, tc.proc.Mp())
		err = tc.arg.Prepare(tc.proc)
		require.NoError(t, err)
		_, err = vm.Exec(tc.arg, tc.proc)
		require.NoError(t, err)
		tc.arg.Free(tc.proc, false, nil)
		tc.proc.Free()
		require.Equal(t, int64(0), tc.proc.Mp().CurrNB())
	}
}

func TestWindowFrameEvaluationHonorsCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const rows = cancellationCheckInterval * 2
	values := make([]int32, rows)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(rows)

	arg := &Window{
		WinSpecList: []*plan.Expr{makeWindowSpec()},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.bat = bat
	require.NoError(t, arg.ctr.evalAggVector(bat, proc))

	arg.ctr.batAggs = make([]aggexec.AggFuncExec, 1)
	var err error
	arg.ctr.batAggs[0], err = aggexec.MakeAgg(
		proc.Mp(),
		arg.Aggs[0].GetAggID(),
		arg.Aggs[0].IsDistinct(),
		types.T_int32.ToType(),
	)
	require.NoError(t, err)
	require.NoError(t, arg.ctr.batAggs[0].GroupGrow(bat.RowCount()))

	// processFunc checks once at the outer row, then every 1024 frame rows.
	// Cancel on the third check so the test proves an already-running frame is
	// interrupted, rather than only proving that a pre-canceled call is rejected.
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 3)

	err = arg.ctr.processFunc(0, arg, proc, arg.OpAnalyzer)
	require.ErrorIs(t, err, context.Canceled)

	arg.Free(proc, true, err)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestCumulativeWindowCancellationReleasesState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const rows = cancellationCheckInterval * 2
	values := make([]int32, rows)
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeCumulativeFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.bat = bat
	require.NoError(t, arg.ctr.evalAggVector(bat, proc))

	// Cancel at the second polling interval, after the running aggregate has
	// accumulated state, to exercise the mid-chunk cleanup path.
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
	err := arg.ctr.processFunc(0, arg, proc, arg.OpAnalyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, arg.ctr.batAggs)
	require.Nil(t, arg.ctr.runningAgg)

	arg.Free(proc, true, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowCallHonorsPreCancellation(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()

	arg := &Window{}
	result, err := arg.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, vm.CancelResult, result)

	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func resetChildren(arg *Window, m *mpool.MPool) {
	bat := colexec.MakeMockBatchs(m)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.Children = nil
	arg.AppendChild(op)
}

func makeFullFrame() *plan.FrameClause {
	return &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_PRECEDING,
			UnBounded: true,
		},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_FOLLOWING,
			UnBounded: true,
		},
	}
}

func makeCurrentRowFrame() *plan.FrameClause {
	return &plan.FrameClause{
		Type:  plan.FrameClause_ROWS,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
}

func makeCumulativeFrame() *plan.FrameClause {
	return &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_PRECEDING,
			UnBounded: true,
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
}

func makeFiniteCumulativeFrame(preceding uint64) *plan.FrameClause {
	return &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_U64Val{U64Val: preceding},
			}}},
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
}

func makePreparedRowsBoundExpr(t *testing.T, pos int32) *plan.Expr {
	t.Helper()
	param := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
	}
	targetType := plan.Type{Id: int32(types.T_uint64), NotNullable: true}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		param,
		{Typ: targetType, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
	})
	require.NoError(t, err)
	return expr
}

func makePreparedRowsFrame(t *testing.T, startPos, endPos int32) *plan.FrameClause {
	t.Helper()
	return &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  makePreparedRowsBoundExpr(t, startPos),
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  makePreparedRowsBoundExpr(t, endPos),
		},
	}
}

func makeWindowWithFrame(frame *plan.FrameClause) *Window {
	spec := makeWindowSpec()
	spec.GetW().Frame = frame
	return &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
}

func setWindowPrepareParams(t *testing.T, proc *process.Process, values ...*string) *vector.Vector {
	t.Helper()
	params := vector.NewVec(types.T_text.ToType())
	for _, value := range values {
		var raw []byte
		isNull := value == nil
		if value != nil {
			raw = []byte(*value)
		}
		require.NoError(t, vector.AppendBytes(params, raw, isNull, proc.Mp()))
	}
	proc.SetPrepareParams(params)
	return params
}

func stringPtr(value string) *string {
	return &value
}

func requirePreparedRowsBoundUnchanged(t *testing.T, expr *plan.Expr, pos int32) {
	t.Helper()
	require.NotNil(t, expr.GetF())
	require.NotEmpty(t, expr.GetF().Args)
	require.NotNil(t, expr.GetF().Args[0].GetP())
	require.Equal(t, pos, expr.GetF().Args[0].GetP().Pos)
}

func TestWindowPrepareMaterializesRowsFrameBounds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	planned := makePreparedRowsFrame(t, 0, 1)
	arg := makeWindowWithFrame(planned)
	firstParams := setWindowPrepareParams(t, proc, stringPtr("1"), stringPtr("2"))

	require.NoError(t, arg.Prepare(proc))
	require.Len(t, arg.ctr.runtimeFrames, 1)
	require.NotSame(t, planned, arg.ctr.runtimeFrames[0])
	require.Equal(t, uint64(1), arg.ctr.runtimeFrames[0].Start.Val.GetLit().GetU64Val())
	require.Equal(t, uint64(2), arg.ctr.runtimeFrames[0].End.Val.GetLit().GetU64Val())
	requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
	requirePreparedRowsBoundUnchanged(t, planned.End.Val, 1)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.runtimeFrames)

	proc.SetPrepareParams(nil)
	firstParams.Free(proc.Mp())
	secondParams := setWindowPrepareParams(t, proc, stringPtr("3"), stringPtr("4"))
	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, uint64(3), arg.ctr.runtimeFrames[0].Start.Val.GetLit().GetU64Val())
	require.Equal(t, uint64(4), arg.ctr.runtimeFrames[0].End.Val.GetLit().GetU64Val())
	requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
	requirePreparedRowsBoundUnchanged(t, planned.End.Val, 1)

	arg.Free(proc, false, nil)
	require.Nil(t, arg.ctr.runtimeFrames)
	proc.SetPrepareParams(nil)
	secondParams.Free(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPrepareValidatesRowsFrameBounds(t *testing.T) {
	tests := []struct {
		name       string
		value      *string
		missing    bool
		emptyParam bool
		want       uint64
		wantErr    bool
	}{
		{name: "zero", value: stringPtr("0"), want: 0},
		{name: "maximum", value: stringPtr("18446744073709551615"), want: math.MaxUint64},
		{name: "negative", value: stringPtr("-1"), wantErr: true},
		{name: "fractional", value: stringPtr("1.5"), wantErr: true},
		{name: "null", value: nil, wantErr: true},
		{name: "overflow", value: stringPtr("18446744073709551616"), wantErr: true},
		{name: "conversion failure", value: stringPtr("not-a-number"), wantErr: true},
		{name: "missing vector", missing: true, wantErr: true},
		{name: "missing element", emptyParam: true, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			planned := makePreparedRowsFrame(t, 0, 0)
			arg := makeWindowWithFrame(planned)
			var params *vector.Vector
			switch {
			case test.missing:
				proc.SetPrepareParams(nil)
			case test.emptyParam:
				params = setWindowPrepareParams(t, proc)
			default:
				params = setWindowPrepareParams(t, proc, test.value)
			}

			err := arg.Prepare(proc)
			if test.wantErr {
				require.Error(t, err)
				require.Nil(t, arg.ctr.runtimeFrames)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.want, arg.ctr.runtimeFrames[0].Start.Val.GetLit().GetU64Val())
				require.Equal(t, test.want, arg.ctr.runtimeFrames[0].End.Val.GetLit().GetU64Val())
			}
			requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
			requirePreparedRowsBoundUnchanged(t, planned.End.Val, 0)

			arg.Free(proc, false, nil)
			proc.SetPrepareParams(nil)
			if params != nil {
				params.Free(proc.Mp())
			}
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestWindowPrepareClearsPartialRowsFrameBoundsOnError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	valid := makePreparedRowsFrame(t, 0, 0)
	invalid := makePreparedRowsFrame(t, 1, 1)
	arg := makeWindowWithFrame(valid)
	secondSpec := makeWindowSpec()
	secondSpec.GetW().Frame = invalid
	arg.WinSpecList = append(arg.WinSpecList, secondSpec)
	arg.Aggs = append(arg.Aggs, newAggExpr())
	params := setWindowPrepareParams(t, proc, stringPtr("1"), stringPtr("-1"))

	require.Error(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.runtimeFrames)
	requirePreparedRowsBoundUnchanged(t, valid.Start.Val, 0)
	requirePreparedRowsBoundUnchanged(t, invalid.Start.Val, 1)

	arg.Free(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPrepareHandlesNilAndLiteralFrameBounds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	literal := &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  plan2.MakePlan2Uint64ConstExprWithType(1),
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  plan2.MakePlan2Uint64ConstExprWithType(2),
		},
	}
	arg := makeWindowWithFrame(nil)
	secondSpec := makeWindowSpec()
	secondSpec.GetW().Frame = literal
	arg.WinSpecList = append(arg.WinSpecList, secondSpec)
	arg.Aggs = append(arg.Aggs, newAggExpr())

	require.NoError(t, arg.Prepare(proc))
	require.Len(t, arg.ctr.runtimeFrames, 2)
	require.Nil(t, arg.ctr.runtimeFrames[0])
	require.NotSame(t, literal, arg.ctr.runtimeFrames[1])
	require.NotSame(t, literal.Start, arg.ctr.runtimeFrames[1].Start)
	require.NotSame(t, literal.End, arg.ctr.runtimeFrames[1].End)
	require.Same(t, literal.Start.Val, arg.ctr.runtimeFrames[1].Start.Val)
	require.Same(t, literal.End.Val, arg.ctr.runtimeFrames[1].End.Val)

	arg.Free(proc, false, nil)
	require.Nil(t, arg.ctr.runtimeFrames)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPrepareFrameBoundsStayUnpublishedAfterLaterError(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	planned := makePreparedRowsFrame(t, 0, 0)
	arg := makeWindowWithFrame(planned)
	aggID := arg.Aggs[0].GetAggID()
	arg.Aggs[0] = aggexec.MakeAggFunctionExpression(aggID, false, []*plan.Expr{{}}, nil)
	params := setWindowPrepareParams(t, proc, stringPtr("1"))

	require.Error(t, arg.Prepare(proc))
	require.Nil(t, arg.ctr.runtimeFrames)
	requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
	requirePreparedRowsBoundUnchanged(t, planned.End.Val, 0)

	arg.Free(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPrepareFrameBoundsFeedAggregateConsumer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	planned := makePreparedRowsFrame(t, 0, 1)
	arg := makeWindowWithFrame(planned)
	bat := makeInt32Batch(proc.Mp(), []int32{10, 20, 30, 40})
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)
	params := setWindowPrepareParams(t, proc, stringPtr("1"), stringPtr("1"))

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{30, 60, 90, 70},
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
	requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
	requirePreparedRowsBoundUnchanged(t, planned.End.Val, 1)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPreparedCumulativeBoundUsesRuntimeValue(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	planned := &plan.FrameClause{
		Type: plan.FrameClause_ROWS,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  makePreparedRowsBoundExpr(t, 0),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	arg := makeWindowWithFrame(planned)
	bat := makeInt32Batch(proc.Mp(), []int32{10, 20, 30, 40})
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)
	params := setWindowPrepareParams(t, proc, stringPtr("2147483647"))

	require.NoError(t, arg.Prepare(proc))
	require.True(t, cumulativeRowsFrame(arg.ctr.runtimeFrames[0], nil, bat.RowCount()))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 30, 60, 100},
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1]))
	requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.SetPrepareParams(nil)
	params.Free(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPrepareFrameBoundsFeedValueConsumers(t *testing.T) {
	tests := []struct {
		name string
		want []int32
	}{
		{name: "first_value", want: []int32{10, 10, 20, 30}},
		{name: "last_value", want: []int32{20, 30, 40, 40}},
		{name: "nth_value", want: []int32{10, 10, 20, 30}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			planned := makePreparedRowsFrame(t, 0, 1)
			spec := makeValueWindowSpecWithName(test.name, int32(types.T_int32))
			spec.GetW().Frame = planned
			arg := &Window{
				WinSpecList: []*plan.Expr{spec},
				Aggs:        []aggexec.AggFuncExecExpression{makeValueWindowAggExpr(test.name)},
			}
			bat := makeInt32Batch(proc.Mp(), []int32{10, 20, 30, 40})
			op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
			arg.AppendChild(op)
			params := setWindowPrepareParams(t, proc, stringPtr("1"), stringPtr("1"))

			require.NoError(t, arg.Prepare(proc))
			result, err := vm.Exec(arg, proc)
			require.NoError(t, err)
			require.Equal(t, test.want,
				vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[1]))
			requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
			requirePreparedRowsBoundUnchanged(t, planned.End.Val, 1)

			arg.Free(proc, false, nil)
			op.Free(proc, false, nil)
			proc.SetPrepareParams(nil)
			params.Free(proc.Mp())
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestBuildRowsIntervalSaturatesLargeOffsets(t *testing.T) {
	largeOffset := &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: math.MaxInt64}}},
	}
	testCases := []struct {
		name      string
		frame     *plan.FrameClause
		wantStart int
		wantEnd   int
	}{
		{
			name: "start preceding",
			frame: &plan.FrameClause{
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: largeOffset},
				End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
			},
			wantStart: 10,
			wantEnd:   13,
		},
		{
			name: "start following",
			frame: &plan.FrameClause{
				Start: &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: largeOffset},
				End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true},
			},
			wantStart: 15,
			wantEnd:   15,
		},
		{
			name: "end preceding",
			frame: &plan.FrameClause{
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
				End:   &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: largeOffset},
			},
			wantStart: 10,
			wantEnd:   10,
		},
		{
			name: "end following",
			frame: &plan.FrameClause{
				Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
				End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: largeOffset},
			},
			wantStart: 12,
			wantEnd:   15,
		},
	}

	ctr := &container{}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			start, end := ctr.buildRowsInterval(12, 10, 15, testCase.frame)
			require.Equal(t, testCase.wantStart, start)
			require.Equal(t, testCase.wantEnd, end)
		})
	}
}

func TestCumulativeRowsFrameEligibility(t *testing.T) {
	tests := []struct {
		name       string
		frame      *plan.FrameClause
		partitions []int64
		rows       int
		want       bool
	}{
		{name: "unbounded", frame: makeCumulativeFrame(), rows: 4, want: true},
		{name: "finite covers partition", frame: makeFiniteCumulativeFrame(3), rows: 4, want: true},
		{name: "finite shorter than partition", frame: makeFiniteCumulativeFrame(2), rows: 4},
		{name: "finite covers largest partition", frame: makeFiniteCumulativeFrame(2), partitions: []int64{0, 3, 5}, rows: 7, want: true},
		{name: "finite shorter than one partition", frame: makeFiniteCumulativeFrame(1), partitions: []int64{0, 3, 5}, rows: 7},
		{name: "current row start", frame: makeCurrentRowFrame(), rows: 4},
		{name: "following end", frame: makeFullFrame(), rows: 4},
		{name: "range frame", frame: &plan.FrameClause{
			Type:  plan.FrameClause_RANGE,
			Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
			End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		}, rows: 4},
		{name: "invalid partitions", frame: makeFiniteCumulativeFrame(10), partitions: []int64{1}, rows: 4},
		{name: "empty partition", frame: makeFiniteCumulativeFrame(10), partitions: []int64{0, 0}, rows: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want,
				cumulativeRowsFrame(test.frame, test.partitions, test.rows))
		})
	}
}

func makeWindowSpec() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				//OrderBy:    []*plan.OrderBySpec{&plan.OrderBySpec{Expr: newColExpr(0)}},
				WindowFunc: newFunExpr("sum"),
				Frame:      makeFullFrame(),
			},
		},
	}
}

// makeAggWindowSpec builds a window spec for a generic (non win-value) aggregate
// window function such as json_objectagg.
func makeAggWindowSpec(name string) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{},
		Expr: &plan.Expr_W{
			W: &plan.WindowSpec{
				Name:       name,
				WindowFunc: newFunExpr(name),
				Frame:      makeFullFrame(),
			},
		},
	}
}

func newColExpr(pos int32) *plan.Expr {
	// col 0 of the mock batch is int32; keep the arg type in sync so the window
	// operator can build the aggregate executor from the argument expression.
	return newColExprWithType(pos, types.T_int32.ToType())
}

func newColExprWithType(pos int32, typ types.Type) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				ColPos: pos,
			},
		},
	}
}

func newAggExpr() aggexec.AggFuncExecExpression {
	return newAggExprAt(0)
}

func newAggExprAt(pos int32) aggexec.AggFuncExecExpression {
	e, _ := function.GetFunctionByName(context.Background(), "sum", []types.Type{types.T_int32.ToType()})
	id := e.GetEncodedOverloadID()
	return aggexec.MakeAggFunctionExpression(id, false, []*plan.Expr{newColExpr(pos)}, nil)
}

func newTypedSumAggExpr(t *testing.T, pos int32, typ types.Type) aggexec.AggFuncExecExpression {
	e, err := function.GetFunctionByName(context.Background(), "sum", []types.Type{typ})
	require.NoError(t, err)
	return aggexec.MakeAggFunctionExpression(
		e.GetEncodedOverloadID(), false, []*plan.Expr{newColExprWithType(pos, typ)}, nil)
}

func newRowNumberAggExpr(t *testing.T) aggexec.AggFuncExecExpression {
	return newOrderWindowAggExpr(t, "row_number")
}

func newOrderWindowAggExpr(t *testing.T, name string) aggexec.AggFuncExecExpression {
	e, err := function.GetFunctionByName(context.Background(), name, nil)
	require.NoError(t, err)
	return aggexec.MakeAggFunctionExpression(e.GetEncodedOverloadID(), false, nil, nil)
}

// newJsonObjectAggExpr builds a two-argument aggregate expression:
// json_objectagg(varchar_key, int32_value), using mock batch col 2 (varchar) and col 0 (int32).
func newJsonObjectAggExpr(t *testing.T) aggexec.AggFuncExecExpression {
	return jsonObjectAggColExpr(t, 2, 0)
}

// jsonObjectAggColExpr builds json_objectagg(varchar@keyPos, int32@valPos).
func jsonObjectAggColExpr(t *testing.T, keyPos, valPos int32) aggexec.AggFuncExecExpression {
	keyType := types.T_varchar.ToType()
	valType := types.T_int32.ToType()
	e, err := function.GetFunctionByName(context.Background(), "json_objectagg", []types.Type{keyType, valType})
	require.NoError(t, err)
	id := e.GetEncodedOverloadID()
	return aggexec.MakeAggFunctionExpression(id, false,
		[]*plan.Expr{newColExprWithType(keyPos, keyType), newColExprWithType(valPos, valType)}, nil)
}

// makeKeyValBatch builds a batch of (varchar key, int32 value) columns for
// json_objectagg tests. keyNullPos lists the NULL key row positions (may be nil).
func makeKeyValBatch(mp *mpool.MPool, keys []string, keyNullPos []uint64, vals []int32) *batch.Batch {
	bat := batch.New([]string{"k", "v"})
	bat.Vecs[0] = testutil.MakeVarcharVector(keys, keyNullPos, mp)
	bat.Vecs[1] = testutil.MakeInt32Vector(vals, nil, mp)
	bat.SetRowCount(len(keys))
	return bat
}

// TestWindowJsonObjectAggOutput drives the window operator end-to-end for a
// two-argument aggregate and asserts the actual JSON output, so a regression in
// multi-argument passing (issue #25483) cannot pass silently.
func TestWindowJsonObjectAggOutput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := makeKeyValBatch(proc.Mp(), []string{"k1", "k2", "k3"}, nil, []int32{10, 20, 30})

	spec := makeAggWindowSpec("json_objectagg")
	spec.GetW().Frame = makeCumulativeFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{jsonObjectAggColExpr(t, 0, 1)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)

	resVec := result.Batch.Vecs[len(result.Batch.Vecs)-1]
	require.Equal(t, 3, resVec.Length())
	// JSON aggregate Merge may consume its source state, so it must retain the
	// ordinary frame evaluator instead of entering the SUM/AVG cumulative path.
	want := []string{
		`{"k1": 10}`,
		`{"k1": 10, "k2": 20}`,
		`{"k1": 10, "k2": 20, "k3": 30}`,
	}
	for i := 0; i < resVec.Length(); i++ {
		require.Equal(t, want[i], types.DecodeJson(resVec.GetBytesAt(i)).String(), "row %d", i)
	}
	require.Nil(t, arg.ctr.runningAgg)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
}

// TestWindowJsonObjectAggNullKeyNoLeak reproduces the NULL-key error exit
// (json_objectagg key cannot be NULL) mid-aggregation and asserts that the
// chunk-local aggregator is released immediately.
func TestWindowJsonObjectAggNullKeyNoLeak(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	// Row 1 has a NULL key: json_objectagg errors while filling the frame.
	bat := makeKeyValBatch(proc.Mp(), []string{"k1", ""}, []uint64{1}, []int32{10, 20})

	arg := &Window{
		WinSpecList: []*plan.Expr{makeAggWindowSpec("json_objectagg")},
		Aggs:        []aggexec.AggFuncExecExpression{jsonObjectAggColExpr(t, 0, 1)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	_, err := vm.Exec(arg, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "key cannot be NULL")
	// Chunk-local aggregators do not need to survive until pipeline Reset.
	require.Nil(t, arg.ctr.batAggs)

	arg.Reset(proc, true, err)
	require.Nil(t, arg.ctr.batAggs, "Reset must release window aggregators after an error")

	arg.Free(proc, true, err)
	op.Free(proc, true, err)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB(), "no mpool leak on the json_objectagg error path")
}

func collectFixedWindowColumn[T types.FixedSizeT](
	t *testing.T,
	arg *Window,
	proc *process.Process,
	column int,
) []T {
	t.Helper()
	var values []T
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			return values
		}
		require.LessOrEqual(t, result.Batch.RowCount(), colexec.DefaultBatchSize)
		values = append(values, vector.MustFixedColWithTypeCheck[T](result.Batch.Vecs[column])...)
	}
}

// TestWindowAggResultAcrossChunks verifies that a cumulative aggregate retains
// its running state across bounded output batches.
func TestWindowAggResultAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := aggexec.AggBatchSize + 17
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i + 1)
	}
	split := aggexec.AggBatchSize / 2
	first := batch.NewWithSize(1)
	first.Vecs[0] = testutil.MakeInt32Vector(values[:split], nil, proc.Mp())
	first.SetRowCount(split)
	second := batch.NewWithSize(1)
	second.Vecs[0] = testutil.MakeInt32Vector(values[split:], nil, proc.Mp())
	second.SetRowCount(rows - split)

	spec := makeWindowSpec()
	spec.Expr.(*plan.Expr_W).W.Frame = makeCumulativeFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	resultValues := collectFixedWindowColumn[int64](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		want := int64(idx+1) * int64(idx+2) / 2
		require.Equal(t, want, resultValues[idx], "row %d", idx)
	}
	require.Nil(t, arg.ctr.runningAgg)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestWindowAggregateResultDropsTailNulls verifies that the window output
// exposes null bits only for logical result rows. Aggregate state may retain
// null bits in unused capacity, which must not make a fully populated output
// look nullable to downstream operators.
func TestWindowAggregateResultDropsTailNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := aggexec.AggBatchSize - 12
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i + 1)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(rows)

	spec := makeWindowSpec()
	spec.Expr.(*plan.Expr_W).W.Frame = makeCurrentRowFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	resultVec := result.Batch.Vecs[1]
	resultValues := vector.MustFixedColWithTypeCheck[int64](resultVec)
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, rows - 1} {
		require.Equal(t, int64(values[idx]), resultValues[idx], "row %d", idx)
	}
	require.False(t, resultVec.HasNull(), "tail capacity must not make a non-null window result nullable")

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowAggregateResultKeepsLogicalNulls(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0, 3}, []uint64{1}, proc.Mp())
	bat.SetRowCount(3)

	spec := makeWindowSpec()
	spec.Expr.(*plan.Expr_W).W.Frame = makeCurrentRowFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	resultVec := result.Batch.Vecs[1]
	require.True(t, resultVec.HasNull())
	require.False(t, resultVec.IsNull(0))
	require.True(t, resultVec.IsNull(1))
	require.False(t, resultVec.IsNull(2))

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowSkipsEmptyInputBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	empty := batch.NewWithSize(1)
	empty.Vecs[0] = vector.NewVec(types.T_int32.ToType())
	nonEmpty := makeInt32Batch(proc.Mp(), []int32{7})

	spec := makeWindowSpec()
	spec.Expr.(*plan.Expr_W).W.Frame = makeCurrentRowFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{empty, nonEmpty})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, []int64{7}, collectFixedWindowColumn[int64](t, arg, proc, 1))

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestWindowPartitionedAggResultAcrossChunks covers the receive-per-partition
// path. The upstream Partition operator guarantees one logical partition per
// input batch; the constant first column models that contract here.
func TestWindowPartitionedAggResultAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := aggexec.AggBatchSize + 17
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i + 1)
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector(make([]int32, rows), nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(rows)

	spec := makeWindowSpec()
	w := spec.Expr.(*plan.Expr_W).W
	w.PartitionBy = []*plan.Expr{newColExpr(0)}
	w.Frame = makeCurrentRowFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExprAt(1)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	resultValues := collectFixedWindowColumn[int64](t, arg, proc, 2)
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		require.Equal(t, int64(values[idx]), resultValues[idx], "row %d", idx)
	}

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestWindowDecimalAggResultAcrossChunks matches the DECIMAL(20,2) SUM shape
// from issue #25813 and exercises the decimal aggregate implementation.
func TestWindowDecimalAggResultAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := aggexec.AggBatchSize + 17
	typ := types.New(types.T_decimal128, 20, 2)
	values := make([]types.Decimal128, rows)
	for i := range values {
		values[i] = types.Decimal128{B0_63: uint64(i + 1)}
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.NewDecimal128Vector(rows, typ, proc.Mp(), false, nil, values)
	bat.SetRowCount(rows)

	spec := makeWindowSpec()
	spec.Expr.(*plan.Expr_W).W.Frame = makeCurrentRowFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newTypedSumAggExpr(t, 0, typ)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	resultValues := collectFixedWindowColumn[types.Decimal128](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		require.Equal(t, values[idx], resultValues[idx], "row %d", idx)
	}

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestWindowOrderResultAcrossChunks covers bounded rank-family output and the
// row-number fast path across an output boundary.
func TestWindowOrderResultAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := aggexec.AggBatchSize + 17
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(make([]int32, rows), nil, proc.Mp())
	bat.SetRowCount(rows)

	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:       "row_number",
				WindowFunc: newFunExpr("row_number"),
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	resultValues := collectFixedWindowColumn[int64](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		require.Equal(t, int64(idx+1), resultValues[idx], "row %d", idx)
	}

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowRankPeerAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := colexec.DefaultBatchSize + 17
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i / 3)
	}
	bat := makeInt32Batch(proc.Mp(), values)
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:       "rank",
				WindowFunc: newFunExpr("rank"),
				OrderBy: []*plan.OrderBySpec{{
					Expr: newColExpr(0),
				}},
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newOrderWindowAggExpr(t, "rank")},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	resultValues := collectFixedWindowColumn[int64](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, row := range []int{0, 1, colexec.DefaultBatchSize - 2, colexec.DefaultBatchSize - 1, colexec.DefaultBatchSize, rows - 1} {
		want := int64(row/3*3 + 1)
		require.Equal(t, want, resultValues[row], "row %d", row)
	}

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowRankTreatsFloatNaNsAsLastPeerGroup(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []float64{
		math.Float64frombits(0x7ff8000000000002), 1,
		math.Float64frombits(0x7ff8000000000001), -1,
	}, nil, proc.Mp()))
	bat.SetRowCount(4)

	orderExpr := newColExprWithType(0, types.T_float64.ToType())
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:       "rank",
				WindowFunc: newFunExpr("rank"),
				OrderBy:    []*plan.OrderBySpec{{Expr: orderExpr}},
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newOrderWindowAggExpr(t, "rank")},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, []int64{1, 2, 3, 3}, collectFixedWindowColumn[int64](t, arg, proc, 1))

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowPartitionedRankTreatsFloatNaNsAsPeers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1, 1}, nil, proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []float64{
		math.Float64frombits(0x7ff8000000000002), 1,
		math.Float64frombits(0x7ff8000000000001), -1,
	}, nil, proc.Mp()))
	bat.SetRowCount(4)

	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:        "rank",
				WindowFunc:  newFunExpr("rank"),
				PartitionBy: []*plan.Expr{newColExprWithType(0, types.T_int32.ToType())},
				OrderBy: []*plan.OrderBySpec{{
					Expr: newColExprWithType(1, types.T_float64.ToType()),
				}},
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newOrderWindowAggExpr(t, "rank")},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, []int64{1, 2, 3, 3}, collectFixedWindowColumn[int64](t, arg, proc, 2))

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowPartitionedFloatNaNPeersUseLaterOrderKey(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1}, nil, proc.Mp())
	bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []float64{
		math.Float64frombits(0x7ff8000000000001),
		math.Float64frombits(0x7ff8000000000002),
		-1,
	}, nil, proc.Mp()))
	bat.Vecs[2] = testutil.MakeInt32Vector([]int32{2, 1, 0}, nil, proc.Mp())
	bat.SetRowCount(3)

	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:        "row_number",
				WindowFunc:  newFunExpr("row_number"),
				PartitionBy: []*plan.Expr{newColExprWithType(0, types.T_int32.ToType())},
				OrderBy: []*plan.OrderBySpec{
					{Expr: newColExprWithType(1, types.T_float64.ToType())},
					{Expr: newColExprWithType(2, types.T_int32.ToType())},
				},
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int32{0, 1, 2},
		vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[2]))
	require.Equal(t, []int64{1, 2, 3},
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[3]))

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowValueResultAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := colexec.DefaultBatchSize + 17
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i + 1)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(rows)

	arg := &Window{
		WinSpecList: []*plan.Expr{makeLagWindowSpec()},
		Aggs:        []aggexec.AggFuncExecExpression{makeValueWindowAggExpr("lag")},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	var resultValues []int32
	var resultNulls []bool
	for {
		result, err := vm.Exec(arg, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		require.LessOrEqual(t, result.Batch.RowCount(), colexec.DefaultBatchSize)
		vec := result.Batch.Vecs[1]
		resultValues = append(resultValues, vector.MustFixedColWithTypeCheck[int32](vec)...)
		for row := range vec.Length() {
			resultNulls = append(resultNulls, vec.IsNull(uint64(row)))
		}
	}
	require.Len(t, resultValues, rows)
	require.True(t, resultNulls[0])
	for row := 1; row < rows; row++ {
		require.False(t, resultNulls[row], "row %d", row)
		require.Equal(t, values[row-1], resultValues[row], "row %d", row)
	}

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowOrderFunctionsUsePeerBoundaries(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 10, 20, 30}, nil, proc.Mp())
	bat.SetRowCount(4)

	tests := []struct {
		name        string
		wantInt     []int64
		wantFloat   []float64
		bucketCount int64
	}{
		{name: "row_number", wantInt: []int64{2, 3, 4}},
		{name: "rank", wantInt: []int64{1, 3, 4}},
		{name: "dense_rank", wantInt: []int64{1, 2, 3}},
		{name: "percent_rank", wantFloat: []float64{0, 2.0 / 3.0, 1}},
		{name: "cume_dist", wantFloat: []float64{0.5, 0.75, 1}},
		{name: "ntile", wantInt: []int64{1, 2, 3}, bucketCount: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctr := container{
				bat: bat,
				// Sorted rows have peer groups [0,2), [2,3), [3,4).
				os: []int64{0, 2, 3},
			}
			if test.name == "ntile" {
				bucketVec, err := vector.NewConstFixed(
					types.T_int64.ToType(), test.bucketCount, bat.RowCount(), proc.Mp())
				require.NoError(t, err)
				defer bucketVec.Free(proc.Mp())
				ctr.aggVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{bucketVec}}}
			}
			arg := &Window{WinSpecList: []*plan.Expr{{
				Expr: &plan.Expr_W{W: &plan.WindowSpec{Name: test.name}},
			}}}
			// Start in the middle of a peer group to prove chunk boundaries do
			// not reset rank state.
			result, err := ctr.processOrderFuncRange(0, arg, proc, 1, 4)
			require.NoError(t, err)
			defer result.Free(proc.Mp())
			if test.wantFloat != nil {
				require.Equal(t, test.wantFloat, vector.MustFixedColWithTypeCheck[float64](result))
			} else {
				require.Equal(t, test.wantInt, vector.MustFixedColWithTypeCheck[int64](result))
			}
		})
	}

	bat.Clean(proc.Mp())
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestNtileBucketCountRejectsNull(t *testing.T) {
	mp := mpool.MustNewZero()
	bucketVec := vector.NewConstNull(types.T_int64.ToType(), 1, mp)
	defer bucketVec.Free(mp)

	ctr := container{
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bucketVec}}},
	}
	_, err := ctr.ntileBucketCount(0)
	require.ErrorContains(t, err, "ntile bucket count cannot be NULL")
}

func TestWindowResetBeforeAllChunksReleasesState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := colexec.DefaultBatchSize * 2
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i + 1)
	}
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
	bat.SetRowCount(rows)

	spec := makeWindowSpec()
	spec.Expr.(*plan.Expr_W).W.Frame = makeCumulativeFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, colexec.DefaultBatchSize, result.Batch.RowCount())
	require.Equal(t, colexec.DefaultBatchSize, arg.ctr.emitOffset)
	require.Equal(t, emit, arg.ctr.status)
	require.Nil(t, arg.ctr.batAggs)
	require.NotNil(t, arg.ctr.runningAgg)

	// Model LIMIT stopping the pipeline after the first output chunk.
	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.runningAgg)
	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestCumulativeAggregateResetsAtPartitionBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := makeInt32Batch(proc.Mp(), []int32{1, 2, 10, 20})
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	ctr := &container{
		bat:     bat,
		ps:      []int64{0, 2},
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 10, 30},
		vector.MustFixedColWithTypeCheck[int64](result))
	require.Nil(t, ctr.runningAgg)

	result.Free(proc.Mp())
	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestCumulativeAggregatePreservesNullSemantics(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0, 3}, []uint64{1}, proc.Mp())
	bat.SetRowCount(3)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeCumulativeFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	ctr := &container{
		bat:     bat,
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 1, 4},
		vector.MustFixedColWithTypeCheck[int64](result))
	require.False(t, result.HasNull())

	result.Free(proc.Mp())
	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowOrdersPartitionedInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{2, 1, 2, 1}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeInt32Vector([]int32{20, 10, 10, 20}, nil, proc.Mp())
	bat.SetRowCount(4)

	partitionExpr := newColExpr(0)
	orderExpr := newColExpr(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:        "row_number",
				WindowFunc:  newFunExpr("row_number"),
				PartitionBy: []*plan.Expr{partitionExpr},
				// The planner presents partition expressions before the explicit
				// ORDER BY expressions to the physical window operator.
				OrderBy: []*plan.OrderBySpec{
					{Expr: partitionExpr, Flag: plan.OrderBySpec_NULLS_FIRST},
					{Expr: orderExpr, Flag: plan.OrderBySpec_DESC | plan.OrderBySpec_NULLS_LAST},
				},
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.Equal(t, vm.Window, arg.OpType())
	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int32{1, 1, 2, 2},
		vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int32{20, 10, 20, 10},
		vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[1]))
	require.Len(t, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]), 4)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestWindowPartitionTopNCoalescesAndResetsRowNumber(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	first := batch.NewWithSize(2)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
	first.SetRowCount(2)
	second := batch.NewWithSize(2)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{2, 2}, nil, proc.Mp())
	// Deliberately reuse the same order values in both groups. Without the
	// partition-key prefix this would look like one peer stream.
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{20, 10}, nil, proc.Mp())
	second.SetRowCount(2)

	partitionExpr := newColExpr(0)
	orderExpr := newColExpr(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:        "row_number",
				WindowFunc:  newFunExpr("row_number"),
				PartitionBy: []*plan.Expr{partitionExpr},
				OrderBy: []*plan.OrderBySpec{
					{Expr: orderExpr, Flag: plan.OrderBySpec_DESC},
				},
			}},
		}},
		Aggs:          []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
		PartitionTopN: true,
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Len(t, arg.Fs, 2)
	require.Len(t, arg.ctr.orderVecs, 2)
	require.Equal(t, []int64{0, 2}, arg.ctr.ps)
	require.Equal(t, []int32{1, 1, 2, 2}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int64{1, 2, 1, 2}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]))

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowOrderHonorsCancellation(t *testing.T) {
	testCases := []struct {
		name   string
		checks int32
	}{
		{name: "building selections", checks: 1},
		{name: "before first sort", checks: 2},
		{name: "after first sort", checks: 3},
		{name: "before secondary sort", checks: 4},
		{name: "during partition sort", checks: 5},
		{name: "after secondary sort", checks: 6},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = testutil.MakeInt32Vector([]int32{2, 1, 2, 1}, nil, proc.Mp())
			bat.Vecs[1] = testutil.MakeInt32Vector([]int32{20, 10, 10, 20}, nil, proc.Mp())
			bat.SetRowCount(4)

			partitionExpr := newColExpr(0)
			orderExpr := newColExpr(1)
			spec := &plan.Expr{
				Expr: &plan.Expr_W{W: &plan.WindowSpec{
					Name:        "row_number",
					WindowFunc:  newFunExpr("row_number"),
					PartitionBy: []*plan.Expr{partitionExpr},
					OrderBy: []*plan.OrderBySpec{
						{Expr: partitionExpr},
						{Expr: orderExpr},
					},
				}},
			}
			arg := &Window{
				WinSpecList: []*plan.Expr{spec},
				Aggs:        []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
			}
			require.NoError(t, arg.Prepare(proc))
			arg.Fs = makeOrderBy(spec)
			arg.ctr.orderVecs = make([]colexec.ExprEvalVector, len(arg.Fs))
			for i := range arg.Fs {
				var err error
				arg.ctr.orderVecs[i], err = colexec.MakeEvalVector(proc, []*plan.Expr{arg.Fs[i].Expr})
				require.NoError(t, err)
			}

			proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, tc.checks)
			_, err := arg.ctr.processOrder(0, arg, bat, proc)
			require.ErrorIs(t, err, context.Canceled)

			arg.Free(proc, true, err)
			bat.Clean(proc.Mp())
			proc.Free()
			require.Equal(t, int64(0), proc.Mp().CurrNB())
		})
	}
}

func newFunExpr(name string) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					ObjName: name,
				},
			},
		},
	}
}

func TestSearchLeftUnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	err := vector.AppendBytes(vec, []byte("abc"), false, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	_, err = searchLeft(0, 1, 0, vec, nil, false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

func TestSearchLeftWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	// Simulate sorted order with ASC NULLS FIRST: [NULL, NULL, 1, 2, 2, 4]
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{0, 0, 1, 2, 2, 4}
	nullRows := []bool{true, true, false, false, false, false}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows should be treated as peers
	// For rowIdx=0 (NULL), searchLeft should return 0 (start of NULL peer group)
	left, err := searchLeft(0, 6, 0, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "NULL row at idx 0: all NULL peers should share the same left boundary")

	// For rowIdx=1 (NULL), searchLeft should also return 0 (peer with row 0)
	left, err = searchLeft(0, 6, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "NULL row at idx 1: should return start of NULL peer group, not its own index")

	// For non-NULL row (k=1 at idx=2), searchLeft with 1 PRECEDING should NOT include NULL rows
	// Target = 1 - 1 = 0, but NULL rows' raw value=0 should NOT match
	left, err = searchLeft(0, 6, 2, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: 1},
			},
		},
	}, false, false)
	require.NoError(t, err)
	require.Equal(t, 2, left, "k=1 with 1 PRECEDING: should start at first non-NULL (idx 2), not include NULLs")
}

func TestSearchRightWithNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	// Simulate sorted order with ASC NULLS FIRST: [NULL, NULL, 1, 2, 2, 4]
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{0, 0, 1, 2, 2, 4}
	nullRows := []bool{true, true, false, false, false, false}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows should be treated as peers
	// For rowIdx=0 (NULL), searchRight should return 2 (end of NULL peer group, exclusive)
	right, err := searchRight(0, 6, 0, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 2, right, "NULL row at idx 0: should return end of NULL peer group (idx 2)")

	// For rowIdx=1 (NULL), searchRight should also return 2
	right, err = searchRight(0, 6, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 2, right, "NULL row at idx 1: should return end of NULL peer group (idx 2)")
}

// TestSearchLeftWithNullsDesc covers DESC NULLS LAST ordering.
// Raw values are [4, 2, 1, 0, 0] — NOT monotonically sorted!
// P2 must confine binary search to the non-NULL subrange [0, 3).
func TestSearchLeftWithNullsDesc(t *testing.T) {
	mp := mpool.MustNewZero()
	// DESC NULLS LAST: raw values = [4, 2, 1, 0, 0], nulls at positions 3, 4
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, true, true}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows should be treated as peers (DESC NULLS LAST)
	left, err := searchLeft(0, 5, 3, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, left, "NULL row at idx 3: all NULL peers share same left boundary (start of NULL group)")

	left, err = searchLeft(0, 5, 4, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, left, "NULL row at idx 4: should return start of NULL peer group (idx 3)")

	// Verify P2 correctly identifies the non-NULL data range [0, 3) when NULLs are at end.
	// After P2 trimming, start=0, end=3 for this [4, 2, 1, NULL, NULL] data.
	// This is validated indirectly: if P2 failed to trim, binary search would operate
	// on the full unsorted [4, 2, 1, 0, 0] and produce garbage results.
	// (Explicit CURRENT ROW search on non-NULL row not tested here because
	// genericSearchLeft assumes ascending order and is not DESC-aware.)
}

// TestSearchRightWithNullsDesc covers DESC NULLS LAST ordering.
// Raw values are [4, 2, 1, 0, 0] — NOT monotonically sorted!
func TestSearchRightWithNullsDesc(t *testing.T) {
	mp := mpool.MustNewZero()
	// DESC NULLS LAST: raw values = [4, 2, 1, 0, 0], nulls at positions 3, 4
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, true, true}

	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// NULL rows are peers
	right, err := searchRight(0, 5, 3, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 5, right, "NULL row at idx 3: should return end of NULL peer group (idx 5)")

	right, err = searchRight(0, 5, 4, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 5, right, "NULL row at idx 4: should return end of NULL peer group (idx 5)")

	// Verify P2 correctly identifies the non-NULL data range [0, 3) when NULLs are at end.
	// After P2 trimming, start=0, end=3 for this [4, 2, 1, NULL, NULL] data.
	// (Explicit CURRENT ROW search on non-NULL row not tested here because
	// genericSearchEqualRight assumes ascending order and is not DESC-aware.)
}

// TestSearchLeftAllNulls verifies NULL peer grouping when all values are NULL.
func TestSearchLeftAllNulls(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	for i := 0; i < 5; i++ {
		require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
	}
	defer vec.Free(mp)

	// All rows are NULL peers — every row should return 0 (start of the NULL group)
	left, err := searchLeft(0, 5, 0, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "all NULL: row 0 should start at 0")

	left, err = searchLeft(0, 5, 4, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 0, left, "all NULL: row 4 should start at 0 (all peers)")
}

func TestSearchRightUnsupportedType(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	err := vector.AppendBytes(vec, []byte("abc"), false, mp)
	require.NoError(t, err)
	defer vec.Free(mp)

	_, err = searchRight(0, 1, 0, vec, nil, false, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported type")
}

// TestSearchLeftDescRange verifies searchLeft with desc=true.
// DESC NULLS LAST ordering: raw values [4, 2, 2, 1, NULL, NULL].
func TestSearchLeftDescRange(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, false, true, true}
	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// CURRENT ROW (desc=true): find first equal to 2
	left, err := searchLeft(0, 4, 1, vec, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, left, "DESC CURRENT ROW k=2: should find first peer at idx 1")

	// 1 PRECEDING from k=2 (desc): target = 2+1 = 3, find first <= 3
	left, err = searchLeft(0, 4, 1, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, left, "DESC k=2 1 PRECEDING: should find first <= 3 (idx 1, value 2)")

	// 1 PRECEDING from k=4 (desc): target = 4+1 = 5, find first <= 5
	left, err = searchLeft(0, 4, 0, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, false, true)
	require.NoError(t, err)
	require.Equal(t, 0, left, "DESC k=4 1 PRECEDING: should find first <= 5 (idx 0, value 4)")

	// 1 FOLLOWING from k=2 (desc): target = 2-1 = 1, find first <= 1
	left, err = searchLeft(0, 4, 1, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, true, true)
	require.NoError(t, err)
	require.Equal(t, 3, left, "DESC k=2 1 FOLLOWING: should find first <= 1 (idx 3, value 1)")
}

// TestSearchRightDescRange verifies searchRight with desc=true.
func TestSearchRightDescRange(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{4, 2, 2, 1, 0, 0}
	nullRows := []bool{false, false, false, false, true, true}
	for i, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, nullRows[i], mp))
	}
	defer vec.Free(mp)

	// CURRENT ROW (desc=true): find last equal to 2
	right, err := searchRight(0, 4, 1, vec, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 3, right, "DESC CURRENT ROW k=2: should find exclusive end after last peer (idx 3)")

	// 1 FOLLOWING from k=2 (desc): target = 2-1 = 1, find last >= 1
	right, err = searchRight(0, 4, 1, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, false, true)
	require.NoError(t, err)
	require.Equal(t, 4, right, "DESC k=2 1 FOLLOWING: should include idx 3 (value 1), exclusive end = 4")

	// 1 PRECEDING from k=1 (desc): target = 1+1 = 2, find last >= 2
	right, err = searchRight(0, 4, 3, vec, &plan.Expr{
		Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}},
	}, true, true)
	require.NoError(t, err)
	require.Equal(t, 3, right, "DESC k=1 1 PRECEDING: should include up to idx 2 (value 2), exclusive end = 3")
}

// TestBuildRangeIntervalEmptyDesc verifies buildRangeInterval does not panic
// when ctr.desc is empty (RANGE frame without an ORDER BY spec).
func TestBuildRangeIntervalEmptyDesc(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{1, 2, 2, 4}
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	defer vec.Free(mp)

	ctr := &container{}
	ctr.orderVecs = make([]colexec.ExprEvalVector, 1)
	ctr.orderVecs[0].Vec = []*vector.Vector{vec}
	// ctr.desc intentionally left empty (no ORDER BY spec).

	// RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
	frame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}},
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}

	start, end, err := ctr.buildRangeInterval(3, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 3, start, "empty desc: 1 PRECEDING from value 4 starts at idx 3")
	require.Equal(t, 4, end, "empty desc: CURRENT ROW ends after last value 4")

	start, end, err = ctr.buildRangeInterval(1, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 0, start, "empty desc: 1 PRECEDING from value 2 reaches idx 0 (value 1)")
	require.Equal(t, 3, end, "empty desc: CURRENT ROW ends after last value 2")
}

// TestBuildRangeIntervalEmptyDescUnbounded verifies the UNBOUNDED branches
// also tolerate an empty ctr.desc.
func TestBuildRangeIntervalEmptyDescUnbounded(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_int64.ToType())
	values := []int64{1, 2, 2, 4}
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	defer vec.Free(mp)

	ctr := &container{}
	ctr.orderVecs = make([]colexec.ExprEvalVector, 1)
	ctr.orderVecs[0].Vec = []*vector.Vector{vec}
	// ctr.desc intentionally left empty.

	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}

	start, end, err := ctr.buildRangeInterval(1, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 0, start, "empty desc: UNBOUNDED PRECEDING keeps start at 0")
	require.Equal(t, 3, end, "empty desc: CURRENT ROW ends after last value 2")
}

func TestBuildRangeIntervalVarcharPeers(t *testing.T) {
	mp := mpool.MustNewZero()
	vec := vector.NewVec(types.T_varchar.ToType())
	for _, value := range []string{"2026-01", "2026-02", "2026-02", "2026-03"} {
		require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
	}
	defer vec.Free(mp)

	ctr := &container{os: []int64{0, 1, 3}}
	ctr.orderVecs = make([]colexec.ExprEvalVector, 1)
	ctr.orderVecs[0].Vec = []*vector.Vector{vec}
	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}

	start, end, err := ctr.buildRangeInterval(1, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 0, start)
	require.Equal(t, 3, end)

	frame.Start = &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW}
	start, end, err = ctr.buildRangeInterval(2, 0, 4, frame)
	require.NoError(t, err)
	require.Equal(t, 1, start)
	require.Equal(t, 3, end)
}

func TestWindowRangeVarcharOrderBy(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{200, 100, 50}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeVarcharVector([]string{"2026-02", "2026-01", "2026-02"}, nil, proc.Mp())
	bat.SetRowCount(3)

	orderType := types.T_varchar.ToType()
	spec := &plan.Expr{
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			Name:       "sum",
			WindowFunc: newFunExpr("sum"),
			OrderBy: []*plan.OrderBySpec{{
				Expr: newColExprWithType(1, orderType),
				Flag: plan.OrderBySpec_ASC,
			}},
			Frame: &plan.FrameClause{
				Type:  plan.FrameClause_RANGE,
				Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
				End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
			},
		}},
	}
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExprAt(0)},
	}
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []string{"2026-01", "2026-02", "2026-02"}, []string{
		string(result.Batch.Vecs[1].GetBytesAt(0)),
		string(result.Batch.Vecs[1].GetBytesAt(1)),
		string(result.Batch.Vecs[1].GetBytesAt(2)),
	})
	require.Equal(t, []int64{100, 350, 350}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]))

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// ---------------------------------------------------------------------------
// All-type coverage tests — each type case in searchLeft/searchRight must be
// exercised at least once by a test to satisfy the 75% diff-coverage gate.
// Data layout: sorted ascending [1, 2, 2, 4] (no NULLs).
//   - ASC CURRENT ROW at idx 1 (value 2):    left=1, right=3
//   - DESC CURRENT ROW at idx 1 (value 2):   left=1, right=3 (equal ignores order)
//   - ASC 1 PRECEDING  at idx 1 (value 2):   left=0 (first >= 1)
//   - ASC 1 FOLLOWING  at idx 1 (value 2):   right=4 (last  <= 3 is idx 3, excl=4)
// ---------------------------------------------------------------------------

// helper to build a simple fixed-type vector without nulls.
func makeFixedVec[T types.OrderedT](t *testing.T, mp *mpool.MPool, oid types.T, values []T) *vector.Vector {
	vec := vector.NewVec(oid.ToType())
	for _, v := range values {
		require.NoError(t, vector.AppendFixed(vec, v, false, mp))
	}
	return vec
}

// Type-specific literal helpers — each searchLeft/searchRight type case casts to
// a distinct proto literal type, so a generic I64Val would panic on e.g. int8.
func i8Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I8Val{I8Val: 1}}}}
}
func i16Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I16Val{I16Val: 1}}}}
}
func i32Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: 1}}}}
}
func i64Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}
}
func u8Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U8Val{U8Val: 1}}}}
}
func u16Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U16Val{U16Val: 1}}}}
}
func u32Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U32Val{U32Val: 1}}}}
}
func u64Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 1}}}}
}
func f32Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Fval{Fval: 1}}}}
}
func f64Lit() *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 1}}}}
}

// testSearchLeftRightNumeric covers each type's case block in both searchLeft and
// searchRight. ASC tests use [1,2,2,4] (ascending order). DESC tests use [4,2,2,1]
// (descending order) so the binary-search comparators are exercised correctly.
func testSearchLeftRightNumeric[T types.OrderedT](t *testing.T, mp *mpool.MPool,
	oid types.T, ascValues, descValues []T, litOffset func() *plan.Expr,
) {
	// ── ASC (data in ascending order) ──
	vec := makeFixedVec[T](t, mp, oid, ascValues)
	defer vec.Free(mp)
	n := len(ascValues)

	l, err := searchLeft(0, n, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 1, l, "ASC CURRENT ROW left")

	r, err := searchRight(0, n, 1, vec, nil, false, false)
	require.NoError(t, err)
	require.Equal(t, 3, r, "ASC CURRENT ROW right")

	l, err = searchLeft(0, n, 1, vec, litOffset(), false, false)
	require.NoError(t, err)
	require.Equal(t, 0, l, "ASC 1 PRECEDING left")

	// 1 FOLLOWING at idx 1 (value 2): target 3, last <= 3 is idx 2 → right = 3
	r, err = searchRight(0, n, 1, vec, litOffset(), false, false)
	require.NoError(t, err)
	require.Equal(t, 3, r, "ASC 1 FOLLOWING right")

	vec.Free(mp)

	// ── DESC (data in descending order) ──
	vec2 := makeFixedVec[T](t, mp, oid, descValues)
	defer vec2.Free(mp)

	l, err = searchLeft(0, n, 0, vec2, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 0, l, "DESC CURRENT ROW left (value 4)")

	r, err = searchRight(0, n, 0, vec2, nil, false, true)
	require.NoError(t, err)
	require.Equal(t, 1, r, "DESC CURRENT ROW right (value 4)")
}

// TestSearchLeftRightAllIntTypes covers int8/16/32/64 + bit.
func TestSearchLeftRightAllIntTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("int8", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int8,
			[]int8{1, 2, 2, 4}, []int8{4, 2, 2, 1}, i8Lit)
	})
	t.Run("int16", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int16,
			[]int16{1, 2, 2, 4}, []int16{4, 2, 2, 1}, i16Lit)
	})
	t.Run("int32", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int32,
			[]int32{1, 2, 2, 4}, []int32{4, 2, 2, 1}, i32Lit)
	})
	t.Run("int64", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_int64,
			[]int64{1, 2, 2, 4}, []int64{4, 2, 2, 1}, i64Lit)
	})
	t.Run("bit", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_bit,
			[]uint64{1, 2, 2, 4}, []uint64{4, 2, 2, 1}, u64Lit)
	})
}

// TestSearchLeftRightAllUintTypes covers uint8/16/32/64.
func TestSearchLeftRightAllUintTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("uint8", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint8,
			[]uint8{1, 2, 2, 4}, []uint8{4, 2, 2, 1}, u8Lit)
	})
	t.Run("uint16", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint16,
			[]uint16{1, 2, 2, 4}, []uint16{4, 2, 2, 1}, u16Lit)
	})
	t.Run("uint32", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint32,
			[]uint32{1, 2, 2, 4}, []uint32{4, 2, 2, 1}, u32Lit)
	})
	t.Run("uint64", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_uint64,
			[]uint64{1, 2, 2, 4}, []uint64{4, 2, 2, 1}, u64Lit)
	})
}

// TestSearchLeftRightAllFloatTypes covers float32/64.
func TestSearchLeftRightAllFloatTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("float32", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_float32,
			[]float32{1, 2, 2, 4}, []float32{4, 2, 2, 1}, f32Lit)
	})
	t.Run("float64", func(t *testing.T) {
		testSearchLeftRightNumeric(t, mp, types.T_float64,
			[]float64{1, 2, 2, 4}, []float64{4, 2, 2, 1}, f64Lit)
	})
}

// TestSearchLeftRightDecimalTypes covers decimal64/128.
func TestSearchLeftRightDecimalTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("decimal64", func(t *testing.T) {
		// ASC
		vec := testutil.NewDecimal64Vector(0, types.T_decimal64.ToType(), mp, false, nil,
			[]types.Decimal64{1, 2, 2, 4},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		// DESC: descending data [4, 2, 2, 1]
		vec2 := testutil.NewDecimal64Vector(0, types.T_decimal64.ToType(), mp, false, nil,
			[]types.Decimal64{4, 2, 2, 1},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("decimal128", func(t *testing.T) {
		vec := testutil.NewDecimal128Vector(0, types.T_decimal128.ToType(), mp, false, nil,
			[]types.Decimal128{
				{B0_63: 1, B64_127: 0}, {B0_63: 2, B64_127: 0},
				{B0_63: 2, B64_127: 0}, {B0_63: 4, B64_127: 0},
			},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewDecimal128Vector(0, types.T_decimal128.ToType(), mp, false, nil,
			[]types.Decimal128{
				{B0_63: 4, B64_127: 0}, {B0_63: 2, B64_127: 0},
				{B0_63: 2, B64_127: 0}, {B0_63: 1, B64_127: 0},
			},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}

// TestSearchLeftRightDateTimeTypes covers date/datetime/time/timestamp (CURRENT ROW only).
func TestSearchLeftRightDateTimeTypes(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("date", func(t *testing.T) {
		vec := testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil,
			[]string{"2024-01-01", "2024-01-02", "2024-01-02", "2024-01-04"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil,
			[]string{"2024-01-04", "2024-01-02", "2024-01-02", "2024-01-01"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("datetime", func(t *testing.T) {
		vec := testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil,
			[]string{"2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-04 10:00:00"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil,
			[]string{"2024-01-04 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-01 10:00:00"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("time", func(t *testing.T) {
		vec := testutil.NewTimeVector(0, types.T_time.ToType(), mp, false, nil,
			[]string{"10:00:00", "12:00:00", "12:00:00", "14:00:00"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewTimeVector(0, types.T_time.ToType(), mp, false, nil,
			[]string{"14:00:00", "12:00:00", "12:00:00", "10:00:00"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})

	t.Run("timestamp", func(t *testing.T) {
		vec := testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil,
			[]string{"2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-04 10:00:00"},
		)
		require.NotNil(t, vec)
		defer vec.Free(mp)

		l, err := searchLeft(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 1, l)
		r, err := searchRight(0, 4, 1, vec, nil, false, false)
		require.NoError(t, err)
		require.Equal(t, 3, r)
		vec.Free(mp)

		vec2 := testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil,
			[]string{"2024-01-04 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-01 10:00:00"},
		)
		require.NotNil(t, vec2)
		defer vec2.Free(mp)

		l, err = searchLeft(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 0, l)
		r, err = searchRight(0, 4, 0, vec2, nil, false, true)
		require.NoError(t, err)
		require.Equal(t, 1, r)
	})
}

func intervalExpr(diff int64, unit types.IntervalType) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: diff},
			}}},
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_I64Val{I64Val: int64(unit)},
			}}},
		}}},
	}
}

func assertIntervalSearches(t *testing.T, vec *vector.Vector, expr *plan.Expr, wantLeftSub, wantLeftAdd, wantRightSub, wantRightAdd int) {
	t.Helper()
	left, err := searchLeft(0, vec.Length(), 1, vec, expr, false, false)
	require.NoError(t, err)
	require.Equal(t, wantLeftSub, left)
	left, err = searchLeft(0, vec.Length(), 1, vec, expr, true, false)
	require.NoError(t, err)
	require.Equal(t, wantLeftAdd, left)

	right, err := searchRight(0, vec.Length(), 1, vec, expr, true, false)
	require.NoError(t, err)
	require.Equal(t, wantRightSub, right)
	right, err = searchRight(0, vec.Length(), 1, vec, expr, false, false)
	require.NoError(t, err)
	require.Equal(t, wantRightAdd, right)
}

func TestSearchLeftRightDateTimeIntervals(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("date", func(t *testing.T) {
		vec := testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil,
			[]string{"2024-01-01", "2024-01-02", "2024-01-02", "2024-01-04"})
		require.NotNil(t, vec)
		defer vec.Free(mp)
		assertIntervalSearches(t, vec, intervalExpr(1, types.Day), 0, 3, 1, 3)
	})

	t.Run("datetime", func(t *testing.T) {
		vec := testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil,
			[]string{"2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-04 10:00:00"})
		require.NotNil(t, vec)
		defer vec.Free(mp)
		assertIntervalSearches(t, vec, intervalExpr(1, types.Day), 0, 3, 1, 3)
	})

	t.Run("time", func(t *testing.T) {
		vec := testutil.NewTimeVector(0, types.T_time.ToType(), mp, false, nil,
			[]string{"10:00:00", "12:00:00", "12:00:00", "14:00:00"})
		require.NotNil(t, vec)
		defer vec.Free(mp)
		assertIntervalSearches(t, vec, intervalExpr(2, types.Hour), 0, 3, 1, 4)
	})

	t.Run("timestamp", func(t *testing.T) {
		vec := testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil,
			[]string{"2024-01-01 10:00:00", "2024-01-02 10:00:00", "2024-01-02 10:00:00", "2024-01-04 10:00:00"})
		require.NotNil(t, vec)
		defer vec.Free(mp)
		assertIntervalSearches(t, vec, intervalExpr(1, types.Day), 0, 3, 1, 3)
	})
}
