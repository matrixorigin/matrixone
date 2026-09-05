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
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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
	execpartition "github.com/matrixorigin/matrixone/pkg/sql/colexec/partition"
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

func TestBoundedSlidingWindowCancellationReleasesState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const rows = cancellationCheckInterval * 2
	values := make([]int32, rows)
	for i := range values {
		values[i] = 1
	}
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(31)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	require.NoError(t, arg.Prepare(proc))
	arg.ctr.bat = bat
	require.NoError(t, arg.ctr.evalAggVector(bat, proc))

	// Cancel after the sliding state has both added entering rows and removed
	// expired rows, then verify the error path releases both aggregate owners.
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
	err := arg.ctr.processFunc(0, arg, proc, arg.OpAnalyzer)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, arg.ctr.batAggs)
	require.Nil(t, arg.ctr.runningAgg)

	arg.Free(proc, true, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingRangeWindowCancellationReleasesState(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	const rows = 128
	values := make([]int32, rows)
	for i := range values {
		values[i] = 1
	}
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeBoundedRangeFrame(2, 2)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			newTypedAvgAggExpr(t, 0, types.T_int32.ToType()),
		},
	}
	arg.ctr.bat = bat
	arg.ctr.os = []int64{0}
	arg.ctr.orderVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}
	arg.ctr.aggVecs = []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}}

	// One peer fills the complete frame while evaluating its first row. Cancel
	// on the first inner-loop poll to prove a large peer remains interruptible.
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
	result, err := arg.ctr.processAggregateFuncRange(0, arg, proc, 0, rows)
	if result != nil {
		result.Free(proc.Mp())
	}
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, arg.ctr.batAggs)
	require.Nil(t, arg.ctr.runningAgg)

	bat.Clean(proc.Mp())
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

func makeBoundedRangeFrame(preceding, following int32) *plan.FrameClause {
	return &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: preceding},
			}}},
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_I32Val{I32Val: following},
			}}},
		},
	}
}

func makePreparedRowsBoundExpr(t *testing.T, pos int32) *plan.Expr {
	return makePreparedWindowBoundExpr(t, pos, types.T_uint64.ToType())
}

func makePreparedWindowBoundExpr(t *testing.T, pos int32, target types.Type) *plan.Expr {
	t.Helper()
	param := &plan.Expr{
		Typ:  plan.Type{Id: int32(types.T_text)},
		Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: pos}},
	}
	targetType := plan.Type{Id: int32(target.Oid), Width: target.Width, Scale: target.Scale, NotNullable: true}
	expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "cast", []*plan.Expr{
		param,
		{Typ: targetType, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
	})
	require.NoError(t, err)
	return expr
}

func makePreparedRangeFrame(t *testing.T, startPos, endPos int32, target types.Type) *plan.FrameClause {
	t.Helper()
	return &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  makePreparedWindowBoundExpr(t, startPos, target),
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  makePreparedWindowBoundExpr(t, endPos, target),
		},
	}
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

func TestWindowPrepareMaterializesRangeFrameBounds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	planned := makePreparedRangeFrame(t, 0, 1, types.T_int32.ToType())
	arg := makeWindowWithFrame(planned)
	firstParams := setWindowPrepareParams(t, proc, stringPtr("1"), stringPtr("2"))

	require.NoError(t, arg.Prepare(proc))
	require.Len(t, arg.ctr.runtimeFrames, 1)
	require.NotSame(t, planned, arg.ctr.runtimeFrames[0])
	require.Equal(t, int32(1), arg.ctr.runtimeFrames[0].Start.Val.GetLit().GetI32Val())
	require.Equal(t, int32(2), arg.ctr.runtimeFrames[0].End.Val.GetLit().GetI32Val())
	requirePreparedRowsBoundUnchanged(t, planned.Start.Val, 0)
	requirePreparedRowsBoundUnchanged(t, planned.End.Val, 1)

	arg.Reset(proc, false, nil)
	require.Nil(t, arg.ctr.runtimeFrames)

	proc.SetPrepareParams(nil)
	firstParams.Free(proc.Mp())
	secondParams := setWindowPrepareParams(t, proc, stringPtr("3"), stringPtr("4"))
	require.NoError(t, arg.Prepare(proc))
	require.Equal(t, int32(3), arg.ctr.runtimeFrames[0].Start.Val.GetLit().GetI32Val())
	require.Equal(t, int32(4), arg.ctr.runtimeFrames[0].End.Val.GetLit().GetI32Val())
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

func TestWindowPrepareValidatesRangeFrameBounds(t *testing.T) {
	tests := []struct {
		name       string
		value      *string
		missing    bool
		emptyParam bool
		want       int32
		wantErr    bool
	}{
		{name: "zero", value: stringPtr("0"), want: 0},
		{name: "maximum", value: stringPtr("2147483647"), want: math.MaxInt32},
		{name: "negative", value: stringPtr("-1"), wantErr: true},
		{name: "fractional", value: stringPtr("1.5"), wantErr: true},
		{name: "null", value: nil, wantErr: true},
		{name: "overflow", value: stringPtr("2147483648"), wantErr: true},
		{name: "conversion failure", value: stringPtr("not-a-number"), wantErr: true},
		{name: "missing vector", missing: true, wantErr: true},
		{name: "missing element", emptyParam: true, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			planned := makePreparedRangeFrame(t, 0, 0, types.T_int32.ToType())
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
				require.Equal(t, test.want, arg.ctr.runtimeFrames[0].Start.Val.GetLit().GetI32Val())
				require.Equal(t, test.want, arg.ctr.runtimeFrames[0].End.Val.GetLit().GetI32Val())
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

func TestValidateRangeFrameBound(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() {
		require.Zero(t, mp.CurrNB())
	}()

	newVec := func(typ types.Type, value any) *vector.Vector {
		var vec *vector.Vector
		var err error
		switch value := value.(type) {
		case uint8:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case int8:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case int16:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case int32:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case int64:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case float32:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case float64:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case types.Decimal64:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case types.Decimal128:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		case bool:
			vec, err = vector.NewConstFixed(typ, value, 1, mp)
		default:
			t.Fatalf("unsupported test value type %T", value)
		}
		require.NoError(t, err)
		return vec
	}

	tests := []struct {
		name    string
		typ     types.Type
		value   any
		wantErr bool
	}{
		{name: "unsigned", typ: types.T_uint8.ToType(), value: uint8(1)},
		{name: "int8", typ: types.T_int8.ToType(), value: int8(1)},
		{name: "int16", typ: types.T_int16.ToType(), value: int16(1)},
		{name: "int32", typ: types.T_int32.ToType(), value: int32(-1), wantErr: true},
		{name: "int64", typ: types.T_int64.ToType(), value: int64(1)},
		{name: "float32", typ: types.T_float32.ToType(), value: float32(1.5)},
		{name: "float32 infinity", typ: types.T_float32.ToType(), value: float32(math.Inf(1)), wantErr: true},
		{name: "float64", typ: types.T_float64.ToType(), value: float64(1.5)},
		{name: "float64 nan", typ: types.T_float64.ToType(), value: math.NaN(), wantErr: true},
		{name: "decimal64", typ: types.T_decimal64.ToType(), value: types.Decimal64(1)},
		{name: "decimal64 negative", typ: types.T_decimal64.ToType(), value: types.Decimal64Min, wantErr: true},
		{name: "decimal128", typ: types.T_decimal128.ToType(), value: types.Decimal128FromInt64(1)},
		{name: "decimal128 negative", typ: types.T_decimal128.ToType(), value: types.Decimal128FromInt64(-1), wantErr: true},
		{name: "non-numeric", typ: types.T_bool.ToType(), value: true, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			vec := newVec(test.typ, test.value)
			defer vec.Free(mp)
			if test.wantErr {
				require.Error(t, validateRangeFrameBound(context.Background(), vec))
			} else {
				require.NoError(t, validateRangeFrameBound(context.Background(), vec))
			}
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

func TestWindowPreparePreservesRangeIntervalBounds(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	interval := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_interval)},
		Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
			plan2.MakePlan2Int64ConstExprWithType(1),
			plan2.MakePlan2Int64ConstExprWithType(int64(types.Day)),
		}}},
	}
	planned := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  interval,
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	arg := makeWindowWithFrame(planned)

	require.NoError(t, arg.Prepare(proc))
	require.Len(t, arg.ctr.runtimeFrames, 1)
	require.NotSame(t, planned, arg.ctr.runtimeFrames[0])
	require.Same(t, interval, arg.ctr.runtimeFrames[0].Start.Val)

	arg.Free(proc, false, nil)
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

func TestWindowPrepareRangeFrameBoundsFeedAggregateConsumer(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	planned := makePreparedRangeFrame(t, 0, 1, types.T_int32.ToType())
	spec := &plan.Expr{
		Expr: &plan.Expr_W{W: &plan.WindowSpec{
			Name:       "sum",
			WindowFunc: newFunExpr("sum"),
			OrderBy: []*plan.OrderBySpec{{
				Expr: newColExprWithType(1, types.T_int32.ToType()),
				Flag: plan.OrderBySpec_ASC,
			}},
			Frame: planned,
		}},
	}
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExprAt(0)},
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	bat.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 3, 4}, nil, proc.Mp())
	bat.SetRowCount(3)
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	arg.AppendChild(op)
	params := setWindowPrepareParams(t, proc, stringPtr("1"), stringPtr("1"))

	require.NoError(t, arg.Prepare(proc))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 50, 50},
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]))
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

func TestWindowPreparedBoundedSlidingSumUsesRuntimeValue(t *testing.T) {
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
	params := setWindowPrepareParams(t, proc, stringPtr("1"))

	require.NoError(t, arg.Prepare(proc))
	require.True(t, boundedSlidingRowsFrame(arg.ctr.runtimeFrames[0]))
	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{10, 30, 50, 70},
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

func TestBoundedSlidingRowsFrameEligibility(t *testing.T) {
	tests := []struct {
		name  string
		frame *plan.FrameClause
		want  bool
	}{
		{name: "finite preceding", frame: makeFiniteCumulativeFrame(31), want: true},
		{name: "zero preceding", frame: makeFiniteCumulativeFrame(0), want: true},
		{name: "unbounded", frame: makeCumulativeFrame()},
		{name: "current row start", frame: makeCurrentRowFrame()},
		{name: "following end", frame: makeFullFrame()},
		{name: "range", frame: &plan.FrameClause{
			Type:  plan.FrameClause_RANGE,
			Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: makeFiniteCumulativeFrame(1).Start.Val},
			End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, boundedSlidingRowsFrame(test.frame))
		})
	}
}

func TestBoundedSlidingRangeFrameEligibility(t *testing.T) {
	intOrder := vector.NewVec(types.T_int32.ToType())
	floatOrder := vector.NewVec(types.T_float64.ToType())
	timestampOrder := vector.NewVec(types.T_timestamp.ToType())
	validFrame := makeBoundedRangeFrame(2, 2)

	tests := []struct {
		name      string
		frame     *plan.FrameClause
		orderVecs []colexec.ExprEvalVector
		want      bool
	}{
		{name: "bounded integer", frame: validFrame,
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{intOrder}}}, want: true},
		{name: "float", frame: validFrame,
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{floatOrder}}}},
		{name: "timestamp", frame: validFrame,
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{timestampOrder}}}},
		{name: "multiple order keys", frame: validFrame, orderVecs: []colexec.ExprEvalVector{
			{Vec: []*vector.Vector{intOrder}}, {Vec: []*vector.Vector{intOrder}},
		}},
		{name: "no materialized order vector", frame: validFrame},
		{name: "unbounded start", frame: &plan.FrameClause{
			Type:  plan.FrameClause_RANGE,
			Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
			End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		}, orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{intOrder}}}},
		{name: "rows", frame: makeFiniteCumulativeFrame(2),
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{intOrder}}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, boundedSlidingRangeFrame(test.frame, test.orderVecs))
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

func newTypedAvgAggExpr(t testing.TB, pos int32, typ types.Type) aggexec.AggFuncExecExpression {
	e, err := function.GetFunctionByName(context.Background(), "avg", []types.Type{typ})
	require.NoError(t, err)
	return aggexec.MakeAggFunctionExpression(
		e.GetEncodedOverloadID(), false, []*plan.Expr{newColExprWithType(pos, typ)}, nil)
}

func newTypedMaxAggExpr(t testing.TB, pos int32, typ types.Type) aggexec.AggFuncExecExpression {
	e, err := function.GetFunctionByName(context.Background(), "max", []types.Type{typ})
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
	resultValues := collectFixedWindowColumn[uint64](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, idx := range []int{0, aggexec.AggBatchSize - 1, aggexec.AggBatchSize, rows - 1} {
		require.Equal(t, uint64(idx+1), resultValues[idx], "row %d", idx)
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
	resultValues := collectFixedWindowColumn[uint64](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, row := range []int{0, 1, colexec.DefaultBatchSize - 2, colexec.DefaultBatchSize - 1, colexec.DefaultBatchSize, rows - 1} {
		want := uint64(row/3*3 + 1)
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
	require.Equal(t, []uint64{1, 2, 3, 3}, collectFixedWindowColumn[uint64](t, arg, proc, 1))

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
	require.Equal(t, []uint64{1, 2, 3, 3}, collectFixedWindowColumn[uint64](t, arg, proc, 2))

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
	require.Equal(t, []uint64{1, 2, 3},
		vector.MustFixedColWithTypeCheck[uint64](result.Batch.Vecs[3]))

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
		wantUint    []uint64
		wantFloat   []float64
		bucketCount int64
	}{
		{name: "row_number", wantUint: []uint64{2, 3, 4}},
		{name: "rank", wantUint: []uint64{1, 3, 4}},
		{name: "dense_rank", wantUint: []uint64{1, 2, 3}},
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
				require.Equal(t, types.T_float64, result.GetType().Oid)
				require.Equal(t, test.wantFloat, vector.MustFixedColWithTypeCheck[float64](result))
			} else if test.wantUint != nil {
				require.Equal(t, types.T_uint64, result.GetType().Oid)
				require.Equal(t, test.wantUint, vector.MustFixedColWithTypeCheck[uint64](result))
			} else {
				require.Equal(t, types.T_int64, result.GetType().Oid)
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

func TestCumulativeMaxUsesRunningAggregateAcrossChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	values := make([]int32, 512)
	want := make([]int32, len(values))
	for i := range values {
		values[i] = int32(256 - i%256)
		want[i] = 256
	}
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeCumulativeFrame()
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newTypedMaxAggExpr(t, 0, types.T_int32.ToType())},
	}
	ctr := &container{
		bat:     bat,
		ps:      []int64{0, 256},
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 200)
	require.NoError(t, err)
	require.Equal(t, want[:200], vector.MustFixedColWithTypeCheck[int32](first))
	require.NotNil(t, ctr.runningAgg, "cumulative MIN/MAX must retain one running state")
	first.Free(proc.Mp())

	second, err := ctr.processAggregateFuncRange(0, arg, proc, 200, 300)
	require.NoError(t, err)
	require.Equal(t, want[200:300], vector.MustFixedColWithTypeCheck[int32](second))
	require.NotNil(t, ctr.runningAgg)
	second.Free(proc.Mp())

	third, err := ctr.processAggregateFuncRange(0, arg, proc, 300, len(values))
	require.NoError(t, err)
	require.Equal(t, want[300:], vector.MustFixedColWithTypeCheck[int32](third))
	require.Nil(t, ctr.runningAgg)
	third.Free(proc.Mp())

	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestCumulativePartitionUsesRunning(t *testing.T) {
	for _, test := range []struct {
		name       string
		start, end int
		want       bool
	}{
		{name: "empty", start: 4, end: 4},
		{name: "singleton", start: 3, end: 4},
		{name: "below state chunk cost", end: 128},
		{name: "above state chunk cost", end: 129, want: true},
		{name: "large", end: 256, want: true},
		{name: "large nonzero start", start: 17, end: 273, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want,
				cumulativePartitionUsesRunning(test.start, test.end))
		})
	}
}

func TestBoundedSlidingSumAcrossOutputChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	rows := colexec.DefaultBatchSize*2 + 17
	values := make([]int32, rows)
	for i := range values {
		values[i] = 1
	}
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1024)
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
	resultValues := collectFixedWindowColumn[int64](t, arg, proc, 1)
	require.Len(t, resultValues, rows)
	for _, row := range []int{0, 1023, 1024, colexec.DefaultBatchSize - 1, colexec.DefaultBatchSize, rows - 1} {
		require.Equal(t, int64(min(row+1, 1025)), resultValues[row], "row %d", row)
	}
	require.Nil(t, arg.ctr.runningAgg)

	arg.Free(proc, false, nil)
	op.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingSumRejectsNonSequentialOutput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := makeInt32Batch(proc.Mp(), []int32{1, 2, 3})
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	ctr := &container{
		bat:     bat,
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	first, err := ctr.processAggregateFuncRange(0, arg, proc, 0, 1)
	require.NoError(t, err)
	first.Free(proc.Mp())
	require.NotNil(t, ctr.runningAgg)

	// A retained sliding state is valid only for the immediately following
	// output range; skipping a row must fail and release that state.
	_, err = ctr.processAggregateFuncRange(0, arg, proc, 2, 3)
	require.ErrorContains(t, err, "sliding window output is not sequential")
	require.Nil(t, ctr.runningAgg)

	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingSumResetsAtPartitionBoundary(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := makeInt32Batch(proc.Mp(), []int32{1, 2, 3, 4, 10, 20, 30, 40})
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	ctr := &container{
		bat:     bat,
		ps:      []int64{0, 4},
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 5, 7, 10, 30, 50, 70},
		vector.MustFixedColWithTypeCheck[int64](result))
	require.Nil(t, ctr.runningAgg)

	result.Free(proc.Mp())
	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingSumPreservesNullSemantics(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 0, 0, 4}, []uint64{1, 2}, proc.Mp())
	bat.SetRowCount(4)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1)
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
	require.Equal(t, []int64{1, 1, 0, 4},
		vector.MustFixedColWithTypeCheck[int64](result))
	require.False(t, result.IsNull(0))
	require.False(t, result.IsNull(1))
	require.True(t, result.IsNull(2))
	require.False(t, result.IsNull(3))

	result.Free(proc.Mp())
	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingSumSupportsInt64Arguments(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.NewInt64Vector(
		4, types.T_int64.ToType(), proc.Mp(), false, nil, []int64{1, 2, 3, 4})
	bat.SetRowCount(4)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newTypedSumAggExpr(t, 0, types.T_int64.ToType())},
	}
	ctr := &container{
		bat:     bat,
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []types.Decimal128{
		types.Decimal128FromInt64(1),
		types.Decimal128FromInt64(3),
		types.Decimal128FromInt64(5),
		types.Decimal128FromInt64(7),
	}, vector.MustFixedColWithTypeCheck[types.Decimal128](result))

	result.Free(proc.Mp())
	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingSumSupportsDecimal64Arguments(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	typ := types.New(types.T_decimal64, 18, 2)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = testutil.NewDecimal64Vector(
		5, typ, proc.Mp(), false,
		[]bool{false, true, true, false, false},
		[]types.Decimal64{100, 0, 0, types.Decimal64(300).Minus(), 400})
	bat.SetRowCount(5)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeFiniteCumulativeFrame(1)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newTypedSumAggExpr(t, 0, typ)},
	}
	ctr := &container{
		bat:     bat,
		aggVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []types.Decimal128{
		types.Decimal128FromInt64(100),
		types.Decimal128FromInt64(100),
		{},
		types.Decimal128FromInt64(-300),
		types.Decimal128FromInt64(100),
	}, vector.MustFixedColWithTypeCheck[types.Decimal128](result))
	require.False(t, result.IsNull(0))
	require.False(t, result.IsNull(1))
	require.True(t, result.IsNull(2))
	require.False(t, result.IsNull(3))
	require.False(t, result.IsNull(4))

	result.Free(proc.Mp())
	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingRangeAvgAcrossPeersAndOutputChunks(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	values := []int32{1, 1, 2, 4, 4, 7}
	bat := makeInt32Batch(proc.Mp(), values)
	spec := makeWindowSpec()
	spec.GetW().Frame = makeBoundedRangeFrame(2, 2)
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs: []aggexec.AggFuncExecExpression{
			newTypedAvgAggExpr(t, 0, types.T_int32.ToType()),
		},
	}
	ctr := &container{
		bat:       bat,
		os:        []int64{0, 2, 3, 5},
		orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
		aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
	}

	var got []float64
	for _, output := range [][2]int{{0, 1}, {1, 4}, {4, len(values)}} {
		result, err := ctr.processAggregateFuncRange(0, arg, proc, output[0], output[1])
		require.NoError(t, err)
		require.Equal(t, types.T_decimal128, result.GetType().Oid)
		for _, value := range vector.MustFixedColWithTypeCheck[types.Decimal128](result) {
			got = append(got, types.Decimal128ToFloat64(value, result.GetType().Scale))
		}
		result.Free(proc.Mp())
		if output[1] < len(values) {
			require.NotNil(t, ctr.runningAgg)
		}
	}

	require.InDeltaSlice(t, []float64{4.0 / 3, 4.0 / 3, 2.4, 10.0 / 3, 10.0 / 3, 7}, got, 1e-4)
	require.Nil(t, ctr.runningAgg)
	require.Zero(t, ctr.runningPeerEnd)

	bat.Clean(proc.Mp())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestBoundedSlidingRangeAvgOrderShapes(t *testing.T) {
	tests := []struct {
		name       string
		aggValues  []int32
		orderValue []int32
		orderNulls []uint64
		partitions []int64
		peers      []int64
		desc       bool
		want       []float64
	}{
		{
			name:       "descending peers",
			aggValues:  []int32{7, 4, 4, 2, 1, 1},
			orderValue: []int32{7, 4, 4, 2, 1, 1},
			peers:      []int64{0, 1, 3, 4},
			desc:       true,
			want:       []float64{7, 10.0 / 3, 10.0 / 3, 2.4, 4.0 / 3, 4.0 / 3},
		},
		{
			name:       "partition reset",
			aggValues:  []int32{1, 2, 2, 4, 1, 1, 3, 5},
			orderValue: []int32{1, 2, 2, 4, 1, 1, 3, 5},
			partitions: []int64{0, 4},
			peers:      []int64{0, 1, 3, 4, 6, 7},
			want:       []float64{5.0 / 3, 2.25, 2.25, 8.0 / 3, 5.0 / 3, 5.0 / 3, 2.5, 4},
		},
		{
			name:       "null order peers",
			aggValues:  []int32{10, 20, 1, 2},
			orderValue: []int32{0, 0, 1, 2},
			orderNulls: []uint64{0, 1},
			peers:      []int64{0, 2, 3},
			want:       []float64{15, 15, 1.5, 1.5},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			bat := batch.NewWithSize(2)
			bat.Vecs[0] = testutil.MakeInt32Vector(test.aggValues, nil, proc.Mp())
			bat.Vecs[1] = testutil.MakeInt32Vector(test.orderValue, test.orderNulls, proc.Mp())
			bat.SetRowCount(len(test.aggValues))
			spec := makeWindowSpec()
			spec.GetW().Frame = makeBoundedRangeFrame(2, 2)
			arg := &Window{
				WinSpecList: []*plan.Expr{spec},
				Aggs: []aggexec.AggFuncExecExpression{
					newTypedAvgAggExpr(t, 0, types.T_int32.ToType()),
				},
			}
			ctr := &container{
				bat:       bat,
				ps:        test.partitions,
				os:        test.peers,
				desc:      []bool{test.desc},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[1]}}},
				aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
			}

			result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
			require.NoError(t, err)
			require.Equal(t, types.T_decimal128, result.GetType().Oid)
			got := make([]float64, 0, result.Length())
			for _, value := range vector.MustFixedColWithTypeCheck[types.Decimal128](result) {
				got = append(got, types.Decimal128ToFloat64(value, result.GetType().Scale))
			}
			require.InDeltaSlice(t, test.want, got, 1e-4)
			require.Nil(t, ctr.runningAgg)

			result.Free(proc.Mp())
			bat.Clean(proc.Mp())
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestBoundedSlidingRangeSumUint8MaximumBoundary(t *testing.T) {
	frame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U8Val{U8Val: 2},
		}}}},
		End: &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, Val: &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
			Value: &plan.Literal_U8Val{U8Val: 2},
		}}}},
	}
	for _, test := range []struct {
		name   string
		values []uint8
		desc   bool
		want   []uint64
	}{
		{name: "ascending", values: []uint8{252, 253, 254, 255}, want: []uint64{759, 1014, 1014, 762}},
		{name: "descending", values: []uint8{255, 254, 253, 252}, desc: true, want: []uint64{762, 1014, 1014, 759}},
	} {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = testutil.MakeUint8Vector(test.values, nil, proc.Mp())
			bat.SetRowCount(len(test.values))
			arg := &Window{
				WinSpecList: []*plan.Expr{{Expr: &plan.Expr_W{W: &plan.WindowSpec{Frame: frame}}}},
				Aggs: []aggexec.AggFuncExecExpression{
					newTypedSumAggExpr(t, 0, types.T_uint8.ToType()),
				},
			}
			ctr := &container{
				bat:       bat,
				os:        []int64{0, 1, 2, 3},
				desc:      []bool{test.desc},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
				aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{bat.Vecs[0]}}},
			}

			result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
			require.NoError(t, err)
			require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[uint64](result))
			require.Nil(t, ctr.runningAgg)

			result.Free(proc.Mp())
			bat.Clean(proc.Mp())
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
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
	require.Len(t, vector.MustFixedColWithTypeCheck[uint64](result.Batch.Vecs[2]), 4)

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
	require.Equal(t, []uint64{1, 2, 1, 2}, vector.MustFixedColWithTypeCheck[uint64](result.Batch.Vecs[2]))

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPartitionTopNUsesSQLOrderForFloatNaNPeers(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	makeBatch := func(partitionValue int32) *batch.Batch {
		bat := batch.NewWithSize(3)
		bat.Vecs[0] = testutil.MakeInt32Vector(
			[]int32{partitionValue, partitionValue, partitionValue}, nil, proc.Mp())
		bat.Vecs[1] = vector.NewVec(types.T_float64.ToType())
		require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []float64{
			math.Float64frombits(0x7ff8000000000002),
			math.Float64frombits(0x7ff8000000000001),
			-1,
		}, nil, proc.Mp()))
		bat.Vecs[2] = testutil.MakeInt32Vector([]int32{2, 1, 0}, nil, proc.Mp())
		bat.SetRowCount(3)
		return bat
	}
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
		Aggs:          []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
		PartitionTopN: true,
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{makeBatch(1), makeBatch(2)})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{0, 3}, arg.ctr.ps)
	require.Equal(t, []int32{1, 1, 1, 2, 2, 2},
		vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int32{0, 1, 2, 0, 1, 2},
		vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[2]))
	require.Equal(t, []uint64{1, 2, 3, 1, 2, 3},
		vector.MustFixedColWithTypeCheck[uint64](result.Batch.Vecs[3]))

	arg.Free(proc, false, nil)
	child.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowPartitionTopNReducerUsesSQLOrderForFloatNaNs(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag plan.OrderBySpec_OrderByFlag
		want []int32
	}{
		{name: "asc", want: []int32{10, 20}},
		{name: "desc", flag: plan.OrderBySpec_DESC, want: []int32{60, 40}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			input := batch.NewWithSize(3)
			input.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 1, 1, 1, 1, 1}, nil, proc.Mp())
			input.Vecs[1] = vector.NewVec(types.T_float64.ToType())
			require.NoError(t, vector.AppendFixedList(input.Vecs[1], []float64{
				math.Float64frombits(0x7ff8000000000002), math.Inf(1), 1, 0, -1,
				math.Inf(-1), math.Float64frombits(0x7ff8000000000001),
			}, nil, proc.Mp()))
			input.Vecs[2] = testutil.MakeInt32Vector([]int32{70, 60, 40, 31, 20, 10, 71}, nil, proc.Mp())
			input.SetRowCount(7)

			// This is the physical shape of ROW_NUMBER() ... WHERE rn <= 2:
			// the PARTITION reducer must retain the SQL-order prefix before the
			// window operator assigns row numbers.
			partitionArg := &execpartition.Partition{
				OrderBySpecs: []*plan.OrderBySpec{
					{Expr: newColExprWithType(0, types.T_int32.ToType())},
					{Expr: newColExprWithType(1, types.T_float64.ToType()), Flag: tc.flag},
					{Expr: newColExprWithType(2, types.T_int32.ToType())},
				},
				Limit: &plan.Expr{
					Typ:  plan.Type{Id: int32(types.T_uint64), NotNullable: true},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_U64Val{U64Val: 2}}},
				},
				PartitionByCount: 1,
				PreReduce:        true,
			}
			partitionChild := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
			partitionArg.AppendChild(partitionChild)
			require.NoError(t, partitionArg.Prepare(proc))

			partitionExpr := newColExprWithType(0, types.T_int32.ToType())
			orderExpr := newColExprWithType(1, types.T_float64.ToType())
			windowArg := &Window{
				WinSpecList: []*plan.Expr{{
					Expr: &plan.Expr_W{W: &plan.WindowSpec{
						Name:        "row_number",
						WindowFunc:  newFunExpr("row_number"),
						PartitionBy: []*plan.Expr{partitionExpr},
						OrderBy: []*plan.OrderBySpec{
							{Expr: orderExpr, Flag: tc.flag},
							{Expr: newColExprWithType(2, types.T_int32.ToType())},
						},
					}},
				}},
				Aggs:          []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
				PartitionTopN: true,
			}
			windowArg.AppendChild(partitionArg)
			require.NoError(t, windowArg.Prepare(proc))

			result, err := vm.Exec(windowArg, proc)
			require.NoError(t, err)
			require.NotNil(t, result.Batch)
			require.Equal(t, tc.want, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[2]))
			require.Equal(t, []uint64{1, 2}, vector.MustFixedColWithTypeCheck[uint64](result.Batch.Vecs[3]))

			windowArg.Free(proc, false, nil)
			partitionArg.Free(proc, false, nil)
			partitionChild.Free(proc, false, nil)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestWindowResetReleasesInheritedAccountedBuffers(t *testing.T) {
	mp := mpool.MustNewZero()
	registry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)
	selection, err := vector.NewAllocationAccountSelection(
		account, mpool.AllocationOwnerOrder, 1, 2, 3, 4)
	require.NoError(t, err)

	partitionVec := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	require.NoError(t, partitionVec.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(partitionVec, int32(1), false, mp))
	orderVec := vector.NewOffHeapVecWithType(types.T_int32.ToType())
	require.NoError(t, orderVec.SetAllocationAccount(selection))
	require.NoError(t, vector.AppendFixed(orderVec, int32(10), false, mp))
	input := batch.NewOffHeapWithSize(2)
	input.SetVector(0, partitionVec)
	input.SetVector(1, orderVec)
	input.SetRowCount(1)

	partitionExpr := newColExpr(0)
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:        "row_number",
				WindowFunc:  newFunExpr("row_number"),
				PartitionBy: []*plan.Expr{partitionExpr},
				OrderBy: []*plan.OrderBySpec{{
					Expr: newColExpr(1),
				}},
			}},
		}},
		Aggs: []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	proc := testutil.NewProcessWithMPool(t, "", mp)
	require.NoError(t, arg.Prepare(proc))
	result, err := arg.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)

	// Mirror child-first pipeline cleanup. The remaining account usage belongs
	// to Window's materialized input and expression duplicates.
	child.Free(proc, false, nil)
	require.Positive(t, account.Snapshot().Used)
	arg.Reset(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)

	require.NoError(t, arg.Prepare(proc))
	arg.Free(proc, false, nil)
	snapshot := account.Seal()
	require.Zero(t, snapshot.Used)
	_, err = registry.Finalize(account)
	require.NoError(t, err)
	proc.Free()
	require.Zero(t, mp.CurrNB())
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

func uint64RangeOffset(value uint64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{
		Value: &plan.Literal_U64Val{U64Val: value},
	}}}
}

func unsignedRangeOffset(oid types.T, value uint64) *plan.Expr {
	lit := &plan.Literal{}
	switch oid {
	case types.T_uint8:
		lit.Value = &plan.Literal_U8Val{U8Val: uint32(value)}
	case types.T_uint16:
		lit.Value = &plan.Literal_U16Val{U16Val: uint32(value)}
	case types.T_uint32:
		lit.Value = &plan.Literal_U32Val{U32Val: uint32(value)}
	case types.T_uint64:
		lit.Value = &plan.Literal_U64Val{U64Val: value}
	default:
		panic("unsupported unsigned RANGE type")
	}
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: lit}}
}

func TestUint64RangeBound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       uint64
		offset      uint64
		subtract    bool
		wantBound   uint64
		wantAbove   bool
		wantInRange bool
	}{
		{name: "add", value: 1, offset: 10, wantBound: 11, wantInRange: true},
		{name: "subtract", value: 10, offset: 10, subtract: true, wantBound: 0, wantInRange: true},
		{name: "subtract underflow", value: 9, offset: 10, subtract: true},
		{name: "add maximum", value: math.MaxUint64, wantBound: math.MaxUint64, wantInRange: true},
		{name: "add overflow", value: math.MaxUint64, offset: 1, wantAbove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, above, ok := uint64RangeBound(tc.value, tc.offset, tc.subtract)
			require.Equal(t, tc.wantBound, bound)
			require.Equal(t, tc.wantAbove, above)
			require.Equal(t, tc.wantInRange, ok)
		})
	}

	require.Equal(t, 0, outOfDomainRangeBoundary(0, 21, false, false))
	require.Equal(t, 21, outOfDomainRangeBoundary(0, 21, true, false))
	require.Equal(t, 21, outOfDomainRangeBoundary(0, 21, false, true))
	require.Equal(t, 0, outOfDomainRangeBoundary(0, 21, true, true))
}

func TestBuildRangeIntervalUint64Boundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	currentToFollowing := func(offset uint64) *plan.FrameClause {
		return &plan.FrameClause{
			Type:  plan.FrameClause_RANGE,
			Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
			End: &plan.FrameBound{
				Type: plan.FrameBound_FOLLOWING,
				Val:  uint64RangeOffset(offset),
			},
		}
	}
	precedingOnly := func(offset uint64) *plan.FrameClause {
		return &plan.FrameClause{
			Type: plan.FrameClause_RANGE,
			Start: &plan.FrameBound{
				Type: plan.FrameBound_PRECEDING,
				Val:  uint64RangeOffset(offset),
			},
			End: &plan.FrameBound{
				Type: plan.FrameBound_PRECEDING,
				Val:  uint64RangeOffset(offset),
			},
		}
	}
	check := func(t *testing.T, oid types.T, values []uint64, desc bool, row int, frame *plan.FrameClause, wantStart, wantEnd int) {
		t.Helper()
		vec := makeFixedVec(t, mp, oid, values)
		defer vec.Free(mp)
		ctr := &container{
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			desc:      []bool{desc},
		}
		start, end, err := ctr.buildRangeInterval(row, 0, len(values), frame)
		require.NoError(t, err)
		require.Equal(t, wantStart, start)
		require.Equal(t, wantEnd, end)
	}

	asc := make([]uint64, 21)
	desc := make([]uint64, 21)
	for i := range asc {
		asc[i] = uint64(i)
		desc[i] = uint64(20 - i)
	}

	for _, oid := range []types.T{types.T_uint64, types.T_bit} {
		t.Run(oid.String(), func(t *testing.T) {
			check(t, oid, asc, false, 0, currentToFollowing(10), 0, 11)
			check(t, oid, asc, false, 0, currentToFollowing(0), 0, 1)
			check(t, oid, asc, false, 10, currentToFollowing(10), 10, 21)
			check(t, oid, desc, true, 11, currentToFollowing(10), 11, 21)
			check(t, oid, asc, false, 10, precedingOnly(10), 0, 1)
			check(t, oid, asc, false, 5, precedingOnly(10), 0, 0)
			check(t, oid, []uint64{math.MaxUint64 - 2, math.MaxUint64 - 1, math.MaxUint64}, false, 0, currentToFollowing(10), 0, 3)
		})
	}

	t.Run("null peers", func(t *testing.T) {
		vec := vector.NewVec(types.T_uint64.ToType())
		for i, value := range []uint64{0, 0, 0, 1} {
			require.NoError(t, vector.AppendFixed(vec, value, i < 2, mp))
		}
		defer vec.Free(mp)
		ctr := &container{
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			desc:      []bool{false},
		}
		start, end, err := ctr.buildRangeInterval(0, 0, vec.Length(), currentToFollowing(10))
		require.NoError(t, err)
		require.Equal(t, 0, start)
		require.Equal(t, 2, end)
		start, end, err = ctr.buildRangeInterval(2, 0, vec.Length(), currentToFollowing(10))
		require.NoError(t, err)
		require.Equal(t, 2, start)
		require.Equal(t, 4, end)
	})
}

func testBuildRangeIntervalUnsignedBoundaries[T unsignedRangeInteger](
	t *testing.T,
	mp *mpool.MPool,
	oid types.T,
	maxValue T,
) {
	t.Helper()
	currentToFollowing := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  unsignedRangeOffset(oid, 1),
		},
	}
	precedingToCurrent := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  unsignedRangeOffset(oid, 1),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	zeroPreceding := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  unsignedRangeOffset(oid, 0),
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  unsignedRangeOffset(oid, 0),
		},
	}
	check := func(t *testing.T, values []T, desc bool, row int, frame *plan.FrameClause, wantStart, wantEnd int) {
		t.Helper()
		vec := makeFixedVec(t, mp, oid, values)
		defer vec.Free(mp)
		ctr := &container{
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			desc:      []bool{desc},
		}
		start, end, err := ctr.buildRangeInterval(row, 0, len(values), frame)
		require.NoError(t, err)
		require.Equal(t, wantStart, start)
		require.Equal(t, wantEnd, end)
	}

	var zeroValue T
	asc := []T{zeroValue, maxValue}
	desc := []T{maxValue, zeroValue}

	// ASC addition overflow, subtraction underflow, and exact-zero PRECEDING
	// retain the correct rows without wrapping or turning zero into underflow.
	check(t, asc, false, 1, currentToFollowing, 1, 2)
	check(t, asc, false, 0, precedingToCurrent, 0, 1)
	check(t, asc, false, 0, zeroPreceding, 0, 1)

	// DESC swaps the arithmetic direction but keeps the same frame invariant.
	check(t, desc, true, 1, currentToFollowing, 1, 2)
	check(t, desc, true, 0, precedingToCurrent, 0, 1)
	check(t, desc, true, 1, zeroPreceding, 1, 2)
}

func TestBuildRangeIntervalUnsignedBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("uint8", func(t *testing.T) {
		testBuildRangeIntervalUnsignedBoundaries(t, mp, types.T_uint8, uint8(math.MaxUint8))
	})
	t.Run("uint16", func(t *testing.T) {
		testBuildRangeIntervalUnsignedBoundaries(t, mp, types.T_uint16, uint16(math.MaxUint16))
	})
	t.Run("uint32", func(t *testing.T) {
		testBuildRangeIntervalUnsignedBoundaries(t, mp, types.T_uint32, uint32(math.MaxUint32))
	})
	t.Run("uint64", func(t *testing.T) {
		testBuildRangeIntervalUnsignedBoundaries(t, mp, types.T_uint64, uint64(math.MaxUint64))
	})
}

func signedRangeOffset(oid types.T, value int64) *plan.Expr {
	lit := &plan.Literal{}
	switch oid {
	case types.T_int8:
		lit.Value = &plan.Literal_I8Val{I8Val: int32(value)}
	case types.T_int16:
		lit.Value = &plan.Literal_I16Val{I16Val: int32(value)}
	case types.T_int32:
		lit.Value = &plan.Literal_I32Val{I32Val: int32(value)}
	case types.T_int64:
		lit.Value = &plan.Literal_I64Val{I64Val: value}
	default:
		panic("unsupported signed RANGE type")
	}
	return &plan.Expr{Expr: &plan.Expr_Lit{Lit: lit}}
}

func TestSignedRangeBound(t *testing.T) {
	for _, tc := range []struct {
		name        string
		value       int8
		offset      int8
		subtract    bool
		wantBound   int8
		wantAbove   bool
		wantInRange bool
	}{
		{name: "add", value: 1, offset: 1, wantBound: 2, wantInRange: true},
		{name: "subtract", value: 1, offset: 1, subtract: true, wantBound: 0, wantInRange: true},
		{name: "add maximum zero", value: math.MaxInt8, wantBound: math.MaxInt8, wantInRange: true},
		{name: "subtract minimum zero", value: math.MinInt8, subtract: true, wantBound: math.MinInt8, wantInRange: true},
		{name: "add overflow", value: math.MaxInt8, offset: 1, wantAbove: true},
		{name: "subtract underflow", value: math.MinInt8, offset: 1, subtract: true},
		{name: "add negative underflow", value: math.MinInt8, offset: -1},
		{name: "subtract negative overflow", value: math.MaxInt8, offset: -1, subtract: true, wantAbove: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bound, above, ok := signedRangeBound(tc.value, tc.offset, tc.subtract)
			require.Equal(t, tc.wantBound, bound)
			require.Equal(t, tc.wantAbove, above)
			require.Equal(t, tc.wantInRange, ok)
		})
	}

	require.Equal(t, 0, outOfDomainRangeBoundary(0, 3, false, false))
	require.Equal(t, 3, outOfDomainRangeBoundary(0, 3, true, false))
	require.Equal(t, 3, outOfDomainRangeBoundary(0, 3, false, true))
	require.Equal(t, 0, outOfDomainRangeBoundary(0, 3, true, true))
}

func testBuildRangeIntervalSignedBoundaries[T types.OrderedT](
	t *testing.T,
	mp *mpool.MPool,
	oid types.T,
	minValue T,
	maxValue T,
) {
	t.Helper()

	currentToFollowing := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  signedRangeOffset(oid, 1),
		},
	}
	precedingToCurrent := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  signedRangeOffset(oid, 1),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	check := func(t *testing.T, values []T, desc bool, row int, frame *plan.FrameClause, wantStart, wantEnd int) {
		t.Helper()
		vec := makeFixedVec(t, mp, oid, values)
		defer vec.Free(mp)
		ctr := &container{
			orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			desc:      []bool{desc},
		}
		start, end, err := ctr.buildRangeInterval(row, 0, len(values), frame)
		require.NoError(t, err)
		require.Equal(t, wantStart, start)
		require.Equal(t, wantEnd, end)
	}

	var zeroValue T
	asc := []T{minValue, zeroValue, maxValue}
	desc := []T{maxValue, zeroValue, minValue}

	// ASC addition overflow and subtraction underflow both retain the current row.
	check(t, asc, false, 2, currentToFollowing, 2, 3)
	check(t, asc, false, 0, precedingToCurrent, 0, 1)

	// DESC reverses the arithmetic direction while preserving the same frame invariant.
	check(t, desc, true, 2, currentToFollowing, 2, 3)
	check(t, desc, true, 0, precedingToCurrent, 0, 1)
}

func TestBuildRangeIntervalSignedBoundaries(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	t.Run("int8", func(t *testing.T) {
		testBuildRangeIntervalSignedBoundaries(t, mp, types.T_int8, int8(math.MinInt8), int8(math.MaxInt8))
	})
	t.Run("int16", func(t *testing.T) {
		testBuildRangeIntervalSignedBoundaries(t, mp, types.T_int16, int16(math.MinInt16), int16(math.MaxInt16))
	})
	t.Run("int32", func(t *testing.T) {
		testBuildRangeIntervalSignedBoundaries(t, mp, types.T_int32, int32(math.MinInt32), int32(math.MaxInt32))
	})
	t.Run("int64", func(t *testing.T) {
		testBuildRangeIntervalSignedBoundaries(t, mp, types.T_int64, int64(math.MinInt64), int64(math.MaxInt64))
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

func TestBuildRangeIntervalFloatNaNsUseSQLOrder(t *testing.T) {
	for _, tc := range []struct {
		name       string
		oid        types.T
		ascValues  []any
		descValues []any
		literal    func() *plan.Expr
	}{
		{
			name: "float32",
			oid:  types.T_float32,
			ascValues: []any{
				float32(1), float32(2), math.Float32frombits(0x7fc00001), math.Float32frombits(0x7fc00002),
			},
			descValues: []any{
				float32(2), float32(1), math.Float32frombits(0x7fc00001), math.Float32frombits(0x7fc00002),
			},
			literal: f32Lit,
		},
		{
			name: "float64",
			oid:  types.T_float64,
			ascValues: []any{
				float64(1), float64(2), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002),
			},
			descValues: []any{
				float64(2), float64(1), math.Float64frombits(0x7ff8000000000001), math.Float64frombits(0x7ff8000000000002),
			},
			literal: f64Lit,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

			for _, direction := range []struct {
				name   string
				values []any
				desc   bool
			}{
				{name: "asc", values: tc.ascValues},
				{name: "desc", values: tc.descValues, desc: true},
			} {
				t.Run(direction.name, func(t *testing.T) {
					vec := vector.NewVec(tc.oid.ToType())
					switch tc.oid {
					case types.T_float32:
						values := make([]float32, len(direction.values))
						for i, value := range direction.values {
							values[i] = value.(float32)
						}
						require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
					case types.T_float64:
						values := make([]float64, len(direction.values))
						for i, value := range direction.values {
							values[i] = value.(float64)
						}
						require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
					}
					defer vec.Free(mp)

					ctr := &container{
						orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
						desc:      []bool{direction.desc},
					}
					frame := &plan.FrameClause{
						Type: plan.FrameClause_RANGE,
						Start: &plan.FrameBound{
							Type: plan.FrameBound_PRECEDING,
							Val:  tc.literal(),
						},
						End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
					}

					// A finite row adjacent to the NaN peer group must still use
					// the SQL relation for the binary search.
					start, end, err := ctr.buildRangeInterval(1, 0, 4, frame)
					require.NoError(t, err)
					require.Equal(t, 0, start)
					require.Equal(t, 2, end)

					// All NaN payloads are one peer group, and an offset from a
					// NaN remains NaN for RANGE boundary purposes.
					start, end, err = ctr.buildRangeInterval(2, 0, 4, frame)
					require.NoError(t, err)
					require.Equal(t, 2, start)
					require.Equal(t, 4, end)
				})
			}
		})
	}
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

func TestWindowTimestampRangeUsesSessionTimeZone(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	precedingFrame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  intervalExpr(1, types.Hour),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	followingFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(1, types.Hour),
		},
	}
	for _, test := range []struct {
		name             string
		utcValues        []string
		rowIdx           int
		newYorkPreceding [2]int
		utcPreceding     [2]int
		newYorkFollowing [2]int
		utcFollowing     [2]int
	}{
		{
			name: "spring forward",
			utcValues: []string{
				"2024-03-10 06:59:59.999999",
				"2024-03-10 07:00:00.000000",
				"2024-03-10 07:30:00.000000",
			},
			rowIdx:           2,
			newYorkPreceding: [2]int{1, 3},
			utcPreceding:     [2]int{0, 3},
			newYorkFollowing: [2]int{0, 2},
			utcFollowing:     [2]int{0, 3},
		},
		{
			name: "fall back",
			utcValues: []string{
				"2024-11-03 05:30:00.000000",
				"2024-11-03 06:30:00.000000",
				"2024-11-03 07:00:00.000000",
			},
			rowIdx:           2,
			newYorkPreceding: [2]int{0, 3},
			utcPreceding:     [2]int{1, 3},
			newYorkFollowing: [2]int{0, 3},
			utcFollowing:     [2]int{0, 2},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Equal(t, int64(0), mp.CurrNB())
			}()

			values := make([]types.Timestamp, len(test.utcValues))
			for i, value := range test.utcValues {
				values[i], err = types.ParseTimestamp(time.UTC, value, 6)
				require.NoError(t, err)
			}
			vec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
			defer vec.Free(mp)

			ctr := &container{
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			}

			proc.GetSessionInfo().TimeZone = newYork
			start, end, err := ctr.buildInterval(proc, test.rowIdx, 0, vec.Length(), precedingFrame)
			require.NoError(t, err)
			require.Equal(t, test.newYorkPreceding, [2]int{start, end})

			// Reuse the same process/container generation after a session zone change.
			proc.GetSessionInfo().TimeZone = time.UTC
			start, end, err = ctr.buildInterval(proc, test.rowIdx, 0, vec.Length(), precedingFrame)
			require.NoError(t, err)
			require.Equal(t, test.utcPreceding, [2]int{start, end})

			proc.GetSessionInfo().TimeZone = newYork
			start, end, err = ctr.buildInterval(proc, 0, 0, vec.Length(), followingFrame)
			require.NoError(t, err)
			require.Equal(t, test.newYorkFollowing, [2]int{start, end})

			proc.GetSessionInfo().TimeZone = time.UTC
			start, end, err = ctr.buildInterval(proc, 0, 0, vec.Length(), followingFrame)
			require.NoError(t, err)
			require.Equal(t, test.utcFollowing, [2]int{start, end})
		})
	}
}

func TestWindowTimestampRangeFoldMembership(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	precedingFrame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  intervalExpr(30, types.Minute),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	followingFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(30, types.Minute),
		},
	}
	unboundedPrecedingFrame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type:      plan.FrameBound_PRECEDING,
			UnBounded: true,
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	unboundedFollowingFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type:      plan.FrameBound_FOLLOWING,
			UnBounded: true,
		},
	}

	utcValues := []string{
		"2024-11-03 05:00:00.000000", // 01:00 EDT
		"2024-11-03 05:30:00.000000", // 01:30 EDT
		"2024-11-03 05:59:00.000000", // 01:59 EDT
		"2024-11-03 06:00:00.000000", // 01:00 EST
		"2024-11-03 06:30:00.000000", // 01:30 EST
		"2024-11-03 06:59:00.000000", // 01:59 EST
		"2024-11-03 07:00:00.000000", // 02:00 EST
		"2024-11-03 07:30:00.000000", // 02:30 EST
	}
	values := make([]types.Timestamp, len(utcValues))
	for i, value := range utcValues {
		values[i], err = types.ParseTimestamp(time.UTC, value, 6)
		require.NoError(t, err)
	}

	selectionRows := func(left, right int, selection *timestampRangeSelection) []int {
		if selection == nil {
			rows := make([]int, 0, right-left)
			for row := left; row < right; row++ {
				rows = append(rows, row)
			}
			return rows
		}
		var rows []int
		for _, span := range selection.spans {
			for row := span.start; row < span.end; row++ {
				rows = append(rows, row)
			}
		}
		return rows
	}

	for _, test := range []struct {
		name     string
		desc     bool
		frame    *plan.FrameClause
		rowIdx   int
		wantRows []int
	}{
		{
			name:     "asc preceding excludes intervening repeated lower wall time",
			frame:    precedingFrame,
			rowIdx:   4,
			wantRows: []int{0, 1, 3, 4},
		},
		{
			name:     "asc following includes both repeated upper wall times",
			frame:    followingFrame,
			rowIdx:   1,
			wantRows: []int{1, 2, 4, 5, 6},
		},
		{
			name:     "asc unbounded preceding excludes later civil rows before the fold",
			frame:    unboundedPrecedingFrame,
			rowIdx:   4,
			wantRows: []int{0, 1, 3, 4},
		},
		{
			name:     "asc unbounded following excludes earlier civil rows after the fold",
			frame:    unboundedFollowingFrame,
			rowIdx:   1,
			wantRows: []int{1, 2, 4, 5, 6, 7},
		},
		{
			name:     "desc preceding includes both repeated upper wall times",
			desc:     true,
			frame:    precedingFrame,
			rowIdx:   3,
			wantRows: []int{1, 2, 3, 5, 6},
		},
		{
			name:     "desc following excludes intervening repeated higher wall time",
			desc:     true,
			frame:    followingFrame,
			rowIdx:   6,
			wantRows: []int{3, 4, 6, 7},
		},
		{
			name:     "desc unbounded preceding excludes later civil rows before the fold",
			desc:     true,
			frame:    unboundedPrecedingFrame,
			rowIdx:   3,
			wantRows: []int{0, 1, 2, 3, 5, 6},
		},
		{
			name:     "desc unbounded following excludes earlier civil rows after the fold",
			desc:     true,
			frame:    unboundedFollowingFrame,
			rowIdx:   6,
			wantRows: []int{3, 4, 6, 7},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Equal(t, int64(0), mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = newYork

			orderedValues := append([]types.Timestamp(nil), values...)
			if test.desc {
				for i := range orderedValues[:len(orderedValues)/2] {
					j := len(orderedValues) - 1 - i
					orderedValues[i], orderedValues[j] = orderedValues[j], orderedValues[i]
				}
			}
			vec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(vec, orderedValues, nil, mp))
			defer vec.Free(mp)

			ctr := &container{
				desc:      []bool{test.desc},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			}
			left, right, selection, err := ctr.buildIntervalRows(proc, test.rowIdx, 0, vec.Length(), test.frame)
			require.NoError(t, err)
			require.Equal(t, test.wantRows, selectionRows(left, right, selection))
		})
	}
}

func TestWindowTimestampRangeFoldMembershipDetectsSparseTransitions(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	currentRowFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	followingFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(30, types.Minute),
		},
	}

	for _, test := range []struct {
		name     string
		desc     bool
		utc      []string
		frame    *plan.FrameClause
		wantRows []int
	}{
		{
			name: "sparse increasing civil values cross fall-back transition",
			utc: []string{
				"2024-11-03 05:00:00.000000", // 01:00 EDT
				"2024-11-03 06:30:00.000000", // 01:30 EST
			},
			frame:    followingFrame,
			wantRows: []int{0, 1},
		},
		{
			name: "descending sparse civil values cross fall-back transition",
			desc: true,
			utc: []string{
				"2024-11-03 06:30:00.000000", // 01:30 EST
				"2024-11-03 05:00:00.000000", // 01:00 EDT
			},
			frame:    followingFrame,
			wantRows: []int{0, 1},
		},
		{
			name: "equal civil values cross fall-back transition",
			utc: []string{
				"2024-11-03 05:30:00.000000", // 01:30 EDT
				"2024-11-03 06:30:00.000000", // 01:30 EST
			},
			frame:    currentRowFrame,
			wantRows: []int{0, 1},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = newYork

			values := make([]types.Timestamp, len(test.utc))
			for i, value := range test.utc {
				values[i], err = types.ParseTimestamp(time.UTC, value, 6)
				require.NoError(t, err)
			}
			vec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
			defer vec.Free(mp)

			ctr := &container{
				desc:      []bool{test.desc},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}},
			}
			_, _, selection, err := ctr.buildIntervalRows(proc, 0, 0, vec.Length(), test.frame)
			require.NoError(t, err)
			require.NotNil(t, selection)
			var gotRows []int
			for _, span := range selection.spans {
				for row := span.start; row < span.end; row++ {
					gotRows = append(gotRows, row)
				}
			}
			require.Equal(t, test.wantRows, gotRows)
		})
	}
}

func TestWindowTimestampRangeFoldMembershipRefreshesMaterializedOrderVector(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = newYork

	source := vector.NewVec(types.T_timestamp.ToType())
	input := batch.NewWithSize(1)
	input.Vecs[0] = source
	defer input.Clean(mp)

	order, err := colexec.MakeEvalVector(proc, []*plan.Expr{newColExprWithType(0, types.T_timestamp.ToType())})
	require.NoError(t, err)
	ctr := &container{orderVecs: []colexec.ExprEvalVector{order}}
	defer ctr.freeExes()
	defer ctr.freeVector(mp)

	frame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  intervalExpr(30, types.Minute),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	appendBatch := func(values []string) {
		source.CleanOnlyData()
		for _, value := range values {
			ts, parseErr := types.ParseTimestamp(time.UTC, value, 6)
			require.NoError(t, parseErr)
			require.NoError(t, vector.AppendFixed(source, ts, false, mp))
		}
		input.SetRowCount(source.Length())
		require.NoError(t, ctr.evalOrderVector(input, proc))
	}

	// The first materialization has no fold and caches that fact against the
	// reusable order-vector pointer.
	appendBatch([]string{
		"2024-11-03 08:00:00.000000", "2024-11-03 08:30:00.000000",
		"2024-11-03 09:00:00.000000", "2024-11-03 09:30:00.000000",
		"2024-11-03 10:00:00.000000", "2024-11-03 10:30:00.000000",
		"2024-11-03 11:00:00.000000", "2024-11-03 11:30:00.000000",
	})
	materialized := ctr.orderVecs[0].Vec[0]
	_, _, selection, err := ctr.buildIntervalRows(proc, 4, 0, materialized.Length(), frame)
	require.NoError(t, err)
	require.Nil(t, selection)

	// evalOrderVector keeps the same materialized vector but replaces its data.
	// The second batch crosses the New York fall-back fold, so the cache must be
	// rebuilt and return its non-contiguous civil-time membership.
	appendBatch([]string{
		"2024-11-03 05:00:00.000000", "2024-11-03 05:30:00.000000",
		"2024-11-03 05:59:00.000000", "2024-11-03 06:00:00.000000",
		"2024-11-03 06:30:00.000000", "2024-11-03 06:59:00.000000",
		"2024-11-03 07:00:00.000000", "2024-11-03 07:30:00.000000",
	})
	require.Same(t, materialized, ctr.orderVecs[0].Vec[0])
	_, _, selection, err = ctr.buildIntervalRows(proc, 4, 0, materialized.Length(), frame)
	require.NoError(t, err)
	require.NotNil(t, selection)
	var rows []int
	for _, span := range selection.spans {
		for row := span.start; row < span.end; row++ {
			rows = append(rows, row)
		}
	}
	require.Equal(t, []int{0, 1, 3, 4}, rows)
}

func TestWindowTimestampRangeFoldIndexHonorsCancellation(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = newYork

	const rows = cancellationCheckInterval * 2
	start, err := types.ParseTimestamp(time.UTC, "2024-11-03 04:00:00.000000", 6)
	require.NoError(t, err)
	values := make([]types.Timestamp, rows)
	for i := range values {
		values[i] = start + types.Timestamp(int64(i)*60*types.MicroSecsPerSec)
	}
	vec := vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixedList(vec, values, nil, mp))
	defer vec.Free(mp)

	ctr := &container{orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{vec}}}}
	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	// The index checks at row 0 and then every cancellationCheckInterval rows.
	// Cancel on the second check to prove that a long initial span build can be
	// interrupted rather than only rejecting an already-canceled invocation.
	proc.Ctx = newCancelAfterDoneChecksContext(proc.Ctx, 2)
	_, _, _, err = ctr.buildIntervalRows(proc, rows-1, 0, rows, frame)
	require.ErrorIs(t, err, context.Canceled)
}

func TestWindowTimestampRangeFoldAggregateMembership(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer func() {
		proc.Free()
		require.Equal(t, int64(0), mp.CurrNB())
	}()
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = newYork

	orderValues := []string{
		"2024-11-03 05:00:00.000000",
		"2024-11-03 05:30:00.000000",
		"2024-11-03 05:59:00.000000",
		"2024-11-03 06:00:00.000000",
		"2024-11-03 06:30:00.000000",
		"2024-11-03 06:59:00.000000",
		"2024-11-03 07:00:00.000000",
		"2024-11-03 07:30:00.000000",
	}
	timestamps := make([]types.Timestamp, len(orderValues))
	for i, value := range orderValues {
		timestamps[i], err = types.ParseTimestamp(time.UTC, value, 6)
		require.NoError(t, err)
	}
	orderVec := vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixedList(orderVec, timestamps, nil, mp))
	defer orderVec.Free(mp)

	values := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil, mp)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = values
	bat.SetRowCount(values.Length())
	defer bat.Clean(mp)

	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(30, types.Minute),
		},
	}
	spec := makeWindowSpec()
	spec.GetW().Frame = frame
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	ctr := &container{
		bat:       bat,
		aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{values}}},
		orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{orderVec}}},
	}
	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{12, 23, 16, 12, 23, 16, 15, 8},
		vector.MustFixedColWithTypeCheck[int64](result))
	result.Free(mp)
}

func TestWindowTimestampRangeFoldAggregateMembershipHandlesConstOrderVector(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()

	timestamp, err := types.ParseTimestamp(time.UTC, "2024-11-03 05:30:00.000000", 6)
	require.NoError(t, err)
	orderVec, err := vector.NewConstFixed(types.T_timestamp.ToType(), timestamp, 4, mp)
	require.NoError(t, err)
	defer orderVec.Free(mp)

	values := testutil.MakeInt32Vector([]int32{1, 2, 4, 8}, nil, mp)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = values
	bat.SetRowCount(values.Length())
	defer bat.Clean(mp)

	spec := makeWindowSpec()
	spec.GetW().Frame = &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
	}
	ctr := &container{
		bat:       bat,
		aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{values}}},
		orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{orderVec}}},
	}
	result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{15, 15, 15, 15}, vector.MustFixedColWithTypeCheck[int64](result))
	result.Free(mp)

	// Const storage must still obey finite RANGE bounds instead of treating
	// every frame as its peer group.
	futureFrame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(1, types.Minute),
		},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(2, types.Minute),
		},
	}
	left, right, buildErr := ctr.buildInterval(proc, 0, 0, bat.RowCount(), futureFrame)
	require.NoError(t, buildErr)
	require.Equal(t, [2]int{bat.RowCount(), bat.RowCount()}, [2]int{left, right})
}

func TestWindowTimestampRangeFoldAggregateMembershipPreservesUnboundedNullPeers(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	unboundedPreceding := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	unboundedFollowing := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End:   &plan.FrameBound{Type: plan.FrameBound_FOLLOWING, UnBounded: true},
	}

	for _, test := range []struct {
		name      string
		desc      bool
		nullsLast bool
		utc       []string
		nulls     []bool
		values    []int32
		frame     *plan.FrameClause
		want      []int64
	}{
		{
			name:      "asc nulls first unbounded preceding",
			nullsLast: false,
			utc: []string{
				"2024-11-03 00:00:00.000000", // NULL
				"2024-11-03 05:00:00.000000", // 01:00 EDT
				"2024-11-03 05:30:00.000000", // 01:30 EDT
				"2024-11-03 06:00:00.000000", // 01:00 EST
				"2024-11-03 06:30:00.000000", // 01:30 EST
			},
			nulls:  []bool{true, false, false, false, false},
			values: []int32{100, 1, 2, 4, 8},
			frame:  unboundedPreceding,
			want:   []int64{100, 105, 115, 105, 115},
		},
		{
			name:      "asc nulls last unbounded following",
			nullsLast: true,
			utc: []string{
				"2024-11-03 05:00:00.000000", // 01:00 EDT
				"2024-11-03 05:30:00.000000", // 01:30 EDT
				"2024-11-03 06:00:00.000000", // 01:00 EST
				"2024-11-03 06:30:00.000000", // 01:30 EST
				"2024-11-03 00:00:00.000000", // NULL
			},
			nulls:  []bool{false, false, false, false, true},
			values: []int32{1, 2, 4, 8, 100},
			frame:  unboundedFollowing,
			want:   []int64{115, 110, 115, 110, 100},
		},
		{
			name:      "desc nulls first unbounded preceding",
			desc:      true,
			nullsLast: false,
			utc: []string{
				"2024-11-03 00:00:00.000000", // NULL
				"2024-11-03 06:30:00.000000", // 01:30 EST
				"2024-11-03 06:00:00.000000", // 01:00 EST
				"2024-11-03 05:30:00.000000", // 01:30 EDT
				"2024-11-03 05:00:00.000000", // 01:00 EDT
			},
			nulls:  []bool{true, false, false, false, false},
			values: []int32{100, 8, 4, 2, 1},
			frame:  unboundedPreceding,
			want:   []int64{100, 110, 115, 110, 115},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = newYork

			timestamps := make([]types.Timestamp, len(test.utc))
			for i, value := range test.utc {
				timestamps[i], err = types.ParseTimestamp(time.UTC, value, 6)
				require.NoError(t, err)
			}
			orderVec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(orderVec, timestamps, test.nulls, mp))
			defer orderVec.Free(mp)

			values := testutil.MakeInt32Vector(test.values, nil, mp)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = values
			bat.SetRowCount(values.Length())
			defer bat.Clean(mp)

			spec := makeWindowSpec()
			spec.GetW().Frame = test.frame
			arg := &Window{
				WinSpecList: []*plan.Expr{spec},
				Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
			}
			ctr := &container{
				desc:      []bool{test.desc},
				nullsLast: []bool{test.nullsLast},
				bat:       bat,
				aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{values}}},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{orderVec}}},
			}
			result, runErr := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
			require.NoError(t, runErr)
			require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[int64](result))
			result.Free(mp)
		})
	}
}

func TestWindowTimestampRangeFoldAggregateMembershipSmallPartitions(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	currentRowFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	precedingFrame := &plan.FrameClause{
		Type: plan.FrameClause_RANGE,
		Start: &plan.FrameBound{
			Type: plan.FrameBound_PRECEDING,
			Val:  intervalExpr(1, types.Hour),
		},
		End: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	followingFrame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(30, types.Minute),
		},
	}

	for _, test := range []struct {
		name  string
		utc   []string
		vals  []int32
		frame *plan.FrameClause
		want  []int64
	}{
		{
			name:  "repeated civil peer remains in preceding frame",
			utc:   []string{"2024-11-03 05:30:00.000000", "2024-11-03 06:30:00.000000", "2024-11-03 07:00:00.000000"},
			vals:  []int32{10, 20, 30},
			frame: precedingFrame,
			want:  []int64{30, 30, 60},
		},
		{
			name:  "sparse transition includes later civil boundary",
			utc:   []string{"2024-11-03 05:00:00.000000", "2024-11-03 06:30:00.000000"},
			vals:  []int32{1, 10},
			frame: followingFrame,
			want:  []int64{11, 10},
		},
		{
			name:  "equal civil timestamps are peers",
			utc:   []string{"2024-11-03 05:30:00.000000", "2024-11-03 06:30:00.000000"},
			vals:  []int32{1, 10},
			frame: currentRowFrame,
			want:  []int64{11, 11},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = newYork

			timestamps := make([]types.Timestamp, len(test.utc))
			for i, value := range test.utc {
				timestamps[i], err = types.ParseTimestamp(time.UTC, value, 6)
				require.NoError(t, err)
			}
			orderVec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(orderVec, timestamps, nil, mp))
			defer orderVec.Free(mp)

			values := testutil.MakeInt32Vector(test.vals, nil, mp)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = values
			bat.SetRowCount(values.Length())
			defer bat.Clean(mp)

			spec := makeWindowSpec()
			spec.GetW().Frame = test.frame
			arg := &Window{
				WinSpecList: []*plan.Expr{spec},
				Aggs:        []aggexec.AggFuncExecExpression{newAggExpr()},
			}
			ctr := &container{
				bat:       bat,
				aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{values}}},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{orderVec}}},
			}
			result, err := ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
			require.NoError(t, err)
			require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[int64](result))
			result.Free(mp)
		})
	}
}

func TestWindowTimestampRangeFoldAggregateMembershipAfterOrderMaterialization(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(30, types.Minute),
		},
	}

	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()
	proc.GetSessionInfo().TimeZone = newYork

	timestamps := make([]types.Timestamp, 2)
	for i, value := range []string{
		"2024-11-03 05:00:00.000000", // 01:00 EDT
		"2024-11-03 06:30:00.000000", // 01:30 EST
	} {
		timestamps[i], err = types.ParseTimestamp(time.UTC, value, 6)
		require.NoError(t, err)
	}
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], timestamps, nil, mp))
	bat.Vecs[1] = testutil.MakeInt32Vector([]int32{1, 10}, nil, mp)
	bat.SetRowCount(2)
	defer bat.Clean(mp)

	orderExpr := newColExprWithType(0, types.T_timestamp.ToType())
	spec := makeWindowSpec()
	spec.GetW().OrderBy = []*plan.OrderBySpec{{Expr: orderExpr}}
	spec.GetW().Frame = frame
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExprAt(1)},
	}
	require.NoError(t, arg.Prepare(proc))
	defer arg.Free(proc, false, nil)

	arg.ctr.bat = bat
	require.NoError(t, arg.ctr.evalAggVector(bat, proc))
	arg.Fs = makeOrderBy(spec)
	arg.ctr.orderVecs = make([]colexec.ExprEvalVector, len(arg.Fs))
	for i := range arg.Fs {
		arg.ctr.orderVecs[i], err = colexec.MakeEvalVector(proc, []*plan.Expr{arg.Fs[i].Expr})
		require.NoError(t, err)
	}
	_, err = arg.ctr.processOrder(0, arg, bat, proc)
	require.NoError(t, err)

	result, err := arg.ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{11, 10}, vector.MustFixedColWithTypeCheck[int64](result))
	result.Free(mp)
}

func TestWindowTimestampRangeFoldAggregateMembershipPreservesMultiKeyOrder(t *testing.T) {
	mp := mpool.MustNewZero()
	proc := testutil.NewProcessWithMPool(t, "", mp)
	defer func() {
		proc.Free()
		require.Zero(t, mp.CurrNB())
	}()

	// Multi-key RANGE frames use ctr.os to preserve the complete ORDER BY tuple
	// peer boundary. The last TIMESTAMP key repeats from B to A at the next k
	// group, which is a normal lexicographic reset rather than a timezone fold.
	// Keep this in UTC so the expected tuple semantics do not depend on DST.
	timestampA, err := types.ParseTimestamp(time.UTC, "2024-01-01 00:00:00.000000", 6)
	require.NoError(t, err)
	timestampB, err := types.ParseTimestamp(time.UTC, "2024-01-01 01:00:00.000000", 6)
	require.NoError(t, err)
	bat := batch.NewWithSize(3)
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1, 2, 2}, nil, mp)
	bat.Vecs[1] = vector.NewVec(types.T_timestamp.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], []types.Timestamp{
		timestampA, timestampB, timestampA, timestampB,
	}, nil, mp))
	bat.Vecs[2] = testutil.MakeInt32Vector([]int32{1, 2, 4, 8}, nil, mp)
	bat.SetRowCount(4)
	defer bat.Clean(mp)

	spec := makeWindowSpec()
	spec.GetW().OrderBy = []*plan.OrderBySpec{
		{Expr: newColExprWithType(0, types.T_int32.ToType())},
		{Expr: newColExprWithType(1, types.T_timestamp.ToType())},
	}
	spec.GetW().Frame = &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}
	arg := &Window{
		WinSpecList: []*plan.Expr{spec},
		Aggs:        []aggexec.AggFuncExecExpression{newAggExprAt(2)},
	}
	require.NoError(t, arg.Prepare(proc))
	defer arg.Free(proc, false, nil)

	arg.ctr.bat = bat
	require.NoError(t, arg.ctr.evalAggVector(bat, proc))
	arg.Fs = makeOrderBy(spec)
	arg.ctr.orderVecs = make([]colexec.ExprEvalVector, len(arg.Fs))
	for i := range arg.Fs {
		arg.ctr.orderVecs[i], err = colexec.MakeEvalVector(proc, []*plan.Expr{arg.Fs[i].Expr})
		require.NoError(t, err)
	}
	_, err = arg.ctr.processOrder(0, arg, bat, proc)
	require.NoError(t, err)

	result, err := arg.ctr.processAggregateFuncRange(0, arg, proc, 0, bat.RowCount())
	require.NoError(t, err)
	require.Equal(t, []int64{1, 3, 7, 15}, vector.MustFixedColWithTypeCheck[int64](result))
	result.Free(mp)
}

func TestWindowTimestampRangeFoldValueMembership(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)

	orderValues := []string{
		"2024-11-03 05:00:00.000000", // 01:00 EDT
		"2024-11-03 05:30:00.000000", // 01:30 EDT
		"2024-11-03 05:59:00.000000", // 01:59 EDT
		"2024-11-03 06:00:00.000000", // 01:00 EST
		"2024-11-03 06:30:00.000000", // 01:30 EST
		"2024-11-03 06:59:00.000000", // 01:59 EST
		"2024-11-03 07:00:00.000000", // 02:00 EST
		"2024-11-03 07:30:00.000000", // 02:30 EST
	}

	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
		End: &plan.FrameBound{
			Type: plan.FrameBound_FOLLOWING,
			Val:  intervalExpr(30, types.Minute),
		},
	}

	for _, test := range []struct {
		name      string
		want      []int32
		lastIsNil bool
	}{
		{name: "first_value", want: []int32{1, 2, 3, 1, 2, 3, 7, 8}},
		{name: "last_value", want: []int32{5, 7, 7, 5, 7, 7, 8, 8}},
		{name: "nth_value", want: []int32{2, 3, 6, 2, 3, 6, 8, 0}, lastIsNil: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(t, "", mp)
			defer func() {
				proc.Free()
				require.Zero(t, mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = newYork

			timestamps := make([]types.Timestamp, len(orderValues))
			for i, value := range orderValues {
				timestamps[i], err = types.ParseTimestamp(time.UTC, value, 6)
				require.NoError(t, err)
			}
			orderVec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(orderVec, timestamps, nil, mp))
			defer orderVec.Free(mp)

			values := testutil.MakeInt32Vector([]int32{1, 2, 3, 4, 5, 6, 7, 8}, nil, mp)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = values
			bat.SetRowCount(values.Length())
			defer bat.Clean(mp)

			spec := makeValueWindowSpecWithName(test.name, int32(types.T_int32))
			spec.GetW().Frame = frame
			arg := &Window{WinSpecList: []*plan.Expr{spec}}
			valueVecs := []*vector.Vector{values}
			var nthVec *vector.Vector
			if test.name == "nth_value" {
				nthVec = testutil.MakeInt32Vector([]int32{2, 2, 2, 2, 2, 2, 2, 2}, nil, mp)
				valueVecs = append(valueVecs, nthVec)
				defer nthVec.Free(mp)
			}
			ctr := &container{
				bat:       bat,
				aggVecs:   []colexec.ExprEvalVector{{Vec: valueVecs}},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{orderVec}}},
			}

			result, err := ctr.processValueFuncRange(0, arg, proc, 0, bat.RowCount())
			require.NoError(t, err)
			require.Equal(t, test.want, vector.MustFixedColWithTypeCheck[int32](result))
			if test.lastIsNil {
				require.True(t, result.IsNull(uint64(result.Length()-1)))
			}
			result.Free(mp)
		})
	}
}

// BenchmarkWindowTimestampRangeFoldUnboundedValue guards the value-function
// path that used to rescan a folded partition for every output row. Each size
// crosses the New York fall-back transition; the production first_value path
// must reuse its civil-time spans and only binary-search them per frame.
func BenchmarkWindowTimestampRangeFoldUnboundedValue(b *testing.B) {
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(b, err)
	start, err := types.ParseTimestamp(time.UTC, "2024-11-03 04:00:00.000000", 6)
	require.NoError(b, err)
	frame := &plan.FrameClause{
		Type:  plan.FrameClause_RANGE,
		Start: &plan.FrameBound{Type: plan.FrameBound_PRECEDING, UnBounded: true},
		End:   &plan.FrameBound{Type: plan.FrameBound_CURRENT_ROW},
	}

	for _, size := range []int{1000, 2000, 4000} {
		b.Run(fmt.Sprintf("rows=%d", size), func(b *testing.B) {
			mp := mpool.MustNewZero()
			proc := testutil.NewProcessWithMPool(b, "", mp)
			defer func() {
				proc.Free()
				require.Zero(b, mp.CurrNB())
			}()
			proc.GetSessionInfo().TimeZone = newYork

			timestamps := make([]types.Timestamp, size)
			for i := range timestamps {
				timestamps[i] = start + types.Timestamp(int64(i)*60*types.MicroSecsPerSec)
			}
			orderVec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(b, vector.AppendFixedList(orderVec, timestamps, nil, mp))
			defer orderVec.Free(mp)

			values := make([]int32, size)
			for i := range values {
				values[i] = int32(i)
			}
			valueVec := testutil.MakeInt32Vector(values, nil, mp)
			bat := batch.NewWithSize(1)
			bat.Vecs[0] = valueVec
			bat.SetRowCount(size)
			defer bat.Clean(mp)

			spec := makeValueWindowSpecWithName("first_value", int32(types.T_int32))
			spec.GetW().Frame = frame
			arg := &Window{WinSpecList: []*plan.Expr{spec}}
			ctr := &container{
				bat:       bat,
				aggVecs:   []colexec.ExprEvalVector{{Vec: []*vector.Vector{valueVec}}},
				orderVecs: []colexec.ExprEvalVector{{Vec: []*vector.Vector{orderVec}}},
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				result, runErr := ctr.processValueFuncRange(0, arg, proc, 0, size)
				require.NoError(b, runErr)
				result.Free(mp)
			}
		})
	}
}

func TestSearchLeftRightTemporalRangeOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Equal(t, int64(0), mp.CurrNB()) }()

	tests := []struct {
		name                string
		ascending           []string
		descending          []string
		minimumAsc          []string
		minimumDesc         []string
		aboveDomainBy       int64
		aboveDomainUnit     types.IntervalType
		belowDomainBy       int64
		belowDomainUnit     types.IntervalType
		invalidIntervalUnit types.IntervalType
		newVector           func([]string) *vector.Vector
	}{
		{
			name:                "date",
			ascending:           []string{"9999-12-30", "9999-12-31"},
			descending:          []string{"9999-12-31", "9999-12-30"},
			minimumAsc:          []string{"0001-01-01", "0001-01-02"},
			minimumDesc:         []string{"0001-01-02", "0001-01-01"},
			aboveDomainBy:       1,
			aboveDomainUnit:     types.Year,
			belowDomainBy:       2,
			belowDomainUnit:     types.Year,
			invalidIntervalUnit: types.Year,
			newVector: func(values []string) *vector.Vector {
				return testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil, values)
			},
		},
		{
			name:                "datetime",
			ascending:           []string{"9999-12-30 23:59:59.999999", "9999-12-31 23:59:59.999999"},
			descending:          []string{"9999-12-31 23:59:59.999999", "9999-12-30 23:59:59.999999"},
			minimumAsc:          []string{"0001-01-01 00:00:00.000000", "0001-01-02 00:00:00.000000"},
			minimumDesc:         []string{"0001-01-02 00:00:00.000000", "0001-01-01 00:00:00.000000"},
			aboveDomainBy:       1,
			aboveDomainUnit:     types.Year,
			belowDomainBy:       1,
			belowDomainUnit:     types.Year,
			invalidIntervalUnit: types.Year,
			newVector: func(values []string) *vector.Vector {
				return testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil, values)
			},
		},
		{
			name:                "time",
			ascending:           []string{"2562047787:59:59.999998", "2562047787:59:59.999999"},
			descending:          []string{"2562047787:59:59.999999", "2562047787:59:59.999998"},
			minimumAsc:          []string{"-2562047787:59:59.999999", "-2562047787:59:59.999998"},
			minimumDesc:         []string{"-2562047787:59:59.999998", "-2562047787:59:59.999999"},
			aboveDomainBy:       1,
			aboveDomainUnit:     types.MicroSecond,
			belowDomainBy:       1,
			belowDomainUnit:     types.MicroSecond,
			invalidIntervalUnit: types.Hour,
			newVector: func(values []string) *vector.Vector {
				return testutil.NewTimeVector(0, types.T_time.ToType(), mp, false, nil, values)
			},
		},
		{
			name:                "timestamp",
			ascending:           []string{"9999-12-30 23:59:59.999999", "9999-12-31 23:59:59.999999"},
			descending:          []string{"9999-12-31 23:59:59.999999", "9999-12-30 23:59:59.999999"},
			minimumAsc:          []string{"0001-01-01 00:00:00.000000", "0001-01-02 00:00:00.000000"},
			minimumDesc:         []string{"0001-01-02 00:00:00.000000", "0001-01-01 00:00:00.000000"},
			aboveDomainBy:       1,
			aboveDomainUnit:     types.Day,
			belowDomainBy:       1,
			belowDomainUnit:     types.Day,
			invalidIntervalUnit: types.Year,
			newVector: func(values []string) *vector.Vector {
				return testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil, values)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+"_asc", func(t *testing.T) {
			vec := tt.newVector(tt.ascending)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(tt.aboveDomainBy, tt.aboveDomainUnit)
			left, err := searchLeft(0, vec.Length(), 1, vec, expr, true, false)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), left)
			right, err := searchRight(0, vec.Length(), 1, vec, expr, false, false)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), right)
		})

		t.Run(tt.name+"_desc", func(t *testing.T) {
			vec := tt.newVector(tt.descending)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(tt.aboveDomainBy, tt.aboveDomainUnit)
			left, err := searchLeft(0, vec.Length(), 0, vec, expr, false, true)
			require.NoError(t, err)
			require.Equal(t, 0, left)
			right, err := searchRight(0, vec.Length(), 0, vec, expr, true, true)
			require.NoError(t, err)
			require.Equal(t, 0, right)
		})

		t.Run(tt.name+"_below_domain_asc", func(t *testing.T) {
			vec := tt.newVector(tt.minimumAsc)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(tt.belowDomainBy, tt.belowDomainUnit)
			left, err := searchLeft(0, vec.Length(), 0, vec, expr, false, false)
			require.NoError(t, err)
			require.Equal(t, 0, left)
			right, err := searchRight(0, vec.Length(), 0, vec, expr, true, false)
			require.NoError(t, err)
			require.Equal(t, 0, right)
		})

		t.Run(tt.name+"_below_domain_desc", func(t *testing.T) {
			vec := tt.newVector(tt.minimumDesc)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(tt.belowDomainBy, tt.belowDomainUnit)
			left, err := searchLeft(0, vec.Length(), 1, vec, expr, true, true)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), left)
			right, err := searchRight(0, vec.Length(), 1, vec, expr, false, true)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), right)
		})
	}

	for _, tt := range tests {
		t.Run(tt.name+"_negative_add_below_domain_asc", func(t *testing.T) {
			vec := tt.newVector(tt.minimumAsc)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(-tt.belowDomainBy, tt.belowDomainUnit)
			left, err := searchLeft(0, vec.Length(), 0, vec, expr, true, false)
			require.NoError(t, err)
			require.Equal(t, 0, left)
			right, err := searchRight(0, vec.Length(), 0, vec, expr, false, false)
			require.NoError(t, err)
			require.Equal(t, 0, right)
		})

		t.Run(tt.name+"_negative_sub_above_domain_asc", func(t *testing.T) {
			vec := tt.newVector(tt.ascending)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(-tt.aboveDomainBy, tt.aboveDomainUnit)
			left, err := searchLeft(0, vec.Length(), 1, vec, expr, false, false)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), left)
			right, err := searchRight(0, vec.Length(), 1, vec, expr, true, false)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), right)
		})

		t.Run(tt.name+"_negative_add_below_domain_desc", func(t *testing.T) {
			vec := tt.newVector(tt.minimumDesc)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(-tt.belowDomainBy, tt.belowDomainUnit)
			left, err := searchLeft(0, vec.Length(), 1, vec, expr, false, true)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), left)
			right, err := searchRight(0, vec.Length(), 1, vec, expr, true, true)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), right)
		})

		t.Run(tt.name+"_negative_sub_above_domain_desc", func(t *testing.T) {
			vec := tt.newVector(tt.descending)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(-tt.aboveDomainBy, tt.aboveDomainUnit)
			left, err := searchLeft(0, vec.Length(), 0, vec, expr, true, true)
			require.NoError(t, err)
			require.Equal(t, 0, left)
			right, err := searchRight(0, vec.Length(), 0, vec, expr, false, true)
			require.NoError(t, err)
			require.Equal(t, 0, right)
		})
	}

	for _, tt := range tests {
		t.Run(tt.name+"_invalid_interval_magnitude", func(t *testing.T) {
			vec := tt.newVector(tt.ascending)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			_, err := searchRight(0, vec.Length(), 1, vec, intervalExpr(math.MaxInt64, tt.invalidIntervalUnit), false, false)
			require.Error(t, err, "an invalid interval magnitude must not be treated as a domain boundary")
			_, err = searchRight(0, vec.Length(), 1, vec, intervalExpr(math.MinInt64, tt.invalidIntervalUnit), false, false)
			require.Error(t, err, "an invalid negative interval magnitude must not be treated as a domain boundary")
		})
	}

	for _, tt := range tests {
		t.Run(tt.name+"_max_microsecond_above_domain", func(t *testing.T) {
			vec := tt.newVector(tt.ascending)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(math.MaxInt64, types.MicroSecond)
			left, err := searchLeft(0, vec.Length(), 1, vec, expr, true, false)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), left)
			right, err := searchRight(0, vec.Length(), 1, vec, expr, false, false)
			require.NoError(t, err)
			require.Equal(t, vec.Length(), right)
		})

		t.Run(tt.name+"_max_microsecond_below_domain", func(t *testing.T) {
			vec := tt.newVector(tt.minimumAsc)
			require.NotNil(t, vec)
			defer vec.Free(mp)

			expr := intervalExpr(math.MaxInt64, types.MicroSecond)
			left, err := searchLeft(0, vec.Length(), 0, vec, expr, false, false)
			require.NoError(t, err)
			require.Equal(t, 0, left)
			right, err := searchRight(0, vec.Length(), 0, vec, expr, true, false)
			require.NoError(t, err)
			require.Equal(t, 0, right)
		})
	}
}

func TestTimestampRangeMicrosecondDSTGapBoundary(t *testing.T) {
	loc, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	beforeGap, err := types.ParseTimestamp(loc, "2024-03-10 01:59:59.999999", 6)
	require.NoError(t, err)
	gapEnd, err := types.ParseTimestamp(loc, "2024-03-10 03:00:00.000000", 6)
	require.NoError(t, err)

	add, err := doTimestampAdd(loc, beforeGap, 1, int64(types.MicroSecond))
	require.NoError(t, err)
	require.Equal(t, gapEnd, add)
	sub, err := doTimestampSub(loc, gapEnd, 1, int64(types.MicroSecond))
	require.NoError(t, err)
	require.Equal(t, gapEnd, sub)

	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()
	for _, tc := range []struct {
		name      string
		values    []types.Timestamp
		addRow    int
		subRow    int
		desc      bool
		wantLeft  int
		wantRight int
	}{
		{"asc", []types.Timestamp{beforeGap, gapEnd}, 0, 1, false, 1, 2},
		{"desc", []types.Timestamp{gapEnd, beforeGap}, 1, 0, true, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vec := vector.NewVec(types.T_timestamp.ToType())
			require.NoError(t, vector.AppendFixedList(vec, tc.values, nil, mp))
			defer vec.Free(mp)

			expr := intervalExpr(1, types.MicroSecond)
			leftAdd, rightAdd := true, false
			leftSub, rightSub := false, true
			if tc.desc {
				leftAdd, rightAdd = false, true
				leftSub, rightSub = true, false
			}
			left, err := searchLeftWithLocation(loc, 0, vec.Length(), tc.addRow, vec, expr, leftAdd, tc.desc)
			require.NoError(t, err)
			require.Equal(t, tc.wantLeft, left)
			right, err := searchRightWithLocation(loc, 0, vec.Length(), tc.addRow, vec, expr, rightAdd, tc.desc)
			require.NoError(t, err)
			require.Equal(t, tc.wantRight, right)

			left, err = searchLeftWithLocation(loc, 0, vec.Length(), tc.subRow, vec, expr, leftSub, tc.desc)
			require.NoError(t, err)
			require.Equal(t, tc.wantLeft, left)
			right, err = searchRightWithLocation(loc, 0, vec.Length(), tc.subRow, vec, expr, rightSub, tc.desc)
			require.NoError(t, err)
			require.Equal(t, tc.wantRight, right)
		})
	}
}

func TestTemporalRangeFixedUnitConversionOverflow(t *testing.T) {
	const magnitude = int64(307445734562)
	date := types.DateFromCalendar(2024, 1, 1)
	datetime := types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0)
	timestamp := datetime.ToTimestamp(time.UTC)
	timeValue, err := types.ParseTime("12:00:00", 6)
	require.NoError(t, err)

	for _, unit := range []types.IntervalType{types.Minute, types.Hour, types.Day, types.Week} {
		for _, diff := range []int64{magnitude, -magnitude} {
			t.Run(fmt.Sprintf("%s_%d", unit, diff), func(t *testing.T) {
				for _, call := range []func() error{
					func() error { _, err := doDateAdd(date, diff, int64(unit)); return err },
					func() error { _, err := doDateSub(date, diff, int64(unit)); return err },
					func() error { _, err := doTimeAdd(timeValue, diff, int64(unit)); return err },
					func() error { _, err := doTimeSub(timeValue, diff, int64(unit)); return err },
					func() error { _, err := doDatetimeAdd(datetime, diff, int64(unit)); return err },
					func() error { _, err := doDatetimeSub(datetime, diff, int64(unit)); return err },
					func() error { _, err := doTimestampAdd(time.UTC, timestamp, diff, int64(unit)); return err },
					func() error { _, err := doTimestampSub(time.UTC, timestamp, diff, int64(unit)); return err },
				} {
					err := call()
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "fixed-unit conversion overflow must become a temporal domain boundary")
				}
			})
		}
	}
}

func TestTemporalRangeCalendarIntervalOverflow(t *testing.T) {
	mp := mpool.MustNewZero()
	defer func() { require.Zero(t, mp.CurrNB()) }()

	tests := []struct {
		name       string
		ascending  []string
		descending []string
		newVector  func([]string) *vector.Vector
		add        func(int64, int64) error
		sub        func(int64, int64) error
	}{
		{
			name:       "date",
			ascending:  []string{"2024-01-01", "2024-01-02"},
			descending: []string{"2024-01-02", "2024-01-01"},
			newVector: func(values []string) *vector.Vector {
				return testutil.NewDateVector(0, types.T_date.ToType(), mp, false, nil, values)
			},
			add: func(diff, unit int64) error {
				_, err := doDateAdd(types.DateFromCalendar(2024, 1, 1), diff, unit)
				return err
			},
			sub: func(diff, unit int64) error {
				_, err := doDateSub(types.DateFromCalendar(2024, 1, 1), diff, unit)
				return err
			},
		},
		{
			name:       "datetime",
			ascending:  []string{"2024-01-01 00:00:00", "2024-01-02 00:00:00"},
			descending: []string{"2024-01-02 00:00:00", "2024-01-01 00:00:00"},
			newVector: func(values []string) *vector.Vector {
				return testutil.NewDatetimeVector(0, types.T_datetime.ToType(), mp, false, nil, values)
			},
			add: func(diff, unit int64) error {
				_, err := doDatetimeAdd(types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0), diff, unit)
				return err
			},
			sub: func(diff, unit int64) error {
				_, err := doDatetimeSub(types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0), diff, unit)
				return err
			},
		},
		{
			name:       "timestamp",
			ascending:  []string{"2024-01-01 00:00:00", "2024-01-02 00:00:00"},
			descending: []string{"2024-01-02 00:00:00", "2024-01-01 00:00:00"},
			newVector: func(values []string) *vector.Vector {
				return testutil.NewTimestampVector(0, types.T_timestamp.ToType(), mp, false, nil, values)
			},
			add: func(diff, unit int64) error {
				start := types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0).ToTimestamp(time.UTC)
				_, err := doTimestampAdd(time.UTC, start, diff, unit)
				return err
			},
			sub: func(diff, unit int64) error {
				start := types.DatetimeFromClock(2024, 1, 1, 0, 0, 0, 0).ToTimestamp(time.UTC)
				_, err := doTimestampSub(time.UTC, start, diff, unit)
				return err
			},
		},
	}

	for _, tc := range tests {
		for _, interval := range []struct {
			unit types.IntervalType
			diff int64
		}{
			{types.Month, 12 * (1 << 32)},
			{types.Quarter, 4 * (1 << 32)},
			{types.Year, 1 << 32},
		} {
			t.Run(fmt.Sprintf("%s_%s", tc.name, interval.unit), func(t *testing.T) {
				require.True(t, moerr.IsMoErrCode(tc.add(interval.diff, int64(interval.unit)), moerr.ErrOutOfRange))
				require.True(t, moerr.IsMoErrCode(tc.sub(interval.diff, int64(interval.unit)), moerr.ErrOutOfRange))

				expr := intervalExpr(interval.diff, interval.unit)
				for _, order := range []struct {
					name   string
					values []string
					desc   bool
				}{
					{name: "asc", values: tc.ascending},
					{name: "desc", values: tc.descending, desc: true},
				} {
					t.Run(order.name, func(t *testing.T) {
						vec := tc.newVector(order.values)
						defer vec.Free(mp)

						for _, operation := range []struct {
							name         string
							add          bool
							wantBoundary int
						}{
							{name: "add", add: true, wantBoundary: temporalRangeOverflowBoundary(0, vec.Length(), true, order.desc)},
							{name: "sub", wantBoundary: temporalRangeOverflowBoundary(0, vec.Length(), false, order.desc)},
						} {
							t.Run(operation.name, func(t *testing.T) {
								// RANGE bounds are expressed in sort order, while the
								// search helpers flip their arithmetic flag for DESC.
								left, err := searchLeft(0, vec.Length(), 0, vec, expr, operation.add != order.desc, order.desc)
								require.NoError(t, err)
								require.Equal(t, operation.wantBoundary, left)

								right, err := searchRight(0, vec.Length(), 0, vec, expr, !operation.add != order.desc, order.desc)
								require.NoError(t, err)
								require.Equal(t, operation.wantBoundary, right)
							})
						}
					})
				}
			})
		}
	}
}
