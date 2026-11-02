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

package colexec

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeferredJoinConstantDiagnosticActivationAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetBaseProcessRunningStatus(true)
	session := &preparedCastWarningSession{}
	proc.Session = session
	sourceType, targetType := types.T_text.ToType(), types.T_float64.ToType()
	fn, err := function.GetFunctionByName(proc.Ctx, "cast", []types.Type{sourceType, targetType})
	require.NoError(t, err)
	expr := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: fn.GetEncodedOverloadID(), ObjName: "cast"},
		Args: []*plan.Expr{
			{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "12suffix"}}}},
			{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
		},
	}}}
	owner := new(DeferredJoinDiagnostic)
	owner.Prepare(proc)
	executors, err := NewJoinBuildExpressionExecutors(proc, []*plan.Expr{expr}, nil, owner)
	require.NoError(t, err)
	executor := executors[0]
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(3)

	// Empty probe: HashBuild's key calculation must not publish its warning.
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Zero(t, session.warningCount)
	owner.Reset()
	require.Zero(t, session.warningCount)

	// A new execution only reports the selected constant once, even if the
	// build key is reused for several batches.
	executor.ResetForNextQuery()
	owner.Prepare(proc)
	_, err = executor.Eval(proc, []*batch.Batch{input}, []bool{false, false, false})
	require.NoError(t, err)
	require.Zero(t, session.warningCount)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	_, err = executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Zero(t, session.warningCount)
	require.NoError(t, owner.Activate(proc))
	require.NoError(t, owner.Activate(proc))
	require.Equal(t, 1, session.warningCount)
	owner.Reset()
}

func TestDeferredJoinConstantSemanticErrorActivation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	owner := new(DeferredJoinDiagnostic)
	owner.Prepare(proc)
	executor := &deferredJoinConstantExecutor{
		executor: &failingExpressionExecutor{},
		owner:    owner,
		typ:      types.T_int64.ToType(),
	}
	defer executor.Free()
	input := batch.New(nil)
	input.SetRowCount(1)
	value, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.True(t, value.IsNull(0))
	require.ErrorContains(t, owner.Activate(proc), "unexpected branch evaluation")
	owner.Reset()
}

func TestDeferredJoinErrorClasses(t *testing.T) {
	require.True(t, deferJoinExpressionError(moerr.NewWrongArguments(context.Background(), "PERIOD_DIFF")))
	require.True(t, deferJoinExpressionError(moerr.NewInvalidInputNoCtx("invalid time")))
	require.False(t, deferJoinExpressionError(context.Canceled))
	require.False(t, deferJoinExpressionError(context.DeadlineExceeded))
	require.False(t, deferJoinExpressionError(moerr.NewInternalErrorNoCtx("build failed")))
	require.False(t, deferJoinExpressionError(moerr.NewOOMNoCtx()))
}

func TestDeferredJoinDiagnosticResetDiscardsLateBuildWarnings(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	session := &preparedCastWarningSession{}
	proc.Session = session
	owner := new(DeferredJoinDiagnostic)
	owner.Prepare(proc)
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		for i := 0; i < 100; i++ {
			owner.AppendWarningDiagnostic(1292, "truncated")
		}
	}()
	<-started
	owner.Reset()
	<-done
	owner.Prepare(proc)
	require.NoError(t, owner.Activate(proc))
	require.Zero(t, session.warningCount)
	owner.Reset()
}

func TestDeferredJoinTemporalWarningActivation(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.SetBaseProcessRunningStatus(true)
	sink := &preparedCastWarningSession{}
	proc.WarningSink = sink
	src := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "bad"}}}}
	expr := bindTestCast(t, proc, 0, src, types.T_time.ToType())
	owner := new(DeferredJoinDiagnostic)
	owner.Prepare(proc)
	execs, err := NewJoinBuildExpressionExecutors(proc, []*plan.Expr{expr}, nil, owner)
	require.NoError(t, err)
	defer execs[0].Free()
	b := batch.New(nil)
	b.SetRowCount(2)
	_, err = execs[0].Eval(proc, []*batch.Batch{b}, nil)
	require.NoError(t, err)
	require.Zero(t, sink.warningCount)
	owner.Reset()
	require.Zero(t, sink.warningCount)
	execs[0].ResetForNextQuery()
	owner.Prepare(proc)
	_, err = execs[0].Eval(proc, []*batch.Batch{b}, nil)
	require.NoError(t, err)
	require.Zero(t, sink.warningCount)
	require.NoError(t, owner.Activate(proc))
	require.NoError(t, owner.Activate(proc))
	require.Equal(t, 1, sink.warningCount)
	owner.Reset()
}

func TestJoinProbeDiagnosticScopeAcrossSiblings(t *testing.T) {
	for _, guardedFirst := range []bool{true, false} {
		name := "guarded-first"
		if !guardedFirst {
			name = "guarded-last"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t, testutil.WithFileService(nil))
			defer proc.Free()
			checkExpressionStorageAfterCleanup(t, proc)
			proc.SetBaseProcessRunningStatus(true)
			sink := &preparedCastWarningSession{}
			proc.WarningSink = sink
			source := &plan.Expr{Typ: types.T_varchar.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: "bad"}}}}
			bad := bindTestFunction(t, proc, "date", source)
			flag := &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			null := &plan.Expr{Typ: types.T_date.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
			guarded := bindTestFunction(t, proc, "if", flag, bad, null)
			left, right := guarded, bad
			if !guardedFirst {
				left, right = right, left
			}
			expr := bindTestFunction(t, proc, "=", left, right)
			owner := new(DeferredJoinDiagnostic)
			owner.Prepare(proc)
			defer owner.Reset()
			execs, activation, err := NewJoinProbeExpressionExecutors(proc, []*plan.Expr{expr}, nil, owner)
			require.NoError(t, err)
			defer execs[0].Free()
			require.Len(t, activation, 1, "only the unconditional sibling activates before the residual")
			b := batch.NewWithSize(1)
			defer b.Clean(proc.Mp())
			b.Vecs[0] = testutil.MakeBoolVector([]bool{false, false}, nil, proc.Mp())
			b.SetRowCount(2)
			for execution := 0; execution < 2; execution++ {
				if execution > 0 {
					execs[0].ResetForNextQuery()
					owner.Reset()
					owner.Prepare(proc)
					require.NoError(t, vector.SetFixedAtNoTypeCheck(b.Vecs[0], 0, false))
				}
				_, err = activation[0].Eval(proc, nil, nil)
				require.NoError(t, err)
				require.Equal(t, execution*2, sink.warningCount, "activation remains captured until both join sides exist")
				require.NoError(t, owner.Activate(proc))
				result, err := execs[0].Eval(proc, []*batch.Batch{b}, nil)
				require.NoError(t, err)
				require.True(t, result.IsNull(0))
				require.True(t, result.IsNull(1))
				require.Equal(t, execution*2+1, sink.warningCount, "the false branch contributes no warning")
				require.NoError(t, vector.SetFixedAtNoTypeCheck(b.Vecs[0], 0, true))
				for repeat := 0; repeat < 2; repeat++ {
					_, err = execs[0].Eval(proc, []*batch.Batch{b}, nil)
					require.NoError(t, err)
					require.Equal(t, execution*2+2, sink.warningCount, "the selected constant is reported once per execution")
				}
			}
		})
	}
}
