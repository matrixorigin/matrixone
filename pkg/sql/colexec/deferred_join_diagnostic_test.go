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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
