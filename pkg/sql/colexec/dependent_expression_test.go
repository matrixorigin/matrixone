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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func dependentInt(v int64) *plan.Expr {
	return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: v}}}}
}

func dependentBool(v bool) *plan.Expr {
	return &plan.Expr{Typ: types.T_bool.ToType().PlanType(), Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: v}}}}
}

func dependentCall(id uint32) *plan.Expr {
	return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Dependent{Dependent: &plan.DependentExpr{ProgramId: id, OccurrenceId: id, StageId: 1, Reducer: plan.SubqueryRef_SCALAR}}}
}

func dependentFunction(t *testing.T, proc *process.Process, name string, args ...*plan.Expr) *plan.Expr {
	t.Helper()
	typs := make([]types.Type, len(args))
	for i := range args {
		var err error
		typs[i], err = types.TypeFromPlan(args[i].Typ)
		require.NoError(t, err)
	}
	f, err := function.GetFunctionByName(proc.Ctx, name, typs)
	require.NoError(t, err)
	return &plan.Expr{Typ: f.GetReturnType().PlanType(), Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: name}, Args: args}}}
}

type dependentTestFactory struct {
	t          *testing.T
	allocation *vector.AllocationAccountSelection
	opened     []uint32
	closed     []uint32
	fail       map[uint32]error
	closeErr   error
}

func (f *dependentTestFactory) Open(proc *process.Process, call *plan.DependentExpr, inputs, lhs []*vector.Vector) (DependentInvocation, error) {
	f.opened = append(f.opened, call.ProgramId)
	if err := f.fail[call.ProgramId]; err != nil {
		return nil, err
	}
	value := int64(call.ProgramId)
	if len(inputs) > 0 {
		value = vector.GetFixedAtNoTypeCheck[int64](inputs[0], 0)
	}
	v, err := vector.NewOffHeapVecWithTypeAndAllocation(types.T_int64.ToType(), f.allocation)
	if err != nil {
		return nil, err
	}
	if err = vector.AppendFixed(v, value, false, proc.Mp()); err != nil {
		v.Free(proc.Mp())
		return nil, err
	}
	b := batch.NewWithSize(1)
	b.Vecs[0] = v
	b.SetRowCount(1)
	return &dependentTestInvocation{factory: f, proc: proc, id: call.ProgramId, result: b}, nil
}

type dependentTestInvocation struct {
	factory *dependentTestFactory
	proc    *process.Process
	id      uint32
	result  *batch.Batch
	step    int
}

func (i *dependentTestInvocation) Step(context.Context) (vm.ContinuationStep, error) {
	i.step++
	return vm.ContinuationStep{Done: i.step == 2}, nil
}
func (i *dependentTestInvocation) Accept(vm.Continuation) error { return nil }
func (i *dependentTestInvocation) Result() *batch.Batch         { return i.result }
func (i *dependentTestInvocation) Close(error) error {
	i.factory.closed = append(i.factory.closed, i.id)
	i.result.Clean(i.proc.Mp())
	i.result = nil
	return i.factory.closeErr
}

func TestDependentExpressionDemandAndResume(t *testing.T) {
	for _, name := range []string{"case", "iff", "coalesce"} {
		t.Run(name, func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			bad := dependentCall(99)
			var expr *plan.Expr
			if name == "coalesce" {
				null := dependentInt(0)
				null.GetLit().Isnull = true
				expr = dependentFunction(t, proc, name, null, dependentCall(7), bad)
			} else {
				expr = dependentFunction(t, proc, name, dependentBool(false), bad, dependentCall(7))
			}
			compiled, err := CompileDependentExpressions(proc.Ctx, []*plan.Expr{expr})
			require.NoError(t, err)
			factory := &dependentTestFactory{t: t, allocation: allocation, fail: map[uint32]error{99: errors.New("unselected scalar has two rows")}}
			frame, err := compiled.NewFrame(proc, nil, factory, allocation)
			require.NoError(t, err)
			defer frame.Close()
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			root, err := frame.Task(0)
			require.NoError(t, err)
			require.NoError(t, vm.RunContinuation(proc.Ctx, budget, root, nil))
			value, err := frame.Value(0)
			require.NoError(t, err)
			require.Equal(t, int64(7), vector.GetFixedAtNoTypeCheck[int64](value, 0))
			require.Equal(t, []uint32{7}, factory.opened)
			require.Equal(t, []uint32{7}, factory.closed)
			frame.Close()
			require.Zero(t, budget.Used())
		})
	}
}

func TestDependentExpressionMemoAndOccurrence(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	first, alias, separate := dependentCall(3), dependentCall(3), dependentCall(3)
	first.AuxId, alias.AuxId = -1, -1
	compiled, err := CompileDependentExpressions(proc.Ctx, []*plan.Expr{first, alias, separate})
	require.NoError(t, err)
	factory := &dependentTestFactory{t: t, allocation: allocation}
	frame, err := compiled.NewFrame(proc, nil, factory, allocation)
	require.NoError(t, err)
	defer frame.Close()
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	for i := range 3 {
		task, err := frame.Task(i)
		require.NoError(t, err)
		require.NoError(t, vm.RunContinuation(proc.Ctx, budget, task, nil))
		value, err := frame.Value(i)
		require.NoError(t, err)
		require.Equal(t, int64(3), vector.GetFixedAtNoTypeCheck[int64](value, 0))
	}
	require.Equal(t, []uint32{3, 3}, factory.opened)
	require.Equal(t, factory.opened, factory.closed)
	frame.Close()
	require.Zero(t, budget.Used())
}

func TestDependentExpressionCaptureAndCleanupFailure(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	call := dependentCall(1)
	call.GetDependent().Inputs = []*plan.Expr{{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1}}}}
	compiled, err := CompileDependentExpressions(proc.Ctx, []*plan.Expr{call})
	require.NoError(t, err)
	seven, nine := int64(7), int64(9)
	input := dependentTestInts(t, proc, &seven, &nine)
	sentinel := errors.New("reader close failed")
	for _, closeErr := range []error{nil, sentinel} {
		factory := &dependentTestFactory{t: t, allocation: allocation, closeErr: closeErr}
		frame, err := compiled.NewFrame(proc, []DependentInput{{}, {Batch: input, Row: 1}}, factory, allocation)
		require.NoError(t, err)
		task, err := frame.Task(0)
		require.NoError(t, err)
		budget, err := proc.GetExecutionResourceBudget()
		require.NoError(t, err)
		err = vm.RunContinuation(proc.Ctx, budget, task, nil)
		if closeErr == nil {
			require.NoError(t, err)
			value, err := frame.Value(0)
			require.NoError(t, err)
			require.Equal(t, int64(9), vector.GetFixedAtNoTypeCheck[int64](value, 0))
		} else {
			require.ErrorIs(t, err, sentinel)
			_, err = frame.Value(0)
			require.Error(t, err)
		}
		require.Equal(t, []uint32{1}, factory.closed)
		frame.Close()
		require.Zero(t, budget.Used())
	}
}
