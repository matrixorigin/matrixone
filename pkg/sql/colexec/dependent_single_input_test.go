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

package colexec

import (
	"fmt"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func singleInputTestColumn(relation, column int32) *plan.Expr {
	return &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: relation, ColPos: column, Name: "payload"}}}
}

func TestDependentSingleInputProjectionABI(t *testing.T) {
	for _, relation := range []int32{0, -1, 1, 99} {
		t.Run(fmt.Sprint(relation), func(t *testing.T) {
			proc, allocation, _ := dependentTestAllocation(t)
			value := int64(17)
			input := dependentTestInts(t, proc, &value)
			root := singleInputTestColumn(relation, 0)
			original := proto.Clone(root).(*plan.Expr)
			program, err := CompileDependentSingleInputExpressions(proc.Ctx, []*plan.Expr{root})
			require.NoError(t, err)
			require.True(t, proto.Equal(original, root))
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			frame, err := program.NewFrame(proc, []DependentInput{{Batch: input}}, nil, allocation)
			require.NoError(t, err)
			defer frame.Close()
			task, err := frame.Task(0)
			require.NoError(t, err)
			require.NoError(t, vm.RunContinuation(proc.Ctx, budget, task, nil))
			got, err := frame.Value(0)
			require.NoError(t, err)
			require.Equal(t, value, vector.GetFixedAtNoTypeCheck[int64](got, 0))
			// Independent native executor: its documented single-batch ABI
			// ignores the historical relation coordinate too.
			native, err := NewExpressionExecutorWithAllocation(proc, root, allocation)
			require.NoError(t, err)
			defer native.Free()
			control, err := native.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			require.Equal(t, vector.GetFixedAtNoTypeCheck[int64](control, 0), vector.GetFixedAtNoTypeCheck[int64](got, 0))
			frame.Close()
			require.Zero(t, budget.Used())
		})
	}
}

func TestDependentSingleInputCapturesPreserveNamespaces(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	left, right := singleInputTestColumn(-1, 0), singleInputTestColumn(9, 0)
	sum := dependentFunction(t, proc, "+", left, right)
	slot := &plan.Expr{Typ: types.T_int64.ToType().PlanType(), Expr: &plan.Expr_Slot{Slot: &plan.CorrelationSlot{ProgramId: 1}}}
	call := dependentCall(1)
	call.GetDependent().Inputs = []*plan.Expr{sum, slot}
	call.GetDependent().Lhs = []*plan.Expr{singleInputTestColumn(3, 0)}
	call.GetDependent().Comparison = dependentTestEqual(t, proc)
	original := proto.Clone(call).(*plan.Expr)
	program, err := CompileDependentSingleInputExpressions(proc.Ctx, []*plan.Expr{call})
	require.NoError(t, err)
	columns := 0
	for _, instruction := range program.instructions {
		if col := instruction.expr.GetCol(); col != nil {
			columns++
			require.Zero(t, col.RelPos)
			require.Zero(t, col.ColPos)
			require.Equal(t, "payload", col.Name)
		}
	}
	require.Equal(t, 3, columns)
	require.True(t, proto.Equal(original, call))
	require.Same(t, slot, program.instructions[program.instructions[program.roots[0]].args[1]].expr)
	require.Equal(t, int32(1), program.instructions[program.roots[0]].expr.GetDependent().Comparison.GetF().Args[1].GetCol().RelPos)
	// The same nested arithmetic, evaluated as a root, executes with the
	// normalized instruction operands rather than mutating the function AST.
	program, err = CompileDependentSingleInputExpressions(proc.Ctx, []*plan.Expr{sum})
	require.NoError(t, err)
	value := int64(17)
	input := dependentTestInts(t, proc, &value)
	frame, err := program.NewFrame(proc, []DependentInput{{Batch: input}}, nil, allocation)
	require.NoError(t, err)
	defer frame.Close()
	task, err := frame.Task(0)
	require.NoError(t, err)
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	require.NoError(t, vm.RunContinuation(proc.Ctx, budget, task, nil))
	got, err := frame.Value(0)
	require.NoError(t, err)
	require.Equal(t, int64(34), vector.GetFixedAtNoTypeCheck[int64](got, 0))
}

func TestDependentSingleInputPreservesValidation(t *testing.T) {
	proc, allocation, _ := dependentTestAllocation(t)
	cycle := dependentFunction(t, proc, "+", dependentInt(1), dependentInt(2))
	cycle.GetF().Args[0] = cycle
	for _, root := range []*plan.Expr{nil, cycle} {
		program, err := CompileDependentSingleInputExpressions(proc.Ctx, []*plan.Expr{root})
		require.Error(t, err)
		require.Nil(t, program)
	}
	left, right := int64(17), int64(29)
	inputs := []DependentInput{{Batch: dependentTestInts(t, proc, &left)}, {Batch: dependentTestInts(t, proc, &right)}}
	budget, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	for _, tc := range []struct{ relation, width int }{{1, 2}, {2, 2}, {1, 1}} {
		func() {
			program, err := CompileDependentExpressions(proc.Ctx, []*plan.Expr{singleInputTestColumn(int32(tc.relation), 0)})
			require.NoError(t, err)
			frame, err := program.NewFrame(proc, inputs[:tc.width], nil, allocation)
			require.NoError(t, err)
			defer frame.Close()
			task, err := frame.Task(0)
			require.NoError(t, err)
			err = vm.RunContinuation(proc.Ctx, budget, task, nil)
			if tc.relation >= tc.width {
				require.ErrorContains(t, err, "dependent input relation")
			} else {
				require.NoError(t, err)
				got, err := frame.Value(0)
				require.NoError(t, err)
				require.Equal(t, right, vector.GetFixedAtNoTypeCheck[int64](got, 0))
			}
		}()
	}
	program, err := CompileDependentSingleInputExpressions(proc.Ctx, []*plan.Expr{singleInputTestColumn(9, 1)})
	require.NoError(t, err)
	frame, err := program.NewFrame(proc, inputs[:1], nil, allocation)
	require.NoError(t, err)
	defer frame.Close()
	task, err := frame.Task(0)
	require.NoError(t, err)
	require.ErrorContains(t, vm.RunContinuation(proc.Ctx, budget, task, nil), "dependent input row or column")
}
