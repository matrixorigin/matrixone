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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type userVariableAssignmentBatchProbe struct {
	rowCounts []int
}

func (p *userVariableAssignmentBatchProbe) Eval(_ *process.Process, batches []*batch.Batch, _ []bool) (*vector.Vector, error) {
	p.rowCounts = append(p.rowCounts, batches[0].RowCount())
	return batches[0].Vecs[0], nil
}

func (p *userVariableAssignmentBatchProbe) EvalWithoutResultReusing(proc *process.Process, batches []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	return p.Eval(proc, batches, selectList)
}

func (p *userVariableAssignmentBatchProbe) ResetForNextQuery() {}
func (p *userVariableAssignmentBatchProbe) Free()              {}
func (p *userVariableAssignmentBatchProbe) IsColumnExpr() bool { return true }
func (p *userVariableAssignmentBatchProbe) TypeName() string   { return "batch-probe" }

func TestUserVariableAssignmentExecutorPreservesRowReadAfterWrite(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.SetRowCount(2)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(20), false, proc.Mp()))
	defer input.Clean(proc.Mp())

	int64Type := types.T_int64.ToType().PlanType()
	assignment := &plan.Expr{
		Typ: int64Type,
		Expr: &plan.Expr_UserVarAssignment{UserVarAssignment: &plan.UserVarAssignment{
			Name: "a",
			Value: &plan.Expr{
				Typ:  int64Type,
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}},
			},
		}},
	}
	assignmentExec, err := NewExpressionExecutor(proc, assignment)
	require.NoError(t, err)
	defer assignmentExec.Free()

	result, err := assignmentExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](result, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](result, 1))

	read := &plan.Expr{
		Typ:  int64Type,
		Expr: &plan.Expr_V{V: &plan.VarRef{Name: "a"}},
	}
	readExec, err := NewExpressionExecutor(proc, read)
	require.NoError(t, err)
	defer readExec.Free()
	readResult, err := readExec.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](readResult, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](readResult, 1))

	var committed []process.UserVariableAssignment
	proc.SetUserVariableAssignmentCommitFunc(func(value process.UserVariableAssignment) error {
		committed = append(committed, value)
		return nil
	})
	require.NoError(t, proc.CommitUserVariableAssignments())
	require.Len(t, committed, 1)
	require.Equal(t, int64(20), committed[0].Value)
}

func TestUserVariableAssignmentDoesNotShadowSystemVariableReads(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	int64Type := types.T_int64.ToType().PlanType()
	input := batch.New(nil)
	input.SetRowCount(1)
	defer input.Clean(proc.Mp())

	var calls [][2]bool
	proc.SetResolveVariableFunc(func(name string, system, global bool) (any, error) {
		require.Equal(t, "autocommit", name)
		calls = append(calls, [2]bool{system, global})
		return int64(1), nil
	})

	// Stage a user-variable value with the same name as the system variable.
	// User-variable reads may see this overlay, but @@autocommit and
	// @@global.autocommit must still use the resolver.
	proc.BeginUserVariableAssignment("autocommit", 1)
	proc.SetUserVariableAssignmentCurrentRow(0)
	proc.StageUserVariableAssignment(process.UserVariableAssignment{
		Name:  "autocommit",
		Type:  int64Type,
		Value: int64(0),
	}, 0)
	proc.SetUserVariableAssignmentCurrentRow(-1)

	newVarExecutor := func(system, global bool) ExpressionExecutor {
		executor, err := NewExpressionExecutor(proc, &plan.Expr{
			Typ: int64Type,
			Expr: &plan.Expr_V{V: &plan.VarRef{
				Name:   "autocommit",
				System: system,
				Global: global,
			}},
		})
		require.NoError(t, err)
		return executor
	}

	userExecutor := newVarExecutor(false, false)
	userResult, err := userExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(0), vector.GetFixedAtNoTypeCheck[int64](userResult, 0))
	userExecutor.Free()

	sessionExecutor := newVarExecutor(true, false)
	sessionResult, err := sessionExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](sessionResult, 0))
	sessionExecutor.Free()

	globalExecutor := newVarExecutor(true, true)
	globalResult, err := globalExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(1), vector.GetFixedAtNoTypeCheck[int64](globalResult, 0))
	globalExecutor.Free()

	require.Equal(t, [][2]bool{{true, false}, {true, true}}, calls)
}

func TestUserVariableAssignmentExecutorEvaluatesOneInputRowAtATime(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.SetRowCount(3)
	for _, value := range []int64{10, 20, 30} {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], value, false, proc.Mp()))
	}
	defer input.Clean(proc.Mp())

	probe := &userVariableAssignmentBatchProbe{}
	assignment := &UserVarAssignmentExpressionExecutor{
		mp:    proc.Mp(),
		child: probe,
		name:  "a",
		typ:   types.T_int64.ToType(),
	}
	result, err := assignment.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	defer result.Free(proc.Mp())
	require.Equal(t, []int{1, 1, 1}, probe.rowCounts)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](result, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](result, 1))
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](result, 2))
}

func TestUserVariableAssignmentExecutorStopsOnCancellation(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.SetRowCount(1)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(10), false, proc.Mp()))
	defer input.Clean(proc.Mp())

	int64Type := types.T_int64.ToType().PlanType()
	assignment := &plan.Expr{
		Typ: int64Type,
		Expr: &plan.Expr_UserVarAssignment{UserVarAssignment: &plan.UserVarAssignment{
			Name:  "a",
			Value: &plan.Expr{Typ: int64Type, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		}},
	}
	assignmentExec, err := NewExpressionExecutor(proc, assignment)
	require.NoError(t, err)
	defer assignmentExec.Free()

	_, err = assignmentExec.Eval(proc, []*batch.Batch{input}, nil)
	require.ErrorIs(t, err, context.Canceled)
	_, ok := proc.ResolveUserVariableAssignment("a", 0)
	require.False(t, ok)
}

func TestUserVariableAssignmentExecutorSupportsNestedAssignments(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	int64Type := types.T_int64.ToType().PlanType()
	inner := &plan.Expr{Typ: int64Type, Expr: &plan.Expr_UserVarAssignment{
		UserVarAssignment: &plan.UserVarAssignment{
			Name:  "b",
			Value: &plan.Expr{Typ: int64Type, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
		},
	}}
	outer := &plan.Expr{Typ: int64Type, Expr: &plan.Expr_UserVarAssignment{
		UserVarAssignment: &plan.UserVarAssignment{Name: "a", Value: inner},
	}}
	executor, err := NewExpressionExecutor(proc, outer)
	require.NoError(t, err)
	defer executor.Free()

	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	input.SetRowCount(3)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(10), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(20), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(30), false, proc.Mp()))
	defer input.Clean(proc.Mp())
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](result, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](result, 1))
	require.Equal(t, int64(30), vector.GetFixedAtNoTypeCheck[int64](result, 2))
	a, ok := proc.ResolveUserVariableAssignment("a", 1)
	require.True(t, ok)
	require.Equal(t, int64(20), a.Value)
	b, ok := proc.ResolveUserVariableAssignment("b", 1)
	require.True(t, ok)
	require.Equal(t, int64(20), b.Value)

	read := &plan.Expr{Typ: int64Type, Expr: &plan.Expr_V{V: &plan.VarRef{Name: "b"}}}
	readExecutor, err := NewExpressionExecutor(proc, read)
	require.NoError(t, err)
	defer readExecutor.Free()
	readResult, err := readExecutor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(10), vector.GetFixedAtNoTypeCheck[int64](readResult, 0))
	require.Equal(t, int64(20), vector.GetFixedAtNoTypeCheck[int64](readResult, 1))

	secondInput := batch.NewWithSize(1)
	secondInput.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	secondInput.SetRowCount(2)
	require.NoError(t, vector.AppendFixed(secondInput.Vecs[0], int64(40), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(secondInput.Vecs[0], int64(50), false, proc.Mp()))
	defer secondInput.Clean(proc.Mp())
	_, err = executor.Eval(proc, []*batch.Batch{secondInput}, nil)
	require.NoError(t, err)
	readResult, err = readExecutor.Eval(proc, []*batch.Batch{secondInput}, nil)
	require.NoError(t, err)
	require.Equal(t, int64(40), vector.GetFixedAtNoTypeCheck[int64](readResult, 0))
	require.Equal(t, int64(50), vector.GetFixedAtNoTypeCheck[int64](readResult, 1))
}

func TestUserVariableAssignmentSkippedNestedRowDoesNotPublish(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	int64Type := types.T_int64.ToType()
	columnPlan := &plan.Expr{Typ: int64Type.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
	child, err := NewExpressionExecutor(proc, columnPlan)
	require.NoError(t, err)
	assignment := &UserVarAssignmentExpressionExecutor{
		mp:    proc.Mp(),
		child: child,
		name:  "b",
		typ:   int64Type,
	}
	defer assignment.Free()

	proc.BeginUserVariableAssignment("a", 1)
	proc.SetUserVariableAssignmentCurrentRow(0)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(int64Type)
	input.SetRowCount(1)
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, proc.Mp()))
	defer input.Clean(proc.Mp())
	_, err = assignment.Eval(proc, []*batch.Batch{input}, []bool{false})
	require.NoError(t, err)

	var committed []process.UserVariableAssignment
	proc.SetUserVariableAssignmentCommitFunc(func(value process.UserVariableAssignment) error {
		committed = append(committed, value)
		return nil
	})
	require.NoError(t, proc.CommitUserVariableAssignments())
	require.Empty(t, committed)
}

func TestUserVariableAssignmentNestedSkippedRowsKeepHistory(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithMPool(mpool.MustNewZero()), testutil.WithFileService(nil))
	proc.Base.IsFrontend = true

	int64Type := types.T_int64.ToType()
	columnPlan := &plan.Expr{Typ: int64Type.PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}}
	child, err := NewExpressionExecutor(proc, columnPlan)
	require.NoError(t, err)
	assignment := &UserVarAssignmentExpressionExecutor{
		mp:    proc.Mp(),
		child: child,
		name:  "b",
		typ:   int64Type,
	}
	defer assignment.Free()
	proc.BeginUserVariableAssignment("a", 3)

	for row, selected := range []bool{false, true, false} {
		input := batch.NewWithSize(1)
		input.Vecs[0] = vector.NewVec(int64Type)
		input.SetRowCount(1)
		require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(row+1), false, proc.Mp()))
		proc.SetUserVariableAssignmentCurrentRow(row)
		_, err = assignment.Eval(proc, []*batch.Batch{input}, []bool{selected})
		input.Clean(proc.Mp())
		require.NoError(t, err)
	}
	proc.SetUserVariableAssignmentCurrentRow(-1)
	rows, valid, ok := proc.GetUserVariableAssignmentRows("b", 3)
	require.True(t, ok)
	require.Equal(t, []bool{true, true, true}, valid)
	require.Nil(t, rows[0].Value)
	require.Equal(t, int64(2), rows[1].Value)
	require.Equal(t, int64(2), rows[2].Value)
}
