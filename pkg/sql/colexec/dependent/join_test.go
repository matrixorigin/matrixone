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

package dependent_test

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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dedupjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type assignmentFactory struct {
	opened     int
	failAt     int
	allocation *vector.AllocationAccountSelection
}

func (f *assignmentFactory) Open(p *process.Process, _ *plan.DependentExpr, inputs, _ []*vector.Vector) (colexec.DependentInvocation, error) {
	f.opened++
	if f.opened == f.failAt {
		return nil, errors.New("assignment subquery failed")
	}
	v, err := inputs[0].DupOffHeapWithAllocation(p.Mp(), f.allocation)
	if err != nil {
		return nil, err
	}
	b := &batch.Batch{Vecs: []*vector.Vector{v}}
	b.SetRowCount(1)
	return &assignmentInvocation{p: p, b: b}, nil
}

type assignmentInvocation struct {
	p *process.Process
	b *batch.Batch
}

func (i *assignmentInvocation) Step(context.Context) (vm.ContinuationStep, error) {
	return vm.ContinuationStep{Done: true}, nil
}
func (i *assignmentInvocation) Accept(vm.Continuation) error { return nil }
func (i *assignmentInvocation) Result() *batch.Batch         { return i.b }
func (i *assignmentInvocation) Close(error) error            { i.b.Clean(i.p.Mp()); return nil }

func TestDependentOrderedAssignmentImages(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success_and_reuse", true: "failure_stops_later_assignment"}[fail], func(t *testing.T) {
			p := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer p.Free()
			budget, err := p.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<26, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 1, 2, 3, 4)
			require.NoError(t, err)
			typ := types.T_int64.ToType().PlanType()
			col := func(rel, pos int32) *plan.Expr { return plan2.GetColExpr(typ, rel, pos) }
			call := func() *plan.Expr {
				return &plan.Expr{Typ: typ, Expr: &plan.Expr_Dependent{Dependent: &plan.DependentExpr{
					ProgramId: 1, StageId: 1, OccurrenceId: 1, Inputs: []*plan.Expr{col(0, 0)}, Reducer: plan.SubqueryRef_SCALAR,
				}}}
			}
			first, err := plan2.BindFuncExprImplByPlanExpr(p.Ctx, "+", []*plan.Expr{col(0, 0), {Typ: typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 1}}}}})
			require.NoError(t, err)
			last, err := plan2.BindFuncExprImplByPlanExpr(p.Ctx, "+", []*plan.Expr{call(), col(1, 0)})
			require.NoError(t, err)
			arg := &dedupjoin.DedupJoin{}
			j := &dependent.JoinKernel{Join: arg, UpdateCols: []int32{0, 0, 1}}
			defer j.Free(p, false, nil)
			for _, expr := range []*plan.Expr{first, call(), last} {
				compiled, err := colexec.CompileDependentExpressions(p.Ctx, []*plan.Expr{expr})
				require.NoError(t, err)
				j.Updates = append(j.Updates, compiled)
			}
			factory := &assignmentFactory{allocation: allocation}
			if fail {
				factory.failAt = 1
			}
			j.BindDependent(factory, allocation)
			for _, old := range []int64{10, 7} {
				func() {
					current := batch.NewWithSize(2)
					defer current.Clean(p.Mp())
					current.Vecs[0] = testutil.MakeInt64Vector([]int64{old}, nil, p.Mp())
					current.Vecs[1] = testutil.MakeInt64Vector([]int64{20}, nil, p.Mp())
					current.SetRowCount(1)
					incoming := batch.NewWithSize(1)
					defer incoming.Clean(p.Mp())
					incoming.Vecs[0] = testutil.MakeInt64Vector([]int64{3}, nil, p.Mp())
					incoming.SetRowCount(1)
					defer j.Reset(p, fail, nil)
					for step := 0; ; step++ {
						require.Less(t, step, 5)
						values, err := arg.UpdateExpressions(p, current, incoming)
						require.NoError(t, err)
						if values != nil {
							require.Equal(t, []int64{old + 1, old + 1, old + 4}, []int64{
								vector.GetFixedAtNoTypeCheck[int64](values[0], 0), vector.GetFixedAtNoTypeCheck[int64](values[1], 0), vector.GetFixedAtNoTypeCheck[int64](values[2], 0)})
							break
						}
						task := j.TakeContinuation()
						require.NotNil(t, task)
						err = vm.RunContinuation(p.Ctx, budget, task, nil)
						if err != nil {
							require.True(t, fail)
							require.ErrorContains(t, err, "assignment subquery failed")
							require.Equal(t, 1, factory.opened)
							break
						}
					}
					require.Equal(t, old, vector.GetFixedAtNoTypeCheck[int64](current.Vecs[0], 0), "native current image remains unpublished")
					j.Reset(p, fail, nil)
					require.Zero(t, budget.Used())
				}()
				if fail {
					break
				}
			}
			if !fail {
				require.Equal(t, 4, factory.opened)
			}
		})
	}
}
