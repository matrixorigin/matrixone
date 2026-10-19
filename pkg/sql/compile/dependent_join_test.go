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

package compile

import (
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dependent"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/loopjoin"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestDependentJoinAlgorithm(t *testing.T) {
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	defer proc.Free()
	bind := func(name string, args ...*plan.Expr) *plan.Expr {
		expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	typ := plan.Type{Id: int32(types.T_int32)}
	left, right := plan2.GetColExpr(typ, 0, 0), plan2.GetColExpr(typ, 1, 0)
	eq := bind("=", left, right)
	nonNullType := typ
	nonNullType.NotNullable = true
	nonNullComposite := []*plan.Expr{
		bind("=", plan2.GetColExpr(nonNullType, 0, 0), plan2.GetColExpr(nonNullType, 1, 0)),
		bind("=", plan2.GetColExpr(nonNullType, 0, 1), plan2.GetColExpr(nonNullType, 1, 1)),
	}
	dep := &plan.Expr{Typ: typ, Expr: &plan.Expr_Dependent{Dependent: &plan.DependentExpr{ProgramId: 1}}}
	children := []*plan.Node{{ProjectList: []*plan.Expr{left, left}}, {ProjectList: []*plan.Expr{right, right}}}
	cases := []struct {
		name string
		kind plan.Node_JoinType
		on   []*plan.Expr
		want vm.OpType
	}{
		{"inner", plan.Node_INNER, []*plan.Expr{eq}, vm.HashJoin},
		{"inner_shuffle_placement", plan.Node_INNER, []*plan.Expr{eq}, vm.HashJoin},
		{"left", plan.Node_LEFT, []*plan.Expr{eq}, vm.HashJoin},
		{"semi", plan.Node_SEMI, []*plan.Expr{eq}, vm.HashJoin},
		{"anti", plan.Node_ANTI, []*plan.Expr{eq}, vm.HashJoin},
		{"single", plan.Node_SINGLE, []*plan.Expr{eq}, vm.HashJoin},
		{"outer", plan.Node_OUTER, []*plan.Expr{eq}, vm.HashJoin},
		{"residual", plan.Node_INNER, []*plan.Expr{eq, bind("<", left, right)}, vm.HashJoin},
		{"non_equality", plan.Node_LEFT, []*plan.Expr{bind("<", left, right)}, vm.LoopJoin},
		{"cartesian", plan.Node_INNER, nil, vm.LoopJoin},
		{"dependent_key", plan.Node_INNER, []*plan.Expr{bind("=", left, bind("+", right, dep))}, vm.LoopJoin},
		{"dependent_residual", plan.Node_INNER, []*plan.Expr{eq, bind("<", left, dep)}, vm.LoopJoin},
		{"mark_single_nullable", plan.Node_MARK, []*plan.Expr{eq}, vm.HashJoin},
		{"mark_nonnull_composite", plan.Node_MARK, nonNullComposite, vm.HashJoin},
		{"mark_nullable_composite", plan.Node_MARK, []*plan.Expr{eq, bind("=", plan2.GetColExpr(typ, 0, 1), plan2.GetColExpr(typ, 1, 1))}, vm.LoopJoin},
		{"mark_residual", plan.Node_MARK, []*plan.Expr{eq, bind("<", left, right)}, vm.LoopJoin},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &Compile{proc: proc}
			n := &plan.Node{NodeType: plan.Node_JOIN, JoinType: tc.kind, OnList: tc.on}
			if tc.name == "inner_shuffle_placement" {
				n.Stats = &plan.Stats{HashmapStats: &plan.HashMapStats{Shuffle: true}}
			}
			inputs := children
			if tc.name == "mark_nonnull_composite" {
				inputs = []*plan.Node{plan2.DeepCopyNode(children[0]), plan2.DeepCopyNode(children[1])}
				for _, input := range inputs {
					for _, expr := range input.ProjectList {
						expr.Typ.NotNullable = true
					}
				}
			}
			factory, err := c.compileDependentJoin(plan2.DeepCopyNode(n), inputs)
			require.NoError(t, err)
			op, err := factory(proc, []*dependent.DemandInput{{}, {}})
			require.NoError(t, err)
			root := op.(*dependent.JoinKernel)
			defer func() {
				root.Free(proc, false, nil)
				root.Join.Free(proc, false, nil)
				root.Build.Free(proc, false, nil)
				root.Join.Release()
				root.Build.Release()
			}()
			require.Equal(t, tc.want, root.Join.OpType())
			if tc.want == vm.HashJoin {
				require.Nil(t, root.Condition)
				require.False(t, root.Join.(*hashjoin.HashJoin).IsShuffle)
				require.False(t, root.Build.IsShuffle)
				require.True(t, root.Build.NeedHashMap)
				require.True(t, root.Build.NeedBatches, "direct input retains its materialized build owner")
				if tc.kind == plan.Node_MARK {
					require.True(t, root.Build.TrackNullKeys)
					require.False(t, root.Build.NeedAllocateSels)
					if len(tc.on) > 1 {
						for _, keys := range root.Join.(*hashjoin.HashJoin).EqConds {
							for _, key := range keys {
								require.True(t, key.Typ.NotNullable)
							}
						}
					}
				}
			} else {
				require.False(t, root.Build.NeedHashMap)
			}
		})
	}
}

// Count the real LoopJoin pair callback without adding a production hook.
// A hash-selected independent join has no pair expression frames at all.
type dependentJoinWorkCounter struct {
	*dependent.JoinKernel
	pairs *int
}

func (j *dependentJoinWorkCounter) BindDependent(factory colexec.DependentProgramFactory, allocation *vector.AllocationAccountSelection) {
	j.JoinKernel.BindDependent(factory, allocation)
	if join, ok := j.Join.(*loopjoin.LoopJoin); ok && join.PairCondition != nil {
		pair := join.PairCondition
		join.PairCondition = func(p *process.Process, left *batch.Batch, lrow int, right *batch.Batch, rrow int) (*vector.Vector, error) {
			*j.pairs++
			return pair(p, left, lrow, right, rrow)
		}
	}
}

func TestDependentIndependentJoinWork(t *testing.T) {
	for _, size := range []int32{100, 200} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			mock := plan2.NewMockOptimizer(true, proc)
			stmts, err := mysql.Parse(proc.Ctx, "SELECT (SELECT (SELECT a.N_NATIONKEY)) FROM NATION a JOIN NATION b ON a.N_NATIONKEY=b.N_NATIONKEY", 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			query, err := plan2.NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
			require.NoError(t, err)
			require.NotNil(t, query.Correlation)
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(1, 8192)
			require.NoError(t, err)
			account, err := registry.OpenWithController(1<<28, budget)
			require.NoError(t, err)
			defer func() {
				account.Seal()
				_, err := registry.Finalize(account)
				require.NoError(t, err)
			}()
			allocation, err := vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerDependent, 64, 65, 66, 67)
			require.NoError(t, err)
			opened, closed, pairs, hashJoins := 0, 0, 0, 0
			sourceCompiler := func(node *plan.Node) (dependent.SourceFactory, error) {
				return func(p *process.Process, selection *vector.AllocationAccountSelection, _ *mpool.AllocationAccount) (dependent.Source, error) {
					if node.NodeType == plan.Node_VALUE_SCAN {
						opened++
						return &dependentFixtureSource{batch: batch.EmptyForConstFoldBatch, rows: 1, close: func() { closed++ }}, nil
					}
					b := batch.NewWithSize(len(node.TableDef.Cols))
					for i := range b.Vecs {
						b.Vecs[i], err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), selection)
						if err != nil {
							b.Clean(p.Mp())
							return nil, err
						}
						for key := int32(1); key <= size; key++ {
							if err := vector.AppendFixed(b.Vecs[i], key, false, p.Mp()); err != nil {
								b.Clean(p.Mp())
								return nil, err
							}
						}
					}
					b.SetRowCount(int(size))
					opened++
					return &dependentFixtureSource{batch: b, rows: 1, close: func() { closed++; b.Clean(p.Mp()) }}, nil
				}, nil
			}
			c := &Compile{proc: proc, stmt: stmts[0]}
			kernelCompiler := func(node *plan.Node, children []*plan.Node) (dependent.KernelFactory, [][]*plan.Expr, error) {
				factory, inputs, err := c.compileDependentKernel(node, children)
				if err != nil || node.NodeType != plan.Node_JOIN {
					return factory, inputs, err
				}
				return func(p *process.Process, ports []*dependent.DemandInput) (vm.Operator, error) {
					op, err := factory(p, ports)
					if err != nil {
						return nil, err
					}
					join := op.(*dependent.JoinKernel)
					if _, ok := join.Join.(*hashjoin.HashJoin); ok {
						hashJoins++
					}
					return &dependentJoinWorkCounter{JoinKernel: join, pairs: &pairs}, nil
				}, inputs, nil
			}
			template, err := dependent.Compile(proc.Ctx, query, sourceCompiler, kernelCompiler)
			require.NoError(t, err)
			for range 2 {
				func() {
					pairs, hashJoins = 0, 0
					execution, err := template.NewExecution(proc, allocation, account)
					require.NoError(t, err)
					defer func() { require.NoError(t, execution.Close()) }()
					seen := make(map[int32]bool, size)
					for {
						b, err := execution.Next(proc.Ctx)
						require.NoError(t, err)
						if b == nil {
							break
						}
						for row := 0; row < b.RowCount(); row++ {
							key := vector.GetFixedAtNoTypeCheck[int32](b.Vecs[0], row)
							require.False(t, seen[key])
							seen[key] = true
						}
					}
					require.NoError(t, execution.Close())
					require.Equal(t, opened, closed)
					require.Zero(t, account.Snapshot().Used)
					require.Zero(t, budget.Used())
					t.Logf("input_rows=%d output_rows=%d hash_joins=%d pair_calls=%d", size, len(seen), hashJoins, pairs)
					require.Len(t, seen, int(size))
					for key := int32(1); key <= size; key++ {
						require.True(t, seen[key])
					}
					require.Equal(t, 1, hashJoins)
					require.Zero(t, pairs, "independent equi join must not evaluate N*M candidate pairs")
				}()
			}
		})
	}
}
