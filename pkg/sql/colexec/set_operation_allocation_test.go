// Copyright 2026 Matrix Origin
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

package colexec_test

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetOperationAllocationResetAndFailure(t *testing.T) {
	cases := []struct {
		name string
		make func([]*plan.Expr) vm.Operator
		want []int64
	}{
		{"minus", func(keys []*plan.Expr) vm.Operator {
			op := minus.NewArgument()
			op.KeyExprs = keys
			return op
		}, []int64{3}},
		{"minus_all", func(keys []*plan.Expr) vm.Operator {
			op := minusall.NewArgument()
			op.KeyExprs = keys
			return op
		}, []int64{1, 3}},
		{"intersect", func(keys []*plan.Expr) vm.Operator {
			op := intersect.NewArgument()
			op.KeyExprs = keys
			return op
		}, []int64{1, 2}},
		{"intersect_all", func(keys []*plan.Expr) vm.Operator {
			op := intersectall.NewArgument()
			op.KeyExprs = keys
			return op
		}, []int64{1, 2}},
	}
	for _, tc := range cases {
		for _, evaluatedKeys := range []bool{false, true} {
			name := tc.name + "/columns"
			if evaluatedKeys {
				name = tc.name + "/evaluated_keys"
			}
			t.Run(name, func(t *testing.T) {
				proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
				defer proc.Free()
				budget, err := proc.GetExecutionResourceBudget()
				require.NoError(t, err)
				registry, err := mpool.NewAllocationAccountRegistry(3, 2048)
				require.NoError(t, err)
				var keys []*plan.Expr
				if evaluatedKeys {
					key, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "abs", []*plan.Expr{plan2.GetColExpr(types.T_int64.ToType().PlanType(), 0, 0)})
					require.NoError(t, err)
					keys = []*plan.Expr{key}
				}
				op := tc.make(keys)
				defer op.Release()
				defer op.Free(proc, false, nil)
				owner := op.(interface {
					SetAllocationAccount(*mpool.AllocationAccount) error
					ClearAllocationAccount(*mpool.AllocationAccount) error
				})
				// Admission failure and abort after output precede two successful
				// generations. Input bags distinguish ALL from DISTINCT semantics.
				for _, generation := range []struct {
					limit            uint64
					abortAfterOutput bool
				}{{128, false}, {1 << 24, true}, {1 << 24, false}, {1 << 24, false}} {
					func() {
						account, err := registry.OpenWithController(generation.limit, budget)
						require.NoError(t, err)
						retired := false
						defer func() {
							if retired {
								return
							}
							// Failure cleanup owns all remaining resources, including on FailNow.
							op.Free(proc, false, nil)
							for _, child := range op.GetOperatorBase().Children {
								child.Free(proc, false, nil)
							}
							op.GetOperatorBase().ResetChildren()
							require.NoError(t, owner.ClearAllocationAccount(account))
							account.Seal()
							_, err := registry.Finalize(account)
							require.NoError(t, err)
						}()
						require.NoError(t, owner.SetAllocationAccount(account))
						released := false
						cleanup := func(cause error) {
							if released {
								return
							}
							released = true
							// PREPARE cleanup does not call Free until the template is retired.
							op.Reset(proc, cause != nil, cause)
							op.Reset(proc, cause != nil, cause)
							for _, child := range op.GetOperatorBase().Children {
								child.Free(proc, cause != nil, cause)
							}
							op.GetOperatorBase().ResetChildren()
							require.Zero(t, account.Snapshot().Used, "Reset must release this EXECUTE generation")
							require.Zero(t, budget.Used(), "Reset must release output metadata")
							require.NoError(t, owner.ClearAllocationAccount(account))
							account.Seal()
							_, err := registry.Finalize(account)
							require.NoError(t, err)
							retired = true
						}
						for _, values := range [][]int64{{1, 1, 2, 3}, {1, 2, 2}} {
							b := batch.NewWithSize(1)
							b.Vecs[0] = testutil.NewVector(len(values), types.T_int64.ToType(), proc.Mp(), false, values)
							b.SetRowCount(len(values))
							op.GetOperatorBase().AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{b}))
						}
						err = op.Prepare(proc)
						var got []int64
						for err == nil {
							var result vm.CallResult
							result, err = vm.Exec(op, proc)
							if err != nil || result.Batch == nil {
								break
							}
							got = append(got, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...)
							if generation.abortAfterOutput {
								err = moerr.NewInternalErrorNoCtx("downstream failed after set output")
								break
							}
						}
						if generation.limit == 128 {
							require.Error(t, err)
						} else if generation.abortAfterOutput {
							require.ErrorContains(t, err, "downstream failed after set output")
							require.Equal(t, tc.want, got)
							require.Positive(t, account.Snapshot().Used)
						} else {
							require.NoError(t, err)
							require.Equal(t, tc.want, got)
							require.Positive(t, account.Snapshot().Used)
						}
						cleanup(err)
						require.Zero(t, account.Snapshot().Used)
						require.Zero(t, budget.Used())
					}()
				}
			})
		}
	}
}
