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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersect"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/intersectall"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minus"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/minusall"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSetOperationAllocationResetAndFailure(t *testing.T) {
	cases := []struct {
		name string
		make func() vm.Operator
		want []int64
	}{
		{"minus", func() vm.Operator { return minus.NewArgument() }, []int64{3}},
		{"minus_all", func() vm.Operator { return minusall.NewArgument() }, []int64{1, 3}},
		{"intersect", func() vm.Operator { return intersect.NewArgument() }, []int64{1, 2}},
		{"intersect_all", func() vm.Operator { return intersectall.NewArgument() }, []int64{1, 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
			defer proc.Free()
			budget, err := proc.GetExecutionResourceBudget()
			require.NoError(t, err)
			registry, err := mpool.NewAllocationAccountRegistry(3, 2048)
			require.NoError(t, err)
			op := tc.make()
			defer op.Release()
			owner := op.(interface {
				SetAllocationAccount(*mpool.AllocationAccount) error
				ClearAllocationAccount(*mpool.AllocationAccount) error
			})
			// A failed generation is followed by two successful reuses of the same
			// operator. The input bags distinguish ALL from DISTINCT semantics.
			for _, limit := range []uint64{128, 1 << 24, 1 << 24} {
				func() {
					account, err := registry.OpenWithController(limit, budget)
					require.NoError(t, err)
					require.NoError(t, owner.SetAllocationAccount(account))
					released := false
					cleanup := func(cause error) {
						if released {
							return
						}
						released = true
						op.Reset(proc, cause != nil, cause)
						op.Free(proc, cause != nil, cause)
						for _, child := range op.GetOperatorBase().Children {
							child.Free(proc, cause != nil, cause)
						}
						op.GetOperatorBase().ResetChildren()
						require.NoError(t, owner.ClearAllocationAccount(account))
					}
					defer cleanup(nil)
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
					}
					if limit == 128 {
						require.Error(t, err)
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
