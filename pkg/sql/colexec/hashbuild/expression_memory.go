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

package hashbuild

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	executionResourceAllocationSiteExpressionData mpool.AllocationSite = iota + 98
	executionResourceAllocationSiteExpressionArea
	executionResourceAllocationSiteExpressionNulls
	executionResourceAllocationSiteExpressionGrouping
)

// NewExpressionExecutors constructs expression trees used by HashBuild and
// join operators. Every MPool vector owned by the tree, including nested
// function results and reusable selection buffers, shares the query account.
func NewExpressionExecutors(
	proc *process.Process,
	exprs []*plan.Expr,
	account *mpool.AllocationAccount,
) ([]colexec.ExpressionExecutor, error) {
	return newExpressionExecutorsWithCapacityClass(
		proc,
		exprs,
		account,
		mpool.AllocationCapacityClassDefault,
	)
}

func newExpressionExecutorsWithCapacityClass(
	proc *process.Process,
	exprs []*plan.Expr,
	account *mpool.AllocationAccount,
	capacityClass mpool.AllocationCapacityClass,
) ([]colexec.ExpressionExecutor, error) {
	if len(exprs) == 0 {
		return nil, process.ErrExecutionResourceInvalid
	}
	for _, expr := range exprs {
		if expr == nil {
			return nil, process.ErrExecutionResourceInvalid
		}
	}
	selection, err := vector.NewAllocationAccountSelectionWithCapacityClass(
		account,
		mpool.AllocationOwnerHashBuild,
		executionResourceAllocationSiteExpressionData,
		executionResourceAllocationSiteExpressionArea,
		executionResourceAllocationSiteExpressionNulls,
		executionResourceAllocationSiteExpressionGrouping,
		capacityClass,
	)
	if err != nil {
		return nil, err
	}
	return colexec.NewExpressionExecutorsFromPlanExpressionsWithAllocation(
		proc, exprs, selection,
	)
}
