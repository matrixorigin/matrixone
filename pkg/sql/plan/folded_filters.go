// Copyright 2021 Matrix Origin
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

package plan

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// BuildFoldedFilterExprs prepares an isolated copy of a filter list and its
// fold executors. The caller publishes both only after every expression has
// been rewritten (and, for storage filters, evaluated) successfully.
func BuildFoldedFilterExprs(
	proc *process.Process,
	exprs []*plan.Expr,
	existing []colexec.ExpressionExecutor,
	evaluate bool,
) ([]*plan.Expr, []colexec.ExpressionExecutor, error) {
	filters := DeepCopyExprList(exprs)
	executors := append([]colexec.ExpressionExecutor(nil), existing...)
	firstNewExecutor := len(executors)
	rollback := func(err error) ([]*plan.Expr, []colexec.ExpressionExecutor, error) {
		for _, executor := range executors[firstNewExecutor:] {
			executor.Free()
		}
		return nil, existing, err
	}

	for _, expr := range filters {
		if _, err := ReplaceFoldExpr(proc, expr, &executors); err != nil {
			return rollback(err)
		}
	}
	if evaluate {
		for _, expr := range filters {
			if err := EvalFoldExpr(proc, expr, &executors); err != nil {
				return rollback(err)
			}
		}
	}
	return filters, executors, nil
}
