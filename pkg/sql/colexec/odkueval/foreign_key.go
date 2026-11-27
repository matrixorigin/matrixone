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

package odkueval

import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/hashbuild"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// CompileFKChanges compiles the null-safe comparisons used to decide which
// child foreign keys changed in an update. The caller owns the returned
// executors. A failed compilation releases all executors already created.
func CompileFKChanges(checks []*plan.OdkuForeignKeyCheck, leftTypes, rightTypes []types.Type, proc *process.Process, account *mpool.AllocationAccount) (result [][]colexec.ExpressionExecutor, err error) {
	result = make([][]colexec.ExpressionExecutor, len(checks))
	defer func() {
		if err != nil {
			FreeFKChanges(result)
			result = nil
		}
	}()
	for i, check := range checks {
		if check == nil || check.Eligibility.GetCol() == nil || len(check.Columns) == 0 {
			return result, moerr.NewInternalError(proc.Ctx, "invalid ODKU foreign-key metadata")
		}
		pos := check.Eligibility.GetCol().ColPos
		if pos < 0 || int(pos) >= len(rightTypes) || rightTypes[pos].Oid != types.T_bool {
			return result, moerr.NewInternalError(proc.Ctx, "invalid ODKU eligibility column")
		}
		for _, col := range check.Columns {
			if col.ColPos < 0 || int(col.ColPos) >= len(leftTypes) {
				return result, moerr.NewInternalError(proc.Ctx, "invalid ODKU foreign-key image column")
			}
			if !check.CheckUpdates {
				continue
			}
			t := leftTypes[col.ColPos]
			f, e := function.GetFunctionByName(proc.Ctx, "<=>", []types.Type{t, t})
			if e != nil {
				return result, e
			}
			typ := plan.Type{Id: int32(t.Oid), Width: t.Width, Scale: t.Scale}
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_bool)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: "<=>"},
					Args: []*plan.Expr{
						{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: col.ColPos}}},
						{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: col.ColPos}}},
					},
				}},
			}
			execs, e := hashbuild.NewExpressionExecutors(proc, []*plan.Expr{expr}, account)
			if e != nil {
				return result, e
			}
			result[i] = append(result[i], execs...)
		}
	}
	return result, nil
}

// FreeFKChanges releases all compiled comparisons. It may be called again.
func FreeFKChanges(checks [][]colexec.ExpressionExecutor) {
	for i := range checks {
		for j, exec := range checks[i] {
			if exec != nil {
				exec.Free()
				checks[i][j] = nil
			}
		}
	}
}

// EvaluateFKChanges overwrites eligible for the current update. Unaffected
// constraints are false; an affected composite key is true if any component
// differs under SQL's null-safe equality.
func EvaluateFKChanges(checks []*plan.OdkuForeignKeyCheck, execs [][]colexec.ExpressionExecutor, before, after *batch.Batch, eligible []bool, proc *process.Process) error {
	for i, check := range checks {
		eligible[i] = false
		if !check.CheckUpdates {
			continue
		}
		for _, exec := range execs[i] {
			v, err := exec.Eval(proc, []*batch.Batch{before, after}, nil)
			if err != nil {
				return err
			}
			if v.IsNull(0) || !vector.GetFixedAtNoTypeCheck[bool](v, 0) {
				eligible[i] = true
				break
			}
		}
	}
	return nil
}
