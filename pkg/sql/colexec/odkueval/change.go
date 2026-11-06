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

// CompileUpdateChanges uses the same typed SQL equality as the former planner
// no-op guard. Physical key identity and final storage equality are separate.
func CompileUpdateChanges(columns []int32, imageTypes []types.Type, proc *process.Process, account *mpool.AllocationAccount) ([]colexec.ExpressionExecutor, error) {
	exprs := make([]*plan.Expr, len(columns))
	for i, column := range columns {
		if column < 0 || int(column) >= len(imageTypes) {
			return nil, moerr.NewInternalError(proc.Ctx, "invalid ODKU change column")
		}
		t := imageTypes[column]
		f, err := function.GetFunctionByName(proc.Ctx, "<=>", []types.Type{t, t})
		if err != nil {
			return nil, err
		}
		typ := plan.Type{Id: int32(t.Oid), Width: t.Width, Scale: t.Scale}
		exprs[i] = &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: "<=>"},
			Args: []*plan.Expr{
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: column}}},
				{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: column}}},
			},
		}}}
	}
	return hashbuild.NewExpressionExecutors(proc, exprs, account)
}

// UpdateChanged compares the candidate with the immediately preceding image.
// Both inputs remain borrowed; no expression is re-evaluated here.
func UpdateChanged(execs []colexec.ExpressionExecutor, before, after *batch.Batch, proc *process.Process) (bool, error) {
	for _, exec := range execs {
		v, err := exec.Eval(proc, []*batch.Batch{before, after}, nil)
		if err != nil {
			return false, err
		}
		if v.IsNull(0) || !vector.GetFixedAtNoTypeCheck[bool](v, 0) {
			return true, nil
		}
	}
	return false, nil
}
