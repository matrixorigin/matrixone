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

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// preservePersistedFormatCompatibility keeps catalog expressions executable by
// pre-v59 CNs, including after downgrade. Catalog consumers can evaluate locally
// without crossing the remote pipeline gate. Keep their historical string
// argument and approximate rounding contract regardless of the current protocol;
// only transient query expressions opt into typed numeric FORMAT semantics.
// Call before constant folding so folded and non-folded catalog values agree.
func preservePersistedFormatCompatibility(ctx context.Context, expr *planpb.Expr) error {
	return planpb.VisitExprTree(expr, func(current *planpb.Expr) error {
		fn := current.GetF()
		if fn == nil || fn.Func == nil || len(fn.Args) < 2 || fn.Args[0] == nil {
			return nil
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		if id != function.FORMAT || !makeTypeByPlan2Expr(fn.Args[0]).IsNumeric() {
			return nil
		}
		arg, err := appendCastBeforeExpr(ctx, fn.Args[0], planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen})
		if err != nil {
			return err
		}
		fn.Args[0] = arg
		return nil
	})
}
