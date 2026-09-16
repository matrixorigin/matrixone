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
		if id != function.FORMAT {
			return nil
		}
		stringType := planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
		if makeTypeByPlan2Expr(fn.Args[0]).IsNumeric() {
			arg, err := appendCastBeforeExpr(ctx, fn.Args[0], stringType)
			if err != nil {
				return err
			}
			fn.Args[0] = arg
		}
		precision := fn.Args[1]
		if cast := precision.GetF(); cast != nil && cast.Func != nil && len(cast.Args) > 0 {
			id, overload := function.DecodeOverloadID(cast.Func.Obj)
			if id == function.CAST && function.IsIntegerArgumentCastOverload(overload) {
				precision = cast.Args[0]
			}
		}
		if types.T(precision.Typ.Id) != types.T_varchar {
			arg, err := appendCastBeforeExpr(ctx, precision, stringType)
			if err != nil {
				return err
			}
			fn.Args[1] = arg
		} else {
			fn.Args[1] = precision
		}
		fn.Func.Obj = function.EncodeOverloadID(function.FORMAT, int32(len(fn.Args)-2))
		return nil
	})
}
