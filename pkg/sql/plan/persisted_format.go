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
// older CNs, including after downgrade. Catalog consumers can evaluate locally
// without crossing remote pipeline capability gates. Keep historical overloads
// for functions whose newer transient-query semantics require new overload IDs.
// Call before constant folding so folded and non-folded catalog values agree.
func preservePersistedFormatCompatibility(ctx context.Context, expr *planpb.Expr) error {
	return planpb.VisitExprTree(expr, func(current *planpb.Expr) error {
		fn := current.GetF()
		if fn == nil || fn.Func == nil {
			return nil
		}
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		switch id {
		case function.FORMAT:
			if len(fn.Args) < 2 || fn.Args[0] == nil || !makeTypeByPlan2Expr(fn.Args[0]).IsNumeric() {
				return nil
			}
			arg, err := appendCastBeforeExpr(ctx, fn.Args[0], planpb.Type{
				Id: int32(types.T_varchar), Width: types.MaxVarcharLen})
			if err != nil {
				return err
			}
			fn.Args[0] = arg
		case function.SUBSTRING_INDEX:
			if len(fn.Args) < 3 || fn.Args[2] == nil {
				return nil
			}
			countType := makeTypeByPlan2Expr(fn.Args[2])
			if countType.Oid != types.T_decimal64 && countType.Oid != types.T_decimal128 {
				return nil
			}
			count, err := appendCastBeforeExpr(ctx, fn.Args[2], planpb.Type{Id: int32(types.T_float64)})
			if err != nil {
				return err
			}
			fn.Args[2] = count
			fn.Func.Obj = function.EncodeOverloadID(function.SUBSTRING_INDEX, 0)
		}
		return nil
	})
}
