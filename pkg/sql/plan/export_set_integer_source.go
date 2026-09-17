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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// bindExportSetIntegerSource preserves the producer's integer-evaluation
// contract. Conditional selectors delegate it to the selected value; ordinary
// REAL functions (ABS, COALESCE, IFNULL, arithmetic) retain checked conversion.
// It never changes the shared source or distributes EXPORT_SET's other args.
func bindExportSetIntegerSource(ctx context.Context, source *Expr) (*Expr, error) {
	if fn := source.GetF(); fn != nil && fn.Func != nil {
		name := fn.Func.ObjName
		if name == "cast" && len(fn.Args) == 2 && !fn.SyntaxExplicitCast {
			_, overload := function.DecodeOverloadID(fn.Func.Obj)
			if overload == 0 && types.T(source.Typ.Id).IsFloat() && makeTypeByPlan2Expr(fn.Args[0]).IsNumeric() {
				return bindExportSetIntegerSource(ctx, fn.Args[0])
			}
		}
		if (name == "if" || name == "case") && !fn.SyntaxIfNull {
			args := DeepCopyExprList(fn.Args)
			for i := range args {
				if numericFunctionArgKeepsContext(name, i, len(args)) {
					converted, err := bindExportSetIntegerSource(ctx, args[i])
					if err != nil {
						return nil, err
					}
					args[i] = converted
				}
			}
			return BindFuncExprImplByPlanExpr(ctx, name, args)
		}
	}
	overload := int32(1)
	typ := makeTypeByPlan2Expr(source)
	if typ.IsDecimal() {
		overload = 5
	}
	if typ.Oid.IsMySQLString() || typ.Oid.IsFloat() && exportSetRealLeafSaturates(source) {
		overload = 4
	}
	return appendCastBeforeExprWithOverload(ctx, source, makePlan2Type(&types.Type{Oid: types.T_int64}), overload)
}

func exportSetRealIntegerConversionNeeded(source *Expr) bool {
	if exportSetRealLeafSaturates(source) {
		return true
	}
	fn := source.GetF()
	return fn != nil && fn.Func != nil && !fn.SyntaxIfNull && (fn.Func.ObjName == "if" || fn.Func.ObjName == "case")
}

func exportSetRealLeafSaturates(source *Expr) bool {
	if source.GetCol() != nil || source.GetP() != nil {
		return true
	}
	if literal := source.GetLit(); literal != nil {
		if literal.Src != nil {
			return exportSetRealLeafSaturates(literal.Src)
		}
		return true
	}
	// Unary plus is an identity, unlike unary minus or binary arithmetic.
	if fn := source.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "unary_plus" && len(fn.Args) == 1 {
		return exportSetRealLeafSaturates(fn.Args[0])
	}
	if fn := source.GetF(); fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && len(fn.Args) == 2 && !fn.SyntaxExplicitCast {
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		return overload == 0 && exportSetRealLeafSaturates(fn.Args[0])
	}
	return false
}
