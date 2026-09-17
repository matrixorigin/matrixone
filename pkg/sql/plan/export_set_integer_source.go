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

// markPreparedExportSetNumericDefaults applies the integer consumer's default
// only inside its expression. A ColRef/SubqueryRef owns an independent producer;
// never walk into its query to constrain that producer's parameter defaults.
func markPreparedExportSetNumericDefaults(expr *Expr) {
	markPreparedExportSetNumericDefaultsInDomain(expr, types.T_decimal128)
}

func markPreparedExportSetNumericDefaultsInDomain(expr *Expr, domain types.T) {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return
	}
	if fn.Func.ObjName == "cast" {
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		if len(fn.Args) == 2 && overload == 0 && !fn.SyntaxExplicitCast {
			markPreparedExportSetNumericDefaultsInDomain(fn.Args[0], domain)
		}
		return
	}
	valueArg := func(i int) bool {
		return numericFunctionArgKeepsContext(fn.Func.ObjName, i, len(fn.Args)) ||
			preparedExportSetNumericStringAsReal(fn.Func.ObjName, i)
	}
	// A semantic REAL peer constrains the shared numeric context; a FLOAT
	// envelope around an exact peer does not. Ignore selector conditions.
	for i, arg := range fn.Args {
		if !valueArg(i) || containsDynamicParam(arg) {
			continue
		}
		peer := arg
		if source, provisional := provisionalNumericSource(peer); provisional {
			peer = source
		}
		if types.T(peer.Typ.Id).IsFloat() {
			domain = types.T_float64
		}
	}
	if fn.Func.ObjName == "abs" && len(fn.Args) == 1 {
		marker := fn.Args[0]
		for isImplicitPreparedParamCast(marker) {
			marker = marker.GetF().Args[0]
		}
		if marker.GetP() != nil {
			ensurePreparedNumericMetadata(marker).InitialNumericType = int32(domain)
		}
	}
	for i, arg := range fn.Args {
		if valueArg(i) {
			markPreparedExportSetNumericDefaultsInDomain(arg, domain)
		}
	}
}

// bindExportSetScalarIntegerSources lowers the scalar output's conversion at
// its consumer, including native conditional result branches. It does not
// change the query projection, row cardinality, or checked scalar functions.
func (b *baseBinder) bindExportSetScalarIntegerSources(source *Expr) (*Expr, bool, error) {
	if b.builder == nil || !types.T(source.Typ.Id).IsFloat() {
		return source, false, nil
	}
	if sub := source.GetSub(); sub != nil && sub.Typ == planpb.SubqueryRef_SCALAR {
		if sub.NodeId < 0 || int(sub.NodeId) >= len(b.builder.qry.Nodes) {
			return source, false, nil
		}
		node := b.builder.qry.Nodes[sub.NodeId]
		materialized := false
		if int(sub.NodeId) < len(b.builder.ctxByNode) {
			if ctx := b.builder.ctxByNode[sub.NodeId]; ctx != nil {
				materialized = len(ctx.bindingByTag) > 0 || len(ctx.aggregates) > 0
			}
		}
		if node != nil && len(node.ProjectList) == 1 && (materialized || exportSetRealLeafSaturates(node.ProjectList[0])) {
			converted, err := appendBitwiseAggregateCastBeforeExpr(b.GetContext(), source, makeSimplePlan2Type(types.T_int64))
			return converted, true, err
		}
		return source, false, nil
	}
	fn := source.GetF()
	if fn == nil || fn.Func == nil {
		return source, false, nil
	}
	name := fn.Func.ObjName
	if name == "cast" && len(fn.Args) == 2 && !fn.SyntaxExplicitCast {
		_, overload := function.DecodeOverloadID(fn.Func.Obj)
		if overload == 0 {
			converted, changed, err := b.bindExportSetScalarIntegerSources(fn.Args[0])
			if changed || err != nil {
				return converted, changed, err
			}
		}
	}
	if (name != "if" && name != "case") || fn.SyntaxIfNull {
		return source, false, nil
	}
	args := DeepCopyExprList(fn.Args)
	changed := false
	for i := range args {
		if !numericFunctionArgKeepsContext(name, i, len(args)) {
			continue
		}
		converted, argChanged, err := b.bindExportSetScalarIntegerSources(args[i])
		if err != nil {
			return nil, false, err
		}
		args[i] = converted
		changed = changed || argChanged
	}
	if !changed {
		return source, false, nil
	}
	bound, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
	if err != nil {
		return nil, false, err
	}
	preserveReboundFunctionMetadata(fn, bound.GetF())
	bound.PreparedNumeric = copyPreparedNumericMetadata(source.PreparedNumeric)
	return bound, true, nil
}

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
