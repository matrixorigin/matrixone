// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// mysqlSpecialDisplayTypeForComparison recovers context-free ENUM/SET identity.
func mysqlSpecialDisplayTypeForComparison(expr *Expr) *plan.Type {
	return resolveMySQLSpecialComparisonType(expr, nil)
}

// resolveMySQLSpecialComparisonType permits explicit COLLATE adapters to wrap
// any provenance recognized by contextSource. Other casts remain boundaries.
func resolveMySQLSpecialComparisonType(
	expr *Expr,
	contextSource func(*Expr) *plan.Type,
) *plan.Type {
	if expr != nil && types.T(expr.Typ.Id).IsMySQLString() {
		if isEnumOrSetDisplayValueExpr(expr) {
			fn := expr.GetF()
			if len(fn.Args) == 2 && isEnumOrSetPlanType(&fn.Args[1].Typ) {
				return DeepCopyType(&fn.Args[1].Typ)
			}
			return nil
		}
		fn := expr.GetF()
		if fn != nil && fn.Func != nil && fn.Func.ObjName == "cast" && !fn.SyntaxExplicitCast &&
			expr.Typ.CollationCoercibilitySet && expr.Typ.CollationCoercibility == 0 && len(fn.Args) > 0 {
			source := resolveMySQLSpecialComparisonType(fn.Args[0], contextSource)
			if source == nil {
				return nil
			}
			selected, ok := collationForExpression(expr)
			if !ok || selected.rank != 0 {
				return nil
			}
			selected.apply(source)
			return source
		}
	}
	if contextSource != nil {
		return contextSource(expr)
	}
	return nil
}

func (bc *BindContext) setMySQLSpecialComparisonType(colPos int32, typ *plan.Type) {
	if bc.mysqlSpecialComparisonTypes == nil {
		bc.mysqlSpecialComparisonTypes = make(map[int32]*plan.Type)
	}
	bc.mysqlSpecialComparisonTypes[colPos] = DeepCopyType(typ)
}

func (bc *BindContext) mysqlSpecialComparisonTypeForProject(colPos int32) *plan.Type {
	if bc == nil {
		return nil
	}
	if typ, recorded := bc.mysqlSpecialComparisonTypes[colPos]; recorded {
		return DeepCopyType(typ)
	}
	orderType, recorded := bc.mysqlSpecialOrderTypes[colPos]
	if recorded && orderType == nil {
		return nil
	}
	if colPos >= 0 && int(colPos) < len(bc.projects) && bc.projects[colPos] != nil {
		if typ := bc.mysqlSpecialComparisonTypeForExpr(bc.projects[colPos]); typ != nil {
			return typ
		}
	}
	if recorded {
		return DeepCopyType(orderType)
	}
	return nil
}

func (bc *BindContext) mysqlSpecialComparisonTypeForExpr(expr *Expr) *plan.Type {
	return resolveMySQLSpecialComparisonType(expr, func(expr *Expr) *plan.Type {
		if bc == nil {
			return nil
		}
		if corr := expr.GetCorr(); corr != nil {
			if corr.Depth < 1 {
				return nil
			}
			depth := corr.Depth
			for owner := bc.parent; owner != nil; owner = owner.parent {
				if owner.binder == nil {
					continue
				}
				depth--
				if depth != 0 {
					continue
				}
				col := &Expr{Typ: expr.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{
					RelPos: corr.RelPos,
					ColPos: corr.ColPos,
				}}}
				return owner.mysqlSpecialComparisonTypeForExpr(col)
			}
			return nil
		}
		if col := expr.GetCol(); col != nil && col.ColPos >= 0 {
			if binding := bc.bindingByTag[col.RelPos]; binding != nil && int(col.ColPos) < len(binding.mysqlSpecialComparisonTypes) {
				if typ := binding.mysqlSpecialComparisonTypes[col.ColPos]; typ != nil {
					return DeepCopyType(typ)
				}
			}
			if bc.groupTag > 0 && col.RelPos == bc.groupTag && int(col.ColPos) < len(bc.groups) {
				groupExpr := bc.groups[col.ColPos]
				if groupExpr == nil {
					return nil
				}
				if groupCol := groupExpr.GetCol(); groupCol != nil && groupCol.RelPos == bc.groupTag {
					return nil
				}
				return bc.mysqlSpecialComparisonTypeForExpr(groupExpr)
			}
		}
		if sub := expr.GetSub(); sub != nil && sub.Typ == plan.SubqueryRef_SCALAR && sub.RowSize == 1 &&
			sub.NodeId >= 0 && bc.builder != nil && int(sub.NodeId) < len(bc.builder.ctxByNode) {
			subCtx := bc.builder.ctxByNode[sub.NodeId]
			if subCtx != nil && len(subCtx.results) == 1 {
				return subCtx.mysqlSpecialComparisonTypeForProject(0)
			}
		}
		return bc.mysqlSpecialOrderTypeForExpr(expr)
	})
}

func mysqlSpecialComparisonTypesCompatible(left, right *plan.Type) bool {
	return mysqlSpecialOrderTypesCompatible(left, right) &&
		left.Charset == right.Charset && left.CollationVersion == right.CollationVersion
}

func mysqlSpecialGeneralCIComparisonName(name string, arity int) bool {
	switch name {
	case "=", "<=>", "<>", "!=":
		return arity == 2
	case "in", "not_in":
		return arity == 2
	default:
		return false
	}
}

func mysqlSpecialComparisonOperandDomain(name string, args []*Expr) bool {
	if name == "in" || name == "not_in" {
		if args[0] == nil || args[0].GetList() != nil || args[1].GetList() == nil {
			return false
		}
		leftType := makeTypeByPlan2Expr(args[0])
		if !leftType.Oid.IsMySQLString() && !(args[0].Typ.Id == int32(types.T_any) && isNullLiteralExpr(args[0])) {
			return false
		}
		for _, member := range args[1].GetList().List {
			if member == nil {
				return false
			}
			typ := makeTypeByPlan2Expr(member)
			if typ.Oid.IsMySQLString() || typ.IsNumeric() ||
				(member.Typ.Id == int32(types.T_any) && isNullLiteralExpr(member)) {
				continue
			}
			return false
		}
		return true
	}
	for _, arg := range args {
		if arg == nil {
			return false
		}
		typ := makeTypeByPlan2Expr(arg)
		if !typ.Oid.IsMySQLString() && !(arg.Typ.Id == int32(types.T_any) && isNullLiteralExpr(arg)) {
			// Numeric and temporal consumers retain their existing conversion
			// rules. In particular, numeric ENUM/SET comparisons use storage.
			return false
		}
	}
	return true
}

func hasMySQLSpecialComparisonSource(args []*Expr, source func(*Expr) *plan.Type) bool {
	for _, arg := range args {
		if arg == nil {
			continue
		}
		if source(arg) != nil {
			return true
		}
		if list := arg.GetList(); list != nil {
			for _, member := range list.List {
				if source(member) != nil {
					return true
				}
			}
		}
	}
	return false
}

// rewriteMySQLSpecialGeneralCIComparison changes only equality-family string
// comparisons with ENUM/SET provenance. Ordering, LIKE, binary collations and
// numeric storage comparisons keep their existing operators and executors.
func rewriteMySQLSpecialGeneralCIComparison(
	ctx context.Context,
	name string,
	args []*Expr,
	source func(*Expr) *plan.Type,
) ([]*Expr, error) {
	if source == nil || !mysqlSpecialGeneralCIComparisonName(name, len(args)) ||
		!mysqlSpecialComparisonOperandDomain(name, args) ||
		!hasMySQLSpecialComparisonSource(args, source) {
		return args, nil
	}
	indexes := expressionCollationIndexes(name, len(args))
	selected, _, err := resolveExpressionCollation(ctx, name, expressionCollationCandidates(args, indexes))
	if err != nil {
		return nil, err
	}
	if selected.charset != uint32(types.CharsetUTF8) || selected.version != uint32(types.CollationVersionLegacy) {
		return args, nil
	}

	args, err = normalizeExpressionCollations(ctx, name, args)
	if err != nil {
		return nil, err
	}
	var keyOperand func(*Expr) (*Expr, error)
	keyOperand = func(expr *Expr) (*Expr, error) {
		if list := expr.GetList(); list != nil {
			copy := *expr
			items := make([]*Expr, len(list.List))
			for i, item := range list.List {
				items[i], err = keyOperand(item)
				if err != nil {
					return nil, err
				}
			}
			copy.Expr = &plan.Expr_List{List: &plan.ExprList{List: items}}
			return &copy, nil
		}
		if !types.T(expr.Typ.Id).IsMySQLString() {
			textType := types.New(types.T_varchar, types.MaxVarcharLen, 0)
			target := makePlan2Type(&textType)
			selected.apply(&target)
			expr, err = appendCastBeforeExpr(ctx, expr, target)
			if err != nil {
				return nil, err
			}
		}
		return BindFuncExprImplByPlanExpr(ctx, moMySQLSpecialGeneralCIKeyFun, []*Expr{expr})
	}
	result := append([]*Expr(nil), args...)
	for _, index := range indexes {
		result[index], err = keyOperand(args[index])
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (b *baseBinder) rewriteMySQLSpecialGeneralCIComparison(
	ctx context.Context, name string, args []*Expr,
) ([]*Expr, error) {
	if b == nil || b.ctx == nil {
		return args, nil
	}
	return rewriteMySQLSpecialGeneralCIComparison(
		ctx, name, args, b.ctx.mysqlSpecialComparisonTypeForExpr)
}
