// Copyright 2022 Matrix Origin
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
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	mysqlparser "github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/util/errutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var kAlwaysFalseExpr = &plan.Expr{
	Typ: plan.Type{
		Id:          int32(types.T_bool),
		Width:       1,
		Scale:       0,
		NotNullable: true,
	},
	Expr: &plan.Expr_Lit{
		Lit: &plan.Literal{
			Value: &plan.Literal_Bval{
				Bval: false,
			},
		},
	},
}

func (b *baseBinder) baseBindExpr(astExpr tree.Expr, depth int32, isRoot bool) (expr *Expr, err error) {
	if b.numericParamType != nil && !b.isNumericContextNode(astExpr, depth) {
		paramType := b.numericParamType
		b.numericParamType = nil
		defer func() { b.numericParamType = paramType }()
		return b.impl.BindExpr(astExpr, depth, isRoot)
	}

	switch exprImpl := astExpr.(type) {
	case *tree.NumVal:
		expr, err = b.bindNumVal(exprImpl, b.defaultValueBindType())
	case *tree.TimeUnitExpr:
		numVal := tree.NewNumVal(exprImpl.Unit, exprImpl.Unit, false, tree.P_char)
		expr, err = b.bindNumVal(numVal, b.defaultValueBindType())
	case *tree.ParenExpr:
		expr, err = b.impl.BindExpr(exprImpl.Expr, depth, isRoot)

	case *tree.OrExpr:
		expr, err = b.bindFuncExprImplByAstExpr("or", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.NotExpr:
		if subqueryAst, ok := exprImpl.Expr.(*tree.Subquery); ok {
			expr, err = b.impl.BindSubquery(subqueryAst, isRoot)
			if err != nil {
				return
			}

			subquery := expr.GetSub()
			if subquery.Typ == plan.SubqueryRef_EXISTS {
				subquery.Typ = plan.SubqueryRef_NOT_EXISTS
			}
		} else {
			expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
			if err != nil {
				return
			}

			expr, err = BindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{expr})
		}

	case *tree.AndExpr:
		expr, err = b.bindFuncExprImplByAstExpr("and", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.UnaryExpr:
		expr, err = b.bindUnaryExpr(exprImpl, depth, isRoot)

	case *tree.BinaryExpr:
		expr, err = b.bindBinaryExpr(exprImpl, depth, isRoot)

	case *tree.ComparisonExpr:
		expr, err = b.bindComparisonExpr(exprImpl, depth, isRoot)

	case *tree.FuncExpr:
		expr, err = b.bindFuncExpr(exprImpl, depth, isRoot)

	case *tree.RangeCond:
		expr, err = b.bindRangeCond(exprImpl, depth, isRoot)

	case *tree.UnresolvedName:
		if udfArg, ok := b.bindSQLUdfArgument(exprImpl); ok {
			expr = udfArg
			break
		}
		// check existence
		if b.GetContext() != nil && b.GetContext().Value(defines.InSp{}) != nil && b.GetContext().Value(defines.InSp{}).(bool) {
			tmpScope, scopeOK := b.GetContext().Value(defines.VarScopeKey{}).(*[]map[string]interface{})
			typeScopes, typeScopeOK := b.GetContext().Value(defines.VarScopeTypeKey{}).(*[]map[string]plan.Type)
			if scopeOK && tmpScope != nil {
				name := strings.ToLower(exprImpl.ColName())
				for i := len(*tmpScope) - 1; i >= 0; i-- {
					curScope := (*tmpScope)[i]
					if _, ok := curScope[name]; ok {
						typ := types.T_text.ToType()
						expr = &Expr{
							Typ: makePlan2Type(&typ),
							Expr: &plan.Expr_V{
								V: &plan.VarRef{
									Name:   name,
									System: false,
									Global: false,
								},
							},
						}
						if typeScopeOK && typeScopes != nil && i < len(*typeScopes) {
							if targetType, ok := (*typeScopes)[i][name]; ok {
								expr, err = appendCastBeforeExpr(b.GetContext(), expr, targetType)
							}
							if err != nil {
								return nil, err
							}
						}
						err = nil
						return
					}
				}
			}
		}
		expr, err = b.impl.BindColRef(exprImpl, depth, isRoot)

	case *tree.SerialExtractExpr:
		expr, err = b.bindFuncExprImplByAstExpr("serial_extract", []tree.Expr{astExpr}, depth)

	case *tree.CastExpr:
		var typ Type
		typ, err = getTypeFromAst(b.GetContext(), exprImpl.Type)
		if err != nil {
			return
		}
		parentParamType := b.numericParamType
		b.numericParamType = nil
		if b.mysqlSpecialTypeInAst(exprImpl.Expr) && makeTypeByPlan2Type(typ).IsNumeric() {
			expr, err = b.bindWithRawMySQLSpecialTypes(func() (*Expr, error) {
				return b.impl.BindExpr(exprImpl.Expr, depth, false)
			})
		} else if isNumericArithmeticRoot(exprImpl.Expr) ||
			b.isGenericNumericFunctionRoot(exprImpl.Expr, depth, &typ) {
			expr, err = b.bindNumericExprWithContext(exprImpl.Expr, depth, &typ)
		} else {
			expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
		}
		b.numericParamType = parentParamType
		if err != nil {
			return
		}
		// ENUM and SET normally bind as their display strings.  An explicit
		// numeric cast is, however, a numeric operand contract in MySQL and
		// must start from the stored ordinal/bitmap instead of that string.
		if makeTypeByPlan2Type(typ).IsNumeric() {
			expr, _ = storedMySQLSpecialTypeExpr(expr)
		}
		if b.builder != nil {
			var rewritten bool
			expr, rewritten, err = b.builder.rewriteProjectedMySQLSpecialTypeDisplayCast(expr, expr, typ)
			if err != nil {
				return
			}
			if rewritten {
				return
			}
		}
		if useExplicitCastOverload(exprImpl.Type) {
			expr, err = appendExplicitCastBeforeExpr(b.GetContext(), expr, typ)
		} else {
			expr, err = appendSyntaxExplicitCastBeforeExpr(b.GetContext(), expr, typ)
		}

	case *tree.BitCastExpr:
		expr, err = b.bindFuncExprImplByAstExpr("bit_cast", []tree.Expr{astExpr}, depth)

	case *tree.IsNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotNullExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnotnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsUnknownExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotUnknownExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnotnull", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsTrueExpr:
		expr, err = b.bindFuncExprImplByAstExpr("istrue", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotTrueExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnottrue", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsFalseExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isfalse", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.IsNotFalseExpr:
		expr, err = b.bindFuncExprImplByAstExpr("isnotfalse", []tree.Expr{exprImpl.Expr}, depth)

	case *tree.Tuple:
		exprs := make([]*Expr, 0, len(exprImpl.Exprs))
		var planItem *Expr
		for _, astItem := range exprImpl.Exprs {
			planItem, err = b.impl.BindExpr(astItem, depth, false)
			if err != nil {
				return
			}
			exprs = append(exprs, planItem)
		}
		expr = &Expr{
			Expr: &plan.Expr_List{
				List: &plan.ExprList{
					List: exprs,
				},
			},
			Typ: plan.Type{
				Id: int32(types.T_tuple),
			},
		}

	case *tree.CaseExpr:
		expr, err = b.bindCaseExpr(exprImpl, depth, isRoot)

	case *tree.IntervalExpr:
		err = moerr.NewNYIf(b.GetContext(), "expr interval'%v'", exprImpl)

	case *tree.XorExpr:
		expr, err = b.bindFuncExprImplByAstExpr("xor", []tree.Expr{exprImpl.Left, exprImpl.Right}, depth)

	case *tree.Subquery:
		expr, err = b.impl.BindSubquery(exprImpl, isRoot)

	case *tree.DefaultVal:
		return &Expr{
			Typ: plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: true,
			},
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: false,
					Value: &plan.Literal_Defaultval{
						Defaultval: true,
					},
				},
			},
		}, nil
	case *tree.UpdateVal:
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: false,
					Value: &plan.Literal_UpdateVal{
						UpdateVal: true,
					},
				},
			},
		}, nil
	case *tree.MaxValue:
		return &Expr{
			Expr: &plan.Expr_Max{
				Max: &MaxValue{
					Value: "maxvalue",
				},
			},
		}, nil
	case *tree.VarExpr:
		expr, err = b.baseBindVar(exprImpl, depth, isRoot)

	case *tree.ParamExpr:
		if b.builder == nil || !b.builder.isPrepareStatement {
			err = moerr.NewInvalidInput(b.GetContext(), "only prepare statement can use ? expr")
		} else {
			expr, err = b.baseBindParam(exprImpl, depth, isRoot)
		}

	case *tree.StrVal:
		err = moerr.NewNYIf(b.GetContext(), "expr str'%v'", exprImpl)

	case *tree.ExprList:
		err = moerr.NewNYIf(b.GetContext(), "expr plan.ExprList'%v'", exprImpl)

	case tree.UnqualifiedStar:
		// select * from table
		// * should only appear in SELECT clause
		err = moerr.NewInvalidInput(b.GetContext(), "SELECT clause contains unqualified star")

	case *tree.FullTextMatchExpr:
		expr, err = b.bindFullTextMatchExpr(exprImpl, depth, isRoot)
	default:
		err = moerr.NewNYIf(b.GetContext(), "expr '%+v'", exprImpl)
	}

	return
}

func useExplicitCastOverload(typ tree.ResolvableTypeReference) bool {
	t, ok := typ.(*tree.T)
	if !ok {
		return false
	}
	internal := t.InternalType
	switch defines.MysqlType(internal.Oid) {
	case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE,
		defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
		return true
	case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING,
		defines.MYSQL_TYPE_STRING, defines.MYSQL_TYPE_TEXT,
		defines.MYSQL_TYPE_TINY_BLOB, defines.MYSQL_TYPE_MEDIUM_BLOB,
		defines.MYSQL_TYPE_LONG_BLOB, defines.MYSQL_TYPE_BLOB:
		// Character and binary casts are semantic boundaries even though the
		// value conversion is shared with ordinary overload-coercion casts.
		return true
	case defines.MYSQL_TYPE_LONGLONG:
		family := strings.ToLower(internal.FamilyString)
		return family == "signed" || family == "integer" ||
			(internal.Unsigned && (family == "" || family == "unsigned"))
	default:
		return false
	}
}

func unwrapParenExpr(astExpr tree.Expr) tree.Expr {
	for {
		paren, ok := astExpr.(*tree.ParenExpr)
		if !ok {
			return astExpr
		}
		astExpr = paren.Expr
	}
}

func (b *baseBinder) baseBindParam(astExpr *tree.ParamExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	typ := types.T_text.ToType()
	param := &Expr{
		Typ: makePlan2Type(&typ),
		Expr: &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: int32(astExpr.Offset),
			},
		},
	}
	if b.numericParamType != nil {
		return appendCastBeforeExpr(b.GetContext(), param, *b.numericParamType)
	}
	return param, nil
}

func (b *baseBinder) baseBindVar(astExpr *tree.VarExpr, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	typ := types.T_text.ToType()
	if !astExpr.System {
		if resolved, ok := b.resolveUserVariableType(astExpr); ok {
			typ = makeTypeByPlan2Type(resolved)
		}
	}
	variable := &Expr{
		Typ: makePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   astExpr.Name,
				System: astExpr.System,
				Global: astExpr.Global,
			},
		},
	}
	if !astExpr.System && b.numericParamType != nil {
		// User variables are text-backed when their assignment came from a
		// string.  Numeric expressions use MySQL's prefix conversion for such
		// values (for example, '12abc' -> 12), rather than the strict implicit
		// cast which rejects the trailing text.
		if isStringBackedType(typ) {
			return appendExplicitCastBeforeExpr(b.GetContext(), variable, *b.numericParamType)
		}
		return appendCastBeforeExpr(b.GetContext(), variable, *b.numericParamType)
	}
	return variable, nil
}

func isStringBackedType(typ types.Type) bool {
	switch typ.Oid {
	case types.T_char, types.T_varchar, types.T_text,
		types.T_binary, types.T_varbinary, types.T_blob:
		return true
	default:
		return false
	}
}

func (b *baseBinder) resolveUserVariableType(expr *tree.VarExpr) (Type, bool) {
	if b.builder == nil || b.builder.compCtx == nil {
		return Type{}, false
	}
	resolver, ok := b.builder.compCtx.(UserVariableTypeResolver)
	if !ok {
		return Type{}, false
	}
	typ, err := resolver.ResolveVariableType(expr.Name, expr.System, expr.Global)
	if err != nil || typ.Id == 0 {
		return Type{}, false
	}
	return typ, true
}

// resolveUserVariableNumericType returns the numeric type to use when a user
// variable participates in an arithmetic expression. Variables assigned a
// numeric value already carry that assignment type. Text-backed variables are
// evaluated through one value-independent floating-point target: their
// contents can change after PREPARE, so selecting BIGINT/DECIMAL from the
// value observed during binding would freeze the wrong cast in the prepared
// plan. The explicit cast overload consumes MySQL's numeric prefix at runtime.
func (b *baseBinder) resolveUserVariableNumericType(expr *tree.VarExpr) (Type, bool) {
	if typ, ok := b.resolveUserVariableType(expr); ok {
		resolved := makeTypeByPlan2Type(typ)
		if resolved.IsNumeric() {
			return typ, true
		}
		if isStringBackedType(resolved) {
			return makeSimplePlan2Type(types.T_float64), true
		}
	}
	if b.builder == nil || b.builder.compCtx == nil {
		return Type{}, false
	}
	value, err := b.builder.compCtx.ResolveVariable(expr.Name, expr.System, expr.Global)
	if err != nil {
		return Type{}, false
	}
	switch value.(type) {
	case string, []byte:
		// The variable is text-backed but its value is resolved again when a
		// prepared plan executes. Keep the target stable across all contents.
		return makeSimplePlan2Type(types.T_float64), true
	default:
		return Type{}, false
	}
}

const (
	TimeWindowStart = "_wstart"
	TimeWindowEnd   = "_wend"
)

func (b *baseBinder) baseBindColRef(astExpr *tree.UnresolvedName, depth int32, isRoot bool) (expr *plan.Expr, err error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "ambiguous column reference '%v'", astExpr.ColNameOrigin())
	}

	col := astExpr.ColName()
	table := astExpr.TblName()
	db := astExpr.DbName()
	name := semanticAstKey(astExpr)

	if b.ctx.timeTag > 0 && (col == TimeWindowStart || col == TimeWindowEnd) {
		colPos := int32(len(b.ctx.times))
		typ := plan.Type{Id: int32(types.T_timestamp), NotNullable: true}
		if b.ctx.timeBoundaryType != nil {
			typ = *DeepCopyType(b.ctx.timeBoundaryType)
			typ.NotNullable = true
		}
		expr = &plan.Expr{
			Typ: typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: b.ctx.timeTag,
					ColPos: colPos,
					Name:   col,
				},
			},
		}
		b.ctx.timeByAst[name] = colPos
		b.ctx.times = append(b.ctx.times, expr)
		return
	}

	relPos := NotFound
	colPos := NotFound
	var typ *plan.Type
	localErrCtx := errutil.ContextWithNoReport(b.GetContext(), true)

	if len(table) == 0 {
		if binding, ok := b.ctx.bindingByCol[col]; ok {
			if binding != nil {
				relPos = binding.tag
				colPos = binding.colIdByName[col]
				typ = DeepCopyType(binding.types[colPos])
				table = binding.table
			} else {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "ambiguous column reference '%v'", name)
			}
		} else if selectItem, ok := b.ctx.aliasMap[col]; ok {
			// Handle UNION aliases: aliasMap entry exists but column is not in bindingByCol
			// This happens when ORDER BY references a UNION result column inside a function
			if int(selectItem.idx) < len(b.ctx.projects) {
				// Get the tag from the existing project expression
				// In UNION context, ctx.projects[i] references the UNION node's output (lastTag)
				// We need to use the same tag, not ctx.projectTag
				projExpr := b.ctx.projects[selectItem.idx]
				if colExpr, ok := projExpr.Expr.(*plan.Expr_Col); ok {
					return &plan.Expr{
						Typ: projExpr.Typ,
						Expr: &plan.Expr_Col{
							Col: &plan.ColRef{
								RelPos: colExpr.Col.RelPos,
								ColPos: colExpr.Col.ColPos,
								Name:   col,
							},
						},
					}, nil
				}
				// Fallback to projectTag if the project expression is not a column reference
				return &plan.Expr{
					Typ: projExpr.Typ,
					Expr: &plan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: b.ctx.projectTag,
							ColPos: selectItem.idx,
							Name:   col,
						},
					},
				}, nil
			}
			err = moerr.NewBadFieldErrorf(localErrCtx, "invalid input: column %s does not exist", name)
		} else {
			err = moerr.NewBadFieldErrorf(localErrCtx, "invalid input: column %s does not exist", name)
		}
	} else {
		var binding *Binding
		var ok bool
		// try resolve table in current context
		if binding, ok = b.ctx.bindingByTable[table]; !ok {
			// if remap option exists, try with db-qualified name
			if b.ctx.remapOption != nil {
				if len(db) == 0 {
					db = b.builder.compCtx.DefaultDatabase()
				}
				binding, ok = b.ctx.bindingByTable[db+"."+table]
			}
		}
		if ok {
			colPos = binding.FindColumn(col)
			if colPos == AmbiguousName {
				return nil, moerr.NewInvalidInputf(b.GetContext(), "ambiguous column reference '%v'", name)
			}
			if colPos != NotFound {
				typ = DeepCopyType(binding.types[colPos])
				relPos = binding.tag
			} else {
				err = moerr.NewBadFieldErrorf(localErrCtx, "invalid input: column '%s' does not exist", name)
			}
		} else {
			err = moerr.NewInvalidInputf(localErrCtx, "missing FROM-clause entry for table '%v'", table)
		}
	}

	if groupPos, ok := b.correlatedGroupByColPos(depth, name, table, col); ok {
		expr = &plan.Expr{
			Typ: b.ctx.groupOutputType(groupPos),
			Expr: &plan.Expr_Corr{
				Corr: &plan.CorrColRef{
					RelPos: b.ctx.groupTag,
					ColPos: groupPos,
					Depth:  depth,
				},
			},
		}
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
		}
		return
	}

	if colPos != NotFound {
		b.boundCols = append(b.boundCols, boundColumn{
			name:      table + "." + col,
			relation:  relPos,
			columnPos: colPos,
		})
	}

	preserveSpecialValue := typ != nil && b.mysqlSpecialTargetType != nil &&
		typ.Enumvalues == b.mysqlSpecialTargetType.Enumvalues &&
		((isEnumPlanType(typ) && isEnumPlanType(b.mysqlSpecialTargetType)) ||
			(isSetPlanType(typ) && isSetPlanType(b.mysqlSpecialTargetType)))
	// ENUM and SET have distinct storage and display representations. Keep their
	// display value by default. Numeric and bitwise expression binders explicitly
	// enable raw storage binding so they follow MySQL's numeric semantics.
	if !b.bindRawMySQLSpecialType && !preserveSpecialValue && isEnumOrSetPlanType(typ) {
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
			return
		}
		indexToValueFun, _, _, funErr := mysqlSpecialTypeFuncNames(typ)
		if funErr != nil {
			return nil, funErr
		}
		astArgs := []tree.Expr{
			tree.NewNumVal(typ.Enumvalues, typ.Enumvalues, false, tree.P_char),
		}

		// bind ast function's args
		args := make([]*Expr, len(astArgs)+1)
		for idx, arg := range astArgs {
			if idx == len(args)-1 {
				continue
			}
			expr, err := b.impl.BindExpr(arg, depth, false)
			if err != nil {
				return nil, err
			}
			args[idx] = expr
		}
		args[len(args)-1] = &Expr{
			Typ: *typ,
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: relPos,
					ColPos: colPos,
					Name:   col,
				},
			},
		}

		return BindFuncExprImplByPlanExpr(b.GetContext(), indexToValueFun, args)
	}

	if colPos != NotFound {
		expr = &plan.Expr{
			Typ: *typ,
		}

		if depth == 0 {
			expr.Expr = &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: relPos,
					ColPos: colPos,
					Name:   col,
				},
			}
		} else {
			corrRelPos, corrColPos := relPos, colPos
			if aggRelPos, aggColPos, ok := b.correlatedFullGroupByAggregateRef(relPos, colPos, col, typ); ok {
				corrRelPos, corrColPos = aggRelPos, aggColPos
			}
			expr.Expr = &plan.Expr_Corr{
				Corr: &plan.CorrColRef{
					RelPos: corrRelPos,
					ColPos: corrColPos,
					Depth:  depth,
				},
			}
		}
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
		}
		return
	}

	parent := b.ctx.parent
	for parent != nil && parent.binder == nil {
		parent = parent.parent
	}

	if parent == nil {
		if err != nil {
			errutil.ReportError(b.GetContext(), err)
		}
		return
	}

	if b.ctx.aggregateInputParent != nil && parent == b.ctx.aggregateInputParent {
		if targetBinder, ok := parent.binder.(interface{ setAggregateInputCorrelation(bool) bool }); ok {
			previousAggregateInputCorrelation := targetBinder.setAggregateInputCorrelation(true)
			defer targetBinder.setAggregateInputCorrelation(previousAggregateInputCorrelation)
		}
	}
	expr, err = parent.binder.BindColRef(astExpr, depth+1, isRoot)

	if err == nil {
		b.ctx.isCorrelated = true
	}

	return
}

func (b *baseBinder) correlatedGroupByColPos(depth int32, astName, table, col string) (int32, bool) {
	if depth == 0 || b.ctx == nil || len(b.ctx.groupByAst) == 0 {
		return 0, false
	}
	if pos, ok := b.ctx.groupByAst[astName]; ok && int(pos) < len(b.ctx.groups) {
		return pos, true
	}
	if table != "" {
		if pos, ok := b.ctx.groupByAst[table+"."+col]; ok && int(pos) < len(b.ctx.groups) {
			return pos, true
		}
	}
	return 0, false
}

func (b *baseBinder) correlatedFullGroupByAggregateRef(relPos, colPos int32, col string, typ *plan.Type) (int32, int32, bool) {
	if b == nil || b.ctx == nil || b.builder == nil || typ == nil || !b.builder.mysqlFullGroupByCompat {
		return 0, 0, false
	}
	if b.aggregateInputCorrelation {
		return 0, 0, false
	}
	if !b.ctx.aggregateQueryForFullGroupBy() {
		return 0, 0, false
	}
	if groupByContainsColumn(b.ctx, relPos, colPos) {
		return 0, 0, false
	}
	binding := b.ctx.bindingByTag[relPos]
	if binding == nil || !b.builder.mysqlFullGroupByAllowsColumn(b.ctx, binding, colPos) {
		return 0, 0, false
	}

	source := &plan.Expr{
		Typ: *typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relPos,
				ColPos: colPos,
				Name:   col,
			},
		},
	}
	agg, err := BindFuncExprImplByPlanExpr(b.builder.compCtx.GetContext(), "any_value", []*plan.Expr{source})
	if err != nil {
		return 0, 0, false
	}
	aggPos := int32(len(b.ctx.aggregates))
	b.ctx.aggregates = append(b.ctx.aggregates, agg)
	return b.ctx.aggregateTag, aggPos, true
}

func (b *baseBinder) setAggregateInputCorrelation(v bool) bool {
	previous := b.aggregateInputCorrelation
	b.aggregateInputCorrelation = v
	return previous
}

func (b *baseBinder) corrColRefTargetsGroup(corr *plan.CorrColRef) bool {
	ctx := b.corrColRefTargetContext(corr)
	return ctx != nil && ctx.groupTag > 0 && corr.RelPos == ctx.groupTag
}

func (b *baseBinder) corrColRefTargetsCurrentGroup(corr *plan.CorrColRef) bool {
	return corr != nil && b.ctx != nil && b.ctx.groupTag > 0 && corr.RelPos == b.ctx.groupTag
}

func (b *baseBinder) corrColRefAllowedByCurrentQuery(corr *plan.CorrColRef) bool {
	return b.corrColRefAllowedByQueryContext(b.ctx, corr)
}

func (b *baseBinder) corrColRefTargetsCurrentQueryInput(corr *plan.CorrColRef) bool {
	if b == nil || b.ctx == nil || corr == nil {
		return false
	}
	binding, ok := b.ctx.bindingByTag[corr.RelPos]
	return ok && binding != nil
}

func (b *baseBinder) corrColRefTargetsAggregateInputParent(corr *plan.CorrColRef) bool {
	if b == nil || b.ctx == nil || b.ctx.aggregateInputParent == nil || corr == nil {
		return false
	}
	return b.corrColRefTargetContext(corr) == b.ctx.aggregateInputParent
}

func (b *baseBinder) corrColRefAllowedByTargetQuery(corr *plan.CorrColRef) bool {
	return b.corrColRefAllowedByQueryContext(b.corrColRefTargetContext(corr), corr)
}

func (b *baseBinder) corrColRefAllowedByQueryContext(ctx *BindContext, corr *plan.CorrColRef) bool {
	if ctx == nil || corr == nil {
		return false
	}
	if ctx.aggregateTag > 0 && corr.RelPos == ctx.aggregateTag {
		return true
	}
	binding, ok := ctx.bindingByTag[corr.RelPos]
	if !ok || binding == nil {
		return false
	}
	if !ctx.aggregateQueryForFullGroupBy() {
		return true
	}
	return b.builder.mysqlFullGroupByAllowsColumn(ctx, binding, corr.ColPos)
}

func (b *baseBinder) corrColRefTargetContext(corr *plan.CorrColRef) *BindContext {
	if corr == nil {
		return nil
	}
	ctx := b.ctx
	for depth := int32(0); depth < corr.Depth && ctx != nil; depth++ {
		ctx = ctx.parent
	}
	return ctx
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (*Expr, error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInput(b.GetContext(), "field reference doesn't support SUBQUERY")
	}
	subCtx := NewBindContext(b.builder, b.ctx)
	if b.subqueryInAggregateInput {
		subCtx.aggregateInputParent = b.ctx
	}
	if b.numericSubqueryTarget != nil && !astExpr.Exists {
		subCtx.numericProjectionTypes = []Type{*b.numericSubqueryTarget}
	}

	// A subquery is a nested SELECT and must not inherit the outer FOR UPDATE
	// state. MySQL only locks rows in the outer query; rows reached through
	// EXISTS/IN/scalar subqueries are not locked unless the subquery itself
	// also specifies FOR UPDATE.
	savedIsForUpdate := b.builder.isForUpdate
	b.builder.isForUpdate = false
	defer func() {
		b.builder.isForUpdate = savedIsForUpdate
	}()

	var nodeID int32
	var err error
	switch subquery := astExpr.Select.(type) {
	case *tree.ParenSelect:
		nodeID, err = b.builder.bindSelect(subquery.Select, subCtx, false)
		if err != nil {
			return nil, err
		}
	case *tree.Select:
		nodeID, err = b.builder.bindSelect(subquery, subCtx, false)
		if err != nil {
			return nil, err
		}

	default:
		return nil, moerr.NewNYIf(b.GetContext(), "unsupported select statement: %s", tree.String(astExpr, dialect.MYSQL))
	}

	rowSize := int32(len(subCtx.results))

	returnExpr := &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_tuple),
		},
		Expr: &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId:  nodeID,
				RowSize: rowSize,
			},
		},
	}

	if astExpr.Exists {
		returnExpr.Typ = plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: true,
		}
		returnExpr.GetSub().Typ = plan.SubqueryRef_EXISTS
	} else if rowSize == 1 {
		returnExpr.Typ = subCtx.results[0].Typ
	}

	return returnExpr, nil
}

func (b *baseBinder) bindCaseExpr(astExpr *tree.CaseExpr, depth int32, isRoot bool) (*Expr, error) {
	args := make([]tree.Expr, 0, len(astExpr.Whens)+1)
	caseExist := astExpr.Expr != nil

	for _, whenExpr := range astExpr.Whens {
		if caseExist {
			newCandExpr := tree.NewComparisonExpr(tree.EQUAL, astExpr.Expr, whenExpr.Cond)
			args = append(args, newCandExpr)
		} else {
			// An untyped parameter is represented as TEXT until its surrounding
			// expression provides a type.  CASE WHEN is a boolean context, so
			// make that contract explicit before the CASE overload checker sees
			// the parameter.  Without this cast `CASE WHEN ?` is rejected as
			// [TEXT ...], even though the same expression with a literal works.
			args = append(args, bindCaseConditionParam(whenExpr.Cond))
		}
		args = append(args, whenExpr.Val)
	}

	if astExpr.Else != nil {
		args = append(args, astExpr.Else)
	} else {
		args = append(args, tree.NewNumVal("", "", false, tree.P_null))
	}

	return b.bindFuncExprImplByAstExpr("case", args, depth)
}

func bindCaseConditionParam(expr tree.Expr) tree.Expr {
	if _, ok := unwrapParenExpr(expr).(*tree.ParamExpr); !ok {
		return expr
	}
	return tree.NewCastExpr(expr, tree.TYPE_BOOL)
}

func (b *baseBinder) bindRangeCond(astExpr *tree.RangeCond, depth int32, isRoot bool) (*Expr, error) {
	bind := func() (*Expr, error) {
		if astExpr.Not {
			// rewrite 'col not between 1, 20' to 'col < 1 or col > 20'
			newLeftExpr := tree.NewComparisonExpr(tree.LESS_THAN, astExpr.Left, astExpr.From)
			newRightExpr := tree.NewComparisonExpr(tree.GREAT_THAN, astExpr.Left, astExpr.To)
			return b.bindFuncExprImplByAstExpr("or", []tree.Expr{newLeftExpr, newRightExpr}, depth)
		} else {
			if _, ok := astExpr.Left.(*tree.Tuple); ok {
				newLeftExpr := tree.NewComparisonExpr(tree.GREAT_THAN_EQUAL, astExpr.Left, astExpr.From)
				newRightExpr := tree.NewComparisonExpr(tree.LESS_THAN_EQUAL, astExpr.Left, astExpr.To)
				return b.bindFuncExprImplByAstExpr("and", []tree.Expr{newLeftExpr, newRightExpr}, depth)
			}

			return b.bindFuncExprImplByAstExpr("between", []tree.Expr{astExpr.Left, astExpr.From, astExpr.To}, depth)
		}
	}
	if b.mysqlSpecialTypeInAst(astExpr.Left) &&
		b.mysqlSpecialTypeNumericContext(astExpr.From) &&
		b.mysqlSpecialTypeNumericContext(astExpr.To) {
		return b.bindWithRawMySQLSpecialTypes(bind)
	}
	return bind()
}

func (b *baseBinder) bindUnaryExpr(astExpr *tree.UnaryExpr, depth int32, isRoot bool) (*Expr, error) {
	if (astExpr.Op == tree.UNARY_PLUS || astExpr.Op == tree.UNARY_MINUS || astExpr.Op == tree.UNARY_TILDE) &&
		b.mysqlSpecialTypeInAst(astExpr.Expr) {
		return b.bindWithRawMySQLSpecialTypes(func() (*Expr, error) {
			return b.bindUnaryExprWithCurrentContext(astExpr, depth)
		})
	}
	if (astExpr.Op == tree.UNARY_MINUS || astExpr.Op == tree.UNARY_PLUS) && b.numericParamType == nil {
		return b.bindNumericExprWithDefaultContext(astExpr, depth, b.defaultNumericOuterType())
	}
	return b.bindUnaryExprWithCurrentContext(astExpr, depth)
}

func (b *baseBinder) bindUnaryExprWithCurrentContext(astExpr *tree.UnaryExpr, depth int32) (*Expr, error) {
	switch astExpr.Op {
	case tree.UNARY_MINUS:
		return b.bindFuncExprImplByAstExpr("unary_minus", []tree.Expr{astExpr.Expr}, depth)
	case tree.UNARY_PLUS:
		return b.bindFuncExprImplByAstExpr("unary_plus", []tree.Expr{astExpr.Expr}, depth)
	case tree.UNARY_TILDE:
		return b.bindFuncExprImplByAstExpr("unary_tilde", []tree.Expr{astExpr.Expr}, depth)
	case tree.UNARY_MARK:
		return b.bindFuncExprImplByAstExpr("unary_mark", []tree.Expr{astExpr.Expr}, depth)
	}
	return nil, moerr.NewNYIf(b.GetContext(), "'%v'", astExpr)
}

func (b *baseBinder) bindBinaryExpr(astExpr *tree.BinaryExpr, depth int32, isRoot bool) (*Expr, error) {
	if (isNumericBinaryOp(astExpr.Op) || isBitwiseBinaryOp(astExpr.Op)) &&
		(b.mysqlSpecialTypeInAst(astExpr.Left) || b.mysqlSpecialTypeInAst(astExpr.Right)) {
		return b.bindWithRawMySQLSpecialTypes(func() (*Expr, error) {
			if isNumericBinaryOp(astExpr.Op) && b.numericParamType == nil {
				return b.bindNumericExprWithDefaultContext(astExpr, depth, b.defaultNumericOuterType())
			}
			return b.bindBinaryExprWithCurrentContext(astExpr, depth)
		})
	}
	if isNumericBinaryOp(astExpr.Op) && b.numericParamType == nil {
		return b.bindNumericExprWithDefaultContext(astExpr, depth, b.defaultNumericOuterType())
	}
	return b.bindBinaryExprWithCurrentContext(astExpr, depth)
}

func isBitwiseBinaryOp(op tree.BinaryOp) bool {
	switch op {
	case tree.BIT_XOR, tree.BIT_OR, tree.BIT_AND, tree.LEFT_SHIFT, tree.RIGHT_SHIFT:
		return true
	default:
		return false
	}
}

func (b *baseBinder) bindBinaryExprWithCurrentContext(astExpr *tree.BinaryExpr, depth int32) (*Expr, error) {
	switch astExpr.Op {
	case tree.PLUS:
		return b.bindFuncExprImplByAstExpr("+", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MINUS:
		return b.bindFuncExprImplByAstExpr("-", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MULTI:
		return b.bindFuncExprImplByAstExpr("*", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.MOD:
		return b.bindFuncExprImplByAstExpr("%", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.DIV:
		return b.bindFuncExprImplByAstExpr("/", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.INTEGER_DIV:
		return b.bindFuncExprImplByAstExpr("div", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_XOR:
		return b.bindFuncExprImplByAstExpr("^", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_OR:
		return b.bindFuncExprImplByAstExpr("|", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.BIT_AND:
		return b.bindFuncExprImplByAstExpr("&", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.LEFT_SHIFT:
		return b.bindFuncExprImplByAstExpr("<<", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	case tree.RIGHT_SHIFT:
		return b.bindFuncExprImplByAstExpr(">>", []tree.Expr{astExpr.Left, astExpr.Right}, depth)
	}
	return nil, moerr.NewNYIf(b.GetContext(), "'%v' operator", astExpr.Op.ToString())
}

func isNumericBinaryOp(op tree.BinaryOp) bool {
	switch op {
	case tree.PLUS, tree.MINUS, tree.MULTI, tree.MOD, tree.DIV, tree.INTEGER_DIV:
		return true
	default:
		return false
	}
}

func isNumericContextNode(astExpr tree.Expr) bool {
	switch expr := astExpr.(type) {
	case *tree.ParamExpr, *tree.VarExpr, *tree.NumVal, *tree.ParenExpr, *tree.CastExpr:
		return true
	case *tree.BinaryExpr:
		return isNumericBinaryOp(expr.Op)
	case *tree.UnaryExpr:
		return expr.Op == tree.UNARY_PLUS || expr.Op == tree.UNARY_MINUS
	case *tree.FuncExpr:
		return isNumericContextFunction(numericAstFunctionName(expr))
	case *tree.CaseExpr:
		return true
	default:
		return false
	}
}

func (b *baseBinder) isNumericContextNode(astExpr tree.Expr, depth int32) bool {
	if isNumericContextNode(astExpr) {
		return true
	}
	if paren, ok := astExpr.(*tree.ParenExpr); ok {
		return b.isNumericContextNode(paren.Expr, depth)
	}
	functionExpr, ok := astExpr.(*tree.FuncExpr)
	if !ok || b.numericParamType == nil || !b.numericFunctionTarget {
		return false
	}
	_, ok = b.resolveNumericFunctionContext(
		functionExpr, depth, b.numericAstColumnResolver(), b.numericParamType,
	)
	return ok
}

func (b *baseBinder) isGenericNumericFunctionRoot(astExpr tree.Expr, depth int32, target *Type) bool {
	if paren, ok := astExpr.(*tree.ParenExpr); ok {
		return b.isGenericNumericFunctionRoot(paren.Expr, depth, target)
	}
	functionExpr, ok := astExpr.(*tree.FuncExpr)
	if !ok {
		return false
	}
	_, ok = b.resolveNumericFunctionContext(functionExpr, depth, b.numericAstColumnResolver(), target)
	return ok
}

func isNumericArithmeticRoot(astExpr tree.Expr) bool {
	switch expr := astExpr.(type) {
	case *tree.ParenExpr:
		return isNumericArithmeticRoot(expr.Expr)
	case *tree.BinaryExpr:
		return isNumericBinaryOp(expr.Op)
	case *tree.UnaryExpr:
		return expr.Op == tree.UNARY_PLUS || expr.Op == tree.UNARY_MINUS
	case *tree.FuncExpr:
		return numericAstFunctionName(expr) == "mod" && len(expr.Exprs) == 2
	default:
		return false
	}
}

func (b *baseBinder) bindNumericExprWithContext(astExpr tree.Expr, depth int32, outer *Type) (*Expr, error) {
	return b.bindNumericExprWithContextMode(astExpr, depth, outer, true)
}

func (b *baseBinder) bindNumericExprWithDefaultContext(
	astExpr tree.Expr,
	depth int32,
	outer *Type,
) (*Expr, error) {
	return b.bindNumericExprWithContextMode(astExpr, depth, outer, false)
}

func (b *baseBinder) bindNumericExprWithContextMode(
	astExpr tree.Expr,
	depth int32,
	outer *Type,
	functionTarget bool,
) (*Expr, error) {
	// A direct user variable assigned from text must retain string semantics
	// when it is merely being coerced to a DML/projection target.  In
	// particular, inserting CAST(? AS CHAR) into BIT uses the source bytes
	// ("5" -> 53), whereas an arithmetic expression such as @v + 0 must enter
	// the numeric context below.  Do not let the target-type propagation turn
	// the former into a value-dependent numeric cast.
	if outer != nil && isDirectUserVariableExpr(astExpr) {
		paramType := b.numericParamType
		b.numericParamType = nil
		defer func() { b.numericParamType = paramType }()
		return b.impl.BindExpr(astExpr, depth, false)
	}
	if b.numericParamType != nil {
		return b.impl.BindExpr(astExpr, depth, false)
	}
	scan, err := b.numericAstTypesWithHint(astExpr, depth, outer)
	if err != nil || (!scan.hasParam && !scan.hasVar) || scan.incompatible {
		if err != nil {
			return nil, err
		}
		return b.bindNumericExprWithoutNewContext(astExpr, depth)
	}
	if scan.hasParam && (b.builder == nil || !b.builder.isPrepareStatement) {
		return b.bindNumericExprWithoutNewContext(astExpr, depth)
	}

	planType, ok := numericTypeFromAstScan(scan, outer)
	if !ok {
		return b.bindNumericExprWithoutNewContext(astExpr, depth)
	}
	b.numericParamType = &planType
	defer func() { b.numericParamType = nil }()
	previousFunctionTarget := b.numericFunctionTarget
	b.numericFunctionTarget = functionTarget
	defer func() { b.numericFunctionTarget = previousFunctionTarget }()
	previousSubqueryTarget := b.numericSubqueryTarget
	b.numericSubqueryTarget = &planType
	defer func() { b.numericSubqueryTarget = previousSubqueryTarget }()

	return b.bindNumericExprWithCurrentContext(astExpr, depth)
}

func isDirectUserVariableExpr(expr tree.Expr) bool {
	for {
		paren, ok := expr.(*tree.ParenExpr)
		if !ok {
			break
		}
		expr = paren.Expr
	}
	variable, ok := expr.(*tree.VarExpr)
	return ok && !variable.System
}

func (b *baseBinder) bindNumericExprWithoutNewContext(astExpr tree.Expr, depth int32) (*Expr, error) {
	return b.bindNumericExprWithCurrentContext(astExpr, depth)
}

func (b *baseBinder) bindNumericExprWithCurrentContext(astExpr tree.Expr, depth int32) (*Expr, error) {
	if binary, ok := astExpr.(*tree.BinaryExpr); ok {
		return b.bindBinaryExprWithCurrentContext(binary, depth)
	}
	if unary, ok := astExpr.(*tree.UnaryExpr); ok {
		return b.bindUnaryExprWithCurrentContext(unary, depth)
	}
	if function, ok := astExpr.(*tree.FuncExpr); ok && numericAstFunctionName(function) == "mod" {
		return b.bindFuncExprImplByAstExpr("mod", function.Exprs, depth)
	}
	return b.impl.BindExpr(astExpr, depth, false)
}

type numericAstTypeScan struct {
	strong       []Type
	weakDecimals []Type
	hasParam     bool
	// hasParamRef identifies an actual prepared marker in the scanned
	// expression. hasParam is broader: scalar subqueries use it to preserve
	// deferred numeric-context propagation even when their result type cannot
	// be inferred statically. Callers that need to distinguish a marker from a
	// deferred-but-unknown scalar expression must use hasParamRef.
	hasParamRef  bool
	hasVar       bool
	hasUnknown   bool
	incompatible bool
}

func (s numericAstTypeScan) merge(other numericAstTypeScan) numericAstTypeScan {
	s.strong = append(s.strong, other.strong...)
	s.weakDecimals = append(s.weakDecimals, other.weakDecimals...)
	s.hasParam = s.hasParam || other.hasParam
	s.hasParamRef = s.hasParamRef || other.hasParamRef
	s.hasVar = s.hasVar || other.hasVar
	s.hasUnknown = s.hasUnknown || other.hasUnknown
	s.incompatible = s.incompatible || other.incompatible
	return s
}

func numericAstTypedOperand(typ Type) numericAstTypeScan {
	oid := types.T(typ.Id)
	if oid == types.T_any {
		return numericAstTypeScan{}
	}
	if !makeTypeByPlan2Type(typ).IsNumeric() {
		return numericAstTypeScan{incompatible: true}
	}
	return numericAstTypeScan{strong: []Type{typ}}
}

func shouldActivateWeakDecimal(strong []types.Type, outer *types.Type) bool {
	for _, typ := range strong {
		if typ.IsNumeric() {
			return true
		}
	}
	return outer != nil && (outer.Oid.IsInteger() || outer.Oid.IsDecimal() || outer.Oid == types.T_bit)
}

func (b *baseBinder) numericAstTypesWithHint(
	astExpr tree.Expr,
	depth int32,
	hint *Type,
) (numericAstTypeScan, error) {
	return b.numericAstTypesInternalWithHint(astExpr, depth, b.numericAstColumnResolver(), hint)
}

func (b *baseBinder) numericAstColumnResolver() numericAstColumnResolver {
	return func(name *tree.UnresolvedName) (numericAstTypeScan, bool) {
		typ, ok := b.numericColumnType(name)
		return numericAstTypedOperand(typ), ok
	}
}

type numericAstColumnResolver func(*tree.UnresolvedName) (numericAstTypeScan, bool)

func (b *baseBinder) numericAstTypesInternal(
	astExpr tree.Expr,
	depth int32,
	resolveColumn numericAstColumnResolver,
) (numericAstTypeScan, error) {
	return b.numericAstTypesInternalWithHint(astExpr, depth, resolveColumn, nil)
}

func (b *baseBinder) numericAstTypesInternalWithHint(
	astExpr tree.Expr,
	depth int32,
	resolveColumn numericAstColumnResolver,
	hint *Type,
) (numericAstTypeScan, error) {
	switch expr := astExpr.(type) {
	case *tree.ParamExpr:
		return numericAstTypeScan{hasParam: true, hasParamRef: true}, nil
	case *tree.VarExpr:
		if expr.System {
			return numericAstTypeScan{hasUnknown: true}, nil
		}
		if typ, ok := b.resolveUserVariableNumericType(expr); ok {
			scan := numericAstTypedOperand(typ)
			scan.hasVar = true
			return scan, nil
		}
		return numericAstTypeScan{hasVar: true}, nil
	case *tree.Subquery:
		if expr.Exists {
			return numericAstTypeScan{}, nil
		}
		// A scalar subquery is only parameter-bearing when its projection really
		// contains a marker.  Unknown result type and parameter provenance are
		// separate states: treating every literal scalar subquery as a parameter
		// would make an otherwise statically typed expression enter the deferred
		// numeric overload path.
		return b.numericScalarSubqueryAstTypes(expr, depth)
	case *tree.ParenExpr:
		return b.numericAstTypesInternalWithHint(expr.Expr, depth, resolveColumn, hint)
	case *tree.BinaryExpr:
		if !isNumericBinaryOp(expr.Op) {
			return numericAstTypeScan{}, nil
		}
		left, err := b.numericAstTypesInternalWithHint(expr.Left, depth, resolveColumn, hint)
		if err != nil {
			return numericAstTypeScan{}, err
		}
		right, err := b.numericAstTypesInternalWithHint(expr.Right, depth, resolveColumn, hint)
		if err != nil {
			return numericAstTypeScan{}, err
		}
		return left.merge(right), nil
	case *tree.UnaryExpr:
		if expr.Op == tree.UNARY_PLUS || expr.Op == tree.UNARY_MINUS {
			return b.numericAstTypesInternalWithHint(expr.Expr, depth, resolveColumn, hint)
		}
		return numericAstTypeScan{}, nil
	case *tree.CastExpr:
		typ, err := getTypeFromAst(b.GetContext(), expr.Type)
		if err != nil {
			return numericAstTypeScan{}, err
		}
		scan := numericAstTypedOperand(typ)
		// The explicit cast fixes the resulting type, but its source can still
		// contain a prepared marker. Preserve that marker for callers that need
		// to decide whether the value is execution-time supplied.
		source, err := b.numericAstTypesInternalWithHint(expr.Expr, depth, resolveColumn, hint)
		if err != nil {
			return numericAstTypeScan{}, err
		}
		scan.hasParam = source.hasParam
		scan.hasParamRef = source.hasParamRef
		scan.hasVar = source.hasVar
		return scan, nil
	case *tree.BitCastExpr:
		typ, err := getTypeFromAst(b.GetContext(), expr.Type)
		if err != nil {
			return numericAstTypeScan{}, err
		}
		scan := numericAstTypedOperand(typ)
		source, err := b.numericAstTypesInternalWithHint(expr.Expr, depth, resolveColumn, hint)
		if err != nil {
			return numericAstTypeScan{}, err
		}
		scan.hasParam = source.hasParam
		scan.hasParamRef = source.hasParamRef
		scan.hasVar = source.hasVar
		return scan, nil
	case *tree.NumVal:
		bound, err := b.bindNumVal(expr, Type{})
		if err != nil {
			return numericAstTypeScan{}, err
		}
		if types.T(bound.Typ.Id).IsDecimal() {
			return numericAstTypeScan{weakDecimals: []Type{bound.Typ}}, nil
		}
		return numericAstTypedOperand(bound.Typ), nil
	case *tree.FuncExpr:
		name := numericAstFunctionName(expr)
		indexes, ok := numericFunctionResultArgs(name, len(expr.Exprs))
		if ok {
			var scan numericAstTypeScan
			for _, idx := range indexes {
				value, err := b.numericAstTypesInternalWithHint(expr.Exprs[idx], depth, resolveColumn, hint)
				if err != nil {
					return numericAstTypeScan{}, err
				}
				scan = scan.merge(value)
			}
			return scan, nil
		}
		typ, known, err := b.numericAstStaticType(expr, depth, resolveColumn)
		if err != nil || !known {
			if err != nil {
				return numericAstTypeScan{}, err
			}
			resolved, ok := b.resolveNumericFunctionContext(expr, depth, resolveColumn, hint)
			if ok {
				var scan numericAstTypeScan
				if numericFunctionReturnIsStrong(resolved, hint) {
					scan = numericAstTypedOperand(resolved.returnType)
				}
				for _, arg := range expr.Exprs {
					argScan, scanErr := b.numericAstTypesInternalWithHint(arg, depth, resolveColumn, hint)
					if scanErr != nil {
						return numericAstTypeScan{}, scanErr
					}
					scan.hasParam = scan.hasParam || argScan.hasParam
					scan.hasParamRef = scan.hasParamRef || argScan.hasParamRef
					scan.hasVar = scan.hasVar || argScan.hasVar
				}
				return scan, nil
			}
			var scan numericAstTypeScan
			for _, arg := range expr.Exprs {
				argScan, scanErr := b.numericAstTypesInternalWithHint(arg, depth, resolveColumn, hint)
				if scanErr != nil {
					return numericAstTypeScan{}, scanErr
				}
				scan.hasParam = scan.hasParam || argScan.hasParam
				scan.hasParamRef = scan.hasParamRef || argScan.hasParamRef
				scan.hasVar = scan.hasVar || argScan.hasVar
			}
			return scan, nil
		}
		if !makeTypeByPlan2Type(typ).IsNumeric() {
			return numericAstTypeScan{}, nil
		}
		scan := numericAstTypedOperand(typ)
		// A statically known return type does not mean that all of the
		// expression's inputs are known at prepare time. Preserve marker and
		// variable provenance through this branch as well; otherwise a nested
		// expression such as ABS((SELECT ROUND(? + 0))) can silently lose the
		// marker after ROUND's integer type is inferred.
		for _, arg := range expr.Exprs {
			argScan, scanErr := b.numericAstTypesInternalWithHint(arg, depth, resolveColumn, hint)
			if scanErr != nil {
				return numericAstTypeScan{}, scanErr
			}
			scan.hasParam = scan.hasParam || argScan.hasParam
			scan.hasParamRef = scan.hasParamRef || argScan.hasParamRef
			scan.hasVar = scan.hasVar || argScan.hasVar
		}
		return scan, nil
	case *tree.CaseExpr:
		var scan numericAstTypeScan
		for _, when := range expr.Whens {
			if when == nil || when.Val == nil {
				continue
			}
			value, err := b.numericAstTypesInternalWithHint(when.Val, depth, resolveColumn, hint)
			if err != nil {
				return numericAstTypeScan{}, err
			}
			scan = scan.merge(value)
		}
		if expr.Else != nil {
			value, err := b.numericAstTypesInternalWithHint(expr.Else, depth, resolveColumn, hint)
			if err != nil {
				return numericAstTypeScan{}, err
			}
			scan = scan.merge(value)
		}
		return scan, nil
	case *tree.UnresolvedName:
		if resolveColumn != nil {
			if scan, ok := resolveColumn(expr); ok {
				return scan, nil
			}
		}
		return numericAstTypeScan{hasUnknown: true}, nil
	case *tree.Tuple:
		var scan numericAstTypeScan
		for _, item := range expr.Exprs {
			itemScan, err := b.numericAstTypesInternalWithHint(item, depth, resolveColumn, hint)
			if err != nil {
				return numericAstTypeScan{}, err
			}
			scan = scan.merge(itemScan)
		}
		return scan, nil
	default:
		return numericAstTypeScan{}, nil
	}
}

func numericFunctionReturnIsStrong(resolved numericFunctionContext, hint *Type) bool {
	for _, dynamic := range resolved.dynamic {
		if !dynamic {
			return true
		}
	}
	if hint == nil {
		return false
	}
	returnOid := types.T(resolved.returnType.Id)
	hintOid := types.T(hint.Id)
	return (returnOid == types.T_float32 || returnOid == types.T_float64) &&
		hintOid != types.T_float32 && hintOid != types.T_float64
}

func (b *baseBinder) numericAstStaticType(
	astExpr tree.Expr,
	depth int32,
	resolveColumn numericAstColumnResolver,
) (Type, bool, error) {
	switch expr := astExpr.(type) {
	case *tree.ParenExpr:
		return b.numericAstStaticType(expr.Expr, depth, resolveColumn)
	case *tree.CastExpr:
		typ, err := getTypeFromAst(b.GetContext(), expr.Type)
		return typ, err == nil, err
	case *tree.NumVal:
		bound, err := b.bindNumVal(expr, Type{})
		if err != nil {
			return Type{}, false, err
		}
		return bound.Typ, true, nil
	case *tree.BinaryExpr:
		if !isNumericBinaryOp(expr.Op) {
			return Type{}, false, nil
		}
		scan, err := b.numericAstTypesInternal(expr, depth, resolveColumn)
		if err != nil || scan.incompatible || scan.hasUnknown ||
			(scan.hasParam && len(scan.strong) == 0) {
			return Type{}, false, err
		}
		typ, ok := numericTypeFromAstScan(scan, nil)
		return typ, ok, nil
	case *tree.UnaryExpr:
		if expr.Op != tree.UNARY_PLUS && expr.Op != tree.UNARY_MINUS {
			return Type{}, false, nil
		}
		scan, err := b.numericAstTypesInternal(expr, depth, resolveColumn)
		if err != nil || scan.incompatible || scan.hasUnknown ||
			(scan.hasParam && len(scan.strong) == 0) {
			return Type{}, false, err
		}
		typ, ok := numericTypeFromAstScan(scan, nil)
		return typ, ok, nil
	case *tree.UnresolvedName:
		if resolveColumn == nil {
			return Type{}, false, nil
		}
		scan, ok := resolveColumn(expr)
		if !ok || scan.incompatible || scan.hasParam || len(scan.strong) != 1 || len(scan.weakDecimals) != 0 {
			return Type{}, false, nil
		}
		return scan.strong[0], true, nil
	case *tree.Subquery:
		if expr.Exists {
			return Type{}, false, nil
		}
		scan, err := b.numericScalarSubqueryAstTypes(expr, depth)
		if err != nil || scan.incompatible || scan.hasParam || len(scan.strong) != 1 || len(scan.weakDecimals) != 0 {
			return Type{}, false, err
		}
		return scan.strong[0], true, nil
	case *tree.FuncExpr:
		name := numericAstFunctionName(expr)
		if name == "" {
			return Type{}, false, nil
		}
		argTypes := make([]types.Type, len(expr.Exprs))
		for i, arg := range expr.Exprs {
			typ, known, err := b.numericAstStaticType(arg, depth, resolveColumn)
			if err != nil || !known {
				return Type{}, false, err
			}
			argTypes[i] = makeTypeByPlan2Type(typ)
		}
		resolved, err := function.GetFunctionByName(b.GetContext(), name, argTypes)
		if err != nil {
			return Type{}, false, nil
		}
		ret := resolved.GetReturnType()
		return makePlan2Type(&ret), true, nil
	default:
		return Type{}, false, nil
	}
}

func numericTypeFromAstScan(scan numericAstTypeScan, outer *Type) (Type, bool) {
	typesKnown := make([]types.Type, 0, len(scan.strong)+len(scan.weakDecimals))
	for i := range scan.strong {
		typesKnown = append(typesKnown, makeTypeByPlan2Type(scan.strong[i]))
	}
	var outerType *types.Type
	if outer != nil {
		typ := makeTypeByPlan2Type(*outer)
		outerType = &typ
	}
	if len(scan.weakDecimals) > 0 && shouldActivateWeakDecimal(typesKnown, outerType) {
		for i := range scan.weakDecimals {
			typesKnown = append(typesKnown, makeTypeByPlan2Type(scan.weakDecimals[i]))
		}
	}
	resolved, ok := function.InferNumericParameterType(typesKnown, outerType)
	if !ok {
		return Type{}, false
	}
	return makePlan2Type(&resolved), true
}

func (b *baseBinder) numericScalarSubqueryAstTypes(
	subquery *tree.Subquery,
	depth int32,
) (numericAstTypeScan, error) {
	var owner *tree.Select
	switch selectStmt := subquery.Select.(type) {
	case *tree.Select:
		owner = selectStmt
	case *tree.ParenSelect:
		owner = selectStmt.Select
	default:
		// Keep the scanner independent of the parser's choice of wrapper. A
		// scalar subquery can be represented directly by a SELECT clause or a
		// UNION statement in unit-constructed ASTs, and both still expose the
		// same numeric projection information.
		owner = &tree.Select{Select: subquery.Select}
	}
	if owner == nil || owner.Select == nil {
		return numericAstTypeScan{}, nil
	}
	return b.numericScalarSelectAstTypes(owner, owner.Select, depth, make(map[*tree.Select]bool), nil)
}

func (b *baseBinder) numericScalarSelectAstTypes(
	owner *tree.Select,
	stmt tree.SelectStatement,
	depth int32,
	visiting map[*tree.Select]bool,
	ctes map[string]*tree.CTE,
) (numericAstTypeScan, error) {
	_, scans, ok, err := b.numericScalarStatementOutputs(owner, stmt, depth, visiting, ctes)
	if err != nil {
		return numericAstTypeScan{}, err
	}
	if !ok || len(scans) != 1 {
		return numericAstTypeScan{}, nil
	}
	return scans[0], nil
}

type numericScalarSource struct {
	alias string
	name  string
	cols  []string
	types []numericAstTypeScan
	known bool
}

func numericScalarVisibleCtes(owner *tree.Select, inherited map[string]*tree.CTE) map[string]*tree.CTE {
	if owner == nil || owner.With == nil || len(owner.With.CTEs) == 0 {
		return inherited
	}
	visible := make(map[string]*tree.CTE, len(inherited)+len(owner.With.CTEs))
	for name, cte := range inherited {
		visible[name] = cte
	}
	for _, cte := range owner.With.CTEs {
		visible[strings.ToLower(string(cte.Name.Alias))] = cte
	}
	return visible
}

func numericScalarCteSource(cte *tree.CTE, existingCols tree.IdentifierList) (*tree.Select, tree.IdentifierList) {
	if len(existingCols) == 0 {
		existingCols = cte.Name.Cols
	}
	switch source := cte.Stmt.(type) {
	case *tree.Select:
		return source, existingCols
	case *tree.ParenSelect:
		return source.Select, existingCols
	default:
		return nil, existingCols
	}
}

func (b *baseBinder) numericScalarSources(
	owner *tree.Select,
	clause *tree.SelectClause,
	depth int32,
	visiting map[*tree.Select]bool,
	ctes map[string]*tree.CTE,
) ([]numericScalarSource, bool) {
	if clause.From == nil {
		return nil, true
	}
	if len(clause.From.Tables) != 1 {
		return nil, false
	}
	infos := collectNumericProjectionSources(clause.From.Tables[0], "", nil)
	sources := make([]numericScalarSource, len(infos))
	ctes = numericScalarVisibleCtes(owner, ctes)
	for i := range infos {
		sources[i].alias = strings.ToLower(infos[i].alias)
		sources[i].name = strings.ToLower(infos[i].sourceName)
		if infos[i].source == nil && infos[i].sourceSchema == "" {
			if cte := ctes[strings.ToLower(infos[i].sourceName)]; cte != nil {
				infos[i].source, infos[i].aliasCols = numericScalarCteSource(cte, infos[i].aliasCols)
			} else {
				infos[i].source, infos[i].aliasCols = numericProjectionCteSource(
					owner, b.ctx, infos[i].sourceName, infos[i].aliasCols,
				)
			}
		}
		if infos[i].source != nil {
			cols, scans, ok, err := b.numericScalarSelectOutputs(infos[i].source, depth, visiting, ctes)
			if err != nil {
				return nil, false
			}
			if !ok {
				continue
			}
			sources[i].cols = cols
			sources[i].types = scans
			sources[i].known = true
		} else if infos[i].sourceName != "" && b.builder != nil {
			cols := numericPhysicalTableVisibleCols(b.builder, infos[i])
			if cols == nil {
				continue
			}
			for _, col := range cols {
				sources[i].cols = append(sources[i].cols, strings.ToLower(col.Name))
				sources[i].types = append(sources[i].types, numericAstTypedOperand(col.Typ))
			}
			sources[i].known = true
		}
		if !sources[i].known || len(infos[i].aliasCols) == 0 {
			continue
		}
		if len(infos[i].aliasCols) != len(sources[i].cols) {
			sources[i].known = false
			continue
		}
		for pos := range infos[i].aliasCols {
			sources[i].cols[pos] = strings.ToLower(string(infos[i].aliasCols[pos]))
		}
	}
	return sources, true
}

func (b *baseBinder) numericScalarSelectOutputs(
	owner *tree.Select,
	depth int32,
	visiting map[*tree.Select]bool,
	ctes map[string]*tree.CTE,
) ([]string, []numericAstTypeScan, bool, error) {
	if visiting[owner] {
		return nil, nil, false, nil
	}
	visiting[owner] = true
	defer delete(visiting, owner)
	return b.numericScalarStatementOutputs(owner, owner.Select, depth, visiting, ctes)
}

func (b *baseBinder) numericScalarStatementOutputs(
	owner *tree.Select,
	stmt tree.SelectStatement,
	depth int32,
	visiting map[*tree.Select]bool,
	ctes map[string]*tree.CTE,
) ([]string, []numericAstTypeScan, bool, error) {
	switch selectStmt := stmt.(type) {
	case *tree.SelectClause:
		sources, sourcesKnown := b.numericScalarSources(owner, selectStmt, depth, visiting, ctes)
		if !sourcesKnown {
			sources = nil
		}
		cols := make([]string, 0, len(selectStmt.Exprs))
		scans := make([]numericAstTypeScan, 0, len(selectStmt.Exprs))
		for _, selectExpr := range selectStmt.Exprs {
			switch expr := selectExpr.Expr.(type) {
			case tree.UnqualifiedStar:
				if !sourcesKnown {
					return nil, nil, false, nil
				}
				starCols, starScans, ok := numericScalarUnqualifiedStarOutputs(selectStmt, sources)
				if !ok {
					return nil, nil, false, nil
				}
				cols = append(cols, starCols...)
				scans = append(scans, starScans...)
				continue
			case *tree.UnresolvedName:
				if expr.Star {
					if !sourcesKnown {
						return nil, nil, false, nil
					}
					starCols, starScans, ok := numericScalarQualifiedStarOutputs(sources, expr.ColName())
					if !ok {
						return nil, nil, false, nil
					}
					cols = append(cols, starCols...)
					scans = append(scans, starScans...)
					continue
				}
			}
			col := ""
			if selectExpr.As != nil && !selectExpr.As.Empty() {
				col = strings.ToLower(selectExpr.As.Origin())
			} else if name, ok := selectExpr.Expr.(*tree.UnresolvedName); ok {
				col = strings.ToLower(name.ColName())
			}
			scan, err := b.numericAstTypesInternal(
				selectExpr.Expr,
				depth,
				func(name *tree.UnresolvedName) (numericAstTypeScan, bool) {
					return resolveNumericScalarColumn(sources, name)
				},
			)
			if err != nil {
				return nil, nil, false, err
			}
			cols = append(cols, col)
			scans = append(scans, scan)
		}
		return cols, scans, true, nil
	case *tree.ValuesClause:
		if len(selectStmt.Rows) == 0 {
			return nil, nil, false, nil
		}
		width := len(selectStmt.Rows[0])
		cols := make([]string, width)
		scans := make([]numericAstTypeScan, width)
		for i := range cols {
			cols[i] = fmt.Sprintf("column_%d", i)
		}
		for _, row := range selectStmt.Rows {
			if len(row) != width {
				return nil, nil, false, nil
			}
			for i, cell := range row {
				scan, err := b.numericAstTypesInternal(cell, depth, nil)
				if err != nil {
					return nil, nil, false, err
				}
				scans[i] = scans[i].merge(scan)
			}
		}
		return cols, scans, true, nil
	case *tree.UnionClause:
		leftCols, left, leftOK, err := b.numericScalarStatementOutputs(owner, selectStmt.Left, depth, visiting, ctes)
		if err != nil || !leftOK {
			return nil, nil, false, err
		}
		_, right, rightOK, err := b.numericScalarStatementOutputs(owner, selectStmt.Right, depth, visiting, ctes)
		if err != nil || !rightOK || len(left) != len(right) {
			return nil, nil, false, err
		}
		for i := range left {
			left[i] = left[i].merge(right[i])
		}
		return leftCols, left, true, nil
	case *tree.ParenSelect:
		return b.numericScalarStatementOutputs(selectStmt.Select, selectStmt.Select.Select, depth, visiting, ctes)
	default:
		return nil, nil, false, nil
	}
}

func numericScalarProjectionSources(sources []numericScalarSource) []numericProjectionSourceInfo {
	projectionSources := make([]numericProjectionSourceInfo, len(sources))
	for i := range sources {
		projectionSources[i] = numericProjectionSourceInfo{
			sourceName:  sources[i].name,
			alias:       sources[i].alias,
			outputNames: sources[i].cols,
			outputKnown: sources[i].known,
		}
	}
	return projectionSources
}

func numericScalarUnqualifiedStarOutputs(
	clause *tree.SelectClause,
	sources []numericScalarSource,
) ([]string, []numericAstTypeScan, bool) {
	if clause.From == nil || len(clause.From.Tables) != 1 {
		return nil, nil, false
	}
	projectionSources := numericScalarProjectionSources(sources)
	cursor := 0
	outputs, ok := numericProjectionStarOutputs(clause.From.Tables[0], projectionSources, &cursor)
	if !ok || cursor != len(sources) {
		return nil, nil, false
	}
	return numericScalarScansFromStarOutputs(outputs, sources)
}

func numericScalarQualifiedStarOutputs(
	sources []numericScalarSource,
	qualifier string,
) ([]string, []numericAstTypeScan, bool) {
	source := uniqueNumericStarSource(numericScalarProjectionSources(sources), qualifier)
	if source < 0 || source >= len(sources) || len(sources[source].cols) != len(sources[source].types) {
		return nil, nil, false
	}
	return append([]string(nil), sources[source].cols...),
		append([]numericAstTypeScan(nil), sources[source].types...), true
}

func numericScalarScansFromStarOutputs(
	outputs []numericProjectionStarOutput,
	sources []numericScalarSource,
) ([]string, []numericAstTypeScan, bool) {
	cols := make([]string, len(outputs))
	scans := make([]numericAstTypeScan, len(outputs))
	for i, output := range outputs {
		if len(output.refs) == 0 {
			return nil, nil, false
		}
		cols[i] = output.name
		for _, ref := range output.refs {
			if ref.source < 0 || ref.source >= len(sources) ||
				ref.pos < 0 || ref.pos >= len(sources[ref.source].types) {
				return nil, nil, false
			}
			scans[i] = scans[i].merge(sources[ref.source].types[ref.pos])
		}
	}
	return cols, scans, true
}

func resolveNumericScalarColumn(
	sources []numericScalarSource,
	name *tree.UnresolvedName,
) (numericAstTypeScan, bool) {
	column := strings.ToLower(name.ColName())
	table := strings.ToLower(name.TblName())
	found := false
	var result numericAstTypeScan
	for _, source := range sources {
		if table != "" && table != source.alias && table != source.name {
			continue
		}
		if !source.known {
			return numericAstTypeScan{}, false
		}
		for pos, candidate := range source.cols {
			if candidate != column {
				continue
			}
			if found || pos >= len(source.types) {
				return numericAstTypeScan{}, false
			}
			result = source.types[pos]
			found = true
		}
	}
	return result, found
}

func numericAstFunctionName(astExpr *tree.FuncExpr) string {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return ""
	}
	return strings.ToLower(funcRef.ColName())
}

type numericFunctionContext struct {
	returnType Type
	argTypes   []Type
	dynamic    []bool
}

func (b *baseBinder) resolveNumericFunctionContext(
	expr *tree.FuncExpr,
	depth int32,
	resolveColumn numericAstColumnResolver,
	hint *Type,
) (numericFunctionContext, bool) {
	return b.resolveNumericFunctionArgs(
		numericAstFunctionName(expr), expr.Exprs, depth, resolveColumn, hint,
	)
}

func (b *baseBinder) resolveNumericFunctionArgs(
	name string,
	args []tree.Expr,
	depth int32,
	resolveColumn numericAstColumnResolver,
	hint *Type,
) (numericFunctionContext, bool) {
	name = strings.ToLower(name)
	if !supportsGenericNumericFunctionContext(name) || hint == nil || function.GetFunctionIsAggregateByName(name) ||
		function.GetFunctionIsWinFunByName(name) {
		return numericFunctionContext{}, false
	}

	argTypes := make([]types.Type, len(args))
	dynamic := make([]bool, len(args))
	for i, arg := range args {
		typ, known, err := b.numericAstStaticType(arg, depth, resolveColumn)
		if err != nil {
			return numericFunctionContext{}, false
		}
		if known {
			argTypes[i] = makeTypeByPlan2Type(typ)
			continue
		}
		argTypes[i] = makeTypeByPlan2Type(*hint)
		dynamic[i] = true
	}

	resolved, err := function.GetFunctionByName(b.GetContext(), name, argTypes)
	if err != nil || !resolved.GetReturnType().IsNumeric() {
		return numericFunctionContext{}, false
	}
	if targets, shouldCast := resolved.ShouldDoImplicitTypeCast(); shouldCast {
		if len(targets) != len(argTypes) {
			return numericFunctionContext{}, false
		}
		argTypes = targets
	}

	returnType := resolved.GetReturnType()
	context := numericFunctionContext{
		returnType: makePlan2Type(&returnType),
		argTypes:   make([]Type, len(argTypes)),
		dynamic:    dynamic,
	}
	for i := range argTypes {
		context.argTypes[i] = makePlan2Type(&argTypes[i])
	}
	return context, true
}

func supportsGenericNumericFunctionContext(name string) bool {
	switch name {
	// These functions' value arguments are in the same numeric domain as their
	// result. A numeric return type alone is insufficient: FIELD, LENGTH and
	// similar functions return numbers while their arguments belong to another
	// domain.
	case "abs", "ceil", "ceiling", "floor", "round", "truncate",
		"sqrt", "power", "pow", "exp", "ln", "log", "log2", "log10":
		return true
	default:
		return false
	}
}

func isNumericContextFunction(name string) bool {
	switch name {
	case "+", "-", "*", "/", "%", "div", "^", "unary_plus", "unary_minus",
		"mod", "if", "coalesce", "ifnull", "nullif":
		return true
	default:
		return false
	}
}

func numericFunctionResultArgs(name string, argCount int) ([]int, bool) {
	switch name {
	case "mod":
		if argCount != 2 {
			return nil, false
		}
		return []int{0, 1}, true
	case "if":
		if argCount != 3 {
			return nil, false
		}
		return []int{1, 2}, true
	case "coalesce", "ifnull":
		if argCount == 0 {
			return nil, false
		}
		indexes := make([]int, argCount)
		for i := range indexes {
			indexes[i] = i
		}
		return indexes, true
	case "nullif":
		if argCount != 2 {
			return nil, false
		}
		return []int{0}, true
	default:
		return nil, false
	}
}

func numericFunctionArgKeepsContext(name string, idx, argCount int) bool {
	if name == "case" {
		return idx%2 == 1 || idx == argCount-1
	}
	indexes, ok := numericFunctionResultArgs(name, argCount)
	if !ok {
		return false
	}
	for _, resultIdx := range indexes {
		if idx == resultIdx {
			return true
		}
	}
	return false
}

func numericFunctionHasSelectiveContext(name string) bool {
	switch name {
	case "case", "if", "ifnull", "nullif":
		return true
	default:
		return false
	}
}

func (b *baseBinder) numericColumnType(astExpr *tree.UnresolvedName) (Type, bool) {
	if b.ctx == nil {
		return Type{}, false
	}
	ctx := b.ctx
	for ctx != nil {
		if typ, found, stop := b.numericColumnTypeInContext(ctx, astExpr); found || stop {
			return typ, found
		}
		ctx = ctx.parent
		for ctx != nil && ctx.binder == nil {
			ctx = ctx.parent
		}
	}
	return Type{}, false
}

func (b *baseBinder) numericColumnTypeInContext(
	ctx *BindContext,
	astExpr *tree.UnresolvedName,
) (typ Type, found bool, stop bool) {
	col := astExpr.ColName()
	table := astExpr.TblName()
	if table == "" {
		if binding, ok := ctx.bindingByCol[col]; ok {
			if binding == nil {
				return Type{}, false, true
			}
			typ, found = bindingColumnType(binding, col)
			return typ, found, true
		}
		if alias, ok := ctx.aliasMap[col]; ok && int(alias.idx) < len(ctx.projects) {
			return ctx.projects[alias.idx].Typ, true, true
		}
		return Type{}, false, false
	}

	binding, ok := ctx.bindingByTable[table]
	if !ok && ctx.remapOption != nil {
		db := astExpr.DbName()
		if db == "" && b.builder != nil {
			db = b.builder.compCtx.DefaultDatabase()
		}
		binding, ok = ctx.bindingByTable[db+"."+table]
	}
	if !ok {
		return Type{}, false, false
	}
	typ, found = bindingColumnType(binding, col)
	return typ, found, false
}

func bindingColumnType(binding *Binding, col string) (Type, bool) {
	if binding == nil {
		return Type{}, false
	}
	colPos, ok := binding.colIdByName[col]
	if !ok || colPos < 0 || int(colPos) >= len(binding.types) || binding.types[colPos] == nil {
		return Type{}, false
	}
	return *DeepCopyType(binding.types[colPos]), true
}

func (b *baseBinder) bindComparisonExpr(astExpr *tree.ComparisonExpr, depth int32, isRoot bool) (*Expr, error) {
	var op string
	leftAst := unwrapParenExpr(astExpr.Left)
	rightAst := unwrapParenExpr(astExpr.Right)

	switch astExpr.Op {
	case tree.EQUAL:
		op = "="
		switch leftexpr := leftAst.(type) {
		case *tree.Tuple:
			switch rightexpr := rightAst.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := 1; i < len(leftexpr.Exprs); i++ {
						if i == 1 {
							expr1, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[0], rightexpr.Exprs[0]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.NULL_SAFE_EQUAL:
		op = "<=>"
		switch leftexpr := astExpr.Left.(type) {
		case *tree.Tuple:
			switch rightexpr := astExpr.Right.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := 1; i < len(leftexpr.Exprs); i++ {
						if i == 1 {
							expr1, err = b.bindFuncExprImplByAstExpr(op, []tree.Expr{leftexpr.Exprs[0], rightexpr.Exprs[0]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr(op, []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}
	case tree.LESS_THAN:
		op = "<"
		switch leftexpr := leftAst.(type) {
		case *tree.Tuple:
			switch rightexpr := rightAst.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr("<", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr("<", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.LESS_THAN_EQUAL:
		op = "<="
		switch leftexpr := leftAst.(type) {
		case *tree.Tuple:
			switch rightexpr := rightAst.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr("<=", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr("<", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.GREAT_THAN:
		op = ">"
		switch leftexpr := leftAst.(type) {
		case *tree.Tuple:
			switch rightexpr := rightAst.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr(">", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr(">", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.GREAT_THAN_EQUAL:
		op = ">="
		switch leftexpr := leftAst.(type) {
		case *tree.Tuple:
			switch rightexpr := rightAst.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := len(leftexpr.Exprs) - 2; i >= 0; i-- {
						if i == len(leftexpr.Exprs)-2 {
							expr1, err = b.bindFuncExprImplByAstExpr(">=", []tree.Expr{leftexpr.Exprs[i+1], rightexpr.Exprs[i+1]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "and", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
						expr2, err = b.bindFuncExprImplByAstExpr(">", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.NOT_EQUAL:
		op = "<>"
		switch leftexpr := leftAst.(type) {
		case *tree.Tuple:
			switch rightexpr := rightAst.(type) {
			case *tree.Tuple:
				if len(leftexpr.Exprs) == len(rightexpr.Exprs) {
					var expr1, expr2 *plan.Expr
					var err error
					for i := 1; i < len(leftexpr.Exprs); i++ {
						if i == 1 {
							expr1, err = b.bindFuncExprImplByAstExpr("<>", []tree.Expr{leftexpr.Exprs[0], rightexpr.Exprs[0]}, depth)
							if err != nil {
								return nil, err
							}
						}
						expr2, err = b.bindFuncExprImplByAstExpr("<>", []tree.Expr{leftexpr.Exprs[i], rightexpr.Exprs[i]}, depth)
						if err != nil {
							return nil, err
						}
						expr1, err = BindFuncExprImplByPlanExpr(b.GetContext(), "or", []*plan.Expr{expr1, expr2})
						if err != nil {
							return nil, err
						}
					}
					return expr1, nil
				} else {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "two tuples have different length(%v,%v)", len(leftexpr.Exprs), len(rightexpr.Exprs))
				}
			}
		}

	case tree.LIKE:
		op = "like"

	case tree.NOT_LIKE:
		newExpr := tree.NewComparisonExpr(tree.LIKE, astExpr.Left, astExpr.Right)
		newExpr.Escape = astExpr.Escape
		return b.bindFuncExprImplByAstExpr("not", []tree.Expr{newExpr}, depth)

	case tree.ILIKE:
		op = "ilike"

	case tree.NOT_ILIKE:
		newExpr := tree.NewComparisonExpr(tree.ILIKE, astExpr.Left, astExpr.Right)
		newExpr.Escape = astExpr.Escape
		return b.bindFuncExprImplByAstExpr("not", []tree.Expr{newExpr}, depth)

	case tree.IN:
		if leftTuple, ok := leftAst.(*tree.Tuple); ok {
			if rightTuple, ok := rightAst.(*tree.Tuple); ok {
				return b.bindTupleInByAst(leftTuple, rightTuple, depth, false)
			}
		}
		switch r := rightAst.(type) {
		case *tree.Tuple:
			op = "in"
			if r.Partition {
				op = "partition_in"
			}

		default:
			leftArg, err := b.impl.BindExpr(astExpr.Left, depth, false)
			if err != nil {
				return nil, err
			}

			rightArg, err := b.impl.BindExpr(astExpr.Right, depth, false)
			if err != nil {
				return nil, err
			}

			if subquery := rightArg.GetSub(); subquery != nil {
				// IN-subquery construction publishes the left operand directly as
				// SubqueryRef.Child instead of going through the generic function
				// binder. Enforce the scalar-boundary contract before treating an
				// INTERVAL's internal Expr_List as a multi-column tuple.
				if err = rejectBoundIntervalFunctionArgs(b.GetContext(), "in", []*plan.Expr{leftArg}); err != nil {
					return nil, err
				}
				leftArg = b.useStoredMySQLSpecialTypesForNumericSubquery(leftArg, rightArg)
				if list := leftArg.GetList(); list != nil {
					if len(list.List) != int(subquery.RowSize) {
						return nil, moerr.NewNYIf(b.GetContext(), "subquery should return %d columns", len(list.List))
					}
				} else {
					if subquery.RowSize > 1 {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery returns more than 1 column")
					}
				}

				subquery.Typ = plan.SubqueryRef_IN
				subquery.Child = leftArg

				rightArg.Typ = plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: leftArg.Typ.NotNullable && rightArg.Typ.NotNullable,
				}

				return rightArg, nil
			} else {
				return BindFuncExprImplByPlanExpr(b.GetContext(), "in", []*plan.Expr{leftArg, rightArg})
			}
		}

	case tree.NOT_IN:
		if leftTuple, ok := leftAst.(*tree.Tuple); ok {
			if rightTuple, ok := rightAst.(*tree.Tuple); ok {
				return b.bindTupleInByAst(leftTuple, rightTuple, depth, true)
			}
		}
		switch rightAst.(type) {
		case *tree.Tuple:
			op = "not_in"

		default:
			leftArg, err := b.impl.BindExpr(astExpr.Left, depth, false)
			if err != nil {
				return nil, err
			}

			rightArg, err := b.impl.BindExpr(astExpr.Right, depth, false)
			if err != nil {
				return nil, err
			}

			if subquery := rightArg.GetSub(); subquery != nil {
				if err = rejectBoundIntervalFunctionArgs(b.GetContext(), "not_in", []*plan.Expr{leftArg}); err != nil {
					return nil, err
				}
				leftArg = b.useStoredMySQLSpecialTypesForNumericSubquery(leftArg, rightArg)
				if list := leftArg.GetList(); list != nil {
					if len(list.List) != int(subquery.RowSize) {
						return nil, moerr.NewInvalidInputf(b.GetContext(), "subquery should return %d columns", len(list.List))
					}
				} else {
					if subquery.RowSize > 1 {
						return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return 1 column")
					}
				}

				subquery.Typ = plan.SubqueryRef_NOT_IN
				subquery.Child = leftArg

				rightArg.Typ = plan.Type{
					Id:          int32(types.T_bool),
					NotNullable: leftArg.Typ.NotNullable && rightArg.Typ.NotNullable,
				}

				return rightArg, nil
			} else {
				expr, err := BindFuncExprImplByPlanExpr(b.GetContext(), "in", []*plan.Expr{leftArg, rightArg})
				if err != nil {
					return nil, err
				}

				return BindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{expr})
			}
		}
	case tree.REG_MATCH:
		op = "reg_match"
	case tree.NOT_REG_MATCH:
		op = "not_reg_match"
	default:
		return nil, moerr.NewNYIf(b.GetContext(), "'%v'", astExpr)
	}

	if astExpr.SubOp >= tree.ANY {
		expr, err := b.impl.BindExpr(astExpr.Right, depth, false)
		if err != nil {
			return nil, err
		}

		child, err := b.impl.BindExpr(astExpr.Left, depth, false)
		if err != nil {
			return nil, err
		}

		if subquery := expr.GetSub(); subquery != nil {
			if err = rejectBoundIntervalFunctionArgs(b.GetContext(), op, []*plan.Expr{child}); err != nil {
				return nil, err
			}
			child = b.useStoredMySQLSpecialTypesForNumericSubquery(child, expr)
			if list := child.GetList(); list != nil {
				if len(list.List) != int(subquery.RowSize) {
					return nil, moerr.NewInvalidInputf(b.GetContext(), "subquery should return %d columns", len(list.List))
				}
			} else {
				if subquery.RowSize > 1 {
					return nil, moerr.NewInvalidInput(b.GetContext(), "subquery should return 1 column")
				}
			}

			subquery.Op = op
			subquery.Child = child

			switch astExpr.SubOp {
			case tree.ANY, tree.SOME:
				subquery.Typ = plan.SubqueryRef_ANY
			case tree.ALL:
				subquery.Typ = plan.SubqueryRef_ALL
			}

			expr.Typ = plan.Type{
				Id:          int32(types.T_bool),
				NotNullable: expr.Typ.NotNullable && child.Typ.NotNullable,
			}

			return expr, nil
		} else {
			return nil, moerr.NewInvalidInputf(b.GetContext(), "subquery '%s' is not a quantifying subquery", astExpr.SubOp.ToString())
		}
	}

	args := []tree.Expr{astExpr.Left, astExpr.Right}
	if (op == "like" || op == "ilike") && astExpr.Escape != nil {
		args = append(args, astExpr.Escape)
	}
	if b.mysqlSpecialTypeNumericComparison(astExpr.Left, astExpr.Right) {
		return b.bindWithRawMySQLSpecialTypes(func() (*Expr, error) {
			return b.bindFuncExprImplByAstExpr(op, args, depth)
		})
	}
	return b.bindFuncExprImplByAstExpr(op, args, depth)
}

func (b *baseBinder) bindWithRawMySQLSpecialTypes(bind func() (*Expr, error)) (*Expr, error) {
	previous := b.bindRawMySQLSpecialType
	b.bindRawMySQLSpecialType = true
	defer func() { b.bindRawMySQLSpecialType = previous }()
	return bind()
}

func (b *baseBinder) mysqlSpecialTypeNumericComparison(left, right tree.Expr) bool {
	return (b.mysqlSpecialTypeAst(left) && b.mysqlSpecialTypeNumericContext(right)) ||
		(b.mysqlSpecialTypeAst(right) && b.mysqlSpecialTypeNumericContext(left))
}

// mysqlSpecialTypeNumericContext reports AST expressions whose bound contract
// is numeric. ENUM and SET are normally exposed as display strings, but MySQL
// uses their stored ordinal/bitmap when compared with a numeric operand.
func (b *baseBinder) mysqlSpecialTypeNumericContext(expr tree.Expr) bool {
	switch value := unwrapParenExpr(expr).(type) {
	case *tree.NumVal:
		return mysqlSpecialTypeNumericLiteral(value)
	case *tree.UnresolvedName:
		typ, ok := b.numericColumnType(value)
		return ok && makeTypeByPlan2Type(typ).IsNumeric()
	case *tree.UnaryExpr:
		return (value.Op == tree.UNARY_PLUS || value.Op == tree.UNARY_MINUS) &&
			b.mysqlSpecialTypeNumericContext(value.Expr)
	case *tree.BinaryExpr:
		return isNumericBinaryOp(value.Op) || isBitwiseBinaryOp(value.Op)
	case *tree.CastExpr:
		typ, err := getTypeFromAst(b.GetContext(), value.Type)
		return err == nil && makeTypeByPlan2Type(typ).IsNumeric()
	case *tree.FuncExpr:
		return supportsGenericNumericFunctionContext(strings.ToLower(numericAstFunctionName(value)))
	case *tree.Tuple:
		if len(value.Exprs) == 0 {
			return false
		}
		for _, item := range value.Exprs {
			if !b.mysqlSpecialTypeNumericContext(item) {
				return false
			}
		}
		return true
	}
	return false
}

func mysqlSpecialTypeInExprs(b *baseBinder, exprs []tree.Expr) bool {
	for _, expr := range exprs {
		if b.mysqlSpecialTypeInAst(expr) {
			return true
		}
	}
	return false
}

func (b *baseBinder) mysqlSpecialTypeAst(expr tree.Expr) bool {
	name, ok := unwrapParenExpr(expr).(*tree.UnresolvedName)
	if !ok {
		return false
	}
	typ, ok := b.numericColumnType(name)
	return ok && isEnumOrSetPlanType(&typ)
}

func (b *baseBinder) mysqlSpecialTypeInAst(expr tree.Expr) bool {
	if b.mysqlSpecialTypeAst(expr) {
		return true
	}
	switch value := unwrapParenExpr(expr).(type) {
	case *tree.UnaryExpr:
		return b.mysqlSpecialTypeInAst(value.Expr)
	case *tree.BinaryExpr:
		return b.mysqlSpecialTypeInAst(value.Left) || b.mysqlSpecialTypeInAst(value.Right)
	case *tree.CastExpr:
		return b.mysqlSpecialTypeInAst(value.Expr)
	case *tree.FuncExpr:
		return mysqlSpecialTypeInExprs(b, value.Exprs)
	case *tree.Tuple:
		return mysqlSpecialTypeInExprs(b, value.Exprs)
	}
	return false
}

func mysqlSpecialTypeNumericLiteral(expr tree.Expr) bool {
	switch value := unwrapParenExpr(expr).(type) {
	case *tree.NumVal:
		switch value.ValType {
		case tree.P_int64, tree.P_uint64, tree.P_float64:
			return true
		}
	case *tree.Tuple:
		if len(value.Exprs) == 0 {
			return false
		}
		for _, item := range value.Exprs {
			if !mysqlSpecialTypeNumericLiteral(item) {
				return false
			}
		}
		return true
	}
	return false
}

func (b *baseBinder) bindTupleInByAst(leftTuple *tree.Tuple, rightTuple *tree.Tuple, depth int32, isNot bool) (*plan.Expr, error) {
	candidates := make([]*plan.Expr, 0, len(rightTuple.Exprs))
	leftMemoIDs := make([][]int32, len(leftTuple.Exprs))

	for _, rightVal := range rightTuple.Exprs {
		rightTupleVal, ok := unwrapParenExpr(rightVal).(*tree.Tuple)
		if !ok {
			return nil, moerr.NewInternalError(b.GetContext(), "IN list must contain tuples")
		}
		if len(leftTuple.Exprs) != len(rightTupleVal.Exprs) {
			return nil, moerr.NewInternalError(b.GetContext(), "tuple length mismatch")
		}

		equalities := make([]*plan.Expr, 0, len(leftTuple.Exprs))
		for i := 0; i < len(leftTuple.Exprs); i++ {
			eqExpr, err := b.bindFuncExprImplByAstExpr("=", []tree.Expr{leftTuple.Exprs[i], rightTupleVal.Exprs[i]}, depth)
			if err != nil {
				return nil, err
			}
			if eqFunc := eqExpr.GetF(); eqFunc != nil && len(eqFunc.Args) == 2 &&
				containsVolatileFunction(eqFunc.Args[0]) && b.ctx != nil {
				b.markTupleVolatileSources(eqFunc.Args[0], &leftMemoIDs[i])
			}
			equalities = append(equalities, eqExpr)
		}

		candidate, err := combinePlanExprsBalanced(b.GetContext(), "and", equalities)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}

	newExpr, err := combinePlanExprsBalanced(b.GetContext(), "or", candidates)
	if err != nil {
		return nil, err
	}

	if isNot {
		return BindFuncExprImplByPlanExpr(b.GetContext(), "not", []*plan.Expr{newExpr})
	}
	return newExpr, nil
}

func (b *baseBinder) markTupleVolatileSources(expr *plan.Expr, memoIDs *[]int32) {
	sources := make([]*plan.Expr, 0, 1)
	collectVolatileFunctionSources(expr, &sources)
	if len(sources) == 0 {
		// Unknown expression forms stay conservative and preserve the prior
		// whole-operand behavior.
		sources = append(sources, expr)
	}
	for len(*memoIDs) < len(sources) {
		b.ctx.volatileExprMemoID--
		*memoIDs = append(*memoIDs, b.ctx.volatileExprMemoID)
	}
	for i, source := range sources {
		source.AuxId = (*memoIDs)[i]
	}
}

func collectVolatileFunctionSources(expr *plan.Expr, sources *[]*plan.Expr) {
	if expr == nil {
		return
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Sub:
		*sources = append(*sources, expr)
	case *plan.Expr_F:
		if e.F == nil {
			return
		}
		if e.F.Func != nil {
			overload, ok := function.GetFunctionByIdWithoutError(e.F.Func.Obj)
			if ok && overload.CannotFold() {
				*sources = append(*sources, expr)
			}
		}
		for _, arg := range e.F.Args {
			collectVolatileFunctionSources(arg, sources)
		}
	case *plan.Expr_List:
		if e.List != nil {
			for _, item := range e.List.List {
				collectVolatileFunctionSources(item, sources)
			}
		}
	}
}

// combinePlanExprsBalanced preserves the input order while building a
// logarithmic-depth boolean tree. Large tuple or mixed-type IN lists used to
// create a left-deep tree, amplifying binder/optimizer recursion and making
// otherwise valid statements vulnerable to stack growth.
func combinePlanExprsBalanced(ctx context.Context, op string, exprs []*plan.Expr) (*plan.Expr, error) {
	if len(exprs) == 0 {
		return nil, nil
	}

	level := exprs
	for len(level) > 1 {
		next := make([]*plan.Expr, 0, (len(level)+1)/2)
		for i := 0; i < len(level); i += 2 {
			if i+1 == len(level) {
				next = append(next, level[i])
				continue
			}

			combined, err := BindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{level[i], level[i+1]})
			if err != nil {
				return nil, err
			}
			next = append(next, combined)
		}
		level = next
	}
	return level[0], nil
}

func (b *baseBinder) bindFuncExpr(astExpr *tree.FuncExpr, depth int32, isRoot bool) (*Expr, error) {
	funcRef, ok := astExpr.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, moerr.NewNYIf(b.GetContext(), "function expr '%v'", astExpr)
	}
	funcName := funcRef.ColName()
	if strings.EqualFold(funcName, "grouping") {
		return b.bindGroupingFuncExpr(astExpr)
	}
	if strings.EqualFold(funcName, "mod") && b.numericParamType == nil {
		return b.bindNumericExprWithDefaultContext(astExpr, depth, b.defaultNumericOuterType())
	}
	if supportsGenericNumericFunctionContext(strings.ToLower(funcName)) &&
		mysqlSpecialTypeInExprs(b, astExpr.Exprs) {
		return b.bindWithRawMySQLSpecialTypes(func() (*Expr, error) {
			return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
		})
	}
	if (strings.EqualFold(funcName, NamePercentileCont) || strings.EqualFold(funcName, NamePercentileDisc)) &&
		astExpr.WindowSpec != nil {
		return nil, moerr.NewNotSupported(b.GetContext(),
			"ordered-set percentile window functions")
	}
	// Resolve ambiguous scalar numeric overloads while the statement is being
	// prepared. The parameter itself remains a ParamRef under the DOUBLE cast;
	// ABS records this fallback so execution can rebind integer protocol values
	// exactly, while SLEEP keeps the stable DOUBLE domain for the cached plan.
	if b.builder != nil && b.builder.isPrepareStatement {
		if target, ok := preparedNumericFunctionTarget(funcName, len(astExpr.Exprs)); ok && target != nil &&
			(strings.EqualFold(funcName, "abs") || strings.EqualFold(funcName, "sleep")) {
			hasPreparedParam, err := b.hasPreparedNumericParamExprs(astExpr.Exprs, depth)
			if err != nil {
				return nil, err
			}
			if !hasPreparedParam {
				return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
			}
			return b.bindPreparedNumericFuncExpr(funcName, astExpr.Exprs, depth)
		}
	}

	if function.GetFunctionIsAggregateByName(funcName) && astExpr.WindowSpec == nil {

		expr, err := b.impl.BindAggFunc(funcName, astExpr, depth, isRoot)
		if err != nil {
			return expr, err
		}
		if b.ctx.timeTag > 0 {
			return b.impl.BindTimeWindowFunc(funcName, astExpr, depth, isRoot)
		}
		return expr, err
	} else if function.GetFunctionIsWinFunByName(funcName) {
		return b.impl.BindWinFunc(funcName, astExpr, depth, isRoot)
	}

	return b.bindFuncExprImplByAstExpr(funcName, astExpr.Exprs, depth)
}

// bindGroupingFuncExpr binds GROUPING arguments directly to their registered
// GROUP BY columns. A GROUPING argument must identify one complete GROUP BY
// item; recursively binding a miss would incorrectly accept derived
// expressions such as GROUPING(a+b) for GROUP BY a, b.
func (b *baseBinder) bindGroupingFuncExpr(astExpr *tree.FuncExpr) (*plan.Expr, error) {
	if b.ctx == nil || !b.ctx.groupingFuncAllowed {
		return nil, moerr.NewInvalidGroupFuncUse(b.GetContext())
	}

	args := make([]*plan.Expr, len(astExpr.Exprs))
	for i, rawArg := range astExpr.Exprs {
		qualifiedArg, err := b.ctx.qualifyColumnNames(cloneTreeExpr(rawArg), NoAlias)
		if err != nil {
			return nil, err
		}
		colPos, ok := lookupGroupByAst(b.ctx, qualifiedArg, windowExprAstKey(qualifiedArg))
		if !ok {
			return nil, moerr.NewSyntaxErrorf(b.GetContext(),
				"Argument #%d of GROUPING function is not in GROUP BY", i+1)
		}
		if colPos < 0 || int(colPos) >= len(b.ctx.groups) {
			return nil, moerr.NewInternalErrorf(b.GetContext(),
				"GROUPING argument position out of range: %d", colPos)
		}
		args[i] = GetColExpr(b.ctx.groups[colPos].Typ, b.ctx.groupTag, colPos)
	}

	return BindFuncExprImplByPlanExpr(b.GetContext(), "grouping", args)
}

// hasPreparedNumericParamExprs uses the same numeric AST scan that drives
// assignment/operator context inference.  Keeping parameter discovery in that
// scanner is important: it understands scalar subqueries, selective CASE/IF
// result arguments, aliases/CTEs, and the other numeric expression forms that
// may be introduced by the parser.  A second hand-maintained AST allowlist can
// silently route an omitted form back to the broken integer overload.
func (b *baseBinder) hasPreparedNumericParamExprs(exprs []tree.Expr, depth int32) (bool, error) {
	for _, expr := range exprs {
		scan, err := b.numericAstTypesInternalWithHint(
			expr, depth, b.numericAstColumnResolver(), nil,
		)
		if err != nil {
			return false, err
		}
		if scan.hasParamRef {
			return true, nil
		}
	}
	return false, nil
}

func isPreparedNumericAggregate(name string, argCount int) bool {
	return argCount == 1 && (strings.EqualFold(name, "sum") || strings.EqualFold(name, "avg"))
}

func preparedNumericFunctionTarget(name string, argCount int) (*Type, bool) {
	// ABS and SLEEP both have integer and floating-point overloads. A bare
	// prepared parameter has TEXT transport type at PREPARE time, so letting
	// the generic overload resolver choose an integer cast makes valid binary
	// executions such as ABS(-1.5) and SLEEP(0.01) fail before the function can
	// see the value. Use DOUBLE as the deferred prepare-time domain. ABS marks
	// that fallback so execution can restore an exact integer overload when the
	// protocol reports an integer parameter; explicit user DOUBLE casts remain
	// ordinary DOUBLE expressions.
	if argCount == 1 && (strings.EqualFold(name, "abs") || strings.EqualFold(name, "sleep")) {
		typ := types.T_float64.ToType()
		target := makePlan2Type(&typ)
		return &target, true
	}
	if isPreparedNumericAggregate(name, argCount) {
		return nil, true
	}
	if argCount == 1 && strings.EqualFold(name, "ntile") {
		typ := types.T_int64.ToType()
		target := makePlan2Type(&typ)
		return &target, true
	}
	return nil, false
}

func (b *baseBinder) markPreparedNumericFallback(expr *plan.Expr) {
	if expr == nil {
		return
	}
	metadata := ensurePreparedNumericMetadata(expr)
	metadata.Fallback = true
	// Keep an explicitly invalid position until a marker is found.  Protobuf's
	// int32 zero value is a valid marker position, so leaving the field at zero
	// would make a malformed/legacy fallback look as though it belonged to the
	// first parameter.
	metadata.ParamPos = -1
	pos, ok := firstPlanParamPosition(expr)
	if !ok {
		pos, ok = b.firstPreparedParamPosition(expr, make(map[int32]struct{}))
	}
	if ok && pos >= 0 {
		metadata.ParamPos = pos
		// Scalar subqueries are flattened into a projected column before the
		// execute-time replacement rule runs.  Keep the same provenance on the
		// inner projection so rebinding can restore the complete expression
		// (for example ROUND(?) or ? + 0) instead of replacing the projected
		// column with the raw parameter and silently dropping the subquery.
		_, _, _ = b.markPreparedNumericSubquerySources(expr, pos, make(map[int32]struct{}))
	}
}

func firstPlanParamPosition(expr *plan.Expr) (int32, bool) {
	if expr == nil {
		return 0, false
	}
	if param := expr.GetP(); param != nil {
		return param.Pos, true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if pos, ok := firstPlanParamPosition(arg); ok {
				return pos, true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if pos, ok := firstPlanParamPosition(item); ok {
				return pos, true
			}
		}
	}
	if sub := expr.GetSub(); sub != nil {
		return firstPlanParamPosition(sub.Child)
	}
	return 0, false
}

// firstPreparedParamPosition extends firstPlanParamPosition to the query
// nodes referenced by scalar subqueries.  Expr_Sub.Child contains the left
// operand for quantified subqueries, but a scalar subquery's actual parameter
// normally lives in its PROJECT node, so it is not visible from the expression
// wrapper after binding.
func (b *baseBinder) firstPreparedParamPosition(expr *plan.Expr, visited map[int32]struct{}) (int32, bool) {
	if expr == nil {
		return 0, false
	}
	if pos, ok := firstPlanParamPosition(expr); ok {
		return pos, true
	}
	if sub := expr.GetSub(); sub != nil && b.builder != nil && b.builder.qry != nil &&
		sub.Typ == plan.SubqueryRef_SCALAR && sub.NodeId >= 0 && int(sub.NodeId) < len(b.builder.qry.Nodes) {
		if _, ok := visited[sub.NodeId]; ok {
			return 0, false
		}
		visited[sub.NodeId] = struct{}{}
		node := b.builder.qry.Nodes[sub.NodeId]
		if node != nil {
			for _, projection := range node.ProjectList {
				if pos, ok := b.firstPreparedParamPosition(projection, visited); ok {
					return pos, true
				}
			}
		}
	}
	return 0, false
}

func (b *baseBinder) markPreparedNumericSubquerySources(
	expr *plan.Expr,
	pos int32,
	visited map[int32]struct{},
) (int32, int32, bool) {
	if expr == nil {
		return 0, 0, false
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if nodeID, colPos, ok := b.markPreparedNumericSubquerySources(arg, pos, visited); ok {
				metadata := ensurePreparedNumericMetadata(expr)
				metadata.FallbackSource = true
				metadata.FallbackSourceNodeId = nodeID
				metadata.FallbackSourceColPos = colPos
				return nodeID, colPos, true
			}
		}
	case *plan.Expr_List:
		for _, item := range exprImpl.List.List {
			if nodeID, colPos, ok := b.markPreparedNumericSubquerySources(item, pos, visited); ok {
				metadata := ensurePreparedNumericMetadata(expr)
				metadata.FallbackSource = true
				metadata.FallbackSourceNodeId = nodeID
				metadata.FallbackSourceColPos = colPos
				return nodeID, colPos, true
			}
		}
	case *plan.Expr_Sub:
		if exprImpl.Sub == nil || exprImpl.Sub.Typ != plan.SubqueryRef_SCALAR ||
			b.builder == nil || b.builder.qry == nil || exprImpl.Sub.NodeId < 0 ||
			int(exprImpl.Sub.NodeId) >= len(b.builder.qry.Nodes) {
			if exprImpl.Sub != nil {
				return b.markPreparedNumericSubquerySources(exprImpl.Sub.Child, pos, visited)
			}
			return 0, 0, false
		}
		if _, ok := visited[exprImpl.Sub.NodeId]; ok {
			return 0, 0, false
		}
		visited[exprImpl.Sub.NodeId] = struct{}{}
		node := b.builder.qry.Nodes[exprImpl.Sub.NodeId]
		if node == nil {
			return 0, 0, false
		}
		for colPos, projection := range node.ProjectList {
			if projection == nil || !planExprContainsParamPosition(projection, pos) {
				continue
			}
			projectionMetadata := ensurePreparedNumericMetadata(projection)
			projectionMetadata.Fallback = true
			projectionMetadata.ParamPos = pos
			projectionMetadata.FallbackSource = true
			projectionMetadata.FallbackSourceNodeId = exprImpl.Sub.NodeId
			projectionMetadata.FallbackSourceColPos = int32(colPos)
			exprMetadata := ensurePreparedNumericMetadata(expr)
			exprMetadata.FallbackSource = true
			exprMetadata.FallbackSourceNodeId = exprImpl.Sub.NodeId
			exprMetadata.FallbackSourceColPos = int32(colPos)
			b.markPreparedNumericSubquerySources(projection, pos, visited)
			return exprImpl.Sub.NodeId, int32(colPos), true
		}
	}
	return 0, 0, false
}

func planExprContainsParamPosition(expr *plan.Expr, pos int32) bool {
	if expr == nil {
		return false
	}
	found := false
	_ = plan.VisitExprTree(expr, func(candidate *plan.Expr) error {
		if param := candidate.GetP(); param != nil && param.Pos == pos {
			found = true
		}
		return nil
	})
	return found
}

func containsExplicitFloatCast(expr tree.Expr) bool {
	if expr == nil {
		return false
	}
	switch e := expr.(type) {
	case *tree.CastExpr:
		if typ, ok := e.Type.(*tree.T); ok {
			switch defines.MysqlType(typ.InternalType.Oid) {
			case defines.MYSQL_TYPE_FLOAT, defines.MYSQL_TYPE_DOUBLE:
				return true
			}
		}
		return containsExplicitFloatCast(e.Expr)
	case *tree.BitCastExpr:
		return containsExplicitFloatCast(e.Expr)
	case *tree.BinaryExpr:
		return containsExplicitFloatCast(e.Left) || containsExplicitFloatCast(e.Right)
	case *tree.UnaryExpr:
		return containsExplicitFloatCast(e.Expr)
	case *tree.ParenExpr:
		return containsExplicitFloatCast(e.Expr)
	case *tree.FuncExpr:
		return containsExplicitFloatCasts(e.Exprs)
	case *tree.CaseExpr:
		if containsExplicitFloatCast(e.Expr) {
			return true
		}
		for _, when := range e.Whens {
			if when != nil && (containsExplicitFloatCast(when.Cond) || containsExplicitFloatCast(when.Val)) {
				return true
			}
		}
		return containsExplicitFloatCast(e.Else)
	case *tree.Tuple:
		return containsExplicitFloatCasts(e.Exprs)
	case *tree.Subquery:
		return containsExplicitFloatCastInSelect(e.Select)
	default:
		return false
	}
}

func containsExplicitFloatCasts(exprs []tree.Expr) bool {
	for _, expr := range exprs {
		if containsExplicitFloatCast(expr) {
			return true
		}
	}
	return false
}

func containsExplicitFloatCastInSelect(stmt tree.SelectStatement) bool {
	switch stmt := stmt.(type) {
	case *tree.Select:
		return containsExplicitFloatCastInSelect(stmt.Select)
	case *tree.ParenSelect:
		return containsExplicitFloatCastInSelect(stmt.Select)
	case *tree.UnionClause:
		return containsExplicitFloatCastInSelect(stmt.Left) || containsExplicitFloatCastInSelect(stmt.Right)
	case *tree.SelectClause:
		for _, item := range stmt.Exprs {
			if containsExplicitFloatCast(item.Expr) {
				return true
			}
		}
	}
	return false
}

// bindPreparedNumericFuncExpr gives prepared numeric function arguments the
// same static context as prepared arithmetic. SUM/AVG use the inferred numeric
// domain, while NTILE requires an integer domain. ParamRef remains TEXT for
// transport and an explicit cast materializes the computation type.
// Non-parameter expressions stay on their original binding path, so ordinary
// string inputs continue to be rejected by function overload resolution.
func (b *baseBinder) bindPreparedNumericFuncExpr(
	name string,
	astArgs []tree.Expr,
	depth int32,
) (*plan.Expr, error) {
	target, ok := preparedNumericFunctionTarget(name, len(astArgs))
	if b.builder == nil || !b.builder.isPrepareStatement || !ok {
		return b.bindFuncExprImplByAstExpr(name, astArgs, depth)
	}

	// Binding can normalize the parsed CAST node in place. Snapshot the user's
	// explicit floating-point boundary before that mutation so
	// ABS(CAST(? AS DOUBLE)) remains on its fixed DOUBLE overload.
	hasExplicitFloatCast := containsExplicitFloatCast(astArgs[0])
	arg, err := b.bindNumericExprWithContext(astArgs[0], depth, target)
	if err != nil {
		return nil, err
	}
	if strings.EqualFold(name, "abs") && !hasExplicitFloatCast {
		b.markPreparedNumericFallback(arg)
	}
	args, err := b.coerceBoolNumericAggregateArg(name, []*plan.Expr{arg})
	if err != nil {
		return nil, err
	}
	return bindBoundFuncExprAndConstFold(
		b.GetContext(), b.builder.compCtx.GetProcess(), name, args,
	)
}

func (b *baseBinder) bindFullTextMatchExpr(astExpr *tree.FullTextMatchExpr, depth int32, isRoot bool) (*Expr, error) {

	args := make([]*Expr, 2+len(astExpr.KeyParts))

	mode := int64(astExpr.Mode)
	pattern, err := b.impl.BindExpr(astExpr.Pattern, depth, false)
	if err != nil {
		return nil, err
	}
	if pattern.Typ.Id != int32(types.T_varchar) {
		varcharTyp := types.T_varchar.ToType()
		pattern, err = makePlan2CastExpr(b.GetContext(), pattern, makePlan2Type(&varcharTyp))
		if err != nil {
			return nil, err
		}
	}
	args[0] = pattern
	args[1] = makePlan2Int64ConstExprWithType(mode)
	for i, k := range astExpr.KeyParts {
		c, err := b.baseBindColRef(k.ColName, depth, isRoot)
		if err != nil {
			return nil, err
		}
		args[i+2] = c
	}

	return BindFuncExprImplByPlanExpr(b.GetContext(), "fulltext_match", args)
}

// coerceBoolNumericAggregateArg gives SUM/AVG over a BOOL argument the MySQL
// reading under the ENABLE_BOOL_SUMAVG sql_mode by binding that argument as
// TINYINT. MySQL has no BOOL type: a predicate there is an integer 0/1 and
// SUM/AVG over one is ordinary numeric aggregation, while MO types it as BOOL,
// which SumSupportedTypes rejects. The cast is exactly the
// sum(cast(pred as tinyint)) a user writes today, so it reuses the existing
// integer aggregate (no new aggregate state, no executor path, no per-row
// cost) and keeps sum(bool) -> BIGINT consistent with sum(tinyint).
//
// The mode is read from the builder flag that NewQueryBuilder resolved once
// from sql_mode, the same way ONLY_FULL_GROUP_BY is, so the direct and the
// prepared bind paths agree and a binder without a builder stays strict.
func (b *baseBinder) coerceBoolNumericAggregateArg(
	name string, args []*plan.Expr,
) ([]*plan.Expr, error) {
	if b.builder == nil || !b.builder.boolSumAvgCompat || len(args) != 1 ||
		args[0].Typ.Id != int32(types.T_bool) ||
		!(strings.EqualFold(name, "sum") || strings.EqualFold(name, "avg")) {
		return args, nil
	}
	tinyint := types.T_int8.ToType()
	casted, err := appendCastBeforeExpr(b.GetContext(), args[0], makePlan2Type(&tinyint))
	if err != nil {
		return nil, err
	}
	return []*plan.Expr{casted}, nil
}

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	if (name == "utc_time" || name == "utc_timestamp") && len(astArgs) == 1 {
		if _, ok := astArgs[0].(*tree.NumVal); !ok {
			return nil, invalidUTCFunctionFSPError(b.GetContext(), name)
		}
	}
	isIfNull := name == "ifnull"

	// rewrite some ast Exprs before binding
	switch name {
	case "nullif":
		// rewrite 'nullif(expr1, expr2)' to 'case when expr1=expr2 then null else expr1'
		if len(astArgs) != 2 {
			return nil, moerr.NewInvalidArg(b.GetContext(), "nullif need two args", len(astArgs))
		}
		elseExpr := astArgs[0]
		thenExpr := tree.NewNumVal("", "", false, tree.P_null)
		whenExpr := tree.NewComparisonExpr(tree.EQUAL, astArgs[0], astArgs[1])
		astArgs = []tree.Expr{whenExpr, thenExpr, elseExpr}
		name = "case"

	case "ifnull":
		// rewrite 'ifnull(expr1, expr2)' to 'case when isnull(expr1) then expr2 else null'
		if len(astArgs) != 2 {
			return nil, moerr.NewInvalidArg(b.GetContext(), "ifnull function need two args", len(astArgs))
		}
		elseExpr := astArgs[0]
		thenExpr := astArgs[1]
		whenExpr := tree.NewIsNullExpr(astArgs[0])
		astArgs = []tree.Expr{whenExpr, thenExpr, elseExpr}
		name = "case"

		//case "extract":
		//	// "extract(year from col_name)"  parser return year as UnresolvedName.
		//	// we must rewrite it to string。 because binder bind UnresolvedName as column name
		//	unit := astArgs[0].(*tree.UnresolvedName).Parts[0]
		//	astArgs[0] = tree.NewNumVal(constant.MakeString(unit), unit, false)

	case "count":
		if b.ctx == nil {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid field reference to COUNT")
		}
		// we will rewrite "count(*)" to "starcount(col)"
		// count(*) : astExprs[0].(type) is *tree.NumVal
		// count(col_name) : astExprs[0].(type) is *tree.UnresolvedName
		switch nval := astArgs[0].(type) {
		case *tree.NumVal:
			if nval.String() == "*" {
				if len(b.ctx.bindings) == 0 || len(b.ctx.bindings[0].cols) == 0 {
					// sql: 'select count(*)' without from clause. we do nothing
				} else {
					// sql: 'select count(*) from t1',
					// rewrite count(*) to starcount(col_name)
					name = "starcount"

					astArgs = []tree.Expr{tree.NewNumVal(int64(1), "1", false, tree.P_int64)}
				}
			}
		}

	case "approx_count":
		if b.ctx == nil {
			return nil, moerr.NewInvalidInput(b.GetContext(), "invalid field reference to COUNT")
		}
		name = "count"

	case "trim":
		astArgs = astArgs[1:]
	}

	// bind ast function's args
	var args []*Expr
	if name == "bit_cast" {
		bitCastExpr := astArgs[0].(*tree.BitCastExpr)
		binExpr, err := b.impl.BindExpr(bitCastExpr.Expr, depth, false)
		if err != nil {
			return nil, err
		}

		typ, err := getTypeFromAst(b.GetContext(), bitCastExpr.Type)
		if err != nil {
			return nil, err
		}
		typeExpr := &Expr{
			Typ: typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}

		args = []*Expr{binExpr, typeExpr}
	} else if name == "serial_extract" {
		serialExtractExpr := astArgs[0].(*tree.SerialExtractExpr)

		// 1. bind serial expr
		serialExpr, err := b.impl.BindExpr(serialExtractExpr.SerialExpr, depth, false)
		if err != nil {
			return nil, err
		}

		// 2. bind index expr
		idxExpr, err := b.impl.BindExpr(serialExtractExpr.IndexExpr, depth, false)
		if err != nil {
			return nil, err
		}

		// 3. bind type
		typ, err := getTypeFromAst(b.GetContext(), serialExtractExpr.ResultType)
		if err != nil {
			return nil, err
		}
		typeExpr := &Expr{
			Typ: typ,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		}

		// 4. return [serialExpr, idxExpr, typeExpr]. Used in list_builtIn.go
		args = []*Expr{serialExpr, idxExpr, typeExpr}
	} else {
		args = make([]*Expr, len(astArgs))
		var functionContext numericFunctionContext
		hasFunctionContext := false
		if b.numericFunctionTarget {
			functionContext, hasFunctionContext = b.resolveNumericFunctionArgs(
				name, astArgs, depth, b.numericAstColumnResolver(), b.numericParamType,
			)
		}
		for idx, arg := range astArgs {
			paramType := b.numericParamType
			subqueryTarget := b.numericSubqueryTarget
			if paramType != nil && numericFunctionHasSelectiveContext(name) &&
				!numericFunctionArgKeepsContext(name, idx, len(astArgs)) {
				b.numericParamType = nil
				b.numericSubqueryTarget = nil
			} else if paramType != nil && hasFunctionContext {
				if functionContext.dynamic[idx] &&
					makeTypeByPlan2Type(functionContext.argTypes[idx]).IsNumeric() {
					argTarget := functionContext.argTypes[idx]
					b.numericParamType = &argTarget
					b.numericSubqueryTarget = &argTarget
				} else {
					b.numericParamType = nil
					b.numericSubqueryTarget = nil
				}
			} else if paramType != nil && !isNumericContextFunction(name) &&
				!numericFunctionHasSelectiveContext(name) {
				// A function outside the explicit domain-preserving metadata must
				// resolve its arguments independently of the assignment target.
				b.numericParamType = nil
				b.numericSubqueryTarget = nil
			}
			expr, err := b.impl.BindExpr(arg, depth, false)
			b.numericParamType = paramType
			b.numericSubqueryTarget = subqueryTarget
			if err != nil {
				return nil, err
			}

			args[idx] = expr
		}
	}
	preparedNumericPeer := false
	preparedNumericProvenance := false
	if b.builder != nil && b.builder.isPrepareStatement &&
		(isNumericContextFunction(name) || supportsGenericNumericFunctionContext(name) ||
			preparedSQLExecuteNumericResultConsumer(name) || name == "iff") {
		var err error
		preparedNumericProvenance, err = b.hasPreparedNumericParamExprs(astArgs, depth)
		if err != nil {
			return nil, err
		}
		if !preparedNumericProvenance && (preparedSQLExecuteNumericResultConsumer(name) || name == "iff") {
			for _, arg := range args {
				if preparedExprContainsParam(arg) {
					preparedNumericProvenance = true
					break
				}
			}
		}
		preparedNumericPeer = preparedNumericProvenance && name == "/"
	}
	if b.numericParamType != nil || preparedNumericPeer {
		var err error
		args, err = b.resolvePreparedNumericArgs(name, args)
		if err != nil {
			return nil, err
		}
	}
	preparedPeerSources := make([]*plan.Expr, len(args))
	if preparedNumericProvenance {
		for i, arg := range args {
			if arg == nil || preparedExprContainsParam(arg) {
				continue
			}
			source := arg
			fn := arg.GetF()
			explicitCast, explicitPeerCast := astArgs[i].(*tree.CastExpr)
			if explicitPeerCast {
				target, targetErr := getTypeFromAst(b.GetContext(), explicitCast.Type)
				if targetErr != nil {
					return nil, targetErr
				}
				if types.T(target.Id).IsDecimal() {
					inner, innerErr := b.impl.BindExpr(explicitCast.Expr, depth, false)
					if innerErr != nil {
						return nil, innerErr
					}
					source, innerErr = appendCastBeforeExpr(b.GetContext(), inner, target)
					if innerErr != nil {
						return nil, innerErr
					}
				}
			}
			if fn != nil && fn.Func != nil && strings.EqualFold(fn.Func.GetObjName(), "cast") && len(fn.Args) > 0 &&
				(!explicitPeerCast || makeTypeByPlan2Expr(arg).Oid.IsMySQLString()) &&
				!preparedExprContainsParam(fn.Args[0]) &&
				preparedNumericCommonOperandType(makeTypeByPlan2Expr(fn.Args[0]).Oid) {
				source = fn.Args[0]
			}
			sourceType := makeTypeByPlan2Expr(source)
			if preparedNumericCommonOperandType(sourceType.Oid) && !sourceType.Oid.IsFloat() {
				// Preserve only a proven exact peer. Scientific FLOAT literals and
				// explicit FLOAT casts remain source-less semantic FLOAT boundaries.
				preparedPeerSources[i] = DeepCopyExpr(source)
			}
		}
	}
	args = useStoredMySQLSpecialTypesForNumericContract(b.GetContext(), name, args)
	args, coerceErr := b.coerceBoolNumericAggregateArg(name, args)
	if coerceErr != nil {
		return nil, coerceErr
	}
	if name == "avg" && len(astArgs) == 1 && len(args) == 1 {
		// MySQL derives AVG's exact result from an integer literal/constant
		// expression's decimal precision, not from the physical BIGINT container
		// used for untyped literals. Columns and explicit integer casts deliberately
		// keep their full declared domains.
		b.setAvgIntegerLiteralPrecision(astArgs[0], args[0])
	}
	if (name == "in" || name == "not_in") && len(args) == 2 &&
		containsVolatileFunction(args[0]) && b.ctx != nil {
		b.markVolatileInLeft(args[0])
	}
	//promote interval expr rewrite here
	if name == "interval" {
		if len(astArgs) == 2 {
			//interval expr like 'interval 5 day'
			if _, ok := astArgs[1].(*tree.TimeUnitExpr); ok {
				// rewrite interval function to ListExpr, and return directly
				return &plan.Expr{
					Typ: plan.Type{
						Id: int32(types.T_interval),
					},
					Expr: &plan.Expr_List{
						List: &plan.ExprList{
							List: args,
						},
					},
				}, nil
			}
		}
	}
	if name == "name_const" {
		if !validNameConstNameAst(astArgs) ||
			!validNameConstValueAst(astArgs, b.allowCanonicalNameConstValueCast) {
			return nil, moerr.NewInvalidArg(b.GetContext(), "NAME_CONST", "")
		}
		if err := validateNameConstArgs(
			b.GetContext(),
			args,
			b.allowCanonicalNameConstValueCast,
		); err != nil {
			return nil, err
		}
	}

	if b.builder != nil {
		e, err := bindBoundFuncExprAndConstFold(b.GetContext(), b.builder.compCtx.GetProcess(), name, args)
		if err == nil {
			if fn := e.GetF(); fn != nil {
				for i, source := range preparedPeerSources {
					if source == nil || i >= len(fn.Args) {
						continue
					}
					boundType := makeTypeByPlan2Expr(fn.Args[i]).Oid
					if !boundType.IsFloat() && !boundType.IsMySQLString() {
						continue
					}
					if literal := fn.Args[i].GetLit(); literal != nil && literal.Src == nil {
						literal.Src = source
						continue
					}
					castFn := fn.Args[i].GetF()
					if castFn != nil && castFn.Func != nil && castFn.Func.GetObjName() == "cast" && len(castFn.Args) > 0 {
						_, overload := function.DecodeOverloadID(castFn.Func.GetObj())
						if literal := castFn.Args[0].GetLit(); overload == 0 && literal != nil && literal.Src == nil {
							// A later constant-fold pass reconstructs this implicit cast in
							// Literal.Src, preserving proof that its FLOAT result was provisional.
							literal.Src = source
						}
					}
				}
			}
			if isIfNull {
				e.Typ.NotNullable = args[1].Typ.NotNullable || args[2].Typ.NotNullable
			}
			markPreparedResultCastsProvisional(
				b.GetContext(), name, astArgs, preparedPeerSources, e, preparedNumericProvenance)
			return e, nil
		}
		if !strings.Contains(err.Error(), "not supported") {
			return nil, err
		}
		// The builtin binder also uses ErrNotSupported for its interval
		// no-escape postcondition. Preserve that boundary error instead of
		// misinterpreting it as an unknown builtin and falling through to UDF
		// resolution.
		if intervalErr := rejectBoundIntervalFunctionArgs(b.GetContext(), name, args); intervalErr != nil {
			return nil, intervalErr
		}
	} else {
		// return bindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		// first look for builtin func
		builtinExpr, err := bindFuncExprImplByPlanExpr(b.GetContext(), name, args, false)
		if err == nil {
			if isIfNull {
				builtinExpr.Typ.NotNullable = args[1].Typ.NotNullable || args[2].Typ.NotNullable
			}
			return builtinExpr, nil
		}
		if !strings.Contains(err.Error(), "not supported") {
			return nil, err
		}
		if intervalErr := rejectBoundIntervalFunctionArgs(b.GetContext(), name, args); intervalErr != nil {
			return nil, intervalErr
		}
	}

	// not a builtin func, look to resolve udf
	if b.builder == nil {
		return nil, moerr.NewInvalidInputf(
			b.GetContext(),
			"function '%s' is not allowed in this expression",
			name,
		)
	}
	cmpCtx := b.builder.compCtx
	udf, err := cmpCtx.ResolveUdf(name, args)
	if err != nil {
		return nil, err
	}

	return bindFuncExprImplUdf(b, name, udf, astArgs, args, depth)
}

func (b *baseBinder) setAvgIntegerLiteralPrecision(astExpr tree.Expr, arg *plan.Expr) {
	typ := makeTypeByPlan2Expr(arg)
	if !typ.Oid.IsInteger() {
		return
	}
	precision, ok := avgIntegerConstantPrecision(astExpr)
	if !ok || precision <= 0 {
		return
	}
	arg.Typ.Width = precision
	// Integer casts use Scale == -1 to carry physical bit width. Mark the
	// literal-derived precision as decimal metadata so AvgReturnType does
	// not reinterpret it as a cast domain.
	arg.Typ.Scale = 0
}

// AVG adds four fractional digits to an exact integer argument. Keep the
// precision recorded for a constant expression bounded to the largest input
// precision that can produce the public Decimal256(65,4) result. The executor
// applies the same cap; without it a long multiplication/addition chain could
// overflow int32 while constructing planner metadata.
const maxAvgIntegerExpressionPrecision int32 = 65 - 4

func capAvgIntegerExpressionPrecision(precision int32) int32 {
	if precision > maxAvgIntegerExpressionPrecision {
		return maxAvgIntegerExpressionPrecision
	}
	return precision
}

func addAvgIntegerExpressionPrecision(left, right int32) int32 {
	if left >= maxAvgIntegerExpressionPrecision || right >= maxAvgIntegerExpressionPrecision ||
		left > maxAvgIntegerExpressionPrecision-right {
		return maxAvgIntegerExpressionPrecision
	}
	return left + right
}

func avgIntegerConstantPrecision(astExpr tree.Expr) (int32, bool) {
	switch expr := astExpr.(type) {
	case *tree.ParenExpr:
		return avgIntegerConstantPrecision(expr.Expr)
	case *tree.UnaryExpr:
		if expr.Op != tree.UNARY_PLUS && expr.Op != tree.UNARY_MINUS {
			return 0, false
		}
		return avgIntegerConstantPrecision(expr.Expr)
	case *tree.NumVal:
		if expr.ValType != tree.P_int64 && expr.ValType != tree.P_uint64 {
			return 0, false
		}
		literal := strings.TrimLeft(expr.String(), "+-")
		if len(literal) > int(maxAvgIntegerExpressionPrecision) {
			return maxAvgIntegerExpressionPrecision, true
		}
		return int32(len(literal)), true
	case *tree.BinaryExpr:
		left, leftOK := avgIntegerConstantPrecision(expr.Left)
		right, rightOK := avgIntegerConstantPrecision(expr.Right)
		if !leftOK || !rightOK {
			return 0, false
		}
		switch expr.Op {
		case tree.MULTI:
			return addAvgIntegerExpressionPrecision(left, right), true
		case tree.PLUS, tree.MINUS:
			return capAvgIntegerExpressionPrecision(max(left, right) + 1), true
		case tree.MOD:
			return min(left, right), true
		default:
			return 0, false
		}
	default:
		return 0, false
	}
}

func markPreparedResultCastsProvisional(
	ctx context.Context,
	name string,
	astArgs []tree.Expr,
	peerSources []*plan.Expr,
	expr *plan.Expr,
	preparedNumericProvenance bool,
) {
	if !preparedNumericProvenance || expr == nil || expr.GetF() == nil {
		return
	}
	args := expr.GetF().Args
	for i, arg := range args {
		if i < len(peerSources) && peerSources[i] != nil {
			attachPreparedRuntimeParamSource(arg, DeepCopyExpr(peerSources[i]))
			ensurePreparedNumericMetadata(arg).ProvisionalResultPeer = true
		}
		if i < len(astArgs) {
			if explicitCast, ok := astArgs[i].(*tree.CastExpr); ok {
				if target, err := getTypeFromAst(ctx, explicitCast.Type); err == nil && types.T(target.Id).IsDecimal() {
					metadata := ensurePreparedNumericMetadata(arg)
					metadata.ProvisionalResultPeer = true
					metadata.ProvisionalResultPeerTypeId = target.Id
					metadata.ProvisionalResultPeerWidth = target.Width
					metadata.ProvisionalResultPeerScale = target.Scale
				}
			}
		}
		if i >= len(astArgs) || !preparedSQLExecuteNumericResultValueArg(name, i, len(args)) {
			continue
		}
		if _, explicit := astArgs[i].(*tree.CastExpr); explicit {
			continue
		}
		fn := arg.GetF()
		if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.GetObjName(), "cast") ||
			len(fn.Args) == 0 || !preparedExprContainsParam(fn.Args[0]) {
			continue
		}
		// This cast was introduced while the marker still had its prepare-time
		// TEXT domain. Record occurrence provenance because its function overload
		// can be identical to a user-authored CAST, which remains authoritative.
		fn.SyntaxExplicitCast = false
		ensurePreparedNumericMetadata(arg).ProvisionalResultCast = true
	}
}

func (b *baseBinder) markVolatileInLeft(left *plan.Expr) {
	if list := left.GetList(); list != nil {
		for _, elem := range list.List {
			if containsVolatileFunction(elem) {
				b.ctx.volatileExprMemoID--
				elem.AuxId = b.ctx.volatileExprMemoID
			}
		}
		return
	}
	b.ctx.volatileExprMemoID--
	left.AuxId = b.ctx.volatileExprMemoID
}

func (b *baseBinder) resolvePreparedNumericArgs(name string, args []*Expr) ([]*Expr, error) {
	if len(args) != 2 {
		return args, nil
	}

	left, right, _, ok := function.ResolveNumericBinaryTypes(
		name,
		makeTypeByPlan2Expr(args[0]),
		makeTypeByPlan2Expr(args[1]),
		nil,
	)
	if !ok {
		return args, nil
	}

	targets := []types.Type{left, right}
	for i := range args {
		if makeTypeByPlan2Expr(args[i]).Eq(targets[i]) {
			continue
		}
		original := DeepCopyExpr(args[i])
		cast, err := appendCastBeforeExpr(b.GetContext(), args[i], makePlan2Type(&targets[i]))
		if err != nil {
			return nil, err
		}
		if literal := cast.GetLit(); literal != nil && literal.Src == nil &&
			targets[i].Oid.IsFloat() && preparedNumericCommonOperandType(makeTypeByPlan2Expr(original).Oid) &&
			!makeTypeByPlan2Expr(original).Oid.IsFloat() {
			// Constant folding can erase why an exact peer became FLOAT while a
			// prepared marker was still TEXT. Preserve the bounded source expression
			// so execute-time specialization does not guess from a Dval's value.
			literal.Src = original
		}
		args[i] = cast
	}
	return args, nil
}

func bindFuncExprImplUdf(
	b *baseBinder,
	name string,
	udf *function.Udf,
	astArgs []tree.Expr,
	boundArgs []*plan.Expr,
	depth int32,
) (*plan.Expr, error) {
	if udf == nil {
		return nil, moerr.NewNotSupportedf(b.GetContext(), "function '%s'", name)
	}

	switch udf.Language {
	case string(tree.SQL):
		parserSQLMode := "PIPES_AS_CONCAT"
		if udf.SQLMode != nil {
			parserSQLMode = *udf.SQLMode
		}
		sql, udfArgs := b.expandSQLUdfArguments(udf.Body, boundArgs, parserSQLMode)
		restoreUdfArgs := b.pushSQLUdfArguments(udfArgs)
		defer restoreUdfArgs()
		// if does not contain SELECT, an expression. In order to pass the parser,
		// make it start with a 'SELECT'.

		var expr *plan.Expr

		if !strings.Contains(sql, "select") {
			sql = "select " + sql
			substmts, err := parsers.ParseWithSQLMode(b.GetContext(), dialect.MYSQL, sql, 1, parserSQLMode)
			if err != nil {
				return nil, err
			}
			defer func() {
				for _, stmt := range substmts {
					stmt.Free()
				}
			}()
			expr, err = b.impl.BindExpr(substmts[0].(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr, depth, false)
			if err != nil {
				return nil, err
			}
		} else {
			substmts, err := parsers.ParseWithSQLMode(b.GetContext(), dialect.MYSQL, sql, 1, parserSQLMode)
			if err != nil {
				return nil, err
			}
			defer func() {
				for _, stmt := range substmts {
					stmt.Free()
				}
			}()
			subquery := tree.NewSubquery(substmts[0], false)
			expr, err = b.impl.BindSubquery(subquery, false)
			if err != nil {
				return nil, err
			}
		}
		return expr, nil
	case string(tree.PYTHON):
		expr, err := b.bindPythonUdf(udf, astArgs, depth)
		if err != nil {
			return nil, err
		}
		return expr, nil
	default:
		return nil, moerr.NewInvalidArg(b.GetContext(), "function language", udf.Language)
	}
}

// expandSQLUdfArguments replaces each $n parameter with an identifier that is
// provably absent from the original UDF body. The identifier is resolved from
// sqlUdfArgs while the parsed body is bound, so a column argument keeps its
// outer-query identity even when an inner table exposes a column with the same
// name. Checking the entire body (including quotes and comments) is deliberately
// conservative: absence from the raw text guarantees that no user-authored
// identifier in the parsed tree can be captured by the marker.
func (b *baseBinder) expandSQLUdfArguments(sql string, args []*plan.Expr, sqlMode string) (string, map[string]*plan.Expr) {
	if len(args) == 0 {
		return sql, nil
	}

	var callID uint64
	if b.builder != nil {
		b.builder.nextSQLUdfCallID++
		callID = b.builder.nextSQLUdfCallID
	}

	markers := make(map[string]*plan.Expr, len(args))
	markerByOrdinal := make(map[int]string, len(args))
	foldedBody := strings.ToLower(sql)
	markerForOrdinal := func(ordinal int) string {
		if name, ok := markerByOrdinal[ordinal]; ok {
			return "`" + name + "`"
		}

		baseName := fmt.Sprintf("__mo_sql_udf_%d_arg_%d", callID, ordinal)
		name := baseName
		for collisionID := uint64(1); strings.Contains(foldedBody, name); collisionID++ {
			name = fmt.Sprintf("%s_%d", baseName, collisionID)
		}
		markerByOrdinal[ordinal] = name
		markers[name] = args[ordinal-1]
		return "`" + name + "`"
	}
	rewritten := replaceSQLUdfArgMarkers(sql, len(args), sqlMode, markerForOrdinal)
	return rewritten, markers
}

func replaceSQLUdfArgMarkers(sql string, argCount int, sqlMode string, markerForOrdinal func(int) string) string {
	scanner := mysqlparser.NewScannerWithSQLMode(
		dialect.MYSQL,
		sql,
		mysqlparser.ParseSQLModeFlags(sqlMode),
	)
	defer mysqlparser.PutScanner(scanner)

	var result strings.Builder
	result.Grow(len(sql))
	written := 0
	for {
		token, value := scanner.Scan()
		if token == 0 || token == mysqlparser.LEX_ERROR {
			break
		}
		if token != mysqlparser.ID || len(value) < 2 || value[0] != '$' {
			continue
		}

		ordinal, err := strconv.Atoi(value[1:])
		if err != nil || ordinal < 1 || ordinal > argCount {
			continue
		}

		// ID tokens are returned byte-for-byte from the source, so their start is
		// the scanner's current byte offset minus the token length.
		start := scanner.Pos - len(value)
		result.WriteString(sql[written:start])
		result.WriteString(markerForOrdinal(ordinal))
		written = scanner.Pos
	}

	result.WriteString(sql[written:])
	return result.String()
}

func (b *baseBinder) pushSQLUdfArguments(args map[string]*plan.Expr) func() {
	if b.ctx == nil || len(args) == 0 {
		return func() {}
	}

	previous := b.ctx.sqlUdfArgs
	b.ctx.sqlUdfArgs = args
	return func() {
		b.ctx.sqlUdfArgs = previous
	}
}

func (b *baseBinder) bindSQLUdfArgument(name *tree.UnresolvedName) (*plan.Expr, bool) {
	if b.ctx == nil || name.NumParts != 1 {
		return nil, false
	}

	argName := name.ColName()
	depth := int32(0)
	for ctx := b.ctx; ctx != nil; ctx = ctx.parent {
		if arg, ok := ctx.sqlUdfArgs[argName]; ok {
			expr, correlated := correlateSQLUdfArgument(arg, depth)
			if correlated {
				for inner := b.ctx; inner != nil && inner != ctx; inner = inner.parent {
					inner.isCorrelated = true
				}
			}
			return expr, true
		}
		depth++
	}
	return nil, false
}

func correlateSQLUdfArgument(arg *plan.Expr, depth int32) (*plan.Expr, bool) {
	expr := DeepCopyExpr(arg)
	correlated := false

	var rewrite func(*plan.Expr)
	rewrite = func(current *plan.Expr) {
		if current == nil {
			return
		}

		switch item := current.Expr.(type) {
		case *plan.Expr_Col:
			if depth > 0 {
				current.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{
					RelPos: item.Col.RelPos,
					ColPos: item.Col.ColPos,
					Depth:  depth,
				}}
				correlated = true
			}
		case *plan.Expr_Corr:
			item.Corr.Depth += depth
			correlated = true
		case *plan.Expr_Lit:
			rewrite(item.Lit.Src)
		case *plan.Expr_F:
			for _, child := range item.F.Args {
				rewrite(child)
			}
		case *plan.Expr_W:
			rewrite(item.W.WindowFunc)
			for _, child := range item.W.PartitionBy {
				rewrite(child)
			}
			for _, orderBy := range item.W.OrderBy {
				if orderBy != nil {
					rewrite(orderBy.Expr)
				}
			}
			if item.W.Frame != nil {
				if item.W.Frame.Start != nil {
					rewrite(item.W.Frame.Start.Val)
				}
				if item.W.Frame.End != nil {
					rewrite(item.W.Frame.End.Val)
				}
			}
		case *plan.Expr_Sub:
			rewrite(item.Sub.Child)
		case *plan.Expr_List:
			for _, child := range item.List.List {
				rewrite(child)
			}
		}
	}

	rewrite(expr)
	return expr, correlated
}

func (b *baseBinder) bindPythonUdf(udf *function.Udf, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
	args := make([]*Expr, 2*len(astArgs)+2)

	// python udf self info and query context
	args[0] = udf.GetPlanExpr()

	// bind ast function's args
	for idx, arg := range astArgs {
		expr, err := b.impl.BindExpr(arg, depth, false)
		if err != nil {
			return nil, err
		}
		args[idx+1] = expr
	}

	// function args
	fArgTypes := udf.GetArgsPlanType()
	for i, t := range fArgTypes {
		args[len(astArgs)+i+1] = &Expr{Typ: *t}
	}

	// function ret
	fRetType := udf.GetRetPlanType()
	args[2*len(astArgs)+1] = &Expr{Typ: *fRetType}

	return BindFuncExprImplByPlanExpr(b.GetContext(), "python_user_defined_function", args)
}

func bindFuncExprAndConstFold(ctx context.Context, proc *process.Process, name string, args []*Expr) (*plan.Expr, error) {
	return bindFuncExprAndConstFoldInternal(ctx, proc, name, args, true)
}

func bindBoundFuncExprAndConstFold(ctx context.Context, proc *process.Process, name string, args []*Expr) (*plan.Expr, error) {
	return bindFuncExprAndConstFoldInternal(ctx, proc, name, args, false)
}

func bindFuncExprAndConstFoldInternal(
	ctx context.Context,
	proc *process.Process,
	name string,
	args []*Expr,
	descendFunctions bool,
) (*plan.Expr, error) {
	if err := foldDecimalStringComparisonConstants(ctx, proc, name, args); err != nil {
		return nil, err
	}
	retExpr, err := bindFuncExprImplByPlanExpr(ctx, name, args, descendFunctions)
	if err != nil {
		return nil, err
	}

	switch retExpr.GetF().GetFunc().GetObjName() {
	case "nth_value":
		if err := validateNthValueArgs(ctx, proc, retExpr.GetF().Args); err != nil {
			return nil, err
		}

	case "+", "-", "*", "/", "div", "%", "mod", "unary_minus", "unary_plus", "unary_tilde", "cast", "serial", "serial_full":
		if proc != nil {
			tmpexpr, _ := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(retExpr), proc, false, true)
			if tmpexpr != nil {
				retExpr = tmpexpr
			}
		}

	case "name_const":
		if proc == nil {
			return nil, moerr.NewInvalidInput(ctx, "can't use name_const without proc")
		}
		if err := foldNameConstArgs(ctx, proc, retExpr.GetF().Args); err != nil {
			return nil, err
		}

	case "between":
		if proc == nil {
			goto between_fallback
		}

		fnArgs := retExpr.GetF().Args

		arg1, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[1], proc, false, true)
		if err != nil {
			goto between_fallback
		}
		fnArgs[1] = arg1

		lit0 := arg1.GetLit()
		if arg1.Typ.Id == int32(types.T_any) || lit0 == nil {
			if !containsDynamicParam(arg1) {
				goto between_fallback
			}
		}

		arg2, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[2], proc, false, true)
		if err != nil {
			goto between_fallback
		}
		fnArgs[2] = arg2

		lit1 := arg2.GetLit()
		if arg2.Typ.Id == int32(types.T_any) || lit1 == nil {
			if !containsDynamicParam(arg2) {
				goto between_fallback
			}
		}

		rangeCheckFn, _ := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{arg1, arg2})
		rangeCheckRes, _ := ConstantFold(batch.EmptyForConstFoldBatch, rangeCheckFn, proc, false, true)
		rangeCheckVal := rangeCheckRes.GetLit()
		if rangeCheckVal == nil || !rangeCheckVal.GetBval() {
			if !containsDynamicParam(arg1) && !containsDynamicParam(arg2) {
				goto between_fallback
			}
		}

		retExpr, _ = ConstantFold(batch.EmptyForConstFoldBatch, retExpr, proc, false, true)

	case "in_range":
		if proc == nil {
			return nil, moerr.NewInvalidInput(ctx, "can't use in_range without proc")
		}

		fnArgs := retExpr.GetF().Args

		arg3, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[3], proc, false, true)
		if err != nil {
			return nil, err
		}
		fnArgs[3] = arg3

		flagLit := arg3.GetLit()
		if arg3.Typ.Id != int32(types.T_uint8) || flagLit == nil {
			return nil, moerr.NewInvalidInput(ctx, "4th argument of in_range must be unsigned tinyint literal")
		}
		flag := flagLit.GetU8Val()

		arg1, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[1], proc, false, true)
		if err != nil {
			return nil, err
		}
		fnArgs[1] = arg1

		lit1 := arg1.GetLit()
		if arg1.Typ.Id == int32(types.T_any) || lit1 == nil {
			return nil, moerr.NewInvalidInput(ctx, "2nd argument of in_range must be constant")
		}

		arg2, err := ConstantFold(batch.EmptyForConstFoldBatch, fnArgs[2], proc, false, true)
		if err != nil {
			return nil, err
		}
		fnArgs[2] = arg2

		lit2 := arg2.GetLit()
		if arg2.Typ.Id == int32(types.T_any) || lit2 == nil {
			return nil, moerr.NewInvalidInput(ctx, "3rd argument of in_range must be constant")
		}

		fnName := "<="
		if flag != 0 {
			fnName = "<"
		}
		rangeCheckFn, _ := BindFuncExprImplByPlanExpr(ctx, fnName, []*plan.Expr{arg1, arg2})
		rangeCheckRes, _ := ConstantFold(batch.EmptyForConstFoldBatch, rangeCheckFn, proc, false, true)
		rangeCheckVal := rangeCheckRes.GetLit()
		if rangeCheckVal == nil {
			return nil, moerr.NewInvalidInput(ctx, "2nd and 3rd arguments not comparable")
		}
		if !rangeCheckVal.GetBval() {
			retExpr = DeepCopyExpr(kAlwaysFalseExpr)
		} else {
			retExpr, _ = ConstantFold(batch.EmptyForConstFoldBatch, retExpr, proc, false, true)
		}
	}

	return retExpr, nil

between_fallback:
	fnArgs := retExpr.GetF().Args
	leftFn, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{DeepCopyExpr(fnArgs[0]), fnArgs[1]})
	if err != nil {
		return nil, err
	}
	rightFn, err := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{fnArgs[0], fnArgs[2]})
	if err != nil {
		return nil, err
	}

	retExpr, err = BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{leftFn, rightFn})
	if err != nil {
		return nil, err
	}
	retExpr, err = ConstantFold(batch.EmptyForConstFoldBatch, retExpr, proc, false, true)
	if err != nil {
		return nil, err
	}

	return retExpr, nil
}

// validateNthValueArgs enforces MySQL's bind-time contract for NTH_VALUE:
// the offset must be a constant positive integer or a positional parameter.
// Folding first keeps valid constant expressions, such as 1 + 1, compatible
// with MySQL.
func validateNthValueArgs(ctx context.Context, proc *process.Process, args []*plan.Expr) error {
	if len(args) != 2 {
		return moerr.NewWrongArguments(ctx, "nth_value")
	}

	if isDirectDynamicParam(args[1]) {
		// Keep the marker intact so execution can validate both its source type
		// and its value after binding.
		return nil
	}
	if proc == nil {
		return moerr.NewWrongArguments(ctx, "nth_value")
	}

	offset, err := ConstantFold(batch.EmptyForConstFoldBatch, args[1], proc, false, true)
	if err != nil {
		return err
	}
	args[1] = offset

	lit := offset.GetLit()
	if lit == nil || lit.Isnull || !types.T(offset.Typ.Id).IsInteger() || !isPositiveIntegerLiteral(lit) {
		return moerr.NewWrongArguments(ctx, "nth_value")
	}
	return nil
}

func isPositiveIntegerLiteral(lit *plan.Literal) bool {
	switch value := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return value.I8Val > 0
	case *plan.Literal_I16Val:
		return value.I16Val > 0
	case *plan.Literal_I32Val:
		return value.I32Val > 0
	case *plan.Literal_I64Val:
		return value.I64Val > 0
	case *plan.Literal_U8Val:
		return value.U8Val > 0
	case *plan.Literal_U16Val:
		return value.U16Val > 0
	case *plan.Literal_U32Val:
		return value.U32Val > 0
	case *plan.Literal_U64Val:
		return value.U64Val > 0
	default:
		return false
	}
}

func bindSerialFuncOverExprList(ctx context.Context, name string, args []*Expr) (*plan.Expr, bool, error) {
	if name != function.SerialFunctionName && name != function.SerialFullFunctionName {
		return nil, false, nil
	}
	if len(args) != 1 {
		return nil, false, nil
	}

	listExpr, ok := args[0].Expr.(*plan.Expr_List)
	if !ok {
		return nil, false, nil
	}
	if listExpr.List == nil || len(listExpr.List.List) == 0 {
		return args[0], true, nil
	}

	// An IN-list is a set of scalar candidates, not one row-wise vector argument.
	// Bind serial(v0, v1, ...) as list(serial(v0), serial(v1), ...).
	for i, subExpr := range listExpr.List.List {
		newSubExpr, err := BindFuncExprImplByPlanExpr(ctx, name, []*Expr{subExpr})
		if err != nil {
			return nil, true, err
		}
		listExpr.List.List[i] = newSubExpr
		if i == 0 {
			args[0].Typ = newSubExpr.Typ
		}
	}
	return args[0], true, nil
}

func validateApproxPercentileArgs(ctx context.Context, args []*Expr) error {
	if len(args) != 2 {
		return nil
	}
	percentile := args[1]
	if percentile == nil || isNullExpr(percentile) || !rule.IsConstant(percentile, false) {
		return moerr.NewInvalidInput(ctx,
			"percentile argument of approx_percentile must be a non-null constant")
	}
	return nil
}

// validateOrderedPercentileArgs enforces the scalar MVP contract for the
// ordered-set percentile aggregates. The aggregate executor consumes the
// percentile as compile-time configuration, while the first argument is the
// value expression ordered by WITHIN GROUP.
func validateOrderedPercentileArgs(ctx context.Context, name string, args []*Expr) error {
	if len(args) != 2 {
		return moerr.NewInvalidInputf(ctx, "%s requires a value and a percentile argument", name)
	}
	percentile := args[1]
	if percentile == nil || isNullExpr(percentile) || !rule.IsConstant(percentile, false) {
		return moerr.NewInvalidInputf(ctx,
			"percentile argument of %s must be a non-null constant", name)
	}
	return nil
}

// bindMixedInListComparison preserves the scalar comparison domain for a
// single IN candidate. Multi-candidate string lists keep their common REAL
// fallback; applying exact normalization independently to each candidate would
// change the multi-operand coercion contract.
func bindMixedInListComparison(
	ctx context.Context,
	operator string,
	left, right *Expr,
	exactSingleComparison bool,
) (*plan.Expr, error) {
	operands := []*Expr{left, right}
	if exactSingleComparison {
		if err := normalizeDecimalStringLiteralComparisonArgs(ctx, operator, operands); err != nil {
			return nil, err
		}
	}
	left, right = operands[0], operands[1]
	leftType := makeTypeByPlan2Expr(left)
	rightType := makeTypeByPlan2Expr(right)
	stringLeftNumericRight := leftType.Oid.IsMySQLString() && (rightType.IsNumeric() || rightType.Oid == types.T_bool)
	numericLeftStringRight := rightType.Oid.IsMySQLString() && (leftType.IsNumeric() || leftType.Oid == types.T_bool)
	_, directStringRight := decimalStringLiteralValue(right)
	if stringLeftNumericRight || (!exactSingleComparison && numericLeftStringRight && directStringRight) {
		targetType := types.T_float64.ToType()
		operands = []*Expr{left, right}
		for i := range operands {
			var err error
			operands[i], err = appendCastBeforeExpr(ctx, operands[i], makePlan2Type(&targetType))
			if err != nil {
				return nil, err
			}
		}
		left, right = operands[0], operands[1]
	}
	operands = []*Expr{left, right}
	if err := adjustJsonDynamicParamType(ctx, operator, operands); err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(ctx, operator, operands)
}

func BindFuncExprImplByPlanExpr(ctx context.Context, name string, args []*Expr) (*plan.Expr, error) {
	return bindFuncExprImplByPlanExpr(ctx, name, args, true)
}

func bindFuncExprImplByPlanExpr(
	ctx context.Context,
	name string,
	args []*Expr,
	descendFunctions bool,
) (*plan.Expr, error) {
	var err error
	rejectIntervalArgs := rejectBoundIntervalFunctionArgs
	if descendFunctions {
		rejectIntervalArgs = rejectStandaloneIntervalFunctionArgs
	}
	if name == NameApproxPercentile {
		if err = validateApproxPercentileArgs(ctx, args); err != nil {
			return nil, err
		}
	}
	if name == NamePercentileCont || name == NamePercentileDisc {
		if err = validateOrderedPercentileArgs(ctx, name, args); err != nil {
			return nil, err
		}
	}

	if (name == "utc_time" || name == "utc_timestamp") && len(args) == 1 {
		if _, err := utcFunctionFSPFromPlanExpr(ctx, name, args[0]); err != nil {
			return nil, err
		}
	}

	// deal with some special function
	if listExpr, ok, err := bindSerialFuncOverExprList(ctx, name, args); ok || err != nil {
		return listExpr, err
	}
	if err := normalizeDecimalStringLiteralComparisonArgs(ctx, name, args); err != nil {
		return nil, err
	}
	if err := normalizeDecimalParamComparisonArgs(ctx, name, args); err != nil {
		return nil, err
	}
	if err := normalizeDecimalParamInArgs(ctx, name, args); err != nil {
		return nil, err
	}
	if err := normalizeTimeStringComparisonArgs(ctx, name, args); err != nil {
		return nil, err
	}

	switch name {
	case "and", "or", "not", "xor":
		// why not append cast function?
		// for i := 0; i < len(args); i++ {
		// 	if args[i].Typ.Id != types.T_bool {
		// 		arg, err := appendCastBeforeExpr(args[i], &plan.Type{
		// 			Id: types.T_bool,
		// 		})
		// 		if err != nil {
		// 			return nil, err
		// 		}
		// 		args[i] = arg
		// 	}
		// }
		if err := convertValueIntoBool(name, args, true); err != nil {
			return nil, err
		}
	case "=", "<=>", "<", "<=", ">", ">=", "<>":
		// why not append cast function?
		if err := convertValueIntoBool(name, args, false); err != nil {
			return nil, err
		}
		if err := adjustJsonDynamicParamType(ctx, name, args); err != nil {
			return nil, err
		}

		// Early detection for decimal comparisons
		if len(args) == 2 {
			if name == "=" && isDecimalComparisonAlwaysFalse(ctx, args[0], args[1]) {
				// Equality with incompatible precision is always false
				return makePlan2BoolConstExprWithType(false), nil
			}
			if name == "<>" && isDecimalComparisonAlwaysFalse(ctx, args[0], args[1]) {
				// Inequality with incompatible precision is always true
				return makePlan2BoolConstExprWithType(true), nil
			}
		}
	case "date_add", "date_sub":
		// rewrite date_add/date_sub function
		// date_add(col_name, "1 day"), will rewrite to date_add(col_name, number, unit)
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "date_add/date_sub function need two args", len(args))
		}
		// MySQL behavior: NULL literal as second argument should return syntax error
		if isNullExpr(args[1]) {
			return nil, moerr.NewSyntaxError(ctx, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'null)' at line 1")
		}
		args, err = resetDateFunction(ctx, args[0], args[1])
		if err != nil {
			return nil, err
		}
	case "uuid", "uuid_v7", "uuid_v1", "uuid_v6":
		// uuid(interval 1 minute) generates an id whose embedded timestamp is
		// the evaluation-time wall clock shifted by the interval; rewrite the
		// INTERVAL expression to (count, unit) args like date_add does. The
		// resulting (count, unit) overload is internal — reject direct
		// multi-argument calls like uuid_v7(5, 3) so only the rewrite above
		// can reach it.
		if len(args) == 1 && args[0].Typ.Id == int32(types.T_interval) {
			args, err = resetIntervalFunction(ctx, args[0])
			if err != nil {
				return nil, err
			}
		} else if len(args) > 1 {
			return nil, moerr.NewInvalidArg(ctx, name+" function needs zero or one arg", len(args))
		}
	case "mo_win_truncate":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "truncate function need two args", len(args))
		}
		if types.T(args[0].Typ.Id) == types.T_timestamp {
			if timeWindowIntervalUsesMicrosecond(args[1]) && args[0].Typ.Scale < 6 {
				args[0].Typ.Scale = 6
			}
		} else {
			sourceType := makeTypeByPlan2Expr(args[0])
			targetType := types.T_datetime.ToType()
			function.SetTargetScaleFromSource(&sourceType, &targetType)
			if timeWindowIntervalUsesMicrosecond(args[1]) && targetType.Scale < 6 {
				targetType.Scale = 6
			}
			args[0], err = appendCastBeforeExpr(ctx, args[0], makePlan2Type(&targetType))
			if err != nil {
				return nil, err
			}
		}
		args, err = resetDateFunction(ctx, args[0], args[1])
		if err != nil {
			return nil, err
		}
	case "mo_win_divisor":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "divisor function need two args", len(args))
		}
		a1, a2 := args[0], args[1]
		args, err = resetIntervalFunction(ctx, a1)
		if err != nil {
			return nil, err
		}
		args2, err := resetIntervalFunction(ctx, a2)
		if err != nil {
			return nil, err
		}
		args = append(args, args2...)
	case "adddate", "subdate":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "adddate/subdate function need two args", len(args))
		}
		args, err = resetDateFunction(ctx, args[0], args[1])
		if err != nil {
			return nil, err
		}
		if name == "adddate" {
			name = "date_add"
		} else {
			name = "date_sub"
		}
	case "+":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "operator + need two args", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
		if args[0].Typ.Id == int32(types.T_date) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_date) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_datetime) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_timestamp) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_datetime) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_varchar) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_int32) && args[1].Typ.Id == int32(types.T_interval) && intervalUnitIsDayOrLarger(args[1]) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_int64) && args[1].Typ.Id == int32(types.T_interval) && intervalUnitIsDayOrLarger(args[1]) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_int32) && intervalUnitIsDayOrLarger(args[0]) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if args[0].Typ.Id == int32(types.T_interval) && args[1].Typ.Id == int32(types.T_int64) && intervalUnitIsDayOrLarger(args[0]) {
			name = "date_add"
			args, err = resetDateFunctionArgs(ctx, args[1], args[0])
		} else if isCollatedTextPlanType(args[0]) && isCollatedTextPlanType(args[1]) {
			name = "concat"
		}
		if err != nil {
			return nil, err
		}
	case "-":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "operator - need two args", len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
		// rewrite "date '2001' - interval '1 day'" to date_sub(date '2001', 1, day(unit))
		if args[0].Typ.Id == int32(types.T_date) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_datetime) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_timestamp) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_interval) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_int32) && args[1].Typ.Id == int32(types.T_interval) && intervalUnitIsDayOrLarger(args[1]) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		} else if args[0].Typ.Id == int32(types.T_int64) && args[1].Typ.Id == int32(types.T_interval) && intervalUnitIsDayOrLarger(args[1]) {
			name = "date_sub"
			args, err = resetDateFunctionArgs(ctx, args[0], args[1])
		}
		if err != nil {
			return nil, err
		}
	case "*", "/", "%":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, fmt.Sprintf("operator %s need two args", name), len(args))
		}
		if isNullExpr(args[0]) {
			return args[0], nil
		}
		if isNullExpr(args[1]) {
			return args[1], nil
		}
	case "unary_minus":
		if len(args) == 0 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if argLit := args[0].GetLit(); args[0].Typ.Id == int32(types.T_uint64) && argLit != nil && argLit.GetU64Val() == 1<<63 {
			return makePlan2Int64ConstExprWithType(math.MinInt64), nil
		}
		if args[0].Typ.Id == int32(types.T_uint64) {
			args[0], err = appendCastBeforeExpr(ctx, args[0], plan.Type{
				Id:          int32(types.T_decimal128),
				NotNullable: args[0].Typ.NotNullable,
			})
			if err != nil {
				return nil, err
			}
		}
	case "in_range":
		if len(args) != 4 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[3].Typ.Id != int32(types.T_any) && args[3].Typ.Id != int32(types.T_uint8) {
			args[3], err = appendCastBeforeExpr(ctx, args[3], plan.Type{
				Id: int32(types.T_uint8),
			})
			if err != nil {
				return nil, err
			}
		}
	case "oct", "bit_and", "bit_or", "bit_xor":
		if len(args) == 0 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id == int32(types.T_decimal128) || args[0].Typ.Id == int32(types.T_decimal64) {
			args[0], err = appendCastBeforeExpr(ctx, args[0], plan.Type{
				Id:          int32(types.T_float64),
				NotNullable: args[0].Typ.NotNullable,
			})
			if err != nil {
				return nil, err
			}
		}
	case "like", "ilike":
		// sql 'select * from t where col like ?'  the ? Expr's type will be T_any
		if len(args) != 2 && len(args) != 3 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id == int32(types.T_any) {
			args[0].Typ.Id = int32(types.T_varchar)
		}
		if args[1].Typ.Id == int32(types.T_any) {
			args[1].Typ.Id = int32(types.T_varchar)
		}
		if len(args) == 3 && args[2].Typ.Id == int32(types.T_any) {
			args[2].Typ.Id = int32(types.T_varchar)
		}
		if args[0].Typ.Id == int32(types.T_json) {
			targetTp := types.T_varchar.ToType()
			args[0], err = appendCastBeforeExpr(ctx, args[0], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
		}
		if args[1].Typ.Id == int32(types.T_json) {
			targetTp := types.T_varchar.ToType()
			args[1], err = appendCastBeforeExpr(ctx, args[1], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
		}
	case "timediff":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}

	case "str_to_date", "to_date":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}

		if args[1].Typ.Id == int32(types.T_varchar) || args[1].Typ.Id == int32(types.T_char) {
			var tp = types.T_date
			var fsp int
			if exprC := args[1].GetLit(); exprC != nil {
				sval := exprC.Value.(*plan.Literal_Sval)
				tp, fsp = ExtractToDateReturnType(sval.Sval)
			}
			args = append(args, makePlan2DateConstNullExprWithScale(tp, int32(fsp)))

		} else if args[1].Typ.Id == int32(types.T_any) {
			args = append(args, makePlan2DateConstNullExpr(types.T_datetime))
		} else {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
	case "unix_timestamp":
		if len(args) == 1 {
			if types.T(args[0].Typ.Id).IsMySQLString() {
				if exprC := args[0].GetLit(); exprC != nil {
					sval := exprC.Value.(*plan.Literal_Sval)
					tp := judgeUnixTimestampReturnType(sval.Sval)
					if tp == types.T_int64 {
						args = append(args, makePlan2Int64ConstExprWithType(0))
					} else {
						args = append(args, makePlan2Decimal128ConstNullExpr())
					}
				} else {
					args = append(args, makePlan2Decimal128ConstNullExpr())
				}
			}
		} else if len(args) > 1 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args size", len(args))
		}
	case "ascii":
		if len(args) != 1 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		tp := types.T(args[0].Typ.Id)
		switch {
		case tp.IsMySQLString(), tp.IsInteger():
		default:
			targetTp := types.T_varchar.ToType()
			args[0], err = appendCastBeforeExpr(ctx, args[0], makePlan2Type(&targetTp), false)
			if err != nil {
				return nil, err
			}
		}

	case "in", "not_in", "partition_in":
		var partitionIn bool
		if name == "partition_in" {
			partitionIn = true
			name = "in"
		}

		// When the leftside is also tuple.  e.g. where (a, b) in ((1, 2), (3, 4), ...)
		if leftList, ok := args[0].Expr.(*plan.Expr_List); ok {
			if rightList := args[1].GetList(); rightList != nil {
				return handleTupleIn(ctx, name, leftList, rightList)
			}
			return nil, moerr.NewInternalError(ctx, "The right side of IN must be a list")
		}

		//if all the expr in the in list can safely cast to left type, we call it safe
		if rightList := args[1].GetList(); rightList != nil {
			exactSingleComparison := len(rightList.List) == 1
			args[0], err = appendPadSpaceComparisonCastIfNeeded(ctx, args[0])
			if err != nil {
				return nil, err
			}
			typLeft := makeTypeByPlan2Expr(args[0])
			leftIsConstNull := typLeft.Oid == types.T_any && args[0].GetLit() != nil && args[0].GetLit().Isnull
			var inExprList, orExprList []*plan.Expr

			for _, rightVal := range rightList.List {
				if _, ok := rightVal.Expr.(*plan.Expr_List); ok && !partitionIn {
					return nil, moerr.NewOperandColumns(ctx, 1)
				}
				if leftIsConstNull && !partitionIn {
					orExprList = append(orExprList, rightVal)
					continue
				}
				if checkNoNeedCast(makeTypeByPlan2Expr(rightVal), typLeft, rightVal) || partitionIn {
					inExpr := rightVal
					// Keep the partition-IN coercion path unchanged. Ordinary IN can
					// retain an already same-typed constant cast; casting UUID to UUID
					// is both redundant and unsupported.
					if partitionIn || !makeTypeByPlan2Expr(rightVal).Eq(typLeft) {
						inExpr, err = appendCastBeforeExpr(ctx, rightVal, args[0].Typ)
						if err != nil {
							return nil, err
						}
					}
					inExpr, err = appendPadSpaceComparisonCastIfNeeded(ctx, inExpr)
					if err != nil {
						return nil, err
					}
					inExprList = append(inExprList, inExpr)
				} else {
					orExprList = append(orExprList, rightVal)
				}
			}

			var newExpr *plan.Expr

			if len(inExprList) > 1 || partitionIn {
				leftType := makeTypeByPlan2Expr(args[0])
				argsType := []types.Type{leftType, leftType}
				fGet, err := function.GetFunctionByName(ctx, name, argsType)
				if err != nil {
					return nil, err
				}

				funcID := fGet.GetEncodedOverloadID()
				returnType := fGet.GetReturnType()
				rightList.List = inExprList
				exprType := makePlan2Type(&returnType)
				exprType.NotNullable = function.DeduceNotNullable(funcID, args)
				newExpr = &Expr{
					Typ: exprType,
					Expr: &plan.Expr_F{
						F: &plan.Function{
							Func: getFunctionObjRef(funcID, name),
							Args: args,
						},
					},
				}
			} else if len(inExprList) > 0 {
				orExprList = append(inExprList, orExprList...)
			}

			// Expand values that cannot safely share the typed IN vector. Keep
			// the expansion balanced so mixed-type lists do not create an
			// O(N)-deep OR/AND expression tree.
			expanded := make([]*plan.Expr, 0, len(orExprList)+1)
			if newExpr != nil {
				expanded = append(expanded, newExpr)
			}
			if name == "in" {
				for _, expr := range orExprList {
					left := DeepCopyExpr(args[0])
					left.AuxId = args[0].AuxId
					tmpExpr, err := bindMixedInListComparison(ctx, "=", left, expr, exactSingleComparison)
					if err != nil {
						return nil, err
					}
					expanded = append(expanded, tmpExpr)
				}
				return combinePlanExprsBalanced(ctx, "or", expanded)
			} else {
				for _, expr := range orExprList {
					left := DeepCopyExpr(args[0])
					left.AuxId = args[0].AuxId
					tmpExpr, err := bindMixedInListComparison(ctx, "!=", left, expr, exactSingleComparison)
					if err != nil {
						return nil, err
					}
					expanded = append(expanded, tmpExpr)
				}
				return combinePlanExprsBalanced(ctx, "and", expanded)
			}
		}
	case "last_day":
		if len(args) != 1 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
	case "makedate":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
	case "pow":
		name = "power"
	}
	if name == "convert" {
		if err := bindConvertUsingCharset(ctx, args); err != nil {
			return nil, err
		}
	}
	if err := normalizeLagLeadOffsetParam(ctx, name, args); err != nil {
		return nil, err
	}
	if err := validateLagLeadOffsetLiteral(ctx, name, args); err != nil {
		return nil, err
	}

	// get args(exprs) & types
	argsLength := len(args)
	argsType := make([]types.Type, argsLength)
	for idx, expr := range args {
		argsType[idx] = makeTypeByPlan2Expr(expr)
	}
	if err := normalizeNonConstantTemporalComparisonArgs(ctx, name, args, argsType); err != nil {
		return nil, err
	}

	var funcID int64
	var returnType types.Type
	var argsCastType []types.Type

	// get function definition
	fGet, err := function.GetFunctionByName(ctx, name, argsType)
	if err != nil {
		if name == "between" {
			leftFn, err := BindFuncExprImplByPlanExpr(ctx, ">=", []*plan.Expr{DeepCopyExpr(args[0]), args[1]})
			if err != nil {
				return nil, err
			}

			rightFn, err := BindFuncExprImplByPlanExpr(ctx, "<=", []*plan.Expr{args[0], args[2]})
			if err != nil {
				return nil, err
			}

			return BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{leftFn, rightFn})
		}

		// A not-supported result is also the AST binder's signal to try UDF
		// resolution. Do not let a raw INTERVAL pseudo-value cross that
		// boundary. Other resolver failures, especially ErrInvalidArg from a
		// known builtin, are the established public diagnostic and must pass
		// through unchanged.
		if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
			if intervalErr := rejectIntervalArgs(ctx, name, args); intervalErr != nil {
				return nil, intervalErr
			}
		}

		return nil, err
	}
	// Every successful function binding must consume the pseudo-type before it
	// can be published. Check this only after overload resolution so a known
	// builtin can preserve its established invalid-argument diagnostic (for
	// example GREATEST(INTERVAL, DATE) or INT + sub-day INTERVAL). Either
	// resolution fails normally or this postcondition prevents a raw interval
	// list from escaping in a successfully bound expression.
	if err := rejectIntervalArgs(ctx, name, args); err != nil {
		return nil, err
	}

	funcID = fGet.GetEncodedOverloadID()
	returnType = fGet.GetReturnType()
	argsCastType, _ = fGet.ShouldDoImplicitTypeCast()
	// CONVERT's executor consumes a VARCHAR cast, but its declared result bound
	// belongs to the pre-cast source type. Derive metadata before inserting the
	// execution cast so fixed numeric/temporal/UUID widths are not replaced by
	// VARCHAR(65535) and spuriously promoted to BLOB.
	if name == "convert" {
		returnType = function.ConvertReturnTypeForBinder(argsType)
	}
	adjustControlFlowMetadata(name, args, argsType, &returnType, argsCastType)

	// Optimization: avoid casting columns in comparisons to preserve index usage
	switch name {
	case "=", "<", "<=", ">", ">=", "<>":
		if len(args) == 2 && len(argsType) == 2 {
			if len(argsCastType) == 0 {
				argsCastType = []types.Type{argsType[0], argsType[1]}
			}
			if len(argsCastType) == 2 {
				leftIsCol := args[0].GetCol() != nil
				rightIsCol := args[1].GetCol() != nil

				// Check if we can use column type to avoid casting it
				canUse := func(colType, otherType types.Type, colExpr, otherExpr *plan.Expr) bool {
					colOid, otherOid := colType.Oid, otherType.Oid

					// For integers, check if constant value is within column type range
					if colOid.IsInteger() && otherOid.IsInteger() {
						// Use checkNoNeedCast to verify value range
						if otherExpr != nil && otherExpr.GetLit() != nil {
							return checkNoNeedCast(otherType, colType, otherExpr)
						}
						// If not a literal, conservatively allow (e.g., column vs column)
						return true
					}

					// For float types, check if conversion is safe
					if (colOid == types.T_float32 || colOid == types.T_float64) &&
						(otherOid == types.T_float32 || otherOid == types.T_float64 || otherOid.IsDecimal() || otherOid.IsInteger()) {
						// For literals, use checkNoNeedCast to verify range
						if otherExpr != nil && otherExpr.GetLit() != nil {
							return checkNoNeedCast(otherType, colType, otherExpr)
						}
						return true
					}

					// For decimal types, check scale compatibility
					if colOid.IsDecimal() && otherOid.IsDecimal() {
						// Only use column type if it has enough precision (scale)
						// to represent the other value without truncation
						if colType.Scale >= otherType.Scale {
							return true
						}
						// Check if the other value (constant) has trailing zeros that can be truncated
						if otherExpr != nil && hasTrailingZeros(otherExpr, otherType, colType.Scale) {
							return true
						}
						return false
					}

					return false
				}

				// Try column type if column would be cast
				if leftIsCol && !rightIsCol && !argsType[0].Eq(argsCastType[0]) && canUse(argsType[0], argsType[1], args[0], args[1]) {
					if fGet2, err := function.GetFunctionByName(ctx, name, []types.Type{argsType[0], argsType[0]}); err == nil {
						argsCastType = []types.Type{argsType[0], argsType[0]}
						funcID = fGet2.GetEncodedOverloadID()
						returnType = fGet2.GetReturnType()
					}
				} else if !leftIsCol && rightIsCol && !argsType[1].Eq(argsCastType[1]) && canUse(argsType[1], argsType[0], args[1], args[0]) {
					if fGet2, err := function.GetFunctionByName(ctx, name, []types.Type{argsType[1], argsType[1]}); err == nil {
						argsCastType = []types.Type{argsType[1], argsType[1]}
						funcID = fGet2.GetEncodedOverloadID()
						returnType = fGet2.GetReturnType()
					}
				}
			}
		}
	}

	if name == "round" || name == "ceil" || name == "ceiling" || name == "floor" && argsType[0].IsDecimal() {
		if len(argsType) == 1 {
			returnType.Scale = 0
		} else if lit, ok := args[1].Expr.(*plan.Expr_Lit); ok {
			if litval, ok := lit.Lit.GetValue().(*plan.Literal_I64Val); ok {
				scale := litval.I64Val
				if scale > 38 {
					scale = 38
				}
				if scale < 0 {
					scale = 0
				}
				if returnType.Scale > int32(scale) {
					returnType.Scale = int32(scale)
					if returnType.Scale < 0 {
						returnType.Scale = 0
					}
				}
			}
		}
	}

	// Geometry constructors with an explicit constant SRID argument record the
	// SRID in the result type's Width (geometry cells store bare WKB, so SRID
	// lives in the type). A non-constant SRID cannot be represented this way.
	if returnType.Oid == types.T_geometry || returnType.Oid == types.T_geometry32 {
		switch name {
		case "st_geomfromtext", "st_geomfromwkb", "st_geometryfromtext", "st_pointfromtext",
			"st_linefromtext", "st_polygonfromtext", "st_mpointfromtext", "st_mlinefromtext",
			"st_mpolyfromtext", "st_geomcollfromtext", "st_pointfromgeohash",
			"st_geomfromgeojson":
			if len(args) >= 2 {
				// The SRID is carried in the result type's Width, so it must be
				// a constant known at bind time. A non-constant SRID (column,
				// parameter, or CAST/arithmetic expression) cannot be
				// represented this way and is rejected rather than being
				// silently dropped.
				lit, ok := args[len(args)-1].Expr.(*plan.Expr_Lit)
				if !ok || lit.Lit == nil {
					return nil, moerr.NewInvalidInput(ctx, "the SRID argument of a geometry constructor must be a constant integer")
				}
				if !lit.Lit.Isnull {
					iv, ok := lit.Lit.GetValue().(*plan.Literal_I64Val)
					if !ok {
						return nil, moerr.NewInvalidInput(ctx, "the SRID argument of a geometry constructor must be a constant integer")
					}
					if err := validateGeometrySRID(iv.I64Val); err != nil {
						return nil, err
					}
					returnType.Width = encodeGeometrySRIDWidth(uint32(iv.I64Val), true)
				}
			}
		}
	}

	if function.GetFunctionIsAggregateByName(name) {
		if constExpr := args[0].GetLit(); constExpr != nil && constExpr.Isnull {
			args[0].Typ = makePlan2Type(&returnType)
		}
	}

	// rewrite some cast rule:  expr:  int32Col > 10,
	// old rule: cast(int32Col as int64) >10 ,   new rule: int32Col > (cast 10 as int32)
	switch name {
	case "=", "<", "<=", ">", ">=", "<>":
		// if constant's type higher than column's type
		// and constant's value in range of column's type, then no cast was needed
		switch args[0].Expr.(type) {
		case *plan.Expr_Lit:
			if args[1].GetCol() != nil {
				if checkNoNeedCast(argsType[0], argsType[1], args[0]) {
					argsCastType = []types.Type{argsType[1], argsType[1]}
					// need to update function id
					fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
					if err != nil {
						return nil, err
					}
					funcID = fGet.GetEncodedOverloadID()
				}
			}
		case *plan.Expr_Col:
			if checkNoNeedCast(argsType[1], argsType[0], args[1]) {
				argsCastType = []types.Type{argsType[0], argsType[0]}
				fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
				if err != nil {
					return nil, err
				}
				funcID = fGet.GetEncodedOverloadID()
			}
		}

	case "like":
		// if constant's type higher than column's type
		// and constant's value in range of column's type, then no cast was needed
		switch args[0].Expr.(type) {
		case *plan.Expr_Col:
			if argsType[0].IsVarlen() && checkNoNeedCast(argsType[1], argsType[0], args[1]) {
				argsCastType = []types.Type{argsType[0], argsType[0]}
				if len(argsType) == 3 {
					argsCastType = append(argsCastType, argsType[2])
				}
				fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
				if err != nil {
					return nil, err
				}
				funcID = fGet.GetEncodedOverloadID()
			}
		}

	case "between":
		if checkNoNeedCast(argsType[1], argsType[0], args[1]) && checkNoNeedCast(argsType[2], argsType[0], args[2]) {
			argsCastType = []types.Type{argsType[0], argsType[0], argsType[0]}
			fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
			if err != nil {
				return nil, err
			}
			funcID = fGet.GetEncodedOverloadID()
		}

	case "in_range":
		if checkNoNeedCast(argsType[1], argsType[0], args[1]) && checkNoNeedCast(argsType[2], argsType[0], args[2]) {
			argsCastType = []types.Type{argsType[0], argsType[0], argsType[0], argsType[3]}
			fGet, err = function.GetFunctionByName(ctx, name, argsCastType)
			if err != nil {
				return nil, err
			}
			funcID = fGet.GetEncodedOverloadID()
		}

	case "timediff":
		if len(argsType) == len(argsCastType) {
			for i := range argsType {
				if int(argsType[i].Oid) == int(types.T_time) && int(argsCastType[i].Oid) == int(types.T_datetime) {
					return nil, moerr.NewInvalidInput(ctx, name+" function have invalid input args type")
				}
			}
		}

	case "maketime":
		// Hex and bit literals are represented as VARCHAR literals carrying
		// IsBin. They are integral seconds, so they retain TIME(0) metadata even
		// though the VARCHAR seconds overload normally advertises TIME(6).
		if len(args) == 3 {
			if literal := args[2].GetLit(); literal != nil && literal.IsBin {
				returnType.Scale = 0
			}
		}

	case "utc_time", "utc_timestamp":
		// The overload receives only argument types, while the temporal result
		// precision is determined by the literal FSP. Preserve it in the plan
		// type's Width and Scale so views and the MySQL protocol expose
		// TIME/DATETIME(fsp) correctly.
		if len(args) == 1 {
			fsp, _ := utcFunctionFSPFromPlanExpr(ctx, name, args[0])
			returnType.Width = fsp
			returnType.Scale = fsp
		}

	case "current_timestamp", "now", "localtime", "localtimestamp", "sysdate", "current_time", "curtime":
		// The overloads use the default FSP because their return-type callback
		// receives only argument types. A literal FSP is nevertheless known at
		// bind time and must be reflected in the plan metadata. For a runtime
		// FSP expression, retain the timestamp family's conservative six-digit
		// bound; with no argument, the overload's MySQL default FSP=0 remains in
		// effect.
		if len(args) == 1 {
			if fsp, ok := temporalFunctionFSPFromPlanExpr(args[0]); ok {
				returnType.Scale = fsp
			} else if returnType.Oid == types.T_timestamp {
				returnType.Scale = 6
			}
		}

	case "timestamp":
		// The pair overload advertises FSP=6 for string columns and parameters,
		// whose values are unknown while binding. For a direct string literal,
		// MySQL derives DATETIME(fsp) from the parsed value's fractional digits.
		if len(args) == 2 {
			fsp := int32(0)
			for i := range args {
				argumentFSP := argsType[i].Scale
				if argsType[i].Oid == types.T_any {
					argumentFSP = 6
				} else if types.T(args[i].Typ.Id) == types.T_varchar ||
					types.T(args[i].Typ.Id) == types.T_char ||
					types.T(args[i].Typ.Id) == types.T_text {
					argumentFSP = 6
					if literalFSP, ok := timestampPairLiteralFSP(args[i], i == 0); ok {
						argumentFSP = literalFSP
					}
				}
				if argumentFSP > fsp {
					fsp = argumentFSP
				}
			}
			returnType.Width = fsp
			returnType.Scale = fsp
		}

	case "timestampadd":
		if len(args) >= 3 {
			inputType := argsType[2]
			switch inputType.Oid {
			case types.T_date, types.T_datetime, types.T_timestamp:
				unit, known := timestampAddUnitFromPlanExpr(args[0])
				if !known {
					// A runtime unit can still be MICROSECOND, so retain a safe
					// upper bound until execution resolves it.
					if inputType.Oid == types.T_date {
						returnType = types.T_datetime.ToTypeWithScale(6)
					} else {
						returnType.Oid = inputType.Oid
						returnType.Scale = inputType.Scale
						if returnType.Scale < 6 {
							returnType.Scale = 6
						}
					}
					break
				}

				if inputType.Oid == types.T_date {
					if timestampAddDateUnit(unit) {
						returnType = types.T_date.ToType()
					} else {
						returnType = types.T_datetime.ToTypeWithScale(0)
						if unit == types.MicroSecond {
							returnType.Scale = 6
						}
					}
				} else {
					returnType.Oid = inputType.Oid
					returnType.Scale = inputType.Scale
					if unit == types.MicroSecond && returnType.Scale < 6 {
						returnType.Scale = 6
					}
				}
			}
		}

	case "repeat":
		refineRepeatLiteralReturnType(args, &returnType)

	case "lpad", "rpad":
		refinePadLiteralReturnType(args, &returnType)

	case "python_user_defined_function":
		size := (argsLength - 2) / 2
		args = args[:size+1]
		argsLength = len(args)
		argsType = argsType[:size+1]
		if len(argsCastType) > 0 {
			argsCastType = argsCastType[:size+1]
		}

	case "lead", "lag":
		// For lead/lag window functions, cast the default value (3rd arg)
		// to match the value type (1st arg).
		if len(args) >= 3 && !argsType[2].Eq(argsType[0]) {
			argsCastType = []types.Type{argsType[0], argsType[1], argsType[0]}
		}
	}

	if len(argsCastType) != 0 {
		if len(argsCastType) != argsLength {
			return nil, moerr.NewInvalidArg(ctx, "cast types length not match args length", "")
		}
		for idx, castType := range argsCastType {
			if !argsType[idx].Eq(castType) && castType.Oid != types.T_any {
				// MAKETIME uses the scale on its VARCHAR seconds target only to
				// derive the TIME return scale. Recasting an already-VARCHAR
				// argument solely for that metadata clears Literal.IsBin, changing
				// X'..'/B'..' from a binary number into ordinary text.
				if name == "maketime" && idx == 2 &&
					argsType[idx].Oid == types.T_varchar && castType.Oid == types.T_varchar &&
					argsType[idx].Width == castType.Width {
					continue
				}
				if argsType[idx].Oid == castType.Oid && castType.Oid.IsDecimal() && argsType[idx].Scale == castType.Scale {
					continue
				}
				// A direct BIT-to-text cast preserves the BIT payload bytes. In the
				// LEAST/GREATEST type lattice BIT is numeric, so stringify its
				// unsigned value instead when the comparison target is text.
				if (name == "least" || name == "greatest") && argsType[idx].Oid == types.T_bit &&
					(castType.Oid == types.T_char || castType.Oid == types.T_varchar || castType.Oid == types.T_text) {
					uint64Type := types.T_uint64.ToType()
					args[idx], err = appendCastBeforeExpr(ctx, args[idx], makePlan2Type(&uint64Type))
					if err != nil {
						return nil, err
					}
				}
				typ := makePlan2Type(&castType)
				if isPadSpaceComparisonFunction(name) &&
					argsType[idx].Oid == types.T_char && castType.Oid == types.T_varchar {
					args[idx], err = appendComparisonCastBeforeExpr(ctx, args[idx], typ)
				} else {
					args[idx], err = appendCastBeforeExpr(ctx, args[idx], typ)
				}
				if err != nil {
					return nil, err
				}
			}
		}
	}
	if isPadSpaceComparisonFunction(name) {
		for idx := range args {
			args[idx], err = appendPadSpaceComparisonCastIfNeeded(ctx, args[idx])
			if err != nil {
				return nil, err
			}
		}
	}

	// return new expr
	Typ := makePlan2Type(&returnType)
	Typ.NotNullable = function.DeduceNotNullable(funcID, args)
	if returnType.Oid == types.T_varchar || returnType.Oid == types.T_text {
		for _, idx := range padSpaceValueArgumentIndexes(name, len(args)) {
			if hasPadSpaceStringProvenance(args[idx]) {
				Typ.PadSpace = true
				break
			}
		}
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(funcID, name),
				Args: args,
			},
		},
		Typ: Typ,
	}, nil
}

func isCollatedTextPlanType(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch types.T(expr.Typ.Id) {
	case types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func refineRepeatLiteralReturnType(args []*plan.Expr, returnType *types.Type) {
	if len(args) != 2 {
		return
	}
	countLiteral := args[1].GetLit()
	if countLiteral == nil {
		return
	}
	count, ok := literalSignedValue(countLiteral)
	if !ok || count < 0 {
		return
	}
	binary := returnType.Charset == types.CharsetBinary
	sourceWidth, known := stringExprBound(args[0], binary)
	if !known {
		return
	}
	if sourceWidth != 0 && uint64(count) > math.MaxUint64/sourceWidth {
		return
	}
	refineKnownStringResultType(returnType, sourceWidth*uint64(count), binary)
}

func refinePadLiteralReturnType(args []*plan.Expr, returnType *types.Type) {
	if len(args) != 3 {
		return
	}
	targetLiteral := args[1].GetLit()
	if targetLiteral == nil {
		return
	}
	target, ok := literalSignedValue(targetLiteral)
	if !ok || target < 0 {
		return
	}
	if returnType.Charset != types.CharsetBinary {
		refineKnownStringResultType(returnType, uint64(target), false)
		return
	}
	sourceRuneBytes, sourceKnown := binaryExprMaxRuntimeRuneBytes(args[0])
	padRuneBytes, padKnown := binaryExprMaxRuntimeRuneBytes(args[2])
	if !sourceKnown || !padKnown {
		return
	}
	maxRuneBytes := max(sourceRuneBytes, padRuneBytes)
	if maxRuneBytes != 0 && uint64(target) > math.MaxUint64/maxRuneBytes {
		return
	}
	refineKnownStringResultType(returnType, uint64(target)*maxRuneBytes, true)
}

func binaryExprMaxRuntimeRuneBytes(expr *plan.Expr) (uint64, bool) {
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if value, ok := lit.GetValue().(*plan.Literal_Sval); ok {
			var bound uint64
			for input := value.Sval; len(input) > 0; {
				r, size := utf8.DecodeRuneInString(input)
				encoded := uint64(size)
				if r == utf8.RuneError && size == 1 {
					encoded = uint64(utf8.RuneLen(utf8.RuneError))
				}
				bound = max(bound, encoded)
				input = input[size:]
			}
			return bound, true
		}
	}
	if expr.Typ.Width <= 0 || types.T(expr.Typ.Id) == types.T_blob {
		// PAD's target is a character count. Every runtime rune occupies at most
		// UTFMax encoded bytes even when the source declaration itself is
		// unbounded, so a constant target still gives a finite payload bound.
		return uint64(utf8.UTFMax), true
	}
	if oid := types.T(expr.Typ.Id); oid == types.T_char || oid == types.T_varchar {
		// Binary-charset CHAR/VARCHAR width remains a character count.
		return uint64(utf8.UTFMax), true
	}
	// Native binary widths count bytes. Invalid UTF-8 bytes become the
	// three-byte RuneError; valid UTF-8 can consume up to four source bytes.
	return min(max(uint64(expr.Typ.Width), uint64(utf8.RuneLen(utf8.RuneError))), uint64(utf8.UTFMax)), true
}

func stringExprBound(expr *plan.Expr, binary bool) (uint64, bool) {
	if binary {
		return binaryExprByteBound(expr)
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if value, ok := lit.GetValue().(*plan.Literal_Sval); ok {
			return uint64(utf8.RuneCountInString(value.Sval)), true
		}
	}
	if expr.Typ.Width > 0 && types.T(expr.Typ.Id) != types.T_text {
		return uint64(expr.Typ.Width), true
	}
	return 0, false
}

func binaryExprByteBound(expr *plan.Expr) (uint64, bool) {
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if value, ok := lit.GetValue().(*plan.Literal_Sval); ok {
			return uint64(len(value.Sval)), true
		}
	}
	width := expr.Typ.Width
	if width > 0 && types.T(expr.Typ.Id) != types.T_blob {
		if oid := types.T(expr.Typ.Id); oid == types.T_char || oid == types.T_varchar {
			if uint64(width) > math.MaxUint64/uint64(utf8.UTFMax) {
				return 0, false
			}
			return uint64(width) * uint64(utf8.UTFMax), true
		}
		return uint64(width), true
	}
	return 0, false
}

func refineKnownStringResultType(returnType *types.Type, width uint64, binary bool) {
	if binary {
		if width <= uint64(types.MaxVarBinaryLen) {
			*returnType = types.T_varbinary.ToType()
			returnType.Width = int32(width)
			returnType.Charset = types.CharsetBinary
		}
		return
	}
	if width <= uint64(types.MaxVarcharLen) {
		charset := returnType.Charset
		*returnType = types.T_varchar.ToType()
		returnType.Width = int32(width)
		returnType.Charset = charset
	}
}

func bindConvertUsingCharset(ctx context.Context, args []*plan.Expr) error {
	if len(args) != 2 {
		return moerr.NewInvalidArg(ctx, "convert function needs two args", len(args))
	}

	charsetLiteral := args[1].GetLit()
	if charsetLiteral == nil || charsetLiteral.Isnull {
		return moerr.NewInvalidInput(ctx, "CONVERT USING requires a constant character set")
	}

	var charset uint32
	switch strings.ToLower(charsetLiteral.GetSval()) {
	case "binary":
		charset = uint32(types.CharsetBinary)
	case "utf8", "utf8mb3", "utf8mb4":
		charset = uint32(types.CharsetUTF8)
	default:
		return moerr.NewInvalidInputf(ctx, "unsupported character set '%s' for CONVERT USING", charsetLiteral.GetSval())
	}

	// The parser lowers the USING name to a synthetic string literal. Record the
	// selected charset on that argument so the overload's return-type callback
	// can carry it into the bound result without inspecting expression values.
	args[1].Typ.Charset = charset
	return nil
}

// normalizeNonConstantTemporalComparisonArgs keeps both sides of equality and
// ordering keys in one physical domain when neither side is a runtime
// constant. Hash joins, shuffle keys, and runtime filters consume comparison
// operands as keys instead of invoking the scalar comparison function, so raw
// DATETIME and TIMESTAMP encodings cannot safely remain cross-typed there.
// Column-versus-runtime-constant predicates stay cross-typed so storage can
// see the raw column and apply the timezone-aware pruning path.
func normalizeNonConstantTemporalComparisonArgs(
	ctx context.Context,
	name string,
	args []*Expr,
	argsType []types.Type,
) error {
	switch name {
	case "=", "<", "<=", ">", ">=", "<>":
	default:
		return nil
	}
	if len(args) != 2 || len(argsType) != 2 ||
		!((argsType[0].Oid == types.T_datetime && argsType[1].Oid == types.T_timestamp) ||
			(argsType[0].Oid == types.T_timestamp && argsType[1].Oid == types.T_datetime)) ||
		isRuntimeConstExpr(args[0]) || isRuntimeConstExpr(args[1]) {
		return nil
	}

	datetimeIndex, timestampIndex := 0, 1
	if argsType[0].Oid == types.T_timestamp {
		datetimeIndex, timestampIndex = 1, 0
	}
	targetType := argsType[timestampIndex]
	casted, err := appendCastBeforeExpr(ctx, args[datetimeIndex], makePlan2Type(&targetType))
	if err != nil {
		return err
	}
	args[datetimeIndex] = casted
	argsType[datetimeIndex] = targetType
	return nil
}

func invalidUTCFunctionFSPError(ctx context.Context, name string) error {
	return moerr.NewInvalidInputf(ctx, "%s fractional seconds precision must be an integer literal between 0 and 6", strings.ToUpper(name))
}

func utcFunctionFSPFromPlanExpr(ctx context.Context, name string, expr *Expr) (int32, error) {
	literal := expr.GetLit()
	if literal == nil || literal.Isnull {
		return 0, invalidUTCFunctionFSPError(ctx, name)
	}
	fsp, ok := literal.GetValue().(*plan.Literal_I64Val)
	if !ok {
		return 0, invalidUTCFunctionFSPError(ctx, name)
	}
	if fsp.I64Val < 0 {
		return 0, moerr.NewInvalidArg(ctx, name, fmt.Sprintf("negative precision %d specified", fsp.I64Val))
	}
	if fsp.I64Val > 6 {
		return 0, moerr.NewErrTooBigPrecision(ctx, fsp.I64Val, name, 6)
	}
	return int32(fsp.I64Val), nil
}

// temporalFunctionFSPFromPlanExpr extracts an optional literal FSP for the
// current-time family. Unlike UTC_TIME/UTC_TIMESTAMP, MySQL permits a runtime
// integer expression here, so an unknown expression keeps the overload's
// conservative default metadata rather than being rejected during binding.
func temporalFunctionFSPFromPlanExpr(expr *Expr) (int32, bool) {
	literal := expr.GetLit()
	if literal == nil || literal.Isnull {
		return 0, false
	}
	fsp, ok := literal.GetValue().(*plan.Literal_I64Val)
	if !ok || fsp.I64Val < 0 || fsp.I64Val > 6 {
		return 0, false
	}
	return int32(fsp.I64Val), true
}

func timestampPairLiteralFSP(expr *Expr, datetime bool) (int32, bool) {
	literal := expr.GetLit()
	if literal == nil || literal.Isnull {
		return 0, false
	}
	value, ok := literal.GetValue().(*plan.Literal_Sval)
	if !ok {
		return 0, false
	}
	text := strings.TrimSpace(value.Sval)
	if datetime {
		if parsed, err := types.ParseDatetime(text, 6); err != nil || parsed == types.ZeroDatetime {
			return 0, false
		}
	} else {
		if text == "" {
			return 0, false
		}
		if strings.IndexByte(text, 'T') >= 0 {
			parsed, err := types.ParseDatetime(text, 6)
			if err != nil || parsed == types.ZeroDatetime {
				return 0, false
			}
		} else {
			parseText := text
			dateTimeText := false
			if space := strings.IndexByte(text, ' '); space >= 0 {
				day := text[:space]
				if strings.HasPrefix(day, "-") {
					day = day[1:]
					parseText = text[1:]
				}
				if day == "" {
					return 0, false
				}
				if _, err := strconv.ParseUint(day, 10, 64); err != nil {
					parsed, datetimeErr := types.ParseDatetime(text, 6)
					if datetimeErr != nil || parsed == types.ZeroDatetime {
						return 0, false
					}
					dateTimeText = true
				}
			}
			if !dateTimeText {
				if _, err := types.ParseTime(parseText, 6); err != nil {
					return 0, false
				}
			}
		}
	}

	dot := strings.IndexByte(text, '.')
	if dot < 0 {
		return 0, true
	}
	digits := len(text) - dot - 1
	if digits > 6 {
		digits = 6
	}
	return int32(digits), true
}

func timestampAddUnitFromPlanExpr(expr *Expr) (types.IntervalType, bool) {
	literal := expr.GetLit()
	if literal == nil || literal.Isnull {
		return 0, false
	}
	value, ok := literal.GetValue().(*plan.Literal_Sval)
	if !ok {
		return 0, false
	}
	unit, err := types.IntervalTypeOf(strings.ToUpper(value.Sval))
	return unit, err == nil
}

func timestampAddDateUnit(unit types.IntervalType) bool {
	return unit == types.Day || unit == types.Week || unit == types.Month ||
		unit == types.Quarter || unit == types.Year
}

// adjustControlFlowMetadata keeps MySQL-visible metadata for conditional
// expressions precise after overload selection.  The overload resolver only
// sees types, whereas a literal branch has a narrower domain than its default
// INT64 representation.  Keep this adjustment here, rather than in the
// shared type-check helpers, so column/parameter expressions retain their
// conservative runtime capacity and unrelated functions are unaffected.
func adjustControlFlowMetadata(name string, args []*Expr, argTypes []types.Type, returnType *types.Type, argsCastType []types.Type) {
	valueIndexes := controlFlowValueIndexes(name, len(args))
	if len(valueIndexes) == 0 {
		return
	}

	conservativeReturnType := *returnType
	changed := false
	switch {
	case returnType.Oid == types.T_varchar:
		changed = adjustControlFlowVarcharMetadata(args, argTypes, valueIndexes, returnType)
	case returnType.Oid == types.T_varbinary:
		changed = adjustControlFlowBinaryMetadata(args, argTypes, valueIndexes, returnType)
	case returnType.Oid.IsDecimal():
		changed = adjustControlFlowDecimalLiteralMetadata(args, argTypes, valueIndexes, returnType)
	}

	if !changed || len(argsCastType) != len(args) {
		return
	}

	// VARCHAR width is character metadata, while normal casts enforce Width in
	// bytes. Restore the overload's conservative target rather than its
	// intermediate type-check target, which may already have been narrowed by a
	// different value branch. Decimal widths describe the runtime decimal
	// representation, so their existing cast synchronization remains safe.
	if returnType.Oid == types.T_varchar {
		for _, idx := range valueIndexes {
			argsCastType[idx] = conservativeReturnType
		}
		return
	}
	if returnType.Oid == types.T_varbinary {
		for _, idx := range valueIndexes {
			argsCastType[idx] = *returnType
		}
		return
	}
	if returnType.Oid.IsDecimal() {
		for _, idx := range valueIndexes {
			argsCastType[idx] = *returnType
		}
	}
}

func timeWindowIntervalUsesMicrosecond(expr *Expr) bool {
	list := expr.GetList()
	if list == nil || len(list.List) < 2 {
		return false
	}
	lit := list.List[1].GetLit()
	if lit == nil {
		return false
	}
	unit, err := types.IntervalTypeOf(lit.GetSval())
	return err == nil && unit == types.MicroSecond
}

func controlFlowValueIndexes(name string, argsLength int) []int {
	valueIndexes := make([]int, 0, argsLength)
	switch name {
	case "if", "iff":
		if argsLength == 3 {
			valueIndexes = append(valueIndexes, 1, 2)
		}
	case "case":
		for i := 1; i < argsLength; i += 2 {
			valueIndexes = append(valueIndexes, i)
		}
		if argsLength%2 == 1 {
			valueIndexes = append(valueIndexes, argsLength-1)
		}
	case "coalesce":
		for i := 0; i < argsLength; i++ {
			valueIndexes = append(valueIndexes, i)
		}
	}
	return valueIndexes
}

// adjustControlFlowVarcharMetadata derives one bound across every value
// branch. String/numeric/temporal subfamilies must not independently narrow a
// shared return type: the widest proven display bound wins, and any unknown
// relevant branch keeps the overload's conservative capacity.
func adjustControlFlowVarcharMetadata(args []*Expr, argTypes []types.Type, valueIndexes []int, returnType *types.Type) bool {
	hasString := false
	hasConvertible := false
	width := int32(0)
	for _, idx := range valueIndexes {
		if idx >= len(argTypes) {
			return false
		}
		// NULL does not contribute a runtime display value.  Every other arm
		// participates in the implicit VARCHAR cast selected by the overload,
		// even when its type is outside the string/numeric/temporal families.
		// Therefore only NULL may be ignored here; an unsupported display bound
		// must retain the overload's conservative VARCHAR capacity.
		if controlFlowNullExpr(args[idx]) {
			continue
		}
		typ := argTypes[idx]
		var (
			candidate int32
			known     bool
		)
		if typ.Oid.IsMySQLString() {
			hasString = true
			candidate, known = controlFlowStringWidth(args[idx], typ)
		} else if typ.Oid.IsInteger() || typ.Oid.IsFloat() || typ.Oid.IsDecimal() {
			hasConvertible = true
			candidate, known = controlFlowStringWidth(args[idx], typ)
		} else if temporalWidth, ok := temporalDisplayWidthForVarchar(typ); ok {
			hasConvertible = true
			candidate, known = temporalWidth, true
		} else {
			return false
		}
		if !known {
			// Width zero is used both by exact empty literals and by types whose
			// runtime display capacity is unknown (for example TEXT or FLOAT).
			// Do not turn the latter into a narrow implicit cast target.
			return false
		}
		if candidate > width {
			width = candidate
		}
	}
	if hasString && hasConvertible && width > 0 {
		changed := returnType.Width != width
		returnType.Width = width
		return changed
	}
	return false
}

// adjustControlFlowBinaryMetadata narrows a binary/character conditional
// result only when every character branch is a known literal. The result is
// VARBINARY, so a character branch's declared width must first be converted
// from characters to bytes using its effective charset. MatrixOne currently
// exposes UTF-8 text as utf8mb4 on the wire and in persisted view metadata;
// CharsetLegacy is only the zero-value marker on pre-collation plans, not a
// promise that the client session uses utf8mb3. Keep the advertised utf8mb4
// bound for that legacy identity as well. New text expressions carry
// CharsetUTF8/CharsetUTF8MB4Bin and use the same utf8mb4 bound.
func adjustControlFlowBinaryMetadata(args []*Expr, argTypes []types.Type, valueIndexes []int, returnType *types.Type) bool {
	hasBinary := false
	hasCharacter := false
	width := int32(0)
	for _, idx := range valueIndexes {
		if idx >= len(argTypes) || controlFlowNullExpr(args[idx]) {
			continue
		}
		typ := argTypes[idx]
		switch typ.Oid {
		case types.T_binary, types.T_varbinary:
			hasBinary = true
			if typ.Width > width {
				width = typ.Width
			}
		case types.T_char, types.T_varchar:
			hasCharacter = true
			lit := args[idx].GetLit()
			if lit == nil || lit.Isnull {
				return false
			}
			value, ok := lit.Value.(*plan.Literal_Sval)
			if !ok {
				return false
			}
			candidate := int32(0)
			maxBytesPerCharacter := controlFlowMaxBytesPerCharacter(typ.Charset)
			if typ.Width > 0 && typ.Width <= types.MaxVarBinaryLen/maxBytesPerCharacter {
				candidate = typ.Width * maxBytesPerCharacter
			} else if typ.Width > 0 {
				candidate = types.MaxVarBinaryLen
			}
			if byteWidth := int32(len(value.Sval)); byteWidth > candidate {
				candidate = byteWidth
			}
			if candidate > width {
				width = candidate
			}
		default:
			return false
		}
	}
	if !hasBinary || !hasCharacter || width <= 0 || width >= returnType.Width {
		return false
	}
	returnType.Width = width
	return true
}

func controlFlowMaxBytesPerCharacter(charset uint8) int32 {
	switch charset {
	case types.CharsetBinary:
		return 1
	case types.CharsetLegacy:
		// CharsetLegacy is the zero value in plans written before collation
		// metadata became meaningful. MatrixOne's effective/public text charset
		// is utf8mb4, so use its four-byte bound instead of treating this
		// historical marker as an explicit utf8mb3 setting.
		return int32(utf8.UTFMax)
	case types.CharsetUTF8, types.CharsetUTF8MB4Bin:
		// Both explicit text identities are utf8mb4 in MatrixOne. This is the
		// effective charset of newly bound literals and view expressions, so a
		// two-character literal has an eight-byte VARBINARY capacity.
		return int32(utf8.UTFMax)
	default:
		// Unknown identities can come from a plan produced by a newer node. Keep
		// the public utf8mb4 capacity until that identity is understood rather
		// than understate a value that may contain four-byte UTF-8 code points.
		return int32(utf8.UTFMax)
	}
}

func controlFlowNullExpr(expr *Expr) bool {
	// Only a direct NULL literal is neutral for MySQL conditional-expression
	// metadata. A cast gives NULL a declared type, so CAST(NULL AS CHAR(N))
	// must contribute CHAR(N)'s display width.
	return isNullLiteralExpr(expr)
}

func temporalDisplayWidthForVarchar(typ types.Type) (int32, bool) {
	switch typ.Oid {
	case types.T_date:
		return 10, true
	case types.T_time:
		// Time.String2 can format the complete MatrixOne TIME range, including
		// its optional sign and a ten-digit hour field.  The scale determines
		// the fractional suffix; this is a real upper bound, unlike a zero type
		// width on variable-size values such as JSON.
		width := int32(len(strconv.FormatInt(int64(types.MaxHourInTime), 10)) + 7)
		if typ.Scale > 0 {
			width += 1 + typ.Scale
		}
		return width, true
	case types.T_datetime, types.T_timestamp:
		if typ.Scale > 0 {
			return 20 + typ.Scale, true
		}
		return 19, true
	default:
		return 0, false
	}
}

func adjustControlFlowDecimalLiteralMetadata(args []*Expr, argTypes []types.Type, valueIndexes []int, returnType *types.Type) bool {
	hasDecimal := false
	hasIntegerLiteral := false
	maxIntegral := int32(0)
	maxScale := int32(0)

	for _, idx := range valueIndexes {
		if idx >= len(argTypes) {
			return false
		}
		typ := argTypes[idx]
		switch {
		case typ.Oid.IsDecimal():
			hasDecimal = true
			integral := typ.Width - typ.Scale
			if integral > maxIntegral {
				maxIntegral = integral
			}
			if typ.Scale > maxScale {
				maxScale = typ.Scale
			}
		case typ.Oid.IsInteger():
			integral, literal := decimalIntegerWidth(args[idx], typ)
			if integral > maxIntegral {
				maxIntegral = integral
			}
			hasIntegerLiteral = hasIntegerLiteral || literal
		}
	}

	if !hasDecimal || !hasIntegerLiteral {
		return false
	}
	precision := maxIntegral + maxScale
	// This is a metadata narrowing pass.  If another branch needs more room
	// than the overload already selected, leave its conservative type intact.
	if precision <= 0 || precision > returnType.Width {
		return false
	}
	changed := returnType.Width != precision || returnType.Scale != maxScale
	returnType.Width = precision
	returnType.Scale = maxScale
	return changed
}

func decimalIntegerWidth(expr *Expr, typ types.Type) (int32, bool) {
	lit := expr.GetLit()
	if lit == nil || lit.Isnull {
		return integerMetadataWidth(typ.Oid), false
	}

	decimalDigits := func(value string) int32 {
		value = strings.TrimPrefix(value, "-")
		return int32(len(value))
	}
	switch value := lit.Value.(type) {
	case *plan.Literal_I8Val:
		return decimalDigits(strconv.FormatInt(int64(value.I8Val), 10)), true
	case *plan.Literal_I16Val:
		return decimalDigits(strconv.FormatInt(int64(value.I16Val), 10)), true
	case *plan.Literal_I32Val:
		return decimalDigits(strconv.FormatInt(int64(value.I32Val), 10)), true
	case *plan.Literal_I64Val:
		return decimalDigits(strconv.FormatInt(value.I64Val, 10)), true
	case *plan.Literal_U8Val:
		return int32(len(strconv.FormatUint(uint64(value.U8Val), 10))), true
	case *plan.Literal_U16Val:
		return int32(len(strconv.FormatUint(uint64(value.U16Val), 10))), true
	case *plan.Literal_U32Val:
		return int32(len(strconv.FormatUint(uint64(value.U32Val), 10))), true
	case *plan.Literal_U64Val:
		return int32(len(strconv.FormatUint(value.U64Val, 10))), true
	default:
		return integerMetadataWidth(typ.Oid), false
	}
}

// controlFlowStringWidth reports a display bound only when it is safe to use
// that bound as the target of an implicit cast.  A zero type width alone is not
// a bound: TEXT/BLOB columns and default-width floating expressions use it to
// mean that their runtime capacity is unknown.
func controlFlowStringWidth(expr *Expr, typ types.Type) (int32, bool) {
	if typ.Oid.IsMySQLString() {
		if typ.Width > 0 || expr.GetLit() != nil {
			return typ.Width, true
		}
		return 0, false
	}
	if typ.Oid.IsDecimal() {
		if typ.Width <= 0 {
			return 0, false
		}
		return decimalDisplayWidth(typ), true
	}
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		switch value := lit.Value.(type) {
		case *plan.Literal_I8Val:
			return signedIntegerLiteralWidth(int64(value.I8Val)), true
		case *plan.Literal_I16Val:
			return signedIntegerLiteralWidth(int64(value.I16Val)), true
		case *plan.Literal_I32Val:
			return signedIntegerLiteralWidth(int64(value.I32Val)), true
		case *plan.Literal_I64Val:
			return signedIntegerLiteralWidth(value.I64Val), true
		case *plan.Literal_U8Val:
			return int32(len(strconv.FormatUint(uint64(value.U8Val), 10))), true
		case *plan.Literal_U16Val:
			return int32(len(strconv.FormatUint(uint64(value.U16Val), 10))), true
		case *plan.Literal_U32Val:
			return int32(len(strconv.FormatUint(uint64(value.U32Val), 10))), true
		case *plan.Literal_U64Val:
			return int32(len(strconv.FormatUint(value.U64Val, 10))), true
		}
	}
	if typ.Oid.IsInteger() {
		width := integerMetadataWidth(typ.Oid)
		if typ.Oid.IsSignedInt() {
			width++
		}
		return width, true
	}
	return 0, false
}

// decimalDisplayWidth returns the maximum byte width of a DECIMAL value after
// it is converted to a control-flow VARCHAR result. Decimal precision counts
// only significant digits, so it excludes the optional sign, decimal point,
// and (for DECIMAL(M, M)) the displayed leading zero.
func decimalDisplayWidth(typ types.Type) int32 {
	precision := typ.Width
	if precision <= 0 {
		return types.MaxVarcharLen
	}

	width := int64(precision) // significant digits
	if typ.Scale > 0 {
		width++ // decimal point
		if typ.Scale >= precision {
			width++ // leading zero before the decimal point
		}
	}
	width++ // optional sign
	if width > int64(types.MaxVarcharLen) {
		return types.MaxVarcharLen
	}
	return int32(width)
}

func signedIntegerLiteralWidth(value int64) int32 {
	width := int32(len(strconv.FormatInt(value, 10)))
	if value >= 0 {
		width++
	}
	return width
}

func integerMetadataWidth(oid types.T) int32 {
	switch oid {
	case types.T_int8, types.T_uint8:
		return 3
	case types.T_int16, types.T_uint16:
		return 5
	case types.T_int32, types.T_uint32:
		return 10
	case types.T_int64:
		return 19
	case types.T_uint64:
		return 20
	default:
		return 0
	}
}

// foldDecimalStringComparisonConstants materializes only deterministic string
// constants before the generic numeric overload resolver can erase their exact
// value. Runtime expressions retain the existing REAL comparison domain.
func foldDecimalStringComparisonConstants(
	ctx context.Context,
	proc *process.Process,
	name string,
	args []*Expr,
) error {
	if proc == nil || len(args) != 2 {
		return nil
	}

	foldPair := func(pair []*Expr) error {
		for stringPos, decimalPos := range []int{1, 0} {
			candidate, peer := pair[stringPos], pair[decimalPos]
			if candidate == nil || peer == nil ||
				!types.T(candidate.Typ.Id).IsMySQLString() ||
				!types.T(peer.Typ.Id).IsDecimal() ||
				candidate.GetLit() != nil || !rule.IsConstant(candidate, false) {
				continue
			}
			folded, err := ConstantFold(
				batch.EmptyForConstFoldBatch, DeepCopyExpr(candidate), proc, false, true)
			if err != nil || folded == nil || folded.GetLit() == nil ||
				!types.T(folded.Typ.Id).IsMySQLString() {
				continue
			}
			value, ok := decimalStringLiteralValue(folded)
			if !ok {
				continue
			}
			normalized, err := normalizeExactDecimalStringComparisonPair(
				ctx, pair, stringPos, decimalPos, value)
			if err != nil {
				return err
			}
			if normalized {
				return nil
			}
			pair[stringPos] = folded
			return nil
		}
		return nil
	}

	if isDecimalComparisonOperator(name) {
		return foldPair(args)
	}
	if name != "in" && name != "not_in" {
		return nil
	}
	list := args[1].GetList()
	if list == nil || len(list.List) != 1 {
		return nil
	}
	pair := []*Expr{args[0], list.List[0]}
	err := foldPair(pair)
	args[0], list.List[0] = pair[0], pair[1]
	return err
}

func isDecimalComparisonOperator(name string) bool {
	switch name {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=":
		return true
	default:
		return false
	}
}

// A complete decimal string literal paired with DECIMAL has an exact numeric domain.
// Resolve that domain before the generic string/numeric cast matrix selects
// FLOAT64, which cannot distinguish adjacent DECIMAL values above 2^53. Keep
// explicit character casts in the expression, but cast their result to the
// literal's natural DECIMAL type so a narrower peer cannot truncate the value.
func normalizeDecimalStringLiteralComparisonArgs(ctx context.Context, name string, args []*Expr) error {
	if !isDecimalComparisonOperator(name) || len(args) != 2 {
		return nil
	}

	for stringPos, decimalPos := range []int{1, 0} {
		if !types.T(args[decimalPos].Typ.Id).IsDecimal() {
			continue
		}
		value, ok := decimalStringLiteralValue(args[stringPos])
		if !ok {
			continue
		}
		normalized, err := normalizeExactDecimalStringComparisonPair(
			ctx, args, stringPos, decimalPos, value)
		if err != nil {
			return err
		}
		if normalized {
			return nil
		}
	}
	return nil
}

func normalizeExactDecimalStringComparisonPair(
	ctx context.Context,
	args []*Expr,
	stringPos int,
	decimalPos int,
	value string,
) (bool, error) {
	// Only a complete decimal lexeme with mode-independent outer whitespace
	// can enter the exact path. Prefixes, extension tokens, and Unicode-only
	// whitespace must retain their final spelling for runtime SQL compatibility
	// parsing, invalid-token errors, and warning 1292.
	trimmedValue := strings.Trim(value, " \t\n\v\f\r")
	if strings.TrimSpace(value) != trimmedValue {
		return false, nil
	}
	decimalExpr, exact, err := makePlan2ExactDecimalStringExprWithType(ctx, trimmedValue)
	if err != nil || !exact {
		return false, err
	}
	target, ok := mergeExactDecimalComparisonType(args[decimalPos].Typ, decimalExpr.Typ)
	if !ok {
		floatType := makePlan2Type(&types.Type{Oid: types.T_float64})
		for pos := range args {
			argTarget := floatType
			argTarget.NotNullable = args[pos].Typ.NotNullable
			args[pos], err = appendCastBeforeExpr(ctx, args[pos], argTarget)
			if err != nil {
				return false, err
			}
		}
		return true, nil
	}

	normalizedString := decimalExpr
	if args[stringPos].GetLit() == nil {
		normalizedString, err = appendCastBeforeExpr(ctx, args[stringPos], decimalExpr.Typ)
		if err != nil {
			return false, err
		}
	}
	args[stringPos] = normalizedString
	for _, pos := range []int{decimalPos, stringPos} {
		if sameDecimalComparisonType(args[pos].Typ, target) {
			continue
		}
		argTarget := target
		argTarget.NotNullable = args[pos].Typ.NotNullable
		args[pos], err = appendCastBeforeExpr(ctx, args[pos], argTarget)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func mergeExactDecimalComparisonType(left, right plan.Type) (plan.Type, bool) {
	leftIntegral := max(int64(left.Width)-int64(left.Scale), 0)
	rightIntegral := max(int64(right.Width)-int64(right.Scale), 0)
	scale := max(int64(left.Scale), int64(right.Scale))
	width := max(leftIntegral, rightIntegral) + scale
	maxWidth := int64(types.T_decimal256.ToType().Width)
	if width <= 0 || width > maxWidth || scale > maxWidth {
		return plan.Type{}, false
	}

	oid := types.T_decimal64
	if width > int64(types.T_decimal64.ToType().Width) {
		oid = types.T_decimal128
	}
	if width > int64(types.T_decimal128.ToType().Width) {
		oid = types.T_decimal256
	}
	if types.T(left.Id) == types.T_decimal256 || types.T(right.Id) == types.T_decimal256 {
		oid = types.T_decimal256
	} else if oid == types.T_decimal64 &&
		(types.T(left.Id) == types.T_decimal128 || types.T(right.Id) == types.T_decimal128) {
		oid = types.T_decimal128
	}
	return plan.Type{Id: int32(oid), Width: int32(width), Scale: int32(scale)}, true
}

func sameDecimalComparisonType(left, right plan.Type) bool {
	return left.Id == right.Id && left.Width == right.Width && left.Scale == right.Scale
}

func decimalStringEffectiveDomain(expr *Expr) types.StringDomain {
	if expr == nil {
		return types.StringDomainNone
	}
	staticDomain := types.StaticStringDomain(makeTypeByPlan2Expr(expr))
	literal := expr.GetLit()
	if literal == nil {
		return staticDomain
	}
	switch literal.LiteralForm {
	case plan.StringLiteralForm_STRING_LITERAL_NONE:
		return staticDomain
	case plan.StringLiteralForm_STRING_LITERAL_TEXT:
		return types.StringDomainText
	case plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER,
		plan.StringLiteralForm_STRING_LITERAL_HEX,
		plan.StringLiteralForm_STRING_LITERAL_BIT:
		return types.StringDomainBinary
	default:
		return types.StringDomainNone
	}
}

// decimalStringLiteralValue recognizes effective-text literals and transparent
// binder-inserted text cast chains rooted in one. Explicit casts and static
// binary targets are value/domain boundaries; raw hex/bit forms remain on their
// existing runtime numeric interpretation.
func decimalStringLiteralValue(expr *Expr) (string, bool) {
	if expr == nil || decimalStringEffectiveDomain(expr) != types.StringDomainText {
		return "", false
	}
	if literal := expr.GetLit(); literal != nil {
		value, ok := literal.Value.(*plan.Literal_Sval)
		if !ok || literal.Isnull || literal.IsBin {
			return "", false
		}
		return value.Sval, true
	}
	if isExplicitPreparedCast(expr) {
		return "", false
	}

	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) != 2 {
		return "", false
	}
	return decimalStringLiteralValue(fn.Args[0])
}

// A direct prepared parameter in a binary comparison derives its type from
// the other operand. Preserve that contract for DECIMAL before the generic
// string/numeric cast rules see the parameter's transport type (TEXT). Real
// string expressions continue through the ordinary MySQL coercion path.
func normalizeDecimalParamComparisonArgs(ctx context.Context, name string, args []*Expr) error {
	switch name {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=":
		if len(args) != 2 {
			return nil
		}
	default:
		return nil
	}

	for paramPos, peerPos := range []int{1, 0} {
		if !isDirectDynamicParam(args[paramPos]) || !types.T(args[peerPos].Typ.Id).IsDecimal() {
			continue
		}
		castExpr, err := appendCastBeforeExpr(ctx, args[paramPos], args[peerPos].Typ)
		if err != nil {
			return err
		}
		args[paramPos] = castExpr
		return nil
	}
	return nil
}

// normalizeDecimalParamInArgs gives a direct prepared marker on the left of IN
// a provisional DECIMAL envelope when a list member supplies that domain.
// Parameters inside a DECIMAL-left list need no treatment here: the generic IN
// binder already recognizes direct numeric parameters and gives each one the
// left type while preserving a single typed list.
func normalizeDecimalParamInArgs(ctx context.Context, name string, args []*Expr) error {
	if (name != "in" && name != "not_in") || len(args) != 2 {
		return nil
	}
	list := args[1].GetList()
	if list == nil {
		return nil
	}

	if isDirectDynamicParam(args[0]) {
		// Inspect the complete list before choosing a provisional envelope.
		// DECIMAL mixed with any approximate member has one FLOAT64 common
		// domain; selecting an earlier DECIMAL item would freeze the marker and
		// violate the precision/rounding behavior of the complete IN list.
		for _, item := range list.List {
			if item != nil && types.T(item.Typ.Id).IsFloat() {
				var err error
				floatType := makePlan2Type(&types.Type{Oid: types.T_float64})
				args[0], err = appendCastBeforeExpr(ctx, args[0], floatType)
				return err
			}
		}
		for _, item := range list.List {
			if item != nil && types.T(item.Typ.Id).IsDecimal() {
				var err error
				args[0], err = appendCastBeforeExpr(ctx, args[0], item.Typ)
				if err != nil {
					return err
				}
				break
			}
		}
	}
	return nil
}

// MySQL compares scalar TIME expressions to strings as text, but converts a
// constant string or direct prepared parameter to TIME(scale) when the TIME
// side is a column.
func normalizeTimeStringComparisonArgs(ctx context.Context, name string, args []*Expr) error {
	switch name {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=":
		if len(args) != 2 || !isTimeStringComparisonPair(args[0], args[1]) {
			return nil
		}
		if isTimeColumnStringLiteralOrDirectParamPair(args[0], args[1]) {
			return nil
		}
	case "between":
		if len(args) != 3 || !allTimeOrCharacterString(args) {
			return nil
		}
		if args[0].Typ.Id == int32(types.T_time) && args[0].GetCol() != nil &&
			isTimeValueOrCharacterStringLiteralOrDirectParam(args[1]) &&
			isTimeValueOrCharacterStringLiteralOrDirectParam(args[2]) {
			return nil
		}
	default:
		return nil
	}

	varchar := types.T_varchar.ToType()
	varcharType := makePlan2Type(&varchar)
	for i, arg := range args {
		if arg.Typ.Id != int32(types.T_time) {
			continue
		}
		castExpr, err := appendCastBeforeExpr(ctx, arg, varcharType)
		if err != nil {
			return err
		}
		args[i] = castExpr
	}
	return nil
}

func isTimeStringComparisonPair(left, right *Expr) bool {
	return (left.Typ.Id == int32(types.T_time) && isCharacterStringType(right.Typ.Id)) ||
		(right.Typ.Id == int32(types.T_time) && isCharacterStringType(left.Typ.Id))
}

func isTimeColumnStringLiteralOrDirectParamPair(left, right *Expr) bool {
	return (left.Typ.Id == int32(types.T_time) && left.GetCol() != nil && isCharacterStringLiteralOrDirectParam(right)) ||
		(right.Typ.Id == int32(types.T_time) && right.GetCol() != nil && isCharacterStringLiteralOrDirectParam(left))
}

func allTimeOrCharacterString(args []*Expr) bool {
	hasTime := false
	hasString := false
	for _, arg := range args {
		switch {
		case arg.Typ.Id == int32(types.T_time):
			hasTime = true
		case isCharacterStringType(arg.Typ.Id):
			hasString = true
		default:
			return false
		}
	}
	return hasTime && hasString
}

func isCharacterStringLiteral(expr *Expr) bool {
	return isCharacterStringType(expr.Typ.Id) && expr.GetLit() != nil
}

func isCharacterStringLiteralOrDirectParam(expr *Expr) bool {
	return isCharacterStringLiteral(expr) ||
		(isCharacterStringType(expr.Typ.Id) && isDirectDynamicParam(expr))
}

func isTimeValueOrCharacterStringLiteralOrDirectParam(expr *Expr) bool {
	return expr.Typ.Id == int32(types.T_time) || isCharacterStringLiteralOrDirectParam(expr)
}

func isCharacterStringType(typeID int32) bool {
	switch types.T(typeID) {
	case types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func adjustJsonDynamicParamType(ctx context.Context, name string, args []*Expr) error {
	paramFunction := ""
	switch name {
	case "<", "<=", ">", ">=":
		paramFunction = function.JsonOrderingParamFunctionName
	case "=", "<=>", "<>", "!=":
		paramFunction = function.JsonComparisonParamFunctionName
	default:
		return nil
	}
	if len(args) != 2 {
		return nil
	}

	if args[0].Typ.Id == int32(types.T_json) && isDirectDynamicParam(args[1]) {
		var err error
		args[1], err = BindFuncExprImplByPlanExpr(ctx, paramFunction, []*Expr{args[1]})
		return err
	}
	if args[1].Typ.Id == int32(types.T_json) && isDirectDynamicParam(args[0]) {
		var err error
		args[0], err = BindFuncExprImplByPlanExpr(ctx, paramFunction, []*Expr{args[0]})
		return err
	}
	return nil
}

func isDirectDynamicParam(expr *Expr) bool {
	_, ok := expr.Expr.(*plan.Expr_P)
	return ok
}

// A prepared LAG/LEAD offset has TEXT as its transport type, but the window
// executor consumes integer vectors. Give a bare marker the same integer
// computation type as an explicit CAST(? AS SIGNED), while leaving literals,
// columns, and explicitly typed expressions on their existing binding path.
func normalizeLagLeadOffsetParam(ctx context.Context, name string, args []*Expr) error {
	if (name != "lag" && name != "lead") || len(args) < 2 || !isDirectDynamicParam(args[1]) {
		return nil
	}

	int64Type := types.T_int64.ToType()
	offset, err := appendCastBeforeExpr(ctx, args[1], makePlan2Type(&int64Type))
	if err != nil {
		return err
	}
	args[1] = offset
	return nil
}

// LAG/LEAD offsets must be non-NULL, non-negative integers. Prepared markers
// are normalized to int64 above and checked before execution or plan filling;
// row-dependent integer expressions are checked after evaluation by the
// window operator.
func validateLagLeadOffsetLiteral(ctx context.Context, name string, args []*Expr) error {
	if (name != "lag" && name != "lead") || len(args) < 2 {
		return nil
	}

	offsetExpr := args[1]
	if lagLeadOffsetIsNullLiteral(offsetExpr) || !types.T(offsetExpr.Typ.Id).IsInteger() {
		return moerr.NewWrongArguments(ctx, name)
	}
	lit := offsetExpr.GetLit()
	if lit != nil {
		if offset, ok := literalSignedValue(lit); ok && offset < 0 {
			return moerr.NewWrongArguments(ctx, name)
		}
	}
	return nil
}

func lagLeadOffsetIsNullLiteral(expr *Expr) bool {
	for expr != nil {
		if lit := expr.GetLit(); lit != nil {
			return lit.Isnull
		}
		fn := expr.GetF()
		if fn == nil || fn.GetFunc().GetObjName() != "cast" || len(fn.Args) == 0 {
			return false
		}
		expr = fn.Args[0]
	}
	return false
}

func (b *baseBinder) bindNumVal(astExpr *tree.NumVal, typ Type) (*Expr, error) {
	// over_int64_err := moerr.NewInternalError(b.GetContext(), "", "Constants over int64 will support in future version.")
	// rewrite the hexnum process logic
	// for float64, if the number is over 1<<53-1,it will lost, so if typ is float64,
	// don't cast 0xXXXX as float64, use the uint64
	returnDecimalExpr := func(val string) (*Expr, error) {
		if !typ.IsEmpty() {
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(val), typ)
		}
		return makePlan2DecimalExprWithType(b.GetContext(), val)
	}

	returnRawLiteralExpr := func(val string, form plan.StringLiteralForm) (*Expr, error) {
		isBin := form == plan.StringLiteralForm_STRING_LITERAL_HEX ||
			form == plan.StringLiteralForm_STRING_LITERAL_BIT
		expr := makePlan2StringConstExprWithType(val, isBin)
		expr.GetLit().LiteralForm = form
		if !typ.IsEmpty() {
			isFloat := typ.Id == int32(types.T_float32) || typ.Id == int32(types.T_float64)
			return appendCastBeforeExpr(b.GetContext(), expr, typ, isBin, isFloat)
		}
		return expr, nil
	}
	returnBinaryIntroducerExpr := func(val string) (*Expr, error) {
		expr := makePlan2VarBinaryConstExprWithType(val)
		expr.GetLit().LiteralForm = plan.StringLiteralForm_STRING_LITERAL_BINARY_INTRODUCER
		if !typ.IsEmpty() {
			return appendCastBeforeExpr(b.GetContext(), expr, typ)
		}
		return expr, nil
	}

	switch astExpr.ValType {
	case tree.P_null:
		return makePlan2NullConstExprWithType(), nil
	case tree.P_bool:
		val := astExpr.Bool()
		return makePlan2BoolConstExprWithType(val), nil
	case tree.P_int64:
		val, ok := astExpr.Int64()
		if !ok {
			return nil, moerr.NewInvalidInputf(b.GetContext(), "invalid int value '%s'", astExpr.String())
		}
		expr := makePlan2Int64ConstExprWithType(val)
		if !typ.IsEmpty() && typ.Id == int32(types.T_varchar) {
			return appendCastBeforeExpr(b.GetContext(), expr, typ)
		}
		return expr, nil
	case tree.P_uint64:
		val, ok := astExpr.Uint64()
		if !ok {
			return nil, moerr.NewInvalidInputf(b.GetContext(), "invalid int value '%s'", astExpr.String())
		}
		return makePlan2Uint64ConstExprWithType(val), nil
	case tree.P_decimal:
		if !typ.IsEmpty() {
			if typ.Id == int32(types.T_decimal64) {
				d64, err := types.ParseDecimal64(astExpr.String(), typ.Width, typ.Scale)
				if err != nil {
					return nil, err
				}
				return &Expr{
					Expr: &plan.Expr_Lit{
						Lit: &Const{
							Isnull: false,
							Value: &plan.Literal_Decimal64Val{
								Decimal64Val: &plan.Decimal64{A: int64(d64)},
							},
						},
					},
					Typ: typ,
				}, nil
			}
			if typ.Id == int32(types.T_decimal128) {
				d128, err := types.ParseDecimal128(astExpr.String(), typ.Width, typ.Scale)
				if err != nil {
					return nil, err
				}
				a := int64(d128.B0_63)
				b := int64(d128.B64_127)
				return &Expr{
					Expr: &plan.Expr_Lit{
						Lit: &Const{
							Isnull: false,
							Value: &plan.Literal_Decimal128Val{
								Decimal128Val: &plan.Decimal128{A: a, B: b},
							},
						},
					},
					Typ: typ,
				}, nil
			}
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(astExpr.String()), typ)
		}
		// Smart type selection for untyped decimal literals
		// Choose decimal64 if value fits, otherwise decimal128
		d128, scale, err := types.Parse128(astExpr.String())
		if err != nil {
			return makePlan2DecimalExprWithType(b.GetContext(), astExpr.String())
		}

		// Check if value fits in decimal64 (18 digits precision)
		// decimal64 max: 999999999999999999 (18 nines)
		maxDecimal64 := uint64(999999999999999999)
		useDecimal64 := d128.B64_127 == 0 && d128.B0_63 <= maxDecimal64 && scale <= 18

		if useDecimal64 {
			d64 := types.Decimal64(d128.B0_63)
			return &Expr{
				Expr: &plan.Expr_Lit{
					Lit: &Const{
						Isnull: false,
						Value: &plan.Literal_Decimal64Val{
							Decimal64Val: &plan.Decimal64{A: int64(d64)},
						},
					},
				},
				Typ: plan.Type{
					Id:          int32(types.T_decimal64),
					Width:       18,
					Scale:       scale,
					NotNullable: true,
				},
			}, nil
		}

		// Use decimal128 for higher precision
		a := int64(d128.B0_63)
		b := int64(d128.B64_127)
		return &Expr{
			Expr: &plan.Expr_Lit{
				Lit: &Const{
					Isnull: false,
					Value: &plan.Literal_Decimal128Val{
						Decimal128Val: &plan.Decimal128{A: a, B: b},
					},
				},
			},
			Typ: plan.Type{
				Id:          int32(types.T_decimal128),
				Width:       38,
				Scale:       scale,
				NotNullable: true,
			},
		}, nil
	case tree.P_float64:
		originString := astExpr.String()
		if !typ.IsEmpty() && types.T(typ.Id).IsDecimal() {
			return returnDecimalExpr(originString)
		}
		if !strings.Contains(originString, "e") {
			expr, err := returnDecimalExpr(originString)
			if err == nil {
				return expr, nil
			}
		}
		floatValue, ok := astExpr.Float64()
		if !ok {
			return returnDecimalExpr(originString)
		}
		return makePlan2Float64ConstExprWithType(floatValue), nil
	case tree.P_hexnum:
		s := astExpr.String()[2:]
		if len(s)%2 != 0 {
			s = string('0') + s
		}
		bytes, _ := hex.DecodeString(s)
		return returnRawLiteralExpr(string(bytes), plan.StringLiteralForm_STRING_LITERAL_HEX)
	case tree.P_ScoreBinaryHexnum:
		s := astExpr.String()[2:]
		if len(s)%2 != 0 {
			s = string('0') + s
		}
		bytes, _ := hex.DecodeString(s)
		return returnBinaryIntroducerExpr(string(bytes))
	case tree.P_ScoreBinary:
		return returnBinaryIntroducerExpr(astExpr.String())
	case tree.P_bit:
		s := astExpr.String()[2:]
		bytes, _ := util.DecodeBinaryString(s)
		return returnRawLiteralExpr(string(bytes), plan.StringLiteralForm_STRING_LITERAL_BIT)
	case tree.P_char:
		expr := makePlan2StringConstExprWithType(astExpr.String())
		return expr, nil
	case tree.P_star:
		expr := makePlan2StringConstExprWithType(astExpr.String())
		return expr, nil
	case tree.P_nulltext:
		expr := MakePlan2NullTextConstExprWithType(astExpr.String())
		return expr, nil
	default:
		return nil, moerr.NewInvalidInputf(b.GetContext(), "unsupport value '%s'", astExpr.String())
	}
}

func (b *baseBinder) GetContext() context.Context { return b.sysCtx }

// --- util functions ----

func appendCastBeforeExpr(ctx context.Context, expr *Expr, toType Type, isBin ...bool) (*Expr, error) {
	return appendCastBeforeExprWithOverload(ctx, expr, toType, 0, isBin...)
}

func appendExplicitCastBeforeExpr(ctx context.Context, expr *Expr, toType Type) (*Expr, error) {
	return appendCastBeforeExprWithOverload(ctx, expr, toType, 1)
}

// appendSyntaxExplicitCastBeforeExpr keeps the legacy CAST overload on the
// wire while recording syntax provenance in an optional protobuf field. Old
// CNs ignore that field and continue to execute overload 0, while new planners
// can distinguish this user-written CAST from implicit reconciliation casts.
func appendSyntaxExplicitCastBeforeExpr(ctx context.Context, expr *Expr, toType Type) (*Expr, error) {
	cast, err := appendCastBeforeExprWithOverload(ctx, expr, toType, 0)
	if err != nil {
		return nil, err
	}
	if fn := cast.GetF(); fn != nil && fn.Func != nil && fn.Func.GetObjName() == "cast" {
		fn.SyntaxExplicitCast = true
	}
	return cast, nil
}

func appendComparisonCastBeforeExpr(ctx context.Context, expr *Expr, toType Type) (*Expr, error) {
	return appendCastBeforeExprWithOverload(ctx, expr, toType, 2)
}

func appendSetOperationCastBeforeExpr(ctx context.Context, expr *Expr, toType Type) (*Expr, error) {
	return appendCastBeforeExprWithOverload(ctx, expr, toType, 3)
}

// hasPadSpaceStringProvenance identifies value-selecting string expressions
// that can expose representation-only CHAR padding after implicit promotion.
// Do not recurse through byte-transforming functions such as CONCAT: spaces
// produced there are part of the expression result rather than CHAR storage.
func hasPadSpaceStringProvenance(expr *Expr) bool {
	if expr == nil {
		return false
	}
	if expr.Typ.PadSpace {
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil {
		return false
	}
	if fn.Func.ObjName == "cast" && len(fn.Args) > 0 {
		fromType := makeTypeByPlan2Expr(fn.Args[0])
		toType := makeTypeByPlan2Expr(expr)
		if fromType.Oid == types.T_char &&
			(toType.Oid == types.T_varchar || toType.Oid == types.T_text) {
			return true
		}
		// LEAST/GREATEST compare through overload 2 before returning one of the
		// values. Follow that comparison-only wrapper so a value-selecting
		// parent can retain the source value's PAD SPACE provenance.
		return isCastOverload(expr, 2) && hasPadSpaceStringProvenance(fn.Args[0])
	}
	for _, idx := range padSpaceValueArgumentIndexes(fn.Func.ObjName, len(fn.Args)) {
		if hasPadSpaceStringProvenance(fn.Args[idx]) {
			return true
		}
	}
	return false
}

// padSpaceValueArgumentIndexes returns only arguments whose bytes can become
// the function result. In particular, max_by's order and tie arguments must
// not taint the first-argument value returned by the aggregate.
func padSpaceValueArgumentIndexes(name string, argsLength int) []int {
	switch name {
	case "case", "coalesce", "if", "iff":
		return controlFlowValueIndexes(name, argsLength)
	case "ifnull":
		if argsLength == 2 {
			return []int{0, 1}
		}
	case "lag", "lead":
		if argsLength >= 3 {
			return []int{0, 2}
		}
		if argsLength > 0 {
			return []int{0}
		}
	case "first_value", "last_value", "nth_value",
		"any_value", "min", "max", "max_by", "max_by_non_null":
		if argsLength > 0 {
			return []int{0}
		}
	case "least", "greatest":
		indexes := make([]int, argsLength)
		for i := range indexes {
			indexes[i] = i
		}
		return indexes
	}
	return nil
}

func appendPadSpaceComparisonCastIfNeeded(ctx context.Context, expr *Expr) (*Expr, error) {
	argType := makeTypeByPlan2Expr(expr)
	if (argType.Oid == types.T_varchar || argType.Oid == types.T_text) &&
		hasPadSpaceStringProvenance(expr) && !isCastOverload(expr, 2) {
		return appendComparisonCastBeforeExpr(ctx, expr, makePlan2Type(&argType))
	}
	return expr, nil
}

// appendPadSpaceWindowKeyCastIfNeeded canonicalizes direct CHAR window keys
// into the same PAD SPACE comparison domain as promoted string keys. Ordinary
// predicates deliberately keep their existing CHAR comparison binding so that
// optimizer key recognition is unchanged outside window planning.
func appendPadSpaceWindowKeyCastIfNeeded(ctx context.Context, expr *Expr) (*Expr, error) {
	if isCastOverload(expr, 2) {
		return expr, nil
	}
	argType := makeTypeByPlan2Expr(expr)
	if argType.Oid == types.T_char {
		targetType := argType
		targetType.Oid = types.T_varchar
		return appendComparisonCastBeforeExpr(ctx, expr, makePlan2Type(&targetType))
	}
	return appendPadSpaceComparisonCastIfNeeded(ctx, expr)
}

func isCastOverload(expr *Expr, overloadID int32) bool {
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != "cast" {
		return false
	}
	_, actualOverloadID := function.DecodeOverloadID(fn.Func.Obj)
	return actualOverloadID == overloadID
}

func isPadSpaceComparisonFunction(name string) bool {
	switch name {
	case "=", "<=>", "!=", "<>", "<", "<=", ">", ">=", "between",
		"strcmp", "field", "least", "greatest":
		return true
	default:
		return false
	}
}

func appendCastBeforeExprWithOverload(
	ctx context.Context, expr *Expr, toType Type, overloadID int32, isBin ...bool,
) (*Expr, error) {
	fromPadSpace := expr != nil &&
		(types.T(expr.Typ.Id) == types.T_char || hasPadSpaceStringProvenance(expr))
	expr, rewritten, err := rewriteMySQLSpecialTypeDisplayCast(ctx, expr, toType)
	if err != nil {
		return nil, err
	}
	if rewritten {
		return expr, nil
	}
	toType.NotNullable = expr.Typ.NotNullable
	argsType := []types.Type{
		makeTypeByPlan2Expr(expr),
		makeTypeByPlan2Type(toType),
	}
	fGet, err := function.GetFunctionByNameWithOverload(ctx, "cast", argsType, overloadID)
	if err != nil {
		return nil, err
	}
	// for 0xXXXX, if the value is over 1<<53-1, when covert it into float64,it will lost, so just change it into uint64
	typ := toType
	typ.PadSpace = false
	if overloadID <= 1 && fromPadSpace &&
		(types.T(typ.Id) == types.T_varchar || types.T(typ.Id) == types.T_text) {
		typ.PadSpace = true
	}
	if len(isBin) == 2 && isBin[0] && isBin[1] {
		typ.Id = int32(types.T_uint64)
	}
	return &Expr{
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: getFunctionObjRef(fGet.GetEncodedOverloadID(), "cast"),
				Args: []*Expr{
					expr,
					{
						Typ: typ,
						Expr: &plan.Expr_T{
							T: &plan.TargetType{},
						},
					},
				},
			},
		},
		Typ: typ,
	}, nil
}

func rewriteMySQLSpecialTypeDisplayCast(ctx context.Context, expr *Expr, toType Type) (*Expr, bool, error) {
	if isSetDisplayValueExpr(expr) && types.T(toType.Id).IsInteger() && !isSetPlanType(&toType) {
		// SET columns are wrapped with cast_set_index_to_value during column
		// binding so ordinary projections expose their labels. Integer casts
		// require the stored bitmap instead; casting the label is both incorrect
		// and lossy for SET definitions that contain an empty member.
		if bitmap, ok := storedSetBitmapExpr(expr); ok {
			return bitmap, false, nil
		}
	}
	if toType.Id != int32(types.T_json) {
		return expr, false, nil
	}
	if isEnumOrSetPlanType(&expr.Typ) {
		displayValue, err := makeEnumOrSetDisplayValue(ctx, expr)
		if err != nil {
			return nil, false, err
		}
		quoted, err := quoteEnumOrSetDisplayValueAsJSON(ctx, displayValue)
		return quoted, err == nil, err
	}
	if isEnumOrSetDisplayValueExpr(expr) {
		quoted, err := quoteEnumOrSetDisplayValueAsJSON(ctx, expr)
		return quoted, err == nil, err
	}
	return expr, false, nil
}

func makeEnumOrSetDisplayValue(ctx context.Context, expr *Expr) (*Expr, error) {
	if expr == nil || !isEnumOrSetPlanType(&expr.Typ) {
		return expr, nil
	}
	indexToValueFun, _, _, err := mysqlSpecialTypeFuncNames(&expr.Typ)
	if err != nil {
		return nil, err
	}
	return BindFuncExprImplByPlanExpr(ctx, indexToValueFun, []*Expr{
		makePlan2StringConstExprWithType(expr.Typ.Enumvalues),
		expr,
	})
}

func storedSetBitmapExpr(expr *Expr) (*Expr, bool) {
	if !isSetDisplayValueExpr(expr) {
		return expr, false
	}
	fn := expr.GetF()
	if len(fn.Args) != 2 || fn.Args[1] == nil {
		return expr, false
	}
	bitmap := DeepCopyExpr(fn.Args[1])
	// The hidden projection is the physical uint64 representation, not a SQL
	// SET value. Clear the member metadata so downstream assignment treats it
	// as an ordinary bitmap and does not convert it back through SET semantics.
	bitmap.Typ.Enumvalues = ""
	return bitmap, true
}

// storedMySQLSpecialTypeExpr removes the presentation wrapper that column
// binding adds for ENUM and SET.  The wrapper is appropriate for ordinary
// string contexts, but numeric contracts must consume the stored ENUM ordinal
// or SET bitmap.  Keep this narrowly structural: only the wrappers made by
// makeEnumOrSetDisplayValue are unwrapped.
func storedMySQLSpecialTypeExpr(expr *Expr) (*Expr, bool) {
	if isSetDisplayValueExpr(expr) {
		return storedSetBitmapExpr(expr)
	}
	if expr == nil {
		return expr, false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.ObjName != moEnumCastIndexToValueFun || len(fn.Args) != 2 || fn.Args[1] == nil {
		return expr, false
	}
	return DeepCopyExpr(fn.Args[1]), true
}

// useStoredMySQLSpecialTypesForNumericContract chooses ENUM/SET storage from
// the function overload's bound operand contract, rather than from a small
// collection of AST shapes.  This preserves display labels for string
// functions such as LENGTH while allowing numeric consumers such as ABS,
// comparisons against numeric columns, and IN lists to use MySQL ordinals.
func useStoredMySQLSpecialTypesForNumericContract(ctx context.Context, name string, args []*Expr) []*Expr {
	rawArgs := make([]*Expr, len(args))
	hasSpecialArg := false
	for i, arg := range args {
		raw, unwrapped := storedMySQLSpecialTypeExpr(arg)
		rawArgs[i] = raw
		if !unwrapped {
			continue
		}
		hasSpecialArg = true
	}
	if !hasSpecialArg {
		return args
	}

	// The display-bound operands describe the contract selected for this SQL
	// expression.  In particular, resolving a raw ENUM against a string can
	// itself select a numeric comparison rule, so do not use the raw overload
	// to decide whether the caller asked for numeric semantics.
	displayTypes := make([]types.Type, len(args))
	for i, arg := range args {
		displayTypes[i] = makeTypeByPlan2Expr(arg)
	}
	resolved, err := function.GetFunctionByName(ctx, name, displayTypes)
	if err != nil {
		return useStoredMySQLSpecialTypesForNumericInList(name, args, rawArgs)
	}
	targets, shouldCast := resolved.ShouldDoImplicitTypeCast()
	if !shouldCast || len(targets) != len(rawArgs) {
		return useStoredMySQLSpecialTypesForNumericInList(name, args, rawArgs)
	}
	result := args
	changed := false
	for i, arg := range args {
		if _, unwrapped := storedMySQLSpecialTypeExpr(arg); unwrapped && targets[i].IsNumeric() {
			if !changed {
				result = append([]*Expr(nil), args...)
				changed = true
			}
			result[i] = rawArgs[i]
		}
	}
	return result
}

// An IN list is represented as a plan.ExprList and is not assigned one scalar
// cast target by the function registry.  Its already-bound member types are
// therefore the operand contract: use the stored value only when every member
// is numeric.  Mixed lists retain normal string semantics.
func useStoredMySQLSpecialTypesForNumericInList(name string, args, rawArgs []*Expr) []*Expr {
	if (name != "in" && name != "not_in" && name != "partition_in") || len(args) != 2 || args[1].GetList() == nil || len(args[1].GetList().List) == 0 {
		return args
	}
	for _, member := range args[1].GetList().List {
		if !makeTypeByPlan2Expr(member).IsNumeric() {
			return args
		}
	}
	result := append([]*Expr(nil), args...)
	for i, arg := range args {
		if _, unwrapped := storedMySQLSpecialTypeExpr(arg); unwrapped {
			result[i] = rawArgs[i]
		}
	}
	return result
}

// useStoredMySQLSpecialTypesForNumericSubquery applies the numeric operand
// contract in both directions.  Subquery references expose only one scalar
// type for single-column results, so tuple comparisons must inspect the
// subquery projection position-by-position rather than the tuple type itself.
func (b *baseBinder) useStoredMySQLSpecialTypesForNumericSubquery(left, subqueryExpr *Expr) *Expr {
	projectList := b.subqueryProjectList(subqueryExpr)
	if len(projectList) == 0 {
		return left
	}

	left = useStoredMySQLSpecialTypeForNumericProjection(left, projectList)
	for i, project := range projectList {
		if !numericSubqueryOperandAt(left, i) {
			continue
		}
		if raw, ok := storedMySQLSpecialTypeExpr(project); ok {
			projectList[i] = raw
		}
	}
	return left
}

func (b *baseBinder) subqueryProjectList(expr *Expr) []*Expr {
	if b.builder == nil || expr == nil || expr.GetSub() == nil {
		return nil
	}
	nodeID := expr.GetSub().NodeId
	if nodeID < 0 || int(nodeID) >= len(b.builder.qry.Nodes) {
		return nil
	}
	return b.builder.qry.Nodes[nodeID].ProjectList
}

func useStoredMySQLSpecialTypeForNumericProjection(left *Expr, projects []*Expr) *Expr {
	if left == nil || len(projects) == 0 {
		return left
	}
	if list := left.GetList(); list != nil {
		if len(list.List) != len(projects) {
			return left
		}
		var result []*Expr
		for i, item := range list.List {
			if !makeTypeByPlan2Expr(projects[i]).IsNumeric() {
				continue
			}
			raw, ok := storedMySQLSpecialTypeExpr(item)
			if !ok {
				continue
			}
			if result == nil {
				result = append([]*Expr(nil), list.List...)
			}
			result[i] = raw
		}
		if result == nil {
			return left
		}
		return &Expr{Typ: left.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: result}}}
	}
	if len(projects) == 1 && makeTypeByPlan2Expr(projects[0]).IsNumeric() {
		if raw, ok := storedMySQLSpecialTypeExpr(left); ok {
			return raw
		}
	}
	return left
}

func numericSubqueryOperandAt(left *Expr, index int) bool {
	if left == nil {
		return false
	}
	if list := left.GetList(); list != nil {
		return index < len(list.List) && makeTypeByPlan2Expr(list.List[index]).IsNumeric()
	}
	return index == 0 && makeTypeByPlan2Expr(left).IsNumeric()
}

func isSetDisplayValueExpr(expr *Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	return fn != nil && fn.Func != nil && fn.Func.ObjName == moSetCastIndexToValueFun
}

func isEnumOrSetDisplayValueExpr(expr *Expr) bool {
	if isSetDisplayValueExpr(expr) {
		return true
	}
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	return fn != nil && fn.Func != nil && fn.Func.ObjName == moEnumCastIndexToValueFun
}

func quoteEnumOrSetDisplayValueAsJSON(ctx context.Context, expr *Expr) (*Expr, error) {
	quoted, err := BindFuncExprImplByPlanExpr(ctx, "json_quote", []*Expr{expr})
	if err != nil {
		return nil, err
	}
	quoted.Typ.NotNullable = expr.Typ.NotNullable
	return quoted, nil
}

func resetDateFunctionArgs(ctx context.Context, dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
	list := intervalExpr.GetList()
	if list == nil || len(list.List) < 2 {
		return nil, moerr.NewInvalidArg(ctx, "interval expression requires a value and a unit", intervalExpr)
	}
	firstExpr := list.List[0]
	secondExpr := list.List[1]

	// MySQL behavior: INTERVAL NULL SECOND is valid and returns NULL at execution time
	// Only date_add(..., null) (without INTERVAL) should return syntax error
	// This is handled in resetDateFunction, not here

	intervalTypeStr := secondExpr.GetLit().GetSval()
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	if numberExpr, returnType, handled, err := bindStringIntervalExpr(ctx, firstExpr, intervalType); err != nil {
		return nil, err
	} else if handled {
		// "date '2020-10-10' - interval 1 Hour"  will return datetime
		// so we rewrite "date '2020-10-10' - interval 1 Hour"  to  "date_add(datetime, 1, hour)"
		if dateExpr.Typ.Id == int32(types.T_date) {
			switch returnType {
			case types.Day, types.Week, types.Month, types.Quarter, types.Year:
			default:
				dateExpr, err = appendCastBeforeExpr(ctx, dateExpr, plan.Type{
					Id: int32(types.T_datetime),
				})

				if err != nil {
					return nil, err
				}
			}
		}
		return []*Expr{
			dateExpr,
			numberExpr,
			makePlan2Int64ConstExprWithType(int64(returnType)),
		}, nil
	}

	// "date '2020-10-10' - interval 1 Hour"  will return datetime
	// so we rewrite "date '2020-10-10' - interval 1 Hour"  to  "date_add(datetime, 1, hour)"
	if dateExpr.Typ.Id == int32(types.T_date) {
		switch intervalType {
		case types.Day, types.Week, types.Month, types.Quarter, types.Year:
		default:
			dateExpr, err = appendCastBeforeExpr(ctx, dateExpr, plan.Type{
				Id: int32(types.T_datetime),
			})

			if err != nil {
				return nil, err
			}
		}
	}

	// For time units (SECOND, MINUTE, HOUR, DAY), we need to handle decimal/float values
	// by converting them to microseconds. Check if firstExpr is a literal with decimal/float type.
	isTimeUnit := intervalType == types.Second || intervalType == types.Minute ||
		intervalType == types.Hour || intervalType == types.Day
	isDecimalOrFloat := firstExpr.Typ.Id == int32(types.T_decimal64) ||
		firstExpr.Typ.Id == int32(types.T_decimal128) ||
		firstExpr.Typ.Id == int32(types.T_float32) ||
		firstExpr.Typ.Id == int32(types.T_float64)

	// Try to get literal value, either directly or from a cast function
	var lit *plan.Literal
	var innerExpr *plan.Expr // The inner expression (for getting scale from cast target type)
	if firstExpr.GetLit() != nil {
		lit = firstExpr.GetLit()
		innerExpr = firstExpr
	} else if funcExpr, ok := firstExpr.Expr.(*plan.Expr_F); ok && funcExpr.F != nil &&
		funcExpr.F.Func != nil && funcExpr.F.Func.GetObjName() == "cast" {
		// Check if it's a cast function with a literal argument
		if len(funcExpr.F.Args) > 0 && funcExpr.F.Args[0].GetLit() != nil {
			lit = funcExpr.F.Args[0].GetLit()
			innerExpr = firstExpr // Use firstExpr to get the scale from the cast target type
		}
	}

	if isTimeUnit && isDecimalOrFloat && lit != nil {
		// Extract the value from the literal and convert to microseconds
		var floatVal float64
		var hasValue bool

		if !lit.Isnull {
			if dval, ok := lit.Value.(*plan.Literal_Dval); ok {
				floatVal = dval.Dval
				hasValue = true
			} else if fval, ok := lit.Value.(*plan.Literal_Fval); ok {
				floatVal = float64(fval.Fval)
				hasValue = true
			} else if d64val, ok := lit.Value.(*plan.Literal_Decimal64Val); ok {
				// Convert decimal64 to float64
				d64 := types.Decimal64(d64val.Decimal64Val.A)
				scale := innerExpr.Typ.Scale
				if scale < 0 {
					scale = 0
				}
				floatVal = types.Decimal64ToFloat64(d64, scale)
				hasValue = true
			} else if d128val, ok := lit.Value.(*plan.Literal_Decimal128Val); ok {
				// Convert decimal128 to float64
				d128 := types.Decimal128{B0_63: uint64(d128val.Decimal128Val.A), B64_127: uint64(d128val.Decimal128Val.B)}
				scale := innerExpr.Typ.Scale
				if scale < 0 {
					scale = 0
				}
				floatVal = types.Decimal128ToFloat64(d128, scale)
				hasValue = true
			} else if sval, ok := lit.Value.(*plan.Literal_Sval); ok {
				// Handle string literal (from cast function's first argument)
				// Try to parse as decimal128 to get the float value
				d128, scale, err := types.Parse128(sval.Sval)
				if err == nil {
					floatVal = types.Decimal128ToFloat64(d128, scale)
					hasValue = true
				}
			}
		}

		if hasValue {
			// Convert to microseconds based on interval type
			var finalValue int64
			switch intervalType {
			case types.Second:
				// Use math.Round to handle floating point precision issues (e.g., 1.000009 * 1000000 = 1000008.9999999999)
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec)))
			case types.Minute:
				// Use math.Round to handle floating point precision issues
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerMinute)))
			case types.Hour:
				// Use math.Round to handle floating point precision issues
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerHour)))
			case types.Day:
				// Use math.Round to handle floating point precision issues
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerDay)))
			default:
				finalValue = int64(floatVal)
			}
			return []*Expr{
				dateExpr,
				makePlan2Int64ConstExprWithType(finalValue),
				// Use MicroSecond type since we've converted to microseconds
				makePlan2Int64ConstExprWithType(int64(types.MicroSecond)),
			}, nil
		}
	}

	numberExpr, err := appendCastBeforeExpr(ctx, firstExpr, plan.Type{Id: int32(types.T_int64)})
	if err != nil {
		return nil, err
	}

	return []*Expr{
		dateExpr,
		numberExpr,
		makePlan2Int64ConstExprWithType(int64(intervalType)),
	}, nil
}

// bindStringIntervalExpr keeps VARCHAR/CHAR interval semantics identical for
// literals and column expressions. Dynamic values are normalized row-by-row at
// execution time instead of using a normal VARCHAR -> INT64 cast.
func bindStringIntervalExpr(ctx context.Context, expr *Expr, intervalType types.IntervalType) (*Expr, types.IntervalType, bool, error) {
	if expr.Typ.Id != int32(types.T_varchar) && expr.Typ.Id != int32(types.T_char) {
		return nil, types.IntervalTypeInvalid, false, nil
	}
	if lit := expr.GetLit(); lit != nil {
		number, normalizedType, err := types.NormalizeInterval(lit.GetSval(), intervalType)
		if err != nil {
			// Existing literal behavior: date functions recognize this marker and
			// return NULL rather than propagating a parse/cast error.
			number = math.MaxInt64
			normalizedType = intervalType
		}
		return makePlan2Int64ConstExprWithType(number), normalizedType, true, nil
	}

	// The normalized unit depends only on the SQL interval unit, not on a row.
	_, normalizedType, err := types.NormalizeInterval("0", intervalType)
	if err != nil {
		return nil, types.IntervalTypeInvalid, false, err
	}
	numberExpr, err := BindFuncExprImplByPlanExpr(ctx, "to_interval", []*Expr{
		expr,
		makePlan2Int64ConstExprWithType(int64(intervalType)),
	})
	if err != nil {
		return nil, types.IntervalTypeInvalid, false, err
	}
	return numberExpr, normalizedType, true, nil
}

func resetDateFunction(ctx context.Context, dateExpr *Expr, intervalExpr *Expr) ([]*Expr, error) {
	// MySQL behavior: NULL literal as interval argument should return syntax error
	if isNullExpr(intervalExpr) {
		return nil, moerr.NewSyntaxError(ctx, "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'null)' at line 1")
	}
	switch intervalExpr.Expr.(type) {
	case *plan.Expr_List:
		return resetDateFunctionArgs(ctx, dateExpr, intervalExpr)
	}
	list := &plan.ExprList{
		List: make([]*Expr, 2),
	}
	list.List[0] = intervalExpr
	strType := makeGeneratedPlan2Type(types.T_char, 0, 0, false)
	strExpr := &Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Value: &plan.Literal_Sval{
					Sval: "day",
				},
			},
		},
		Typ: strType,
	}
	list.List[1] = strExpr
	expr := &plan.Expr_List{
		List: list,
	}
	listExpr := &Expr{
		Expr: expr,
	}
	return resetDateFunctionArgs(ctx, dateExpr, listExpr)
}

func resetIntervalFunction(ctx context.Context, intervalExpr *Expr) ([]*Expr, error) {
	return resetIntervalFunctionArgs(ctx, intervalExpr)
}

func resetIntervalFunctionArgs(ctx context.Context, intervalExpr *Expr) ([]*Expr, error) {
	list := intervalExpr.GetList()
	if list == nil || len(list.List) < 2 {
		return nil, moerr.NewInvalidArg(ctx, "interval expression requires a value and a unit", intervalExpr)
	}
	firstExpr := list.List[0]
	secondExpr := list.List[1]

	// MySQL behavior: INTERVAL NULL SECOND is valid and returns NULL at execution time
	// NULL values will be handled at execution time (null1 || null2 check)

	intervalTypeStr := secondExpr.GetLit().GetSval()
	intervalType, err := types.IntervalTypeOf(intervalTypeStr)
	if err != nil {
		return nil, err
	}

	if numberExpr, returnType, handled, err := bindStringIntervalExpr(ctx, firstExpr, intervalType); err != nil {
		return nil, err
	} else if handled {
		return []*Expr{
			numberExpr,
			makePlan2Int64ConstExprWithType(int64(returnType)),
		}, nil
	}

	// For time units (SECOND, MINUTE, HOUR, DAY), we need to handle decimal/float values
	// by converting them to microseconds. Check if firstExpr is a literal with decimal/float type.
	isTimeUnit := intervalType == types.Second || intervalType == types.Minute ||
		intervalType == types.Hour || intervalType == types.Day
	isDecimalOrFloat := firstExpr.Typ.Id == int32(types.T_decimal64) ||
		firstExpr.Typ.Id == int32(types.T_decimal128) ||
		firstExpr.Typ.Id == int32(types.T_float32) ||
		firstExpr.Typ.Id == int32(types.T_float64)

	if isTimeUnit && isDecimalOrFloat && firstExpr.GetLit() != nil {
		// Extract the value from the literal and convert to microseconds
		lit := firstExpr.GetLit()
		var floatVal float64
		var hasValue bool

		if !lit.Isnull {
			if dval, ok := lit.Value.(*plan.Literal_Dval); ok {
				floatVal = dval.Dval
				hasValue = true
			} else if fval, ok := lit.Value.(*plan.Literal_Fval); ok {
				floatVal = float64(fval.Fval)
				hasValue = true
			} else if d64val, ok := lit.Value.(*plan.Literal_Decimal64Val); ok {
				// Convert decimal64 to float64
				d64 := types.Decimal64(d64val.Decimal64Val.A)
				scale := firstExpr.Typ.Scale
				if scale < 0 {
					scale = 0
				}
				floatVal = types.Decimal64ToFloat64(d64, scale)
				hasValue = true
			} else if d128val, ok := lit.Value.(*plan.Literal_Decimal128Val); ok {
				// Convert decimal128 to float64
				d128 := types.Decimal128{B0_63: uint64(d128val.Decimal128Val.A), B64_127: uint64(d128val.Decimal128Val.B)}
				scale := firstExpr.Typ.Scale
				if scale < 0 {
					scale = 0
				}
				floatVal = types.Decimal128ToFloat64(d128, scale)
				hasValue = true
			}
		}

		if hasValue {
			// Convert to microseconds based on interval type
			var finalValue int64
			switch intervalType {
			case types.Second:
				// Use math.Round to handle floating point precision issues (e.g., 1.000009 * 1000000 = 1000008.9999999999)
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec)))
			case types.Minute:
				// Use math.Round to handle floating point precision issues
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerMinute)))
			case types.Hour:
				// Use math.Round to handle floating point precision issues
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerHour)))
			case types.Day:
				// Use math.Round to handle floating point precision issues
				finalValue = int64(math.Round(floatVal * float64(types.MicroSecsPerSec*types.SecsPerDay)))
			default:
				finalValue = int64(floatVal)
			}
			return []*Expr{
				makePlan2Int64ConstExprWithType(finalValue),
				// Use MicroSecond type since we've converted to microseconds
				makePlan2Int64ConstExprWithType(int64(types.MicroSecond)),
			}, nil
		}
	}

	numberExpr, err := appendCastBeforeExpr(ctx, firstExpr, plan.Type{Id: int32(types.T_int64)})
	if err != nil {
		return nil, err
	}

	return []*Expr{
		numberExpr,
		makePlan2Int64ConstExprWithType(int64(intervalType)),
	}, nil
}

func intervalUnitIsDayOrLarger(intervalExpr *Expr) bool {
	list := intervalExpr.GetList()
	if list == nil || len(list.List) < 2 {
		return false
	}
	unitStr := list.List[1].GetLit().GetSval()
	iTyp, err := types.IntervalTypeOf(unitStr)
	if err != nil {
		return false
	}
	return types.UnitIsDayOrLarger(iTyp)
}

func handleTupleIn(ctx context.Context, name string, leftList *plan.Expr_List, rightList *plan.ExprList) (*plan.Expr, error) {
	candidates := make([]*plan.Expr, 0, len(rightList.List))

	for _, rightVal := range rightList.List {
		if rightTuple, ok := rightVal.Expr.(*plan.Expr_List); ok {
			if len(leftList.List.List) != len(rightTuple.List.List) {
				return nil, moerr.NewInternalError(ctx, "tuple length mismatch")
			}

			equalities := make([]*plan.Expr, 0, len(leftList.List.List))
			for i := 0; i < len(leftList.List.List); i++ {
				leftElem := leftList.List.List[i]
				rightElem := rightTuple.List.List[i]

				eqExpr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{leftElem, rightElem})
				if err != nil {
					return nil, err
				}
				equalities = append(equalities, eqExpr)
			}

			candidate, err := combinePlanExprsBalanced(ctx, "and", equalities)
			if err != nil {
				return nil, err
			}
			candidates = append(candidates, candidate)

		} else {
			return nil, moerr.NewInternalError(ctx, "IN list must contain tuples")
		}
	}

	newExpr, err := combinePlanExprsBalanced(ctx, "or", candidates)
	if err != nil {
		return nil, err
	}
	if name == "not_in" {
		return BindFuncExprImplByPlanExpr(ctx, "not", []*plan.Expr{newExpr})
	}
	return newExpr, nil
}

func foldNameConstArgs(ctx context.Context, proc *process.Process, args []*plan.Expr) error {
	if err := validateNameConstArgs(ctx, args, false); err != nil {
		return err
	}

	foldedArg, err := ConstantFold(batch.EmptyForConstFoldBatch, args[1], proc, false, true)
	if err != nil {
		return err
	}
	args[1] = foldedArg

	if args[1].GetLit() == nil {
		return moerr.NewInvalidArg(ctx, "NAME_CONST", "")
	}
	return nil
}

func validateNameConstArgs(
	ctx context.Context,
	args []*plan.Expr,
	allowCanonicalStringCast bool,
) error {
	if len(args) != 2 {
		return moerr.NewInvalidArg(ctx, "NAME_CONST", len(args))
	}

	nameLit := args[0].GetLit()
	if nameLit == nil || nameLit.Isnull ||
		!validNameConstValueExpr(args[1], allowCanonicalStringCast) {
		return moerr.NewInvalidArg(ctx, "NAME_CONST", "")
	}
	return nil
}

func validNameConstValueExpr(arg *plan.Expr, allowCanonicalStringCast bool) bool {
	if arg == nil {
		return false
	}
	if arg.GetLit() != nil {
		return true
	}
	if isDecimalLiteralCast(arg) {
		return true
	}
	if allowCanonicalStringCast && isCanonicalStringLiteralCast(arg) {
		return true
	}
	fn := arg.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) != 1 {
		return false
	}
	if fn.Func.GetObjName() != "unary_minus" && fn.Func.GetObjName() != "unary_plus" {
		return false
	}
	return fn.Args[0].GetLit() != nil || isDecimalLiteralCast(fn.Args[0])
}

func validNameConstNameAst(args []tree.Expr) bool {
	if len(args) != 2 {
		return false
	}
	name := stripNameConstParens(args[0])
	if nameLit, ok := name.(*tree.NumVal); ok {
		return validNameConstNameLiteral(nameLit)
	}
	return false
}

func validNameConstValueAst(args []tree.Expr, allowCanonicalStringCast bool) bool {
	if len(args) != 2 {
		return false
	}
	return validNameConstLiteralValueAst(args[1], allowCanonicalStringCast)
}

func validNameConstLiteralValueAst(expr tree.Expr, allowCanonicalStringCast bool) bool {
	expr = stripNameConstParens(expr)
	switch value := expr.(type) {
	case *tree.NumVal:
		return true
	case *tree.CastExpr:
		return allowCanonicalStringCast && isCanonicalStringLiteralCastAst(value)
	case *tree.UnaryExpr:
		if value.Op != tree.UNARY_PLUS && value.Op != tree.UNARY_MINUS {
			return false
		}
		_, ok := stripNameConstParens(value.Expr).(*tree.NumVal)
		return ok
	default:
		return false
	}
}

func isCanonicalStringLiteralCastAst(expr tree.Expr) bool {
	castExpr, ok := stripNameConstParens(expr).(*tree.CastExpr)
	if !ok {
		return false
	}
	lit, ok := stripNameConstParens(castExpr.Expr).(*tree.NumVal)
	if !ok || lit.ValType != tree.P_hexnum {
		return false
	}
	target, ok := castExpr.Type.(*tree.T)
	return ok && target.InternalType.Family == tree.StringFamily
}

func isCanonicalStringLiteralCast(expr *plan.Expr) bool {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return false
	}
	fn := expr.GetF()
	if fn.Func.GetObjName() != "cast" || len(fn.Args) != 2 {
		return false
	}
	return fn.Args[0].GetLit() != nil &&
		fn.Args[0].GetLit().IsBin &&
		types.T(expr.Typ.Id) == types.T_varchar
}

func stripNameConstParens(expr tree.Expr) tree.Expr {
	for {
		paren, ok := expr.(*tree.ParenExpr)
		if !ok {
			break
		}
		expr = paren.Expr
	}
	return expr
}

func isDecimalLiteralCast(arg *plan.Expr) bool {
	fn := arg.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) != 2 {
		return false
	}
	if !types.T(arg.Typ.Id).IsDecimal() || fn.Args[0].GetLit() == nil || fn.Args[1].GetT() == nil {
		return false
	}
	lit := fn.Args[0].GetLit()
	if lit.Isnull || lit.GetSval() == "" {
		return false
	}
	if _, _, err := types.Parse128(lit.GetSval()); err == nil {
		return true
	}
	if _, _, err := types.Parse256(lit.GetSval()); err == nil {
		return true
	}
	return false
}

// defaultValueBindType returns the target column type carried by a
// DefaultBinder or ReplaceValueBinder. For other binder implementations it
// returns an empty Type so literal binding falls back to the generic path.
func (b *baseBinder) defaultValueBindType() plan.Type {
	if d, ok := b.impl.(*DefaultBinder); ok {
		return d.typ
	}
	if r, ok := b.impl.(*ReplaceValueBinder); ok {
		return r.typ
	}
	return plan.Type{}
}

func (b *baseBinder) defaultNumericOuterType() *plan.Type {
	typ := b.defaultValueBindType()
	if types.T(typ.Id).ToType().IsNumeric() {
		return &typ
	}
	return nil
}
