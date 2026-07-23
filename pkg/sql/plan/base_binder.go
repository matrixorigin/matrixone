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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
		// check existence
		if b.GetContext() != nil && b.GetContext().Value(defines.InSp{}) != nil && b.GetContext().Value(defines.InSp{}).(bool) {
			tmpScope := b.GetContext().Value(defines.VarScopeKey{}).(*[]map[string]interface{})
			for i := len(*tmpScope) - 1; i >= 0; i-- {
				curScope := (*tmpScope)[i]
				if _, ok := curScope[exprImpl.ColName()]; ok {
					typ := types.T_text.ToType()
					expr = &Expr{
						Typ: makePlan2Type(&typ),
						Expr: &plan.Expr_V{
							V: &plan.VarRef{
								Name:   exprImpl.ColName(),
								System: false,
								Global: false,
							},
						},
					}
					err = nil
					return
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
		if isNumericArithmeticRoot(exprImpl.Expr) ||
			b.isGenericNumericFunctionRoot(exprImpl.Expr, depth, &typ) {
			expr, err = b.bindNumericExprWithContext(exprImpl.Expr, depth, &typ)
		} else {
			expr, err = b.impl.BindExpr(exprImpl.Expr, depth, false)
		}
		b.numericParamType = parentParamType
		if err != nil {
			return
		}
		if useExplicitCastOverload(exprImpl.Type) {
			expr, err = appendExplicitCastBeforeExpr(b.GetContext(), expr, typ)
		} else {
			expr, err = appendCastBeforeExpr(b.GetContext(), expr, typ)
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
		if b.builder == nil {
			err = moerr.NewInvalidInput(b.GetContext(), "parameter marker is not allowed in this expression")
		} else if !b.builder.isPrepareStatement {
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
	case defines.MYSQL_TYPE_DECIMAL, defines.MYSQL_TYPE_NEWDECIMAL:
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
	if b.builder == nil {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "parameter marker is not allowed in this expression")
	}
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
	return &Expr{
		Typ: makePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   astExpr.Name,
				System: astExpr.System,
				Global: astExpr.Global,
			},
		},
	}, nil
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
	name := tree.String(astExpr, dialect.MYSQL)

	if b.ctx.timeTag > 0 && (col == TimeWindowStart || col == TimeWindowEnd) {
		colPos := int32(len(b.ctx.times))
		expr = &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_timestamp)},
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
			err = moerr.NewInvalidInputf(localErrCtx, "column %s does not exist", name)
		} else {
			err = moerr.NewInvalidInputf(localErrCtx, "column %s does not exist", name)
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
				err = moerr.NewInvalidInputf(localErrCtx, "column '%s' does not exist", name)
			}
		} else {
			err = moerr.NewInvalidInputf(localErrCtx, "missing FROM-clause entry for table '%v'", table)
		}
	}

	if groupPos, ok := b.correlatedGroupByColPos(depth, name, table, col); ok {
		expr = &plan.Expr{
			Typ: b.ctx.groups[groupPos].Typ,
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

	if isEnumOrSetPlanType(typ) {
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
		b.boundCols = append(b.boundCols, table+"."+col)

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
			expr.Expr = &plan.Expr_Corr{
				Corr: &plan.CorrColRef{
					RelPos: relPos,
					ColPos: colPos,
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

func (b *baseBinder) corrColRefTargetsGroup(corr *plan.CorrColRef) bool {
	if corr == nil {
		return false
	}
	ctx := b.ctx
	for depth := int32(0); depth < corr.Depth && ctx != nil; depth++ {
		ctx = ctx.parent
	}
	return ctx != nil && ctx.groupTag > 0 && corr.RelPos == ctx.groupTag
}

func (b *baseBinder) corrColRefTargetsCurrentGroup(corr *plan.CorrColRef) bool {
	return corr != nil && b.ctx != nil && b.ctx.groupTag > 0 && corr.RelPos == b.ctx.groupTag
}

func (b *baseBinder) baseBindSubquery(astExpr *tree.Subquery, isRoot bool) (*Expr, error) {
	if b.ctx == nil {
		return nil, moerr.NewInvalidInput(b.GetContext(), "field reference doesn't support SUBQUERY")
	}
	subCtx := NewBindContext(b.builder, b.ctx)
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
			args = append(args, whenExpr.Cond)
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

func (b *baseBinder) bindRangeCond(astExpr *tree.RangeCond, depth int32, isRoot bool) (*Expr, error) {
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

func (b *baseBinder) bindUnaryExpr(astExpr *tree.UnaryExpr, depth int32, isRoot bool) (*Expr, error) {
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
	if isNumericBinaryOp(astExpr.Op) && b.numericParamType == nil {
		return b.bindNumericExprWithDefaultContext(astExpr, depth, b.defaultNumericOuterType())
	}
	return b.bindBinaryExprWithCurrentContext(astExpr, depth)
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
	case *tree.ParamExpr, *tree.NumVal, *tree.ParenExpr, *tree.CastExpr:
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
	if b.numericParamType != nil {
		return b.impl.BindExpr(astExpr, depth, false)
	}
	if b.builder == nil || !b.builder.isPrepareStatement {
		return b.bindNumericExprWithoutNewContext(astExpr, depth)
	}

	scan, err := b.numericAstTypesWithHint(astExpr, depth, outer)
	if err != nil || !scan.hasParam || scan.incompatible {
		if err != nil {
			return nil, err
		}
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
	hasUnknown   bool
	incompatible bool
}

func (s numericAstTypeScan) merge(other numericAstTypeScan) numericAstTypeScan {
	s.strong = append(s.strong, other.strong...)
	s.weakDecimals = append(s.weakDecimals, other.weakDecimals...)
	s.hasParam = s.hasParam || other.hasParam
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
		return numericAstTypeScan{hasParam: true}, nil
	case *tree.Subquery:
		if expr.Exists {
			return numericAstTypeScan{}, nil
		}
		scan, err := b.numericScalarSubqueryAstTypes(expr, depth)
		// Keep scalar subqueries as deferred parameter-bearing operands even
		// when their projection type cannot be determined statically. This
		// preserves assignment-target propagation for expressions whose
		// parameters are hidden behind unsupported projection shapes.
		scan.hasParam = true
		return scan, err
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
		return numericAstTypedOperand(typ), nil
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
			}
			return scan, nil
		}
		if !makeTypeByPlan2Type(typ).IsNumeric() {
			return numericAstTypeScan{}, nil
		}
		return numericAstTypedOperand(typ), nil
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
		return b.bindFuncExprImplByAstExpr("not", []tree.Expr{newExpr}, depth)

	case tree.ILIKE:
		op = "ilike"

	case tree.NOT_ILIKE:
		newExpr := tree.NewComparisonExpr(tree.ILIKE, astExpr.Left, astExpr.Right)
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

	return b.bindFuncExprImplByAstExpr(op, []tree.Expr{astExpr.Left, astExpr.Right}, depth)
}

func (b *baseBinder) bindTupleInByAst(leftTuple *tree.Tuple, rightTuple *tree.Tuple, depth int32, isNot bool) (*plan.Expr, error) {
	candidates := make([]*plan.Expr, 0, len(rightTuple.Exprs))

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
	if strings.EqualFold(funcName, "mod") && b.numericParamType == nil {
		return b.bindNumericExprWithDefaultContext(astExpr, depth, b.defaultNumericOuterType())
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

func (b *baseBinder) bindFuncExprImplByAstExpr(name string, astArgs []tree.Expr, depth int32) (*plan.Expr, error) {
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
	if b.numericParamType != nil {
		var err error
		args, err = b.resolvePreparedNumericArgs(name, args)
		if err != nil {
			return nil, err
		}
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
		if !validNameConstNameAst(astArgs) || !validNameConstValueAst(astArgs) {
			return nil, moerr.NewInvalidArg(b.GetContext(), "NAME_CONST", "")
		}
		if err := validateNameConstArgs(b.GetContext(), args); err != nil {
			return nil, err
		}
	}

	if b.builder != nil {
		e, err := bindFuncExprAndConstFold(b.GetContext(), b.builder.compCtx.GetProcess(), name, args)
		if err == nil {
			return e, nil
		}
		if !strings.Contains(err.Error(), "not supported") {
			return nil, err
		}
	} else {
		// return bindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		// first look for builtin func
		builtinExpr, err := BindFuncExprImplByPlanExpr(b.GetContext(), name, args)
		if err == nil {
			return builtinExpr, nil
		}
		if !strings.Contains(err.Error(), "not supported") {
			return nil, err
		}
	}

	// not a builtin func, look to resolve udf
	if b.builder == nil {
		return nil, moerr.NewInvalidInputf(b.GetContext(), "function '%s' is not allowed in this expression", name)
	}
	cmpCtx := b.builder.compCtx
	udf, err := cmpCtx.ResolveUdf(name, args)
	if err != nil {
		return nil, err
	}

	return bindFuncExprImplUdf(b, name, udf, astArgs, depth)
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
		cast, err := appendCastBeforeExpr(b.GetContext(), args[i], makePlan2Type(&targets[i]))
		if err != nil {
			return nil, err
		}
		args[i] = cast
	}
	return args, nil
}

func bindFuncExprImplUdf(b *baseBinder, name string, udf *function.Udf, args []tree.Expr, depth int32) (*plan.Expr, error) {
	if udf == nil {
		return nil, moerr.NewNotSupportedf(b.GetContext(), "function '%s'", name)
	}

	switch udf.Language {
	case string(tree.SQL):
		sql := udf.Body
		parserSQLMode := "PIPES_AS_CONCAT"
		if udf.SQLMode != nil {
			parserSQLMode = *udf.SQLMode
		}
		// replace sql with actual arg value
		fmtctx := tree.NewFmtCtx(dialect.MYSQL, tree.WithQuoteString(true))
		for i := 0; i < len(args); i++ {
			args[i].Format(fmtctx)
			sql = strings.Replace(sql, "$"+strconv.Itoa(i+1), fmtctx.String(), 1)
			fmtctx.Reset()
		}

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
		expr, err := b.bindPythonUdf(udf, args, depth)
		if err != nil {
			return nil, err
		}
		return expr, nil
	default:
		return nil, moerr.NewInvalidArg(b.GetContext(), "function language", udf.Language)
	}
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
	retExpr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
	if err != nil {
		return nil, err
	}

	switch retExpr.GetF().GetFunc().GetObjName() {
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

func BindFuncExprImplByPlanExpr(ctx context.Context, name string, args []*Expr) (*plan.Expr, error) {
	var err error

	// deal with some special function
	if listExpr, ok, err := bindSerialFuncOverExprList(ctx, name, args); ok || err != nil {
		return listExpr, err
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
	case "=", "<", "<=", ">", ">=", "<>":
		// why not append cast function?
		if err := convertValueIntoBool(name, args, false); err != nil {
			return nil, err
		}
		if err := adjustJsonOrderingDynamicParamType(ctx, name, args); err != nil {
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
	case "mo_win_truncate":
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, "truncate function need two args", len(args))
		}
		args[0], err = appendCastBeforeExpr(ctx, args[0], plan.Type{
			Id: int32(types.T_datetime),
		})
		if err != nil {
			return nil, err
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
		} else if args[0].Typ.Id == int32(types.T_varchar) && args[1].Typ.Id == int32(types.T_varchar) {
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
	case "like":
		// sql 'select * from t where col like ?'  the ? Expr's type will be T_any
		if len(args) != 2 {
			return nil, moerr.NewInvalidArg(ctx, name+" function have invalid input args length", len(args))
		}
		if args[0].Typ.Id == int32(types.T_any) {
			args[0].Typ.Id = int32(types.T_varchar)
		}
		if args[1].Typ.Id == int32(types.T_any) {
			args[1].Typ.Id = int32(types.T_varchar)
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
					inExpr, err := appendCastBeforeExpr(ctx, rightVal, args[0].Typ)
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
					tmpExpr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{DeepCopyExpr(args[0]), expr})
					if err != nil {
						return nil, err
					}
					expanded = append(expanded, tmpExpr)
				}
				return combinePlanExprsBalanced(ctx, "or", expanded)
			} else {
				for _, expr := range orExprList {
					tmpExpr, err := BindFuncExprImplByPlanExpr(ctx, "!=", []*Expr{DeepCopyExpr(args[0]), expr})
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

	// get args(exprs) & types
	argsLength := len(args)
	argsType := make([]types.Type, argsLength)
	for idx, expr := range args {
		argsType[idx] = makeTypeByPlan2Expr(expr)
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

		return nil, err
	}

	funcID = fGet.GetEncodedOverloadID()
	returnType = fGet.GetReturnType()
	argsCastType, _ = fGet.ShouldDoImplicitTypeCast()

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

	case "timestampadd":
		// For TIMESTAMPADD with DATE input, check if unit is constant and adjust return type
		// MySQL behavior: DATE input + date unit → DATE output, DATE input + time unit → DATETIME output
		// This ensures GetResultColumnsFromPlan returns correct column type for MySQL protocol layer
		if len(args) >= 3 && argsType[2].Oid == types.T_date {
			// Check if first argument (unit) is a constant string
			if unitExpr, ok := args[0].Expr.(*plan.Expr_Lit); ok && unitExpr.Lit != nil && !unitExpr.Lit.Isnull {
				if sval, ok := unitExpr.Lit.GetValue().(*plan.Literal_Sval); ok {
					unitStr := strings.ToUpper(sval.Sval)
					// Parse interval type
					iTyp, err := types.IntervalTypeOf(unitStr)
					if err == nil {
						// Check if it's a date unit (DAY, WEEK, MONTH, QUARTER, YEAR)
						isDateUnit := iTyp == types.Day || iTyp == types.Week ||
							iTyp == types.Month || iTyp == types.Quarter ||
							iTyp == types.Year
						if isDateUnit {
							// Return DATE type for date units (MySQL compatible)
							returnType = types.T_date.ToType()
						}
						// For time units (HOUR, MINUTE, SECOND, MICROSECOND), keep DATETIME (from retType)
					}
				}
			}
			// If unit is not constant, keep DATETIME (conservative approach)
		}

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
				typ := makePlan2Type(&castType)
				args[idx], err = appendCastBeforeExpr(ctx, args[idx], typ)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// return new expr
	Typ := makePlan2Type(&returnType)
	Typ.NotNullable = function.DeduceNotNullable(funcID, args)
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

func adjustJsonOrderingDynamicParamType(ctx context.Context, name string, args []*Expr) error {
	switch name {
	case "<", "<=", ">", ">=":
	default:
		return nil
	}
	if len(args) != 2 {
		return nil
	}

	if args[0].Typ.Id == int32(types.T_json) && isDirectDynamicParam(args[1]) {
		var err error
		args[1], err = BindFuncExprImplByPlanExpr(ctx, function.JsonOrderingParamFunctionName, []*Expr{args[1]})
		return err
	}
	if args[1].Typ.Id == int32(types.T_json) && isDirectDynamicParam(args[0]) {
		var err error
		args[0], err = BindFuncExprImplByPlanExpr(ctx, function.JsonOrderingParamFunctionName, []*Expr{args[0]})
		return err
	}
	return nil
}

func isDirectDynamicParam(expr *Expr) bool {
	_, ok := expr.Expr.(*plan.Expr_P)
	return ok
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

	returnHexNumExpr := func(val string, isBin ...bool) (*Expr, error) {
		if !typ.IsEmpty() {
			isFloat := typ.Id == int32(types.T_float32) || typ.Id == int32(types.T_float64)
			return appendCastBeforeExpr(b.GetContext(), makePlan2StringConstExprWithType(val, isBin[0]), typ, isBin[0], isFloat)
		}
		return makePlan2StringConstExprWithType(val, isBin...), nil
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
		return returnHexNumExpr(string(bytes), true)
	case tree.P_ScoreBinaryHexnum:
		s := astExpr.String()[2:]
		if len(s)%2 != 0 {
			s = string('0') + s
		}
		bytes, _ := hex.DecodeString(s)
		if !typ.IsEmpty() {
			return appendCastBeforeExpr(b.GetContext(), makePlan2VarBinaryConstExprWithType(string(bytes)), typ)
		}
		return makePlan2VarBinaryConstExprWithType(string(bytes)), nil
	case tree.P_ScoreBinary:
		if !typ.IsEmpty() {
			return appendCastBeforeExpr(b.GetContext(), makePlan2VarBinaryConstExprWithType(astExpr.String()), typ)
		}
		return makePlan2VarBinaryConstExprWithType(astExpr.String()), nil
	case tree.P_bit:
		s := astExpr.String()[2:]
		bytes, _ := util.DecodeBinaryString(s)
		return returnHexNumExpr(string(bytes), true)
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

func appendCastBeforeExprWithOverload(
	ctx context.Context, expr *Expr, toType Type, overloadID int32, isBin ...bool,
) (*Expr, error) {
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

	intervalTypeInFunction := &plan.Type{
		Id: int32(types.T_int64),
	}

	if firstExpr.Typ.Id == int32(types.T_varchar) || firstExpr.Typ.Id == int32(types.T_char) {
		s := firstExpr.GetLit().GetSval()
		returnNum, returnType, err := types.NormalizeInterval(s, intervalType)

		if err != nil {
			// MySQL behavior: invalid interval string should return NULL at execution time, not error at parse time
			// Use a special marker value (math.MaxInt64) to indicate invalid interval
			// This will be detected in function execution and return NULL
			returnNum = math.MaxInt64
			returnType = intervalType
		}
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
			makePlan2Int64ConstExprWithType(returnNum),
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
	} else if funcExpr, ok := firstExpr.Expr.(*plan.Expr_F); ok && funcExpr.F != nil {
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

	numberExpr, err := appendCastBeforeExpr(ctx, firstExpr, *intervalTypeInFunction)
	if err != nil {
		return nil, err
	}

	return []*Expr{
		dateExpr,
		numberExpr,
		makePlan2Int64ConstExprWithType(int64(intervalType)),
	}, nil
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
	strType := &plan.Type{
		Id: int32(types.T_char),
	}
	strExpr := &Expr{
		Expr: &plan.Expr_Lit{
			Lit: &Const{
				Value: &plan.Literal_Sval{
					Sval: "day",
				},
			},
		},
		Typ: *strType,
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

	intervalTypeInFunction := &plan.Type{
		Id: int32(types.T_int64),
	}

	if firstExpr.Typ.Id == int32(types.T_varchar) || firstExpr.Typ.Id == int32(types.T_char) {
		s := firstExpr.GetLit().GetSval()
		returnNum, returnType, err := types.NormalizeInterval(s, intervalType)
		if err != nil {
			// MySQL behavior: invalid interval string should return NULL at execution time, not error at parse time
			// Use a special marker value (math.MaxInt64) to indicate invalid interval
			// This will be detected in function execution and return NULL
			returnNum = math.MaxInt64
			returnType = intervalType
		}
		return []*Expr{
			makePlan2Int64ConstExprWithType(returnNum),
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

	numberExpr, err := appendCastBeforeExpr(ctx, firstExpr, *intervalTypeInFunction)
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
	if err := validateNameConstArgs(ctx, args); err != nil {
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

func validateNameConstArgs(ctx context.Context, args []*plan.Expr) error {
	if len(args) != 2 {
		return moerr.NewInvalidArg(ctx, "NAME_CONST", len(args))
	}

	nameLit := args[0].GetLit()
	if nameLit == nil || nameLit.Isnull || !validNameConstValueExpr(args[1]) {
		return moerr.NewInvalidArg(ctx, "NAME_CONST", "")
	}
	return nil
}

func validNameConstValueExpr(arg *plan.Expr) bool {
	if arg == nil {
		return false
	}
	if arg.GetLit() != nil {
		return true
	}
	if isDecimalLiteralCast(arg) {
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
	nameLit, ok := name.(*tree.NumVal)
	return ok && validNameConstNameLiteral(nameLit)
}

func validNameConstValueAst(args []tree.Expr) bool {
	if len(args) != 2 {
		return false
	}
	return validNameConstLiteralValueAst(args[1])
}

func validNameConstLiteralValueAst(expr tree.Expr) bool {
	expr = stripNameConstParens(expr)
	switch value := expr.(type) {
	case *tree.NumVal:
		return true
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
