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
	"bytes"
	"container/list"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"path"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func GetBindings(expr *plan.Expr) []int32 {
	bindingSet := doGetBindings(expr)
	bindings := make([]int32, 0, len(bindingSet))
	for id := range bindingSet {
		bindings = append(bindings, id)
	}
	return bindings
}

func doGetBindings(expr *plan.Expr) map[int32]bool {
	res := make(map[int32]bool)

	switch expr := expr.Expr.(type) {
	case *plan.Expr_Col:
		res[expr.Col.RelPos] = true

	case *plan.Expr_F:
		for _, child := range expr.F.Args {
			for id := range doGetBindings(child) {
				res[id] = true
			}
		}
	}

	return res
}

func hasParam(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P:
		return true

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if hasParam(arg) {
				return true
			}
		}
		return false

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if hasParam(arg) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func hasCorrCol(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Corr:
		return true

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if hasCorrCol(arg) {
				return true
			}
		}
		return false

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if hasCorrCol(arg) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func hasSubquery(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Sub:
		return true

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if hasSubquery(arg) {
				return true
			}
		}
		return false

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if hasSubquery(arg) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func HasTag(expr *plan.Expr, tag int32) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == tag

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if HasTag(arg, tag) {
				return true
			}
		}
		return false

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if HasTag(arg, tag) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

func decreaseDepthAndDispatch(preds []*plan.Expr) ([]*plan.Expr, []*plan.Expr) {
	filterPreds := make([]*plan.Expr, 0, len(preds))
	joinPreds := make([]*plan.Expr, 0, len(preds))

	for _, pred := range preds {
		newPred, correlated := decreaseDepth(pred)
		if !correlated {
			joinPreds = append(joinPreds, newPred)
			continue
		}
		filterPreds = append(filterPreds, newPred)
	}

	return filterPreds, joinPreds
}

func decreaseDepth(expr *plan.Expr) (*plan.Expr, bool) {
	var correlated bool

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Corr:
		if exprImpl.Corr.Depth > 1 {
			exprImpl.Corr.Depth--
			correlated = true
		} else {
			expr.Expr = &plan.Expr_Col{
				Col: &plan.ColRef{
					RelPos: exprImpl.Corr.RelPos,
					ColPos: exprImpl.Corr.ColPos,
				},
			}
		}

	case *plan.Expr_F:
		var tmp bool
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i], tmp = decreaseDepth(arg)
			correlated = correlated || tmp
		}
	}

	return expr, correlated
}

func getJoinSide(expr *plan.Expr, leftTags, rightTags map[int32]bool, markTag int32) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSide(arg, leftTags, rightTags, markTag)
		}

	case *plan.Expr_Col:
		if leftTags[exprImpl.Col.RelPos] {
			side = JoinSideLeft
		} else if rightTags[exprImpl.Col.RelPos] {
			side = JoinSideRight
		} else if exprImpl.Col.RelPos == markTag {
			side = JoinSideMark
		}

	case *plan.Expr_Corr:
		side = JoinSideCorrelated
	}

	return
}

func containsTag(expr *plan.Expr, tag int32) bool {
	var ret bool

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			ret = ret || containsTag(arg, tag)
		}

	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == tag
	}

	return ret
}

func replaceColRefs(expr *plan.Expr, tag int32, projects []*plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColRefs(arg, tag, projects)
		}

	case *plan.Expr_Col:
		colRef := exprImpl.Col
		if colRef.RelPos == tag {
			expr = DeepCopyExpr(projects[colRef.ColPos])
		}
	case *plan.Expr_W:
		replaceColRefs(exprImpl.W.WindowFunc, tag, projects)
		for _, arg := range exprImpl.W.PartitionBy {
			replaceColRefs(arg, tag, projects)
		}
		for _, order := range exprImpl.W.OrderBy {
			replaceColRefs(order.Expr, tag, projects)
		}
	}

	return expr
}

func replaceColRefsForSet(expr *plan.Expr, projects []*plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColRefsForSet(arg, projects)
		}

	case *plan.Expr_Col:
		expr = DeepCopyExpr(projects[exprImpl.Col.ColPos])
	}

	return expr
}

func splitAndBindCondition(astExpr tree.Expr, expandAlias ExpandAliasMode, ctx *BindContext) ([]*plan.Expr, error) {
	conds := splitAstConjunction(astExpr)
	exprs := make([]*plan.Expr, len(conds))

	for i, cond := range conds {
		cond, err := ctx.qualifyColumnNames(cond, expandAlias)
		if err != nil {
			return nil, err
		}

		expr, err := ctx.binder.BindExpr(cond, 0, true)
		if err != nil {
			return nil, err
		}
		// expr must be bool type, if not, try to do type convert
		// but just ignore the subQuery. It will be solved at optimizer.
		if expr.GetSub() == nil {
			expr, err = makePlan2CastExpr(ctx.binder.GetContext(), expr, plan.Type{Id: int32(types.T_bool)})
			if err != nil {
				return nil, err
			}
		}
		exprs[i] = expr
	}

	return exprs, nil
}

// splitAstConjunction split a expression to a list of AND conditions.
func splitAstConjunction(astExpr tree.Expr) []tree.Expr {
	var astExprs []tree.Expr
	switch typ := astExpr.(type) {
	case nil:
	case *tree.AndExpr:
		astExprs = append(astExprs, splitAstConjunction(typ.Left)...)
		astExprs = append(astExprs, splitAstConjunction(typ.Right)...)
	case *tree.ParenExpr:
		astExprs = append(astExprs, splitAstConjunction(typ.Expr)...)
	default:
		astExprs = append(astExprs, astExpr)
	}
	return astExprs
}

// applyDistributivity (X AND B) OR (X AND C) OR (X AND D) => X AND (B OR C OR D)
// TODO: move it into optimizer
func applyDistributivity(ctx context.Context, expr *plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = applyDistributivity(ctx, arg)
		}

		if exprImpl.F.Func.ObjName != "or" {
			break
		}

		leftConds := splitPlanConjunction(exprImpl.F.Args[0])
		rightConds := splitPlanConjunction(exprImpl.F.Args[1])

		condMap := make(map[string]int)

		for _, cond := range rightConds {
			condMap[cond.String()] = JoinSideRight
		}

		var commonConds, leftOnlyConds, rightOnlyConds []*plan.Expr

		for _, cond := range leftConds {
			exprStr := cond.String()

			if condMap[exprStr] == JoinSideRight {
				commonConds = append(commonConds, cond)
				condMap[exprStr] = JoinSideBoth
			} else {
				leftOnlyConds = append(leftOnlyConds, cond)
				condMap[exprStr] = JoinSideLeft
			}
		}

		for _, cond := range rightConds {
			if condMap[cond.String()] == JoinSideRight {
				rightOnlyConds = append(rightOnlyConds, cond)
			}
		}

		if len(commonConds) == 0 {
			return expr
		}

		expr, _ = combinePlanConjunction(ctx, commonConds)

		if len(leftOnlyConds) == 0 || len(rightOnlyConds) == 0 {
			return expr
		}

		leftExpr, _ := combinePlanConjunction(ctx, leftOnlyConds)
		rightExpr, _ := combinePlanConjunction(ctx, rightOnlyConds)

		leftExpr, _ = BindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{leftExpr, rightExpr})

		expr, _ = BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{expr, leftExpr})
	}

	return expr
}

func unionSlice(left, right []string) []string {
	if len(left) < 1 {
		return right
	}
	if len(right) < 1 {
		return left
	}
	m := make(map[string]bool, len(left)+len(right))
	for _, s := range left {
		m[s] = true
	}
	for _, s := range right {
		m[s] = true
	}
	ret := make([]string, 0)
	for s := range m {
		ret = append(ret, s)
	}
	return ret
}

func intersectSlice(left, right []string) []string {
	if len(left) < 1 || len(right) < 1 {
		return left
	}
	m := make(map[string]bool, len(left)+len(right))
	for _, s := range left {
		m[s] = true
	}
	ret := make([]string, 0)
	for _, s := range right {
		if _, ok := m[s]; ok {
			ret = append(ret, s)
		}
	}
	return ret
}

/*
DNF means disjunctive normal form, for example (a and b) or (c and d) or (e and f)
if we have a DNF filter, for example (c1=1 and c2=1) or (c1=2 and c2=2)
we can have extra filter: (c1=1 or c1=2) and (c2=1 or c2=2), which can be pushed down to optimize join

checkDNF scan the expr and return all groups of cond
for example (c1=1 and c2=1) or (c1=2 and c3=2), c1 is a group because it appears in all disjunctives
and c2,c3 is not a group

walkThroughDNF accept a keyword string, walk through the expr,
and extract all the conds which contains the keyword
*/
func checkDNF(expr *plan.Expr) []string {
	var ret []string
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "or" {
			left := checkDNF(exprImpl.F.Args[0])
			right := checkDNF(exprImpl.F.Args[1])
			return intersectSlice(left, right)
		}
		for _, arg := range exprImpl.F.Args {
			ret = unionSlice(ret, checkDNF(arg))
		}
		return ret

	case *plan.Expr_Corr:
		ret = append(ret, exprImpl.Corr.String())
	case *plan.Expr_Col:
		ret = append(ret, exprImpl.Col.String())
	}
	return ret
}

func walkThroughDNF(ctx context.Context, expr *plan.Expr, keywords string) *plan.Expr {
	var retExpr *plan.Expr
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "or" {
			left := walkThroughDNF(ctx, exprImpl.F.Args[0], keywords)
			right := walkThroughDNF(ctx, exprImpl.F.Args[1], keywords)
			if left != nil && right != nil {
				retExpr, _ = BindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{left, right})
				return retExpr
			}
		} else if exprImpl.F.Func.ObjName == "and" {
			left := walkThroughDNF(ctx, exprImpl.F.Args[0], keywords)
			right := walkThroughDNF(ctx, exprImpl.F.Args[1], keywords)
			if left == nil {
				return right
			} else if right == nil {
				return left
			} else {
				retExpr, _ = BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{left, right})
				return retExpr
			}
		} else {
			for _, arg := range exprImpl.F.Args {
				if walkThroughDNF(ctx, arg, keywords) == nil {
					return nil
				}
			}
			return expr
		}

	case *plan.Expr_Corr:
		if exprImpl.Corr.String() == keywords {
			return expr
		} else {
			return nil
		}
	case *plan.Expr_Col:
		if exprImpl.Col.String() == keywords {
			return expr
		} else {
			return nil
		}
	}
	return expr
}

// deduction of new predicates for join on list. for example join on a=b and b=c, then a=c can be deduced
func deduceNewOnList(onList []*plan.Expr) []*plan.Expr {
	var newPreds []*plan.Expr
	lenOnlist := len(onList)
	for i := range onList {
		ok1, col1, col2 := checkStrictJoinPred(onList[i])
		if !ok1 {
			continue
		}
		for j := i + 1; j < lenOnlist; j++ {
			ok2, col3, col4 := checkStrictJoinPred(onList[j])
			if ok2 {
				ok, newPred := deduceTranstivity(onList[i], col1, col2, col3, col4)
				if ok {
					newPreds = append(newPreds, newPred)
				}
			}
		}
	}
	return newPreds
}

// deduction of new predicates. for example join on a=b where b=1, then a=1 can be deduced
func deduceNewFilterList(filters, onList []*plan.Expr) []*plan.Expr {
	var newFilters []*plan.Expr
	for _, onPred := range onList {
		ret, col1, col2 := checkStrictJoinPred(onPred)
		if !ret {
			continue
		}
		for _, filter := range filters {
			col := extractColRefInFilter(filter)
			if col != nil {
				newExpr := DeepCopyExpr(filter)
				if substituteMatchColumn(newExpr, col1, col2) {
					newFilters = append(newFilters, newExpr)
				}
			}
		}
	}
	return newFilters
}

func canMergeToBetweenAnd(expr1, expr2 *plan.Expr) bool {
	col1, _, _, _, _ := extractColRefAndLiteralsInFilter(expr1)
	col2, _, _, _, _ := extractColRefAndLiteralsInFilter(expr2)
	if col1 == nil || col2 == nil {
		return false
	}
	if col1.ColPos != col2.ColPos || col1.RelPos != col2.RelPos {
		return false
	}

	fnName1 := expr1.GetF().Func.ObjName
	fnName2 := expr2.GetF().Func.ObjName
	if fnName1 == ">" || fnName1 == ">=" {
		return fnName2 == "<" || fnName2 == "<="
	}
	if fnName1 == "<" || fnName1 == "<=" {
		return fnName2 == ">" || fnName2 == ">="
	}
	return false
}

func extractColRefAndLiteralsInFilter(expr *plan.Expr) (col *ColRef, litType types.T, literals []*Const, colFnName string, hasDynamicParam bool) {
	fn := expr.GetF()
	if fn == nil || len(fn.Args) == 0 {
		return
	}
	for i := range fn.Args {
		if containsDynamicParam(fn.Args[i]) {
			hasDynamicParam = true
			break
		}
	}

	col = fn.Args[0].GetCol()
	if col == nil {
		if fn0 := fn.Args[0].GetF(); fn0 != nil {
			switch fn0.Func.ObjName {
			case "year":
				colFnName = "year"
				col = fn0.Args[0].GetCol()
			}
		}
	}
	if col == nil {
		return
	}

	switch fn.Func.ObjName {
	case "=", ">", "<", ">=", "<=":
		lit := fn.Args[1].GetLit()
		if lit == nil {
			return
		}
		litType = types.T(fn.Args[0].Typ.Id)
		literals = []*Const{lit}

	case "between":
		litType = types.T(fn.Args[0].Typ.Id)
		literals = []*Const{fn.Args[1].GetLit(), fn.Args[2].GetLit()}
	}

	return
}

// for predicate deduction, filter must be like func(col)>1 , or (col=1) or (col=2)
// and only 1 colRef is allowd in the filter
func extractColRefInFilter(expr *plan.Expr) *ColRef {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "=", ">", "<", ">=", "<=", "prefix_eq", "between", "in", "prefix_in":
			switch e := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_Vec, *plan.Expr_List:
				return extractColRefInFilter(exprImpl.F.Args[0])
			case *plan.Expr_F:
				switch e.F.Func.ObjName {
				case "cast", "serial":
					return extractColRefInFilter(exprImpl.F.Args[0])
				}
				return nil
			default:
				return nil
			}
		default:
			var col *ColRef
			for _, arg := range exprImpl.F.Args {
				c := extractColRefInFilter(arg)
				if c == nil {
					return nil
				}
				if col != nil {
					if col.RelPos != c.RelPos || col.ColPos != c.ColPos {
						return nil
					}
				} else {
					col = c
				}
			}
			return col
		}
	case *plan.Expr_Col:
		return exprImpl.Col
	}
	return nil
}

// for col1=col2 and col3 = col4, trying to deduce new pred
// for example , if col1 and col3 are the same, then we can deduce that col2=col4
func deduceTranstivity(expr *plan.Expr, col1, col2, col3, col4 *ColRef) (bool, *plan.Expr) {
	if col1.String() == col3.String() || col1.String() == col4.String() || col2.String() == col3.String() || col2.String() == col4.String() {
		retExpr := DeepCopyExpr(expr)
		substituteMatchColumn(retExpr, col3, col4)
		return true, retExpr
	}
	return false, nil
}

// if match col1 in expr, substitute it to col2. and othterwise
func substituteMatchColumn(expr *plan.Expr, onPredCol1, onPredCol2 *ColRef) bool {
	var ret bool
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colName := exprImpl.Col.String()
		if colName == onPredCol1.String() {
			exprImpl.Col.RelPos = onPredCol2.RelPos
			exprImpl.Col.ColPos = onPredCol2.ColPos
			exprImpl.Col.Name = onPredCol2.Name
			return true
		} else if colName == onPredCol2.String() {
			exprImpl.Col.RelPos = onPredCol1.RelPos
			exprImpl.Col.ColPos = onPredCol1.ColPos
			exprImpl.Col.Name = onPredCol1.Name
			return true
		}
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if substituteMatchColumn(arg, onPredCol1, onPredCol2) {
				ret = true
			}
		}
	}
	return ret
}

func checkStrictJoinPred(onPred *plan.Expr) (bool, *ColRef, *ColRef) {
	//onPred must be equality, children must be column name
	switch onPredImpl := onPred.Expr.(type) {
	case *plan.Expr_F:
		if onPredImpl.F.Func.ObjName != "=" {
			return false, nil, nil
		}
		args := onPredImpl.F.Args
		var col1, col2 *ColRef
		switch child1 := args[0].Expr.(type) {
		case *plan.Expr_Col:
			col1 = child1.Col
		}
		switch child2 := args[1].Expr.(type) {
		case *plan.Expr_Col:
			col2 = child2.Col
		}
		if col1 != nil && col2 != nil {
			return true, col1, col2
		}
	}
	return false, nil, nil
}

func splitPlanConjunctions(exprList []*plan.Expr) []*plan.Expr {
	var exprs []*plan.Expr
	for _, expr := range exprList {
		exprs = append(exprs, splitPlanConjunction(expr)...)
	}
	return exprs
}

func splitPlanConjunction(expr *plan.Expr) []*plan.Expr {
	var exprs []*plan.Expr
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "and" {
			exprs = append(exprs, splitPlanConjunction(exprImpl.F.Args[0])...)
			exprs = append(exprs, splitPlanConjunction(exprImpl.F.Args[1])...)
		} else {
			exprs = append(exprs, expr)
		}

	default:
		exprs = append(exprs, expr)
	}

	return exprs
}

func combinePlanConjunction(ctx context.Context, exprs []*plan.Expr) (expr *plan.Expr, err error) {
	expr = exprs[0]

	for i := 1; i < len(exprs); i++ {
		expr, err = BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{expr, exprs[i]})

		if err != nil {
			break
		}
	}

	return
}

func rejectsNull(filter *plan.Expr, proc *process.Process) bool {
	filter = replaceColRefWithNull(DeepCopyExpr(filter))

	filter, err := ConstantFold(batch.EmptyForConstFoldBatch, filter, proc, false, true)
	if err != nil {
		return false
	}

	if f, ok := filter.Expr.(*plan.Expr_Lit); ok {
		if f.Lit.Isnull {
			return true
		}

		if fbool, ok := f.Lit.Value.(*plan.Literal_Bval); ok {
			return !fbool.Bval
		}
	}

	return false
}

func replaceColRefWithNull(expr *plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		expr = &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Isnull: true,
				},
			},
		}

	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = replaceColRefWithNull(arg)
		}
	}

	return expr
}

func increaseRefCnt(expr *plan.Expr, inc int, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}] += inc

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			increaseRefCnt(arg, inc, colRefCnt)
		}
	case *plan.Expr_W:
		increaseRefCnt(exprImpl.W.WindowFunc, inc, colRefCnt)
		//for _, arg := range exprImpl.W.PartitionBy {
		//	increaseRefCnt(arg, inc, colRefCnt)
		//}
		for _, order := range exprImpl.W.OrderBy {
			increaseRefCnt(order.Expr, inc, colRefCnt)
		}
	}
}

func getHyperEdgeFromExpr(expr *plan.Expr, leafByTag map[int32]int32, hyperEdge map[int32]bool) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		hyperEdge[leafByTag[exprImpl.Col.RelPos]] = true

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			getHyperEdgeFromExpr(arg, leafByTag, hyperEdge)
		}
	}
}

func getNumOfCharacters(str string) int {
	strRune := []rune(str)
	return len(strRune)
}

func getUnionSelects(ctx context.Context, stmt *tree.UnionClause, selects *[]tree.Statement, unionTypes *[]plan.Node_NodeType) error {
	switch leftStmt := stmt.Left.(type) {
	case *tree.UnionClause:
		err := getUnionSelects(ctx, leftStmt, selects, unionTypes)
		if err != nil {
			return err
		}
	case *tree.SelectClause:
		*selects = append(*selects, leftStmt)
	case *tree.ParenSelect:
		*selects = append(*selects, leftStmt.Select)
	default:
		return moerr.NewParseError(ctx, "unexpected statement in union: '%v'", tree.String(leftStmt, dialect.MYSQL))
	}

	// right is not UNION always
	switch rightStmt := stmt.Right.(type) {
	case *tree.SelectClause:
		if stmt.Type == tree.UNION && !stmt.All {
			rightStr := tree.String(rightStmt, dialect.MYSQL)
			if len(*selects) == 1 && tree.String((*selects)[0], dialect.MYSQL) == rightStr {
				return nil
			}
		}

		*selects = append(*selects, rightStmt)
	case *tree.ParenSelect:
		if stmt.Type == tree.UNION && !stmt.All {
			rightStr := tree.String(rightStmt.Select, dialect.MYSQL)
			if len(*selects) == 1 && tree.String((*selects)[0], dialect.MYSQL) == rightStr {
				return nil
			}
		}

		*selects = append(*selects, rightStmt.Select)
	default:
		return moerr.NewParseError(ctx, "unexpected statement in union2: '%v'", tree.String(rightStmt, dialect.MYSQL))
	}

	switch stmt.Type {
	case tree.UNION:
		if stmt.All {
			*unionTypes = append(*unionTypes, plan.Node_UNION_ALL)
		} else {
			*unionTypes = append(*unionTypes, plan.Node_UNION)
		}
	case tree.INTERSECT:
		if stmt.All {
			*unionTypes = append(*unionTypes, plan.Node_INTERSECT_ALL)
		} else {
			*unionTypes = append(*unionTypes, plan.Node_INTERSECT)
		}
	case tree.EXCEPT, tree.UT_MINUS:
		if stmt.All {
			return moerr.NewNYI(ctx, "EXCEPT/MINUS ALL clause")
		} else {
			*unionTypes = append(*unionTypes, plan.Node_MINUS)
		}
	}
	return nil
}

func GetColumnMapByExpr(expr *plan.Expr, tableDef *plan.TableDef, columnMap map[int]int) {
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			GetColumnMapByExpr(arg, tableDef, columnMap)
		}

	case *plan.Expr_Col:
		idx := exprImpl.Col.ColPos
		colName := exprImpl.Col.Name
		dotIdx := strings.Index(colName, ".")
		colName = colName[dotIdx+1:]
		colIdx := tableDef.Name2ColIndex[colName]
		seqnum := int(colIdx) // for extenal scan case, tableDef has only Name2ColIndex, no Cols, leave seqnum as colIdx
		if len(tableDef.Cols) > 0 {
			seqnum = int(tableDef.Cols[colIdx].Seqnum)
		}
		columnMap[int(idx)] = seqnum
	}
}

func GetColumnMapByExprs(exprs []*plan.Expr, tableDef *plan.TableDef, columnMap map[int]int) {
	for _, expr := range exprs {
		GetColumnMapByExpr(expr, tableDef, columnMap)
	}
}

func GetColumnsByExpr(
	expr *plan.Expr,
	tableDef *plan.TableDef,
) (columnMap map[int]int, defColumns, exprColumns []int, maxCol int) {
	columnMap = make(map[int]int)
	// key = expr's ColPos,  value = tableDef's ColPos
	GetColumnMapByExpr(expr, tableDef, columnMap)

	if len(columnMap) == 0 {
		return
	}

	defColumns = make([]int, len(columnMap))
	exprColumns = make([]int, len(columnMap))

	// k: col pos in expr
	// v: col pos in def
	i := 0
	for k, v := range columnMap {
		if v > maxCol {
			maxCol = v
		}
		exprColumns[i] = k
		defColumns[i] = v
		i = i + 1
	}
	return
}

func EvalFilterExpr(ctx context.Context, expr *plan.Expr, bat *batch.Batch, proc *process.Process) (bool, error) {
	if len(bat.Vecs) == 0 { //that's constant expr
		e, err := ConstantFold(bat, expr, proc, false, true)
		if err != nil {
			return false, err
		}

		if cExpr, ok := e.Expr.(*plan.Expr_Lit); ok {
			if bVal, bOk := cExpr.Lit.Value.(*plan.Literal_Bval); bOk {
				return bVal.Bval, nil
			}
		}
		return false, moerr.NewInternalError(ctx, "cannot eval filter expr")
	} else {
		executor, err := colexec.NewExpressionExecutor(proc, expr)
		if err != nil {
			return false, err
		}
		defer executor.Free()

		vec, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
		if err != nil {
			return false, err
		}
		if vec.GetType().Oid != types.T_bool {
			return false, moerr.NewInternalError(ctx, "cannot eval filter expr")
		}
		cols := vector.MustFixedCol[bool](vec)
		for _, isNeed := range cols {
			if isNeed {
				return true, nil
			}
		}
		return false, nil
	}
}

func exchangeVectors(datas [][2]any, depth int, tmpResult []any, result *[]*vector.Vector, mp *mpool.MPool) {
	for i := 0; i < len(datas[depth]); i++ {
		tmpResult[depth] = datas[depth][i]
		if depth != len(datas)-1 {
			exchangeVectors(datas, depth+1, tmpResult, result, mp)
		} else {
			for j, val := range tmpResult {
				vector.AppendAny((*result)[j], val, false, mp)
			}
		}
	}
}

func BuildVectorsByData(datas [][2]any, dataTypes []uint8, mp *mpool.MPool) []*vector.Vector {
	vectors := make([]*vector.Vector, len(dataTypes))
	for i, typ := range dataTypes {
		vectors[i] = vector.NewVec(types.T(typ).ToType())
	}

	tmpResult := make([]any, len(datas))
	exchangeVectors(datas, 0, tmpResult, &vectors, mp)

	return vectors
}

func ExprIsZonemappable(ctx context.Context, expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		isConst := true
		for _, arg := range exprImpl.F.Args {
			switch arg.Expr.(type) {
			case *plan.Expr_Lit, *plan.Expr_P, *plan.Expr_V, *plan.Expr_T:
				continue
			}
			isConst = false
			isZonemappable := ExprIsZonemappable(ctx, arg)
			if !isZonemappable {
				return false
			}
		}
		if isConst {
			return true
		}

		isZonemappable, _ := function.GetFunctionIsZonemappableById(ctx, exprImpl.F.Func.GetObj())
		if !isZonemappable {
			return false
		}

		return true
	default:
		return true
	}
}

// todo: remove this in the future
func GetSortOrderByName(tableDef *plan.TableDef, colName string) int {
	if tableDef.Pkey != nil {
		if colName == tableDef.Pkey.PkeyColName {
			return 0
		}
		pkNames := tableDef.Pkey.Names
		for i := range pkNames {
			if pkNames[i] == colName {
				return i
			}
		}
	}
	if tableDef.ClusterBy != nil {
		return util.GetClusterByColumnOrder(tableDef.ClusterBy.Name, colName)
	}
	return -1
}

func GetSortOrder(tableDef *plan.TableDef, colPos int32) int {
	colName := tableDef.Cols[colPos].Name
	return GetSortOrderByName(tableDef, colName)
}

func ConstandFoldList(exprs []*plan.Expr, proc *process.Process, varAndParamIsConst bool) ([]*plan.Expr, error) {
	newExprs := DeepCopyExprList(exprs)
	for i := range newExprs {
		foldedExpr, err := ConstantFold(batch.EmptyForConstFoldBatch, newExprs[i], proc, varAndParamIsConst, true)
		if err != nil {
			return nil, err
		}
		if foldedExpr != nil {
			newExprs[i] = foldedExpr
		}
	}
	return newExprs, nil
}

func ConstantFold(bat *batch.Batch, expr *plan.Expr, proc *process.Process, varAndParamIsConst bool, foldInExpr bool) (*plan.Expr, error) {
	if expr.Typ.Id == int32(types.T_interval) {
		panic(moerr.NewInternalError(proc.Ctx, "not supported type INTERVAL"))
	}

	// If it is Expr_List, perform constant folding on its elements
	if elist := expr.GetList(); elist != nil {
		exprList := elist.List
		cannotFold := false
		for i := range exprList {
			foldExpr, err := ConstantFold(bat, exprList[i], proc, varAndParamIsConst, foldInExpr)
			if err != nil {
				return nil, err
			}
			exprList[i] = foldExpr
			if foldExpr.GetLit() == nil {
				cannotFold = true
			}
		}

		if cannotFold || !foldInExpr {
			return expr, nil
		}

		vec, err := colexec.GenerateConstListExpressionExecutor(proc, exprList)
		if err != nil {
			return nil, err
		}
		defer vec.Free(proc.Mp())

		vec.InplaceSortAndCompact()
		data, err := vec.MarshalBinary()
		if err != nil {
			return nil, err
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:  int32(vec.Length()),
					Data: data,
				},
			},
		}, nil
	}

	fn := expr.GetF()
	if fn == nil || proc == nil {
		return expr, nil
	}

	overloadID := fn.Func.GetObj()
	f, err := function.GetFunctionById(proc.Ctx, overloadID)
	if err != nil {
		return nil, err
	}
	if f.CannotFold() {
		return expr, nil
	}
	if f.IsRealTimeRelated() && !varAndParamIsConst {
		return expr, nil
	}
	isVec := false
	for i := range fn.Args {
		foldExpr, errFold := ConstantFold(bat, fn.Args[i], proc, varAndParamIsConst, foldInExpr)
		if errFold != nil {
			return nil, errFold
		}
		fn.Args[i] = foldExpr
		isVec = isVec || foldExpr.GetVec() != nil
	}
	if f.IsAgg() || f.IsWin() {
		return expr, nil
	}
	if !rule.IsConstant(expr, varAndParamIsConst) {
		return expr, nil
	}

	vec, err := colexec.EvalExpressionOnce(proc, expr, []*batch.Batch{bat})
	if err != nil {
		return nil, err
	}
	defer vec.Free(proc.Mp())

	if isVec {
		data, err := vec.MarshalBinary()
		if err != nil {
			return expr, nil
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:  int32(vec.Length()),
					Data: data,
				},
			},
		}, nil
	}

	c := rule.GetConstantValue(vec, false, 0)
	if c == nil {
		return expr, nil
	}
	ec := &plan.Expr_Lit{
		Lit: c,
	}
	expr.Expr = ec
	return expr, nil
}

func unwindTupleComparison(ctx context.Context, nonEqOp, op string, leftExprs, rightExprs []*plan.Expr, idx int) (*plan.Expr, error) {
	if idx == len(leftExprs)-1 {
		return BindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{
			leftExprs[idx],
			rightExprs[idx],
		})
	}

	expr, err := BindFuncExprImplByPlanExpr(ctx, nonEqOp, []*plan.Expr{
		DeepCopyExpr(leftExprs[idx]),
		DeepCopyExpr(rightExprs[idx]),
	})
	if err != nil {
		return nil, err
	}

	eqExpr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		leftExprs[idx],
		rightExprs[idx],
	})
	if err != nil {
		return nil, err
	}

	tailExpr, err := unwindTupleComparison(ctx, nonEqOp, op, leftExprs, rightExprs, idx+1)
	if err != nil {
		return nil, err
	}

	tailExpr, err = BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{eqExpr, tailExpr})
	if err != nil {
		return nil, err
	}

	return BindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{expr, tailExpr})
}

// checkNoNeedCast
// if constant's type higher than column's type
// and constant's value in range of column's type, then no cast was needed
func checkNoNeedCast(constT, columnT types.Type, constExpr *plan.Expr) bool {
	if constExpr.GetP() != nil && columnT.IsNumeric() {
		return true
	}

	lit := constExpr.GetLit()
	if lit == nil {
		return false
	}

	//TODO: Check if T_array is required here?
	if constT.Eq(columnT) {
		return true
	}
	switch constT.Oid {
	case types.T_char, types.T_varchar, types.T_text, types.T_datalink:
		switch columnT.Oid {
		case types.T_char, types.T_varchar:
			return constT.Width <= columnT.Width
		case types.T_text, types.T_datalink:
			return true
		default:
			return false
		}

	case types.T_binary, types.T_varbinary, types.T_blob:
		switch columnT.Oid {
		case types.T_binary, types.T_varbinary:
			if constT.Width <= columnT.Width {
				return true
			} else {
				return false
			}
		case types.T_blob:
			return true
		default:
			return false
		}

	case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
		val, valOk := lit.Value.(*plan.Literal_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val
		switch columnT.Oid {
		case types.T_bit:
			return constVal >= 0 && uint64(constVal) <= uint64(1<<columnT.Width-1)
		case types.T_int8:
			return constVal <= int64(math.MaxInt8) && constVal >= int64(math.MinInt8)
		case types.T_int16:
			return constVal <= int64(math.MaxInt16) && constVal >= int64(math.MinInt16)
		case types.T_int32:
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		case types.T_int64:
			return true
		case types.T_uint8:
			return constVal <= math.MaxUint8 && constVal >= 0
		case types.T_uint16:
			return constVal <= math.MaxUint16 && constVal >= 0
		case types.T_uint32:
			return constVal <= math.MaxUint32 && constVal >= 0
		case types.T_uint64:
			return constVal >= 0
		case types.T_float32:
			//float32 has 6 significant digits.
			return constVal <= 100000 && constVal >= -100000
		case types.T_float64:
			//float64 has 15 significant digits.
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		case types.T_decimal64:
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		default:
			return false
		}

	case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
		val_u, valOk := lit.Value.(*plan.Literal_U64Val)
		if !valOk {
			return false
		}
		constVal := val_u.U64Val
		switch columnT.Oid {
		case types.T_bit:
			return constVal <= uint64(1<<columnT.Width-1)
		case types.T_int8:
			return constVal <= math.MaxInt8
		case types.T_int16:
			return constVal <= math.MaxInt16
		case types.T_int32:
			return constVal <= math.MaxInt32
		case types.T_int64:
			return constVal <= math.MaxInt64
		case types.T_uint8:
			return constVal <= math.MaxUint8
		case types.T_uint16:
			return constVal <= math.MaxUint16
		case types.T_uint32:
			return constVal <= math.MaxUint32
		case types.T_uint64:
			return true
		case types.T_float32:
			//float32 has 6 significant digits.
			return constVal <= 100000
		case types.T_float64:
			//float64 has 15 significant digits.
			return constVal <= math.MaxUint32
		case types.T_decimal64:
			return constVal <= math.MaxInt32
		default:
			return false
		}

	case types.T_decimal64, types.T_decimal128:
		return columnT.Oid == types.T_decimal64 || columnT.Oid == types.T_decimal128

	default:
		return false
	}

}

func InitInfileParam(param *tree.ExternParam) error {
	for i := 0; i < len(param.Option); i += 2 {
		switch strings.ToLower(param.Option[i]) {
		case "filepath":
			param.Filepath = param.Option[i+1]
		case "compression":
			param.CompressType = param.Option[i+1]
		case "format":
			format := strings.ToLower(param.Option[i+1])
			if format != tree.CSV && format != tree.JSONLINE && format != tree.PARQUET {
				return moerr.NewBadConfig(param.Ctx, "the format '%s' is not supported", format)
			}
			param.Format = format
		case "jsondata":
			jsondata := strings.ToLower(param.Option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return moerr.NewBadConfig(param.Ctx, "the jsondata '%s' is not supported", jsondata)
			}
			param.JsonData = jsondata
			param.Format = tree.JSONLINE
		default:
			return moerr.NewBadConfig(param.Ctx, "the keyword '%s' is not support", strings.ToLower(param.Option[i]))
		}
	}
	if len(param.Filepath) == 0 {
		return moerr.NewBadConfig(param.Ctx, "the filepath must be specified")
	}
	if param.Format == tree.JSONLINE && len(param.JsonData) == 0 {
		return moerr.NewBadConfig(param.Ctx, "the jsondata must be specified")
	}
	if len(param.Format) == 0 {
		param.Format = tree.CSV
	}
	return nil
}

func InitS3Param(param *tree.ExternParam) error {
	param.S3Param = &tree.S3Parameter{}
	for i := 0; i < len(param.Option); i += 2 {
		switch strings.ToLower(param.Option[i]) {
		case "endpoint":
			param.S3Param.Endpoint = param.Option[i+1]
		case "region":
			param.S3Param.Region = param.Option[i+1]
		case "access_key_id":
			param.S3Param.APIKey = param.Option[i+1]
		case "secret_access_key":
			param.S3Param.APISecret = param.Option[i+1]
		case "bucket":
			param.S3Param.Bucket = param.Option[i+1]
		case "filepath":
			param.Filepath = param.Option[i+1]
		case "compression":
			param.CompressType = param.Option[i+1]
		case "provider":
			param.S3Param.Provider = param.Option[i+1]
		case "role_arn":
			param.S3Param.RoleArn = param.Option[i+1]
		case "external_id":
			param.S3Param.ExternalId = param.Option[i+1]
		case "format":
			format := strings.ToLower(param.Option[i+1])
			if format != tree.CSV && format != tree.JSONLINE {
				return moerr.NewBadConfig(param.Ctx, "the format '%s' is not supported", format)
			}
			param.Format = format
		case "jsondata":
			jsondata := strings.ToLower(param.Option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return moerr.NewBadConfig(param.Ctx, "the jsondata '%s' is not supported", jsondata)
			}
			param.JsonData = jsondata
			param.Format = tree.JSONLINE

		default:
			return moerr.NewBadConfig(param.Ctx, "the keyword '%s' is not support", strings.ToLower(param.Option[i]))
		}
	}
	if param.Format == tree.JSONLINE && len(param.JsonData) == 0 {
		return moerr.NewBadConfig(param.Ctx, "the jsondata must be specified")
	}
	if len(param.Format) == 0 {
		param.Format = tree.CSV
	}
	return nil
}

func GetForETLWithType(param *tree.ExternParam, prefix string) (res fileservice.ETLFileService, readPath string, err error) {
	if param.ScanType == tree.S3 {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		opts := []string{"s3-opts", "endpoint=" + param.S3Param.Endpoint, "region=" + param.S3Param.Region, "key=" + param.S3Param.APIKey, "secret=" + param.S3Param.APISecret,
			"bucket=" + param.S3Param.Bucket, "role-arn=" + param.S3Param.RoleArn, "external-id=" + param.S3Param.ExternalId}
		if strings.ToLower(param.S3Param.Provider) != "" && strings.ToLower(param.S3Param.Provider) != "minio" {
			return nil, "", moerr.NewBadConfig(param.Ctx, "the provider only support 'minio' now")
		}
		if strings.ToLower(param.S3Param.Provider) == "minio" {
			opts = append(opts, "is-minio=true")
		}
		if err = w.Write(opts); err != nil {
			return nil, "", err
		}
		w.Flush()
		return fileservice.GetForETL(context.TODO(), nil, fileservice.JoinPath(buf.String(), prefix))
	}
	return fileservice.GetForETL(context.TODO(), param.FileService, prefix)
}

func StatFile(param *tree.ExternParam) error {
	filePath := strings.TrimSpace(param.Filepath)
	if strings.HasPrefix(filePath, "etl:") {
		filePath = path.Clean(filePath)
	} else {
		filePath = path.Clean("/" + filePath)
	}
	param.Filepath = filePath
	fs, readPath, err := GetForETLWithType(param, filePath)
	if err != nil {
		return err
	}
	st, err := fs.StatFile(param.Ctx, readPath)
	if err != nil {
		return err
	}
	param.Ctx = nil
	param.FileSize = st.Size
	return nil
}

// ReadDir support "etl:" and "/..." absolute path, NOT support relative path.
func ReadDir(param *tree.ExternParam) (fileList []string, fileSize []int64, err error) {
	filePath := strings.TrimSpace(param.Filepath)
	if strings.HasPrefix(filePath, "etl:") {
		filePath = path.Clean(filePath)
	} else {
		filePath = path.Clean("/" + filePath)
	}

	sep := "/"
	pathDir := strings.Split(filePath, sep)
	l := list.New()
	l2 := list.New()
	if pathDir[0] == "" {
		l.PushBack(sep)
	} else {
		l.PushBack(pathDir[0])
	}

	for i := 1; i < len(pathDir); i++ {
		length := l.Len()
		for j := 0; j < length; j++ {
			prefix := l.Front().Value.(string)
			fs, readPath, err := GetForETLWithType(param, prefix)
			if err != nil {
				return nil, nil, err
			}
			entries, err := fs.List(param.Ctx, readPath)
			if err != nil {
				return nil, nil, err
			}
			for _, entry := range entries {
				if !entry.IsDir && i+1 != len(pathDir) {
					continue
				}
				if entry.IsDir && i+1 == len(pathDir) {
					continue
				}
				matched, err := path.Match(pathDir[i], entry.Name)
				if err != nil {
					return nil, nil, err
				}
				if !matched {
					continue
				}
				l.PushBack(path.Join(l.Front().Value.(string), entry.Name))
				if !entry.IsDir {
					l2.PushBack(entry.Size)
				}
			}
			l.Remove(l.Front())
		}
	}
	length := l.Len()
	for j := 0; j < length; j++ {
		fileList = append(fileList, l.Front().Value.(string))
		l.Remove(l.Front())
		fileSize = append(fileSize, l2.Front().Value.(int64))
		l2.Remove(l2.Front())
	}
	return fileList, fileSize, err
}

// GetUniqueColAndIdxFromTableDef
// if get table:  t1(a int primary key, b int, c int, d int, unique key(b,c));
// return : []map[string]int { {'a'=1},  {'b'=2,'c'=3} }
func GetUniqueColAndIdxFromTableDef(tableDef *TableDef) []map[string]int {
	uniqueCols := make([]map[string]int, 0, len(tableDef.Cols))
	if tableDef.Pkey != nil && !onlyHasHiddenPrimaryKey(tableDef) {
		pkMap := make(map[string]int)
		for _, colName := range tableDef.Pkey.Names {
			pkMap[colName] = int(tableDef.Name2ColIndex[colName])
		}
		uniqueCols = append(uniqueCols, pkMap)
	}

	for _, index := range tableDef.Indexes {
		if index.Unique {
			pkMap := make(map[string]int)
			for _, part := range index.Parts {
				pkMap[part] = int(tableDef.Name2ColIndex[part])
			}
			uniqueCols = append(uniqueCols, pkMap)
		}
	}
	return uniqueCols
}

// GenUniqueColJoinExpr
// if get table:  t1(a int primary key, b int, c int, d int, unique key(b,c));
// uniqueCols is: []map[string]int { {'a'=1},  {'b'=2,'c'=3} }
// we will get expr like: 'leftTag.a = rightTag.a or (leftTag.b = rightTag.b and leftTag.c = rightTag. c)
func GenUniqueColJoinExpr(ctx context.Context, tableDef *TableDef, uniqueCols []map[string]int, leftTag int32, rightTag int32) (*Expr, error) {
	var checkExpr *Expr
	var err error

	for i, uniqueColMap := range uniqueCols {
		var condExpr *Expr
		condIdx := int(0)
		for _, colIdx := range uniqueColMap {
			col := tableDef.Cols[colIdx]
			leftExpr := &Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: leftTag,
						ColPos: int32(colIdx),
					},
				},
			}
			rightExpr := &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: rightTag,
						ColPos: int32(colIdx),
					},
				},
			}
			eqExpr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{leftExpr, rightExpr})
			if err != nil {
				return nil, err
			}
			if condIdx == 0 {
				condExpr = eqExpr
			} else {
				condExpr, err = BindFuncExprImplByPlanExpr(ctx, "and", []*Expr{condExpr, eqExpr})
				if err != nil {
					return nil, err
				}
			}
			condIdx++
		}

		if i == 0 {
			checkExpr = condExpr
		} else {
			checkExpr, err = BindFuncExprImplByPlanExpr(ctx, "or", []*Expr{checkExpr, condExpr})
			if err != nil {
				return nil, err
			}
		}
	}

	return checkExpr, nil
}

// GenUniqueColCheckExpr   like GenUniqueColJoinExpr. but use for on duplicate key clause to check conflict
// if get table:  t1(a int primary key, b int, c int, d int, unique key(b,c));
// we get batch like [1,2,3,4, origin_a, origin_b, origin_c, origin_d, row_id ....]。
// we get expr like:  []*Expr{ 1=origin_a ,  (2 = origin_b and 3 = origin_c) }
func GenUniqueColCheckExpr(ctx context.Context, tableDef *TableDef, uniqueCols []map[string]int, colCount int) ([]*Expr, error) {
	checkExpr := make([]*Expr, len(uniqueCols))

	for i, uniqueColMap := range uniqueCols {
		var condExpr *Expr
		condIdx := int(0)
		for _, colIdx := range uniqueColMap {
			col := tableDef.Cols[colIdx]
			// insert values
			leftExpr := &Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 0,
						ColPos: int32(colIdx),
					},
				},
			}
			rightExpr := &plan.Expr{
				Typ: col.Typ,
				Expr: &plan.Expr_Col{
					Col: &plan.ColRef{
						RelPos: 1,
						ColPos: int32(colIdx + colCount),
					},
				},
			}
			eqExpr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*Expr{leftExpr, rightExpr})
			if err != nil {
				return nil, err
			}
			if condIdx == 0 {
				condExpr = eqExpr
			} else {
				condExpr, err = BindFuncExprImplByPlanExpr(ctx, "and", []*Expr{condExpr, eqExpr})
				if err != nil {
					return nil, err
				}
			}
			condIdx++
		}
		checkExpr[i] = condExpr
	}

	return checkExpr, nil
}
func onlyContainsTag(filter *Expr, tag int32) bool {
	switch ex := filter.Expr.(type) {
	case *plan.Expr_Col:
		return ex.Col.RelPos == tag
	case *plan.Expr_F:
		for _, arg := range ex.F.Args {
			if !onlyContainsTag(arg, tag) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func AssignAuxIdForExpr(expr *plan.Expr, start int32) int32 {
	expr.AuxId = start
	vertexCnt := int32(1)

	if f, ok := expr.Expr.(*plan.Expr_F); ok {
		for _, child := range f.F.Args {
			vertexCnt += AssignAuxIdForExpr(child, start+vertexCnt)
		}
	}

	return vertexCnt
}

func ResetAuxIdForExpr(expr *plan.Expr) {
	expr.AuxId = 0

	if f, ok := expr.Expr.(*plan.Expr_F); ok {
		for _, child := range f.F.Args {
			ResetAuxIdForExpr(child)
		}
	}
}

// func SubstitueParam(expr *plan.Expr, proc *process.Process) *plan.Expr {
// 	switch t := expr.Expr.(type) {
// 	case *plan.Expr_F:
// 		for _, arg := range t.F.Args {
// 			SubstitueParam(arg, proc)
// 		}
// 	case *plan.Expr_P:
// 		vec, _ := proc.GetPrepareParamsAt(int(t.P.Pos))
// 		c := rule.GetConstantValue(vec, false)
// 		ec := &plan.Expr_C{
// 			C: c,
// 		}
// 		expr.Typ = &plan.Type{Id: int32(vec.GetType().Oid), Scale: vec.GetType().Scale, Width: vec.GetType().Width}
// 		expr.Expr = ec
// 	case *plan.Expr_V:
// 		val, _ := proc.GetResolveVariableFunc()(t.V.Name, t.V.System, t.V.Global)
// 		typ := types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)
// 		vec, _ := util.GenVectorByVarValue(proc, typ, val)
// 		c := rule.GetConstantValue(vec, false)
// 		ec := &plan.Expr_C{
// 			C: c,
// 		}
// 		expr.Typ = &plan.Type{Id: int32(vec.GetType().Oid), Scale: vec.GetType().Scale, Width: vec.GetType().Width}
// 		expr.Expr = ec
// 	}
// 	return expr
// }

func FormatExprs(exprs []*plan.Expr) string {
	var w bytes.Buffer
	for _, expr := range exprs {
		w.WriteString(FormatExpr(expr))
		w.WriteByte('\n')
	}
	return w.String()
}

func FormatExpr(expr *plan.Expr) string {
	var w bytes.Buffer
	doFormatExpr(expr, &w, 0)
	return w.String()
}

func doFormatExpr(expr *plan.Expr, out *bytes.Buffer, depth int) {
	out.WriteByte('\n')
	prefix := strings.Repeat("\t", depth)
	switch t := expr.Expr.(type) {
	case *plan.Expr_Col:
		out.WriteString(fmt.Sprintf("%sExpr_Col(%s)", prefix, t.Col.Name))
	case *plan.Expr_Lit:
		out.WriteString(fmt.Sprintf("%sExpr_C(%s)", prefix, t.Lit.String()))
	case *plan.Expr_F:
		out.WriteString(fmt.Sprintf("%sExpr_F(\n%s\tFunc[\"%s\"](nargs=%d)", prefix, prefix, t.F.Func.ObjName, len(t.F.Args)))
		for _, arg := range t.F.Args {
			doFormatExpr(arg, out, depth+1)
		}
		out.WriteString(fmt.Sprintf("\n%s)", prefix))
	case *plan.Expr_P:
		out.WriteString(fmt.Sprintf("%sExpr_P(%d)", prefix, t.P.Pos))
	case *plan.Expr_T:
		out.WriteString(fmt.Sprintf("%sExpr_T(%s)", prefix, t.T.String()))
	case *plan.Expr_Vec:
		out.WriteString(fmt.Sprintf("%sExpr_Vec(len=%d)", prefix, t.Vec.Len))
	default:
		out.WriteString(fmt.Sprintf("%sExpr_Unknown(%s)", prefix, expr.String()))
	}
	out.WriteString(fmt.Sprintf("%sExpr_Selectivity(%v)", prefix, expr.Selectivity))
}

// databaseIsValid checks whether the database exists or not.
func databaseIsValid(dbName string, ctx CompilerContext, snapshot Snapshot) (string, error) {
	connectDBFirst := false
	if len(dbName) == 0 {
		connectDBFirst = true
	}
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}

	if len(dbName) == 0 || !ctx.DatabaseExists(dbName, snapshot) {
		if connectDBFirst {
			return "", moerr.NewNoDB(ctx.GetContext())
		} else {
			return "", moerr.NewBadDB(ctx.GetContext(), dbName)
		}
	}
	return dbName, nil
}

/*
*
getSuitableDBName get the database name which need to be used in next steps.

For Cases:

	SHOW XXX FROM [DB_NAME1].TABLE_NAME [FROM [DB_NAME2]];

	In mysql,
		if the second FROM clause exists, the DB_NAME1 in first FROM clause if it exists will be ignored.
		if the second FROM clause does not exist, the DB_NAME1 in first FROM clause if it exists  will be used.
		if the DB_NAME1 and DB_NAME2 neither does not exist, the current connected database (by USE statement) will be used.
		if neither case above succeeds, an error is reported.
*/
func getSuitableDBName(dbName1 string, dbName2 string) string {
	if len(dbName2) != 0 {
		return dbName2
	}
	return dbName1
}

func detectedExprWhetherTimeRelated(expr *plan.Expr) bool {
	if ef, ok := expr.Expr.(*plan.Expr_F); !ok {
		return false
	} else {
		overloadID := ef.F.Func.GetObj()
		f, exists := function.GetFunctionByIdWithoutError(overloadID)
		// current_timestamp()
		if !exists {
			return false
		}
		if f.IsRealTimeRelated() {
			return true
		}

		// current_timestamp() + 1
		for _, arg := range ef.F.Args {
			if detectedExprWhetherTimeRelated(arg) {
				return true
			}
		}
	}
	return false
}

func ResetPreparePlan(ctx CompilerContext, preparePlan *Plan) ([]*plan.ObjectRef, []int32, error) {
	// dcl tcl is not support
	var schemas []*plan.ObjectRef
	var paramTypes []int32

	switch pp := preparePlan.Plan.(type) {
	case *plan.Plan_Tcl:
		return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare TCL and DCL statement")
	case *plan.Plan_Dcl:
		switch pp.Dcl.GetDclType() {
		case plan.DataControl_CREATE_ACCOUNT,
			plan.DataControl_ALTER_ACCOUNT,
			plan.DataControl_DROP_ACCOUNT:
			return nil, pp.Dcl.GetOther().GetParamTypes(), nil
		default:
			return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare TCL and DCL statement")
		}
	case *plan.Plan_Ddl:
		if pp.Ddl.Query != nil {
			getParamRule := NewGetParamRule()
			VisitQuery := NewVisitPlan(preparePlan, []VisitPlanRule{getParamRule})
			err := VisitQuery.Visit(ctx.GetContext())
			if err != nil {
				return nil, nil, err
			}
			// TODO : need confirm
			if len(getParamRule.params) > 0 {
				return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot plan DDL statement")
			}
		}

	case *plan.Plan_Query:
		// collect args
		getParamRule := NewGetParamRule()
		VisitQuery := NewVisitPlan(preparePlan, []VisitPlanRule{getParamRule})
		err := VisitQuery.Visit(ctx.GetContext())
		if err != nil {
			return nil, nil, err
		}

		// sort arg
		getParamRule.SetParamOrder()
		args := getParamRule.params
		schemas = getParamRule.schemas
		paramTypes = getParamRule.paramTypes

		// reset arg order
		resetParamRule := NewResetParamOrderRule(args)
		VisitQuery = NewVisitPlan(preparePlan, []VisitPlanRule{resetParamRule})
		err = VisitQuery.Visit(ctx.GetContext())
		if err != nil {
			return nil, nil, err
		}
	}
	return schemas, paramTypes, nil
}

func getParamTypes(params []tree.Expr, ctx CompilerContext, isPrepareStmt bool) ([]int32, error) {
	paramTypes := make([]int32, 0, len(params))
	for _, p := range params {
		switch ast := p.(type) {
		case *tree.NumVal:
			if ast.ValType != tree.P_char {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "unsupport value '%s'", ast.String())
			}
		case *tree.ParamExpr:
			if !isPrepareStmt {
				return nil, moerr.NewInvalidInput(ctx.GetContext(), "only prepare statement can use ? expr")
			}
			paramTypes = append(paramTypes, int32(types.T_varchar))
			if ast.Offset != len(paramTypes) {
				return nil, moerr.NewInternalError(ctx.GetContext(), "offset not match")
			}
		default:
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "unsupport value '%s'", ast.String())
		}
	}
	return paramTypes, nil
}

// HasMoCtrl checks whether the expression has mo_ctrl(..,..,..)
func HasMoCtrl(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "mo_ctl" {
			return true
		}
		for _, arg := range exprImpl.F.Args {
			if HasMoCtrl(arg) {
				return true
			}
		}
		return false

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if HasMoCtrl(arg) {
				return true
			}
		}
		return false

	default:
		return false
	}
}

// IsFkSelfRefer checks the foreign key referencing itself
func IsFkSelfRefer(fkDbName, fkTableName, curDbName, curTableName string) bool {
	return fkDbName == curDbName && fkTableName == curTableName
}

// HasFkSelfReferOnly checks the foreign key referencing itself only.
// If there is no children tables, it also returns true
// the tbleId 0 is special. it always denotes the table itself.
func HasFkSelfReferOnly(tableDef *TableDef) bool {
	for _, tbl := range tableDef.RefChildTbls {
		if tbl != 0 {
			return false
		}
	}
	return true
}

func IsFalseExpr(e *Expr) bool {
	if e == nil || e.GetTyp().Id != int32(types.T_bool) || e.GetLit() == nil {
		return false
	}
	if x, ok := e.GetLit().GetValue().(*plan.Literal_Bval); ok {
		return !x.Bval
	}
	return false
}
func MakeFalseExpr() *Expr {
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_bool),
		},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Isnull: false,
				Value:  &plan.Literal_Bval{Bval: false},
			},
		},
	}
}

func MakeRuntimeFilter(tag int32, matchPrefix bool, upperlimit int32, expr *Expr) *plan.RuntimeFilterSpec {
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperlimit,
		Expr:        expr,
		MatchPrefix: matchPrefix,
	}
}

func MakeIntervalExpr(num int64, str string) *Expr {
	arg0 := makePlan2Int64ConstExprWithType(num)
	arg1 := makePlan2StringConstExprWithType(str, false)
	return &plan.Expr{
		Typ: plan.Type{
			Id: int32(types.T_interval),
		},
		Expr: &plan.Expr_List{
			List: &plan.ExprList{
				List: []*Expr{arg0, arg1},
			},
		},
	}
}

func MakeInExpr(ctx context.Context, left *Expr, length int32, data []byte, matchPrefix bool) *Expr {
	rightArg := &plan.Expr{
		Typ: left.Typ,
		Expr: &plan.Expr_Vec{
			Vec: &plan.LiteralVec{
				Len:  length,
				Data: data,
			},
		},
	}

	funcID := function.InFunctionEncodedID
	funcName := function.InFunctionName
	if matchPrefix {
		funcID = function.PrefixInFunctionEncodedID
		funcName = function.PrefixInFunctionName
	}
	args := []types.Type{makeTypeByPlan2Expr(left), makeTypeByPlan2Expr(rightArg)}
	fGet, err := function.GetFunctionByName(ctx, funcName, args)
	if err == nil {
		funcID = fGet.GetEncodedOverloadID()
	}
	inExpr := &plan.Expr{
		Typ: plan.Type{
			Id:          int32(types.T_bool),
			NotNullable: left.Typ.NotNullable,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     funcID,
					ObjName: funcName,
				},
				Args: []*plan.Expr{
					left,
					rightArg,
				},
			},
		},
	}
	return inExpr
}

// FillValuesOfParamsInPlan replaces the params by their values
func FillValuesOfParamsInPlan(ctx context.Context, preparePlan *Plan, paramVals []any) (*Plan, error) {
	copied := preparePlan

	switch pp := copied.Plan.(type) {
	case *plan.Plan_Tcl, *plan.Plan_Dcl:
		return nil, moerr.NewInvalidInput(ctx, "cannot prepare TCL and DCL statement")

	case *plan.Plan_Ddl:
		if pp.Ddl.Query != nil {
			err := replaceParamVals(ctx, preparePlan, paramVals)
			if err != nil {
				return nil, err
			}
		}

	case *plan.Plan_Query:
		err := replaceParamVals(ctx, preparePlan, paramVals)
		if err != nil {
			return nil, err
		}
	}
	return copied, nil
}

func replaceParamVals(ctx context.Context, plan0 *Plan, paramVals []any) error {
	params := make([]*Expr, len(paramVals))
	for i, val := range paramVals {
		pc := &plan.Literal{}
		pc.Value = &plan.Literal_Sval{Sval: fmt.Sprintf("%v", val)}
		params[i] = &plan.Expr{
			Expr: &plan.Expr_Lit{
				Lit: pc,
			},
		}
	}
	paramRule := NewResetParamRefRule(ctx, params)
	VisitQuery := NewVisitPlan(plan0, []VisitPlanRule{paramRule})
	err := VisitQuery.Visit(ctx)
	if err != nil {
		return err
	}
	return nil
}

// XXX: Any code relying on Name in ColRef, except for "explain", is bad design and practically buggy.
func (builder *QueryBuilder) addNameByColRef(tag int32, tableDef *plan.TableDef) {
	for i, col := range tableDef.Cols {
		builder.nameByColRef[[2]int32{tag, int32(i)}] = tableDef.Name + "." + col.Name
	}
}

func GetRowSizeFromTableDef(tableDef *TableDef, ignoreHiddenKey bool) float64 {
	size := int32(0)
	for _, col := range tableDef.Cols {
		if col.Hidden && ignoreHiddenKey {
			continue
		}
		if col.Typ.Width > 0 {
			size += col.Typ.Width
			continue
		}
		typ := types.T(col.Typ.Id).ToType()
		if typ.Width > 0 {
			size += typ.Width
		} else {
			size += typ.Size
		}
	}
	return float64(size)
}

type UnorderedSet[T ~string | ~int] map[T]int

func (set UnorderedSet[T]) Insert(val T) {
	set[val] = 0
}

func (set UnorderedSet[T]) Find(val T) bool {
	if _, ok := set[val]; ok {
		return ok
	}
	return false
}

// RemoveIf removes the elements that pred is true.
func RemoveIf[T any](data []T, pred func(t T) bool) []T {
	if len(data) == 0 {
		return data
	}
	res := 0
	for i := 0; i < len(data); i++ {
		if !pred(data[i]) {
			if res != i {
				data[res] = data[i]
			}
			res++
		}
	}
	return data[:res]
}

func Find[T ~string | ~int, S any](data map[T]S, val T) bool {
	if len(data) == 0 {
		return false
	}
	if _, exists := data[val]; exists {
		return true
	}
	return false
}
