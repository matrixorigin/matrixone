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

func doGetBindings(expr *plan.Expr) map[int32]any {
	res := make(map[int32]any)

	switch expr := expr.Expr.(type) {
	case *plan.Expr_Col:
		res[expr.Col.RelPos] = nil

	case *plan.Expr_F:
		for _, child := range expr.F.Args {
			for id := range doGetBindings(child) {
				res[id] = nil
			}
		}
	}

	return res
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

func hasTag(expr *plan.Expr, tag int32) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == tag

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if hasTag(arg, tag) {
				return true
			}
		}
		return false

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if hasTag(arg, tag) {
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

func getJoinSide(expr *plan.Expr, leftTags, rightTags map[int32]any, markTag int32) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSide(arg, leftTags, rightTags, markTag)
		}

	case *plan.Expr_Col:
		if _, ok := leftTags[exprImpl.Col.RelPos]; ok {
			side = JoinSideLeft
		} else if _, ok := rightTags[exprImpl.Col.RelPos]; ok {
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

func splitAndBindCondition(astExpr tree.Expr, ctx *BindContext) ([]*plan.Expr, error) {
	conds := splitAstConjunction(astExpr)
	exprs := make([]*plan.Expr, len(conds))

	for i, cond := range conds {
		cond, err := ctx.qualifyColumnNames(cond, nil, false)
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
			expr, err = makePlan2CastExpr(ctx.binder.GetContext(), expr, &plan.Type{Id: int32(types.T_bool)})
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

		leftExpr, _ = bindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{leftExpr, rightExpr})

		expr, _ = bindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{expr, leftExpr})
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
				retExpr, _ = bindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{left, right})
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
				retExpr, _ = bindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{left, right})
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
			ret, col := CheckFilter(filter)
			if ret && col != nil {
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
	b1, col1, _, funcName1 := CheckStrictFilter(expr1)
	b2, col2, _, funcName2 := CheckStrictFilter(expr2)
	if !b1 || !b2 {
		return false
	}
	if col1.ColPos != col2.ColPos || col1.RelPos != col2.RelPos {
		return false
	}
	if funcName1 == ">" || funcName1 == ">=" {
		return funcName2 == "<" || funcName2 == "<="
	}
	if funcName1 == "<" || funcName1 == "<=" {
		return funcName2 == ">" || funcName2 == ">="
	}
	return false
}

// function filter means func(col) compared to const. for example year(col1)>1991
func CheckFunctionFilter(expr *plan.Expr) (b bool, col *ColRef, constExpr *Const, childFuncName string) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName := exprImpl.F.Func.ObjName
		switch funcName {
		case "=", ">", "<", ">=", "<=":
			switch child0 := exprImpl.F.Args[0].Expr.(type) {
			case *plan.Expr_F:
				childFuncName = child0.F.Func.ObjName
				switch childFuncName {
				case "year":
					switch child := child0.F.Args[0].Expr.(type) {
					case *plan.Expr_Col:
						col = child.Col
					}
				}
			default:
				return false, nil, nil, childFuncName
			}
			switch child1 := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_C:
				constExpr = child1.C
				b = true
				return
			default:
				return false, nil, nil, childFuncName
			}
		default:
			return false, nil, nil, childFuncName
		}
	default:
		return false, nil, nil, childFuncName
	}
}

// strict filter means col compared to const. for example col1>1
// func(col1)=1 is not strict
func CheckStrictFilter(expr *plan.Expr) (b bool, col *ColRef, constExpr *Const, funcName string) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		funcName = exprImpl.F.Func.ObjName
		switch funcName {
		case "=", ">", "<", ">=", "<=":
			switch child0 := exprImpl.F.Args[0].Expr.(type) {
			case *plan.Expr_Col:
				col = child0.Col
			default:
				return false, nil, nil, funcName
			}
			switch child1 := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_C:
				constExpr = child1.C
				b = true
				return
			default:
				return false, nil, nil, funcName
			}
		default:
			return false, nil, nil, funcName
		}
	default:
		return false, nil, nil, funcName
	}
}

// for predicate deduction, filter must be like func(col)>1 , or (col=1) or (col=2)
// and only 1 colRef is allowd in the filter
func CheckFilter(expr *plan.Expr) (bool, *ColRef) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		switch exprImpl.F.Func.ObjName {
		case "=", ">", "<", ">=", "<=":
			switch e := exprImpl.F.Args[1].Expr.(type) {
			case *plan.Expr_C, *plan.Expr_P, *plan.Expr_V:
				return CheckFilter(exprImpl.F.Args[0])
			case *plan.Expr_F:
				if e.F.Func.ObjName == "cast" {
					return CheckFilter(exprImpl.F.Args[0])
				}
				return false, nil
			default:
				return false, nil
			}
		default:
			var col *ColRef
			for _, arg := range exprImpl.F.Args {
				ret, c := CheckFilter(arg)
				if !ret {
					return false, nil
				} else if c != nil {
					if col != nil {
						if col.RelPos != c.RelPos || col.ColPos != c.ColPos {
							return false, nil
						}
					} else {
						col = c
					}
				}
			}
			return col != nil, col
		}
	case *plan.Expr_Col:
		return true, exprImpl.Col
	}
	return false, nil
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
		expr, err = bindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{expr, exprs[i]})

		if err != nil {
			break
		}
	}

	return
}

func rejectsNull(filter *plan.Expr, proc *process.Process) bool {
	filter = replaceColRefWithNull(DeepCopyExpr(filter))

	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	filter, err := ConstantFold(bat, filter, proc)
	if err != nil {
		return false
	}

	if f, ok := filter.Expr.(*plan.Expr_C); ok {
		if f.C.Isnull {
			return true
		}

		if fbool, ok := f.C.Value.(*plan.Const_Bval); ok {
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
			Expr: &plan.Expr_C{
				C: &plan.Const{
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
		for _, arg := range exprImpl.W.PartitionBy {
			increaseRefCnt(arg, inc, colRefCnt)
		}
		for _, order := range exprImpl.W.OrderBy {
			increaseRefCnt(order.Expr, inc, colRefCnt)
		}
	}
}

func getHyperEdgeFromExpr(expr *plan.Expr, leafByTag map[int32]int32, hyperEdge map[int32]any) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		hyperEdge[leafByTag[exprImpl.Col.RelPos]] = nil

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

	// right is not UNION allways
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

func GetColumnMapByExpr(expr *plan.Expr, tableDef *plan.TableDef, columnMap *map[int]int) {
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
		(*columnMap)[int(idx)] = seqnum
	}
}

func GetColumnMapByExprs(exprs []*plan.Expr, tableDef *plan.TableDef, columnMap *map[int]int) {
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
	GetColumnMapByExpr(expr, tableDef, &columnMap)

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
		e, err := ConstantFold(bat, expr, proc)
		if err != nil {
			return false, err
		}

		if cExpr, ok := e.Expr.(*plan.Expr_C); ok {
			if bVal, bOk := cExpr.C.Value.(*plan.Const_Bval); bOk {
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

		vec, err := executor.Eval(proc, []*batch.Batch{bat})
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

func CheckExprIsMonotonic(ctx context.Context, expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		isConst := true
		for _, arg := range exprImpl.F.Args {
			switch arg.Expr.(type) {
			case *plan.Expr_C, *plan.Expr_P, *plan.Expr_V, *plan.Expr_T:
				continue
			}
			isConst = false
			isMonotonic := CheckExprIsMonotonic(ctx, arg)
			if !isMonotonic {
				return false
			}
		}
		if isConst {
			return true
		}

		isMonotonic, _ := function.GetFunctionIsMonotonicById(ctx, exprImpl.F.Func.GetObj())
		if !isMonotonic {
			return false
		}

		return true
	default:
		return true
	}
}

func GetSortOrder(tableDef *plan.TableDef, colName string) int {
	if tableDef.Pkey != nil {
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

// handle the filter list for Stats. rewrite and constFold
func rewriteFiltersForStats(exprList []*plan.Expr, proc *process.Process) *plan.Expr {
	if proc == nil {
		return nil
	}
	return colexec.RewriteFilterExprList(exprList)
}

func fixColumnName(tableDef *plan.TableDef, expr *plan.Expr) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			fixColumnName(tableDef, arg)
		}
	case *plan.Expr_Col:
		exprImpl.Col.Name = tableDef.Cols[exprImpl.Col.ColPos].Name
	}
}

func ConstantFold(bat *batch.Batch, e *plan.Expr, proc *process.Process) (*plan.Expr, error) {
	// If it is Expr_List, perform constant folding on its elements
	if exprImpl, ok := e.Expr.(*plan.Expr_List); ok {
		exprList := exprImpl.List
		for i, exprElem := range exprList.List {
			_, ok2 := exprElem.Expr.(*plan.Expr_F)
			if ok2 {
				foldExpr, err := ConstantFold(bat, exprElem, proc)
				if err != nil {
					return e, nil
				}
				exprImpl.List.List[i] = foldExpr
			}
		}
		return e, nil
	}

	var err error
	if elist, ok := e.Expr.(*plan.Expr_List); ok {
		for i, expr := range elist.List.List {
			if elist.List.List[i], err = ConstantFold(bat, expr, proc); err != nil {
				return nil, err
			}
		}
		return e, nil
	}

	ef, ok := e.Expr.(*plan.Expr_F)
	if !ok || proc == nil {
		return e, nil
	}

	overloadID := ef.F.Func.GetObj()
	f, err := function.GetFunctionById(proc.Ctx, overloadID)
	if err != nil {
		return nil, err
	}
	if f.CannotFold() { // function cannot be fold
		return e, nil
	}
	for i := range ef.F.Args {
		if ef.F.Args[i], err = ConstantFold(bat, ef.F.Args[i], proc); err != nil {
			return nil, err
		}
	}
	if !rule.IsConstant(e) {
		return e, nil
	}

	vec, err := colexec.EvalExpressionOnce(proc, e, []*batch.Batch{bat})
	if err != nil {
		return nil, err
	}
	defer vec.Free(proc.Mp())
	c := rule.GetConstantValue(vec, false)
	if c == nil {
		return e, nil
	}
	ec := &plan.Expr_C{
		C: c,
	}
	e.Expr = ec
	return e, nil
}

func unwindTupleComparison(ctx context.Context, nonEqOp, op string, leftExprs, rightExprs []*plan.Expr, idx int) (*plan.Expr, error) {
	if idx == len(leftExprs)-1 {
		return bindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{
			leftExprs[idx],
			rightExprs[idx],
		})
	}

	expr, err := bindFuncExprImplByPlanExpr(ctx, nonEqOp, []*plan.Expr{
		DeepCopyExpr(leftExprs[idx]),
		DeepCopyExpr(rightExprs[idx]),
	})
	if err != nil {
		return nil, err
	}

	eqExpr, err := bindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
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

	tailExpr, err = bindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{eqExpr, tailExpr})
	if err != nil {
		return nil, err
	}

	return bindFuncExprImplByPlanExpr(ctx, "or", []*plan.Expr{expr, tailExpr})
}

// checkNoNeedCast
// if constant's type higher than column's type
// and constant's value in range of column's type, then no cast was needed
func checkNoNeedCast(constT, columnT types.Type, constExpr *plan.Expr_C) bool {
	switch constT.Oid {
	case types.T_char, types.T_varchar, types.T_text:
		switch columnT.Oid {
		case types.T_char, types.T_varchar:
			if constT.Width <= columnT.Width {
				return true
			} else {
				return false
			}
		case types.T_text:
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
		val, valOk := constExpr.C.Value.(*plan.Const_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val
		switch columnT.Oid {
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
		case types.T_varchar:
			return true
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
		val_u, valOk := constExpr.C.Value.(*plan.Const_U64Val)
		if !valOk {
			return false
		}
		constVal := val_u.U64Val
		switch columnT.Oid {
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
		return fileservice.GetForETL(nil, fileservice.JoinPath(buf.String(), prefix))
	}
	return fileservice.GetForETL(param.FileService, prefix)
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
			eqExpr, err := bindFuncExprImplByPlanExpr(ctx, "=", []*Expr{leftExpr, rightExpr})
			if err != nil {
				return nil, err
			}
			if condIdx == 0 {
				condExpr = eqExpr
			} else {
				condExpr, err = bindFuncExprImplByPlanExpr(ctx, "and", []*Expr{condExpr, eqExpr})
				if err != nil {
					return nil, err
				}
			}
			condIdx++
		}

		if i == 0 {
			checkExpr = condExpr
		} else {
			checkExpr, err = bindFuncExprImplByPlanExpr(ctx, "or", []*Expr{checkExpr, condExpr})
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
			eqExpr, err := bindFuncExprImplByPlanExpr(ctx, "=", []*Expr{leftExpr, rightExpr})
			if err != nil {
				return nil, err
			}
			if condIdx == 0 {
				condExpr = eqExpr
			} else {
				condExpr, err = bindFuncExprImplByPlanExpr(ctx, "and", []*Expr{condExpr, eqExpr})
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

func SubstitueParam(expr *plan.Expr, proc *process.Process) *plan.Expr {
	switch t := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range t.F.Args {
			SubstitueParam(arg, proc)
		}
	case *plan.Expr_P:
		vec, _ := proc.GetPrepareParamsAt(int(t.P.Pos))
		c := rule.GetConstantValue(vec, false)
		ec := &plan.Expr_C{
			C: c,
		}
		expr.Typ = &plan.Type{Id: int32(vec.GetType().Oid), Scale: vec.GetType().Scale, Width: vec.GetType().Width}
		expr.Expr = ec
	case *plan.Expr_V:
		val, _ := proc.GetResolveVariableFunc()(t.V.Name, t.V.System, t.V.Global)
		typ := types.New(types.T(expr.Typ.Id), expr.Typ.Width, expr.Typ.Scale)
		vec, _ := util.GenVectorByVarValue(proc, typ, val)
		c := rule.GetConstantValue(vec, false)
		ec := &plan.Expr_C{
			C: c,
		}
		expr.Typ = &plan.Type{Id: int32(vec.GetType().Oid), Scale: vec.GetType().Scale, Width: vec.GetType().Width}
		expr.Expr = ec
	}
	return expr
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
	case *plan.Expr_C:
		out.WriteString(fmt.Sprintf("%sExpr_C(%s)", prefix, t.C.String()))
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
	default:
		out.WriteString(fmt.Sprintf("%sExpr_Unknown(%s)", prefix, expr.String()))
	}
}
