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
	"slices"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/stage"
	"github.com/matrixorigin/matrixone/pkg/stage/stageutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"go.uber.org/zap"
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
		needCast := true
		fn := expr.GetF()
		if fn != nil {
			needCast = fn.Func.ObjName != "fulltext_match"
		}
		// expr must be bool type, if not, try to do type convert
		// but just ignore the subQuery. It will be solved at optimizer.
		if expr.GetSub() == nil && needCast {
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

		relPos := int32(-1)
		for _, cond := range rightConds {
			condMap[cond.ExprString()] = JoinSideRight
			args := cond.GetF().GetArgs()
			if len(args) != 2 {
				continue
			}
			if col := args[0].GetCol(); col != nil {
				if relPos == -1 {
					relPos = col.RelPos
				} else if relPos != col.RelPos {
					relPos = -2
				}
			}
		}
		if relPos >= 0 {
			return expr
		}

		var commonConds, leftOnlyConds, rightOnlyConds []*plan.Expr

		for _, cond := range leftConds {
			exprStr := cond.ExprString()

			if condMap[exprStr] == JoinSideRight {
				commonConds = append(commonConds, cond)
				condMap[exprStr] = JoinSideBoth
			} else {
				leftOnlyConds = append(leftOnlyConds, cond)
				condMap[exprStr] = JoinSideLeft
			}
		}

		for _, cond := range rightConds {
			if condMap[cond.ExprString()] == JoinSideRight {
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

	case *plan.Expr_Col:
		ret = append(ret, exprImpl.Col.ColRefString())
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

	case *plan.Expr_Col:
		if exprImpl.Col.ColRefString() == keywords {
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

// extractColRefInFilter extracts a unique column reference from an expression.
// Used for predicate deduction, where filters must contain only one column reference.
//
// This function implements unified logic for extracting column references:
//   - For column expressions: returns the column reference directly
//   - For function expressions:
//   - The first argument MUST contain a column reference (otherwise returns nil)
//   - All other arguments must satisfy one of the following:
//     1. Not contain any column references (i.e., literals/constants), OR
//     2. Contain the same column reference as the first argument
//
// This unified approach works for all function types:
//   - Comparison operators (=, >, <, >=, <=, between, in, etc.):
//   - col = 1 → returns col (literal is allowed)
//   - col = trim(col) → returns col (same column in function is allowed)
//   - col = col2 → returns nil (different column is rejected)
//   - func(col) > 2 → returns col (nested function calls are supported recursively)
//   - Logical operators (and, or, etc.):
//   - and(col, col) → returns col (same column in all args)
//   - and(col, col2) → returns nil (different columns are rejected)
//   - and(col, 1) → returns col (literal is allowed, though may be semantically invalid)
//   - Cast functions:
//   - cast(col, type) → returns col (type argument is literal)
//
// Returns the column reference if the expression contains exactly one unique column reference,
// nil otherwise.
func extractColRefInFilter(expr *plan.Expr) *ColRef {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		return exprImpl.Col
	case *plan.Expr_F:
		args := exprImpl.F.Args
		if len(args) == 0 {
			return nil
		}

		// Extract column reference from the first argument
		col := extractColRefInFilter(args[0])
		if col == nil {
			return nil
		}

		// Verify all remaining arguments either:
		// 1. Don't contain any column references (literals/constants), OR
		// 2. Contain the same column reference as the first argument
		for i := 1; i < len(args); i++ {
			otherCol := extractColRefInFilter(args[i])
			if otherCol != nil {
				// If this argument has a column reference, it must match the first argument's column
				if col.RelPos != otherCol.RelPos || col.ColPos != otherCol.ColPos {
					return nil
				}
			}
			// If otherCol is nil, the argument is a literal/constant (no column reference), which is acceptable
		}

		return col
	}
	return nil
}

// for col1=col2 and col3 = col4, trying to deduce new pred
// for example , if col1 and col3 are the same, then we can deduce that col2=col4
func deduceTranstivity(expr *plan.Expr, col1, col2, col3, col4 *ColRef) (bool, *plan.Expr) {
	if col1.ColRefString() == col3.ColRefString() ||
		col1.ColRefString() == col4.ColRefString() ||
		col2.ColRefString() == col3.ColRefString() ||
		col2.ColRefString() == col4.ColRefString() {
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
		colName := exprImpl.Col.ColRefString()
		if colName == onPredCol1.ColRefString() {
			exprImpl.Col.RelPos = onPredCol2.RelPos
			exprImpl.Col.ColPos = onPredCol2.ColPos
			exprImpl.Col.Name = onPredCol2.Name
			return true
		} else if colName == onPredCol2.ColRefString() {
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
	if filter.GetF() != nil && filter.GetF().Func.ObjName == "in" && filter.GetF().Args[0].GetCol() != nil {
		return true // in is always null rejecting
	}

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
		return moerr.NewParseErrorf(ctx, "unexpected statement in union: '%v'", tree.String(leftStmt, dialect.MYSQL))
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
		return moerr.NewParseErrorf(ctx, "unexpected statement in union2: '%v'", tree.String(rightStmt, dialect.MYSQL))
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
		cols := vector.MustFixedColWithTypeCheck[bool](vec)
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
			if isRuntimeConstExpr(arg) {
				continue
			} else {
				isConst = false
			}
			isZonemappable := ExprIsZonemappable(ctx, arg)
			if !isZonemappable {
				return false
			}
		}
		if isConst {
			return true
		}

		if exprImpl.F.Func.ObjName == "cast" {
			switch exprImpl.F.Args[0].Typ.Id {
			case int32(types.T_date), int32(types.T_time), int32(types.T_datetime), int32(types.T_timestamp), int32(types.T_year):
				if exprImpl.F.Args[1].Typ.Id == int32(types.T_timestamp) {
					//this cast is monotonic, can safely pushdown to block filters
					return true
				}
			}
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

func GetSortOrderByName(tableDef *plan.TableDef, colName string) int {
	if tableDef.ClusterBy != nil {
		return util.GetClusterByColumnOrder(tableDef.ClusterBy.Name, colName)
	}

	if tableDef.Pkey == nil {
		// view has no pk
		logutil.Warn("GetSortOrderByName table has no PK",
			zap.String("dbName", tableDef.DbName),
			zap.String("tableName", tableDef.Name),
			zap.String("relKind", tableDef.TableType))
		return -1
	}

	if catalog.IsFakePkName(tableDef.Pkey.PkeyColName) {
		return -1
	}

	if colName == tableDef.Pkey.PkeyColName {
		return 0
	}
	pkNames := tableDef.Pkey.Names
	for i := range pkNames {
		if pkNames[i] == colName {
			return i
		}
	}
	return -1
}

func GetSortOrder(tableDef *plan.TableDef, colPos int32) int {
	colName := tableDef.Cols[colPos].Name
	return GetSortOrderByName(tableDef, colName)
}

func checkOp(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetCol() != nil || expr.GetLit() != nil {
		return true
	}

	fn := expr.GetF()
	if fn == nil {
		return false
	}

	switch fn.Func.ObjName {
	case "+", "-":
		for _, childExpr := range fn.Args {
			if !checkOp(childExpr) {
				return false
			}
		}
	default:
		return false
	}

	return true
}

func getColRefCnt(expr *plan.Expr) int {
	if expr == nil {
		return 0
	}

	if colRef := expr.GetCol(); colRef != nil {
		return 1
	}

	if fn := expr.GetF(); fn != nil {
		cnt := 0
		for _, arg := range fn.Args {
			cnt += getColRefCnt(arg)
		}
		return cnt
	}

	return 0
}

func canTranspose(expr *plan.Expr) (can bool, leftCnt int, rightCnt int) {
	fn := expr.GetF()
	if fn == nil {
		return false, 0, 0
	}

	switch fn.Func.ObjName {
	case "=":
		if len(fn.Args) != 2 {
			return false, 0, 0
		}

		left, right := fn.Args[0], fn.Args[1]

		if !checkOp(left) || !checkOp(right) {
			return false, 0, 0
		}

		leftCnt = getColRefCnt(left)
		rightCnt = getColRefCnt(right)
		if !((leftCnt == 1 && rightCnt == 0) || (leftCnt == 0 && rightCnt == 1)) {
			return false, 0, 0
		}

	default:
		return false, 0, 0
	}

	return true, leftCnt, rightCnt
}

func getPath(expr *plan.Expr) []int {
	if expr == nil {
		return nil
	}

	if expr.GetCol() != nil {
		return []int{}
	}

	fn := expr.GetF()
	if fn == nil {
		return nil
	}

	if colPath := getPath(fn.Args[0]); colPath != nil {
		return append([]int{0}, colPath...)
	}

	if colPath := getPath(fn.Args[1]); colPath != nil {
		return append([]int{1}, colPath...)
	}

	return nil
}

func ConstantTranspose(expr *plan.Expr, proc *process.Process) (*plan.Expr, error) {
	can, leftCnt, rightCnt := canTranspose(expr)
	if !can {
		return expr, nil
	}

	if leftCnt == 0 && rightCnt == 1 {
		fn := expr.GetF()
		left, right := fn.Args[0], fn.Args[1]
		exchangedExpr, err := BindFuncExprImplByPlanExpr(proc.Ctx, fn.Func.ObjName, []*plan.Expr{right, left})
		if err != nil {
			return nil, err
		}
		expr = exchangedExpr
	}

	fn := expr.GetF()
	curLeft, curRight := fn.Args[0], fn.Args[1]

	colPath := getPath(curLeft)
	if colPath == nil {
		return expr, nil
	}

	for _, direction := range colPath {
		f := curLeft.GetF()
		if f == nil {
			break
		}

		var colSide, constSide *plan.Expr
		if direction == 0 {
			colSide = f.Args[0]
			constSide = f.Args[1]
		} else {
			colSide = f.Args[1]
			constSide = f.Args[0]
		}

		switch f.Func.ObjName {
		case "+":
			newRight, err := BindFuncExprImplByPlanExpr(proc.Ctx, "-", []*plan.Expr{curRight, constSide})
			if err != nil {
				return nil, err
			}
			curLeft = colSide
			curRight = newRight

		case "-":
			if direction == 0 {
				// col - const = right    →    col = right + const
				newRight, err := BindFuncExprImplByPlanExpr(proc.Ctx, "+", []*plan.Expr{curRight, constSide})
				if err != nil {
					return nil, err
				}
				curLeft = colSide
				curRight = newRight
			} else {
				// const - col = right    →    col = const - right
				newRight, err := BindFuncExprImplByPlanExpr(proc.Ctx, "-", []*plan.Expr{constSide, curRight})
				if err != nil {
					return nil, err
				}
				curLeft = colSide
				curRight = newRight
			}
		}
	}
	newExpr, err := BindFuncExprImplByPlanExpr(proc.Ctx, fn.Func.ObjName, []*plan.Expr{curLeft, curRight})
	if err != nil {
		return nil, err
	}

	return newExpr, nil
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

	// Skip constant folding for division/modulo by zero.
	// This allows runtime to check sql_mode and statement type for proper error handling.
	if rule.IsDivisionByZeroConstant(fn) {
		return expr, nil
	}

	vec, free, err := colexec.GetReadonlyResultFromExpression(proc, expr, []*batch.Batch{bat})
	if err != nil {
		return nil, err
	}
	defer free()

	if isVec {
		data, err := vec.MarshalBinary()
		if err != nil {
			return expr, nil
		}

		return &plan.Expr{
			Typ: plan.Type{Id: int32(vec.GetType().Oid), Scale: vec.GetType().Scale, Width: vec.GetType().Width},
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
// hasTrailingZeros checks if a decimal constant has trailing zeros that can be safely truncated
// to match the column's scale, allowing index usage
func hasTrailingZeros(constExpr *plan.Expr, constT types.Type, columnScale int32) bool {
	if constT.Scale <= columnScale {
		return false
	}

	// Try to get the literal value
	// If constExpr is a Cast function, try to extract the inner literal
	var lit *plan.Literal
	if constExpr.GetLit() != nil {
		lit = constExpr.GetLit()
	} else if funcExpr := constExpr.GetF(); funcExpr != nil {
		// Check if it's a cast function with a literal argument
		if len(funcExpr.Args) > 0 {
			if innerLit := funcExpr.Args[0].GetLit(); innerLit != nil {
				lit = innerLit
			}
		}
	}

	if lit == nil || lit.Isnull {
		return false
	}

	// Calculate how many trailing digits we need to check
	trailingDigits := constT.Scale - columnScale
	if trailingDigits <= 0 || trailingDigits > 18 {
		return false
	}

	// Get the decimal value and check trailing zeros
	// Try DECIMAL64, DECIMAL128, and string literals
	divisor := int64(types.Pow10[trailingDigits])

	if val, ok := lit.Value.(*plan.Literal_Decimal64Val); ok {
		return val.Decimal64Val.A%divisor == 0
	} else if val, ok := lit.Value.(*plan.Literal_Decimal128Val); ok {
		// For Decimal128, we need to check if the trailing digits are all zeros
		// using 128-bit arithmetic
		return decimal128HasTrailingZeros(val.Decimal128Val.A, val.Decimal128Val.B, trailingDigits)
	} else if sval, ok := lit.Value.(*plan.Literal_Sval); ok {
		// The literal is a string, parse it as decimal
		dec, _, err := types.Parse128(sval.Sval)
		if err != nil {
			return false
		}
		return decimal128HasTrailingZeros(int64(dec.B0_63), int64(dec.B64_127), trailingDigits)
	}

	return false
}

// decimal128HasTrailingZeros checks if a 128-bit decimal value has trailing zeros
// that can be safely truncated. The value is represented as two int64 parts:
// low (bits 0-63) and high (bits 64-127).
func decimal128HasTrailingZeros(low, high int64, trailingDigits int32) bool {
	if trailingDigits <= 0 || trailingDigits > 18 {
		return false
	}

	divisor := int64(types.Pow10[trailingDigits])

	// If high part is zero, we can just check the low part
	if high == 0 {
		return low%divisor == 0
	}

	// For values with non-zero high part, we need 128-bit modulo
	// Use types.Decimal128 for proper 128-bit arithmetic
	d128 := types.Decimal128{B0_63: uint64(low), B64_127: uint64(high)}
	divisorDec := types.Decimal128{B0_63: uint64(divisor), B64_127: 0}

	// Compute d128 % divisorDec
	remainder, err := d128.Mod128(divisorDec)
	if err != nil {
		return false
	}

	return remainder.B0_63 == 0 && remainder.B64_127 == 0
}

// isDecimalComparisonAlwaysFalseCore checks if a decimal comparison is always false
// This happens when the constant has non-zero digits beyond the column's scale
func isDecimalComparisonAlwaysFalseCore(constExpr *plan.Expr, constT types.Type, columnScale int32) bool {
	if constT.Scale <= columnScale {
		return false
	}

	// If it has trailing zeros, it's not always false (can be optimized instead)
	if hasTrailingZeros(constExpr, constT, columnScale) {
		return false
	}

	// Has non-zero trailing digits, comparison is always false
	return true
}

// isDecimalComparisonAlwaysFalse checks if a decimal equality comparison between two expressions is always false
// Wrapper function that identifies column and constant, then calls the core logic
func isDecimalComparisonAlwaysFalse(ctx context.Context, expr1, expr2 *plan.Expr) bool {
	// Unwrap Cast expressions to get the underlying column/literal
	unwrap1 := unwrapCast(expr1)
	unwrap2 := unwrapCast(expr2)

	// Identify which is column and which is constant
	var colExpr, constExpr *plan.Expr
	var origConstExpr *plan.Expr

	if unwrap1.GetCol() != nil && unwrap2.GetLit() != nil {
		colExpr, constExpr = unwrap1, unwrap2
		origConstExpr = expr2
	} else if unwrap2.GetCol() != nil && unwrap1.GetLit() != nil {
		colExpr, constExpr = unwrap2, unwrap1
		origConstExpr = expr1
	} else {
		return false // Not a column-constant comparison
	}

	// Use unwrapped column for its original type, and original constant for its type
	colType := makeTypeByPlan2Expr(colExpr)
	constType := makeTypeByPlan2Expr(origConstExpr)

	if !colType.Oid.IsDecimal() || !constType.Oid.IsDecimal() {
		return false
	}

	// Call the core logic
	return isDecimalComparisonAlwaysFalseCore(constExpr, constType, colType.Scale)
}

// unwrapCast extracts the underlying expression from a Cast function
// Returns the original expression if it's not a Cast
func unwrapCast(expr *plan.Expr) *plan.Expr {
	if expr == nil {
		return nil
	}

	if funcExpr := expr.GetF(); funcExpr != nil {
		if funcExpr.Func.ObjName == "cast" && len(funcExpr.Args) > 0 {
			return funcExpr.Args[0]
		}
	}

	return expr
}

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
			// float32 has ~7 decimal digits of precision (IEEE 754 single precision: 24 bits mantissa)
			// Safe range: -16777216 to 16777216 (2^24, exact integer representation)
			// For general values, use conservative limit to avoid precision loss
			return constVal <= 16777216 && constVal >= -16777216
		case types.T_float64:
			// float64 has ~15-16 decimal digits of precision (IEEE 754 double precision: 53 bits mantissa)
			// Safe range: -9007199254740992 to 9007199254740992 (2^53, exact integer representation)
			// Use MaxInt32 as conservative limit for practical purposes
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
			// float32 safe range for exact integer representation: 0 to 2^24 (16777216)
			return constVal <= 16777216
		case types.T_float64:
			// float64 safe range for exact integer representation: 0 to 2^53
			// Use MaxUint32 as conservative limit
			return constVal <= math.MaxUint32
		case types.T_decimal64:
			return constVal <= math.MaxInt32
		default:
			return false
		}

	case types.T_decimal64, types.T_decimal128:
		// Allow casting decimal constants to decimal columns only if no precision loss
		if columnT.Oid == types.T_decimal64 || columnT.Oid == types.T_decimal128 {
			// Optimization 1: Check if column scale >= constant scale (already handled)
			if columnT.Scale >= constT.Scale {
				return true
			}

			// Optimization 2: Check if constant has trailing zeros that can be truncated
			if hasTrailingZeros(constExpr, constT, columnT.Scale) {
				return true
			}

			return false
		}
		// Allow casting decimal constants to float columns only if precision is acceptable
		// For FLOAT32: only allow if value has <= 7 significant digits
		// For FLOAT64: only allow if value has <= 15 significant digits
		if columnT.Oid == types.T_float32 || columnT.Oid == types.T_float64 {
			// TODO: Add precision check based on decimal value
			// For now, conservatively return false to avoid precision loss
			return false
		}
		return false

	case types.T_float32, types.T_float64:
		// Allow casting float constants to float/decimal columns
		if columnT.Oid == types.T_float32 || columnT.Oid == types.T_float64 {
			return true
		}
		if columnT.Oid == types.T_decimal64 || columnT.Oid == types.T_decimal128 {
			return true
		}
		return false

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
				return moerr.NewBadConfigf(param.Ctx, "the format '%s' is not supported", format)
			}
			param.Format = format
		case "jsondata":
			jsondata := strings.ToLower(param.Option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return moerr.NewBadConfigf(param.Ctx, "the jsondata '%s' is not supported", jsondata)
			}
			param.JsonData = jsondata
			param.Format = tree.JSONLINE
		default:
			return moerr.NewBadConfigf(param.Ctx, "the keyword '%s' is not support", strings.ToLower(param.Option[i]))
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
			if format != tree.CSV && format != tree.JSONLINE && format != tree.PARQUET {
				return moerr.NewBadConfigf(param.Ctx, "the format '%s' is not supported", format)
			}
			param.Format = format
		case "jsondata":
			jsondata := strings.ToLower(param.Option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return moerr.NewBadConfigf(param.Ctx, "the jsondata '%s' is not supported", jsondata)
			}
			param.JsonData = jsondata
			param.Format = tree.JSONLINE

		default:
			return moerr.NewBadConfigf(param.Ctx, "the keyword '%s' is not support", strings.ToLower(param.Option[i]))
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

func GetFilePathFromParam(param *tree.ExternParam) string {
	fpath := param.Filepath
	for i := 0; i < len(param.Option); i += 2 {
		name := strings.ToLower(param.Option[i])
		if name == "filepath" {
			fpath = param.Option[i+1]
			break
		}
	}

	return fpath
}

func InitStageS3Param(param *tree.ExternParam, s stage.StageDef) error {

	param.ScanType = tree.S3
	param.S3Param = &tree.S3Parameter{}

	if len(s.Url.RawQuery) > 0 {
		return moerr.NewBadConfig(param.Ctx, "S3 URL Query does not support in ExternParam")
	}

	if s.Url.Scheme != stage.S3_PROTOCOL {
		return moerr.NewBadConfig(param.Ctx, "URL protocol is not S3")
	}

	bucket, prefix, _, err := stage.ParseS3Url(s.Url)
	if err != nil {
		return err
	}

	var found bool
	param.S3Param.Bucket = bucket
	param.Filepath = prefix

	// mandatory
	param.S3Param.APIKey, found = s.GetCredentials(stage.PARAMKEY_AWS_KEY_ID, "")
	if !found {
		return moerr.NewBadConfigf(param.Ctx, "Credentials %s not found", stage.PARAMKEY_AWS_KEY_ID)
	}
	param.S3Param.APISecret, found = s.GetCredentials(stage.PARAMKEY_AWS_SECRET_KEY, "")
	if !found {
		return moerr.NewBadConfigf(param.Ctx, "Credentials %s not found", stage.PARAMKEY_AWS_SECRET_KEY)
	}

	param.S3Param.Region, found = s.GetCredentials(stage.PARAMKEY_AWS_REGION, "")
	if !found {
		return moerr.NewBadConfigf(param.Ctx, "Credentials %s not found", stage.PARAMKEY_AWS_REGION)
	}

	param.S3Param.Endpoint, found = s.GetCredentials(stage.PARAMKEY_ENDPOINT, "")
	if !found {
		return moerr.NewBadConfigf(param.Ctx, "Credentials %s not found", stage.PARAMKEY_ENDPOINT)
	}

	// optional
	param.S3Param.Provider, _ = s.GetCredentials(stage.PARAMKEY_PROVIDER, stage.S3_PROVIDER_AMAZON)
	param.CompressType, _ = s.GetCredentials(stage.PARAMKEY_COMPRESSION, "auto")

	for i := 0; i < len(param.Option); i += 2 {
		switch strings.ToLower(param.Option[i]) {
		case "format":
			format := strings.ToLower(param.Option[i+1])
			if format != tree.CSV && format != tree.JSONLINE && format != tree.PARQUET {
				return moerr.NewBadConfigf(param.Ctx, "the format '%s' is not supported", format)
			}
			param.Format = format
		case "jsondata":
			jsondata := strings.ToLower(param.Option[i+1])
			if jsondata != tree.OBJECT && jsondata != tree.ARRAY {
				return moerr.NewBadConfigf(param.Ctx, "the jsondata '%s' is not supported", jsondata)
			}
			param.JsonData = jsondata
			param.Format = tree.JSONLINE

		default:
			return moerr.NewBadConfigf(param.Ctx, "the keyword '%s' is not support", strings.ToLower(param.Option[i]))
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

func InitInfileOrStageParam(param *tree.ExternParam, proc *process.Process) error {

	fpath := GetFilePathFromParam(param)

	if !strings.HasPrefix(fpath, stage.STAGE_PROTOCOL+"://") {
		return InitInfileParam(param)
	}

	s, err := stageutil.UrlToStageDef(fpath, proc)
	if err != nil {
		return err
	}

	if len(s.Url.RawQuery) > 0 {
		return moerr.NewBadConfig(param.Ctx, "Invalid URL: query not supported in ExternParam")
	}

	if s.Url.Scheme == stage.S3_PROTOCOL {
		return InitStageS3Param(param, s)
	} else if s.Url.Scheme == stage.FILE_PROTOCOL {

		err := InitInfileParam(param)
		if err != nil {
			return err
		}

		param.Filepath = s.Url.Path

	} else {
		return moerr.NewBadConfigf(param.Ctx, "invalid URL: protocol %s not supported", s.Url.Scheme)
	}

	return nil
}
func GetForETLWithType(param *tree.ExternParam, prefix string) (res fileservice.ETLFileService, readPath string, err error) {
	if param.ScanType == tree.S3 {
		buf := new(strings.Builder)
		w := csv.NewWriter(buf)
		opts := []string{"s3-opts", "endpoint=" + param.S3Param.Endpoint, "region=" + param.S3Param.Region, "key=" + param.S3Param.APIKey, "secret=" + param.S3Param.APISecret,
			"bucket=" + param.S3Param.Bucket, "role-arn=" + param.S3Param.RoleArn, "external-id=" + param.S3Param.ExternalId}
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
			for entry, err := range fs.List(param.Ctx, readPath) {
				if err != nil {
					return nil, nil, err
				}
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
	length2 := l2.Len()
	// Ensure l and l2 have matching lengths to avoid panic
	if length != length2 {
		return nil, nil, moerr.NewInternalErrorNoCtxf("file list and size list length mismatch: %d vs %d", length, length2)
	}
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
func GetUniqueColAndIdxFromTableDef(tableDef *TableDef) ([]map[string]int, map[string]bool) {
	uniqueCols := make([]map[string]int, 0, len(tableDef.Cols))
	uniqueColNames := make(map[string]bool)
	if tableDef.Pkey != nil && !onlyHasHiddenPrimaryKey(tableDef) {
		pkMap := make(map[string]int)
		for _, colName := range tableDef.Pkey.Names {
			pkMap[colName] = int(tableDef.Name2ColIndex[colName])
			uniqueColNames[colName] = true
		}
		uniqueCols = append(uniqueCols, pkMap)
	}

	for _, index := range tableDef.Indexes {
		if index.Unique {
			pkMap := make(map[string]int)
			for _, part := range index.Parts {
				pkMap[part] = int(tableDef.Name2ColIndex[part])
				uniqueColNames[part] = true
			}
			uniqueCols = append(uniqueCols, pkMap)
		}
	}
	return uniqueCols, uniqueColNames
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

func ExprType2Type(typ *plan.Type) types.Type {
	return types.New(types.T(typ.Id), typ.Width, typ.Scale)
}

func PkColByTableDef(tblDef *plan.TableDef) *plan.ColDef {
	pkColIdx := tblDef.Name2ColIndex[tblDef.Pkey.PkeyColName]
	pkCol := tblDef.Cols[pkColIdx]
	return pkCol
}

type FormatOption struct {
	ExpandVec       bool
	ExpandVecMaxLen int

	// <=0 means no limit
	MaxDepth int
}

func FormatExprs(exprs []*plan.Expr, option FormatOption) string {
	return FormatExprsInConsole(exprs, option)
}

func FormatExpr(expr *plan.Expr, option FormatOption) string {
	return FormatExprInConsole(expr, option)
}

func FormatExprsInConsole(exprs []*plan.Expr, option FormatOption) string {
	var w bytes.Buffer
	for _, expr := range exprs {
		w.WriteString(FormatExpr(expr, option))
		w.WriteByte('\n')
	}
	return w.String()
}

func FormatExprInConsole(expr *plan.Expr, option FormatOption) string {
	var w bytes.Buffer
	doFormatExprInConsole(expr, &w, 0, option)
	return w.String()
}

func doFormatExprInConsole(expr *plan.Expr, out *bytes.Buffer, depth int, option FormatOption) {
	out.WriteByte('\n')
	prefix := strings.Repeat("\t", depth)
	if depth >= option.MaxDepth && option.MaxDepth > 0 {
		out.WriteString(fmt.Sprintf("%s...", prefix))
		return
	}
	switch t := expr.Expr.(type) {
	case *plan.Expr_Col:
		out.WriteString(fmt.Sprintf("%sExpr_Col(%s.%d)", prefix, t.Col.Name, t.Col.ColPos))
	case *plan.Expr_Lit:
		out.WriteString(fmt.Sprintf("%sExpr_C(%s)", prefix, t.Lit.String()))
	case *plan.Expr_F:
		out.WriteString(fmt.Sprintf("%sExpr_F(\n%s\tFunc[\"%s\"](nargs=%d)", prefix, prefix, t.F.Func.ObjName, len(t.F.Args)))
		for _, arg := range t.F.Args {
			doFormatExprInConsole(arg, out, depth+1, option)
		}
		out.WriteString(fmt.Sprintf("\n%s)", prefix))
	case *plan.Expr_P:
		out.WriteString(fmt.Sprintf("%sExpr_P(%d)", prefix, t.P.Pos))
	case *plan.Expr_T:
		out.WriteString(fmt.Sprintf("%sExpr_T(%s)", prefix, t.T.String()))
	case *plan.Expr_Vec:
		if option.ExpandVec {
			expandVecMaxLen := option.ExpandVecMaxLen
			if expandVecMaxLen <= 0 {
				expandVecMaxLen = 1
			}
			var (
				vecStr string
				vec    vector.Vector
			)
			if err := vec.UnmarshalBinary(t.Vec.Data); err != nil {
				vecStr = fmt.Sprintf("error: %s", err.Error())
			} else {
				vecStr = common.MoVectorToString(&vec, expandVecMaxLen)
			}
			out.WriteString(fmt.Sprintf("%sExpr_Vec(%s)", prefix, vecStr))
		} else {
			out.WriteString(fmt.Sprintf("%sExpr_Vec(len=%d)", prefix, t.Vec.Len))
		}
	case *plan.Expr_Fold:
		out.WriteString(fmt.Sprintf("%sExpr_Fold(id=%d)", prefix, t.Fold.Id))
	case *plan.Expr_List:
		out.WriteString(fmt.Sprintf("%sExpr_List(len=%d)", prefix, len(t.List.List)))
		for _, arg := range t.List.List {
			doFormatExprInConsole(arg, out, depth+1, option)
		}
	default:
		out.WriteString(fmt.Sprintf("%sExpr_Unknown(%s)", prefix, expr.String()))
	}
}

// databaseIsValid checks whether the database exists or not.
func databaseIsValid(dbName string, ctx CompilerContext, snapshot *Snapshot) (string, error) {
	connectDBFirst := false
	if len(dbName) == 0 {
		connectDBFirst = true
	}
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}

	// In order to be compatible with various GUI clients and BI tools, lower case db and table name if it's a mysql system table
	if slices.Contains(mysql.CaseInsensitiveDbs, strings.ToLower(dbName)) {
		dbName = strings.ToLower(dbName)
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
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "unsupport value '%s'", ast.String())
			}
		case *tree.ParamExpr:
			if !isPrepareStmt {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "only prepare statement can use ? expr")
			}
			paramTypes = append(paramTypes, int32(types.T_varchar))
			if ast.Offset != len(paramTypes) {
				return nil, moerr.NewInternalError(ctx.GetContext(), "offset not match")
			}
		default:
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "unsupport value '%s'", ast.String())
		}
	}
	return paramTypes, nil
}

// HasMoCtrl checks whether the expression has mo_ctrl(..,..,..)
func HasMoCtrl(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		if exprImpl.F.Func.ObjName == "mo_ctl" || exprImpl.F.Func.ObjName == "fault_inject" {
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

func MakeCPKEYRuntimeFilter(tag int32, upperlimit int32, expr *Expr, tableDef *plan.TableDef, notOnPk bool) *plan.RuntimeFilterSpec {
	cpkeyIdx, ok := tableDef.Name2ColIndex[catalog.CPrimaryKeyColName]
	if !ok {
		panic("fail to convert runtime filter to composite primary key!")
	}
	col := expr.GetCol()
	col.ColPos = cpkeyIdx
	expr.Typ = tableDef.Cols[cpkeyIdx].Typ
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperlimit,
		Expr:        expr,
		MatchPrefix: true,
		NotOnPk:     notOnPk,
	}
}

func MakeSerialRuntimeFilter(ctx context.Context, tag int32, matchPrefix bool, upperlimit int32, expr *Expr, notOnPk bool) *plan.RuntimeFilterSpec {
	serialExpr, _ := BindFuncExprImplByPlanExpr(ctx, "serial", []*plan.Expr{expr})
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperlimit,
		Expr:        serialExpr,
		MatchPrefix: matchPrefix,
		NotOnPk:     notOnPk,
	}
}

func MakeRuntimeFilter(tag int32, matchPrefix bool, upperlimit int32, expr *Expr, notOnPk bool) *plan.RuntimeFilterSpec {
	return &plan.RuntimeFilterSpec{
		Tag:         tag,
		UpperLimit:  upperlimit,
		Expr:        expr,
		MatchPrefix: matchPrefix,
		NotOnPk:     notOnPk,
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

func GetColExpr(typ Type, relpos int32, colpos int32) *plan.Expr {
	return &plan.Expr{
		Typ: typ,
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: relpos,
				ColPos: colpos,
			},
		},
	}
}

func MakeSerialExtractExpr(ctx context.Context, fromExpr *Expr, origType Type, serialIdx int64) (*Expr, error) {
	return BindFuncExprImplByPlanExpr(ctx, "serial_extract", []*plan.Expr{
		fromExpr,
		{
			Typ: plan.Type{
				Id: int32(types.T_int64),
			},
			Expr: &plan.Expr_Lit{
				Lit: &plan.Literal{
					Value: &plan.Literal_I64Val{I64Val: serialIdx},
				},
			},
		},
		{
			Typ: origType,
			Expr: &plan.Expr_T{
				T: &plan.TargetType{},
			},
		},
	})
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
		if val == nil {
			pc := &plan.Literal{
				Isnull: true,
				Value:  &plan.Literal_Sval{Sval: ""},
			}
			params[i] = &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: pc,
				},
			}
		} else {
			pc := &plan.Literal{}
			pc.Value = &plan.Literal_Sval{Sval: fmt.Sprintf("%v", val)}
			params[i] = &plan.Expr{
				Expr: &plan.Expr_Lit{
					Lit: pc,
				},
			}
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

func containGrouping(expr *Expr) bool {
	var ret bool

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			ret = ret || containGrouping(arg)
		}
		ret = ret || (exprImpl.F.Func.ObjName == "grouping")
	case *plan.Expr_Col:
		ret = false
	}

	return ret
}

func checkGrouping(ctx context.Context, expr *Expr) error {
	if containGrouping(expr) {
		return moerr.NewSyntaxError(ctx, "aggregate function grouping not allowed in WHERE clause")
	}
	return nil
}

// a > current_time() + 1 and b < ? + c and d > ? + 2
// =>
// a > foldVal1 and b < foldVal2 + c and d > foldVal3
func ReplaceFoldExpr(proc *process.Process, expr *Expr, exes *[]colexec.ExpressionExecutor) (bool, error) {
	allCanFold := true
	var err error

	fn := expr.GetF()
	if fn == nil {
		switch expr.Expr.(type) {
		case *plan.Expr_List:
			return true, nil
		case *plan.Expr_Col:
			return false, nil
		case *plan.Expr_Vec:
			return false, nil
		default:
			return true, nil
		}
	}

	overloadID := fn.Func.GetObj()
	f, exists := function.GetFunctionByIdWithoutError(overloadID)
	if !exists {
		panic("ReplaceFoldVal: function not exist")
	}
	if f.IsAgg() || f.IsWin() {
		panic("ReplaceFoldVal: agg or window function")
	}

	argFold := make([]bool, len(fn.Args))
	for i := range fn.Args {
		argFold[i], err = ReplaceFoldExpr(proc, fn.Args[i], exes)
		if err != nil {
			return false, err
		}
		if !argFold[i] {
			allCanFold = false
		}
	}

	if allCanFold {
		return true, nil
	} else {
		for i, canFold := range argFold {
			if canFold {
				fn.Args[i], err = ConstantFold(batch.EmptyForConstFoldBatch, fn.Args[i], proc, false, true)
				if err != nil {
					return false, err
				}
				if _, ok := fn.Args[i].Expr.(*plan.Expr_Vec); ok {
					continue
				}

				exprExecutor, err := colexec.NewExpressionExecutor(proc, fn.Args[i])
				if err != nil {
					return false, err
				}
				newID := len(*exes)
				*exes = append(*exes, exprExecutor)

				fn.Args[i] = &plan.Expr{
					Typ: fn.Args[i].Typ,
					Expr: &plan.Expr_Fold{
						Fold: &plan.FoldVal{
							Id: int32(newID),
						},
					},
					AuxId:       fn.Args[i].AuxId,
					Ndv:         fn.Args[i].Ndv,
					Selectivity: fn.Args[i].Selectivity,
				}

			}
		}
		return false, nil
	}
}

func EvalFoldExpr(proc *process.Process, expr *Expr, executors *[]colexec.ExpressionExecutor) (err error) {
	switch ef := expr.Expr.(type) {
	case *plan.Expr_Fold:
		var vec *vector.Vector
		idx := int(ef.Fold.Id)
		if idx >= len(*executors) {
			panic("EvalFoldVal: fold id not exist")
		}
		exe := (*executors)[idx]
		var data []byte
		var err error

		if _, ok := exe.(*colexec.ListExpressionExecutor); ok {
			vec, err = exe.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			if err != nil {
				return err
			}
			if !vec.IsConstNull() {
				vec.InplaceSortAndCompact()
			}
			data, err = vec.MarshalBinary()
			if err != nil {
				return err
			}
			ef.Fold.IsConst = false
		} else {
			vec, err = exe.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			if err != nil {
				return err
			}
			data, _ = getConstantBytes(vec, false, 0)
			ef.Fold.IsConst = true
		}
		ef.Fold.Data = data
	case *plan.Expr_F:
		for i := range ef.F.Args {
			err = EvalFoldExpr(proc, ef.F.Args[i], executors)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func HasFoldExprForList(exprs []*Expr) bool {
	for _, e := range exprs {
		hasFoldExpr := HasFoldValExpr(e)
		if hasFoldExpr {
			return true
		}
	}
	return false
}

func HasFoldValExpr(expr *Expr) bool {
	switch ef := expr.Expr.(type) {
	case *plan.Expr_Fold:
		return true
	case *plan.Expr_F:
		for i := range ef.F.Args {
			hasFoldExpr := HasFoldValExpr(ef.F.Args[i])
			if hasFoldExpr {
				return true
			}
		}
	}
	return false
}

func getConstantBytes(vec *vector.Vector, transAll bool, row uint64) (ret []byte, can bool) {
	if vec.IsConstNull() || vec.GetNulls().Contains(row) {
		return
	}
	can = true
	switch vec.GetType().Oid {
	case types.T_bool:
		val := vector.MustFixedColNoTypeCheck[bool](vec)[row]
		ret = types.EncodeBool(&val)

	case types.T_bit:
		val := vector.MustFixedColNoTypeCheck[uint64](vec)[row]
		ret = types.EncodeUint64(&val)

	case types.T_int8:
		val := vector.MustFixedColNoTypeCheck[int8](vec)[row]
		ret = types.EncodeInt8(&val)

	case types.T_int16:
		val := vector.MustFixedColNoTypeCheck[int16](vec)[row]
		ret = types.EncodeInt16(&val)

	case types.T_int32:
		val := vector.MustFixedColNoTypeCheck[int32](vec)[row]
		ret = types.EncodeInt32(&val)

	case types.T_int64:
		val := vector.MustFixedColNoTypeCheck[int64](vec)[row]
		ret = types.EncodeInt64(&val)

	case types.T_uint8:
		val := vector.MustFixedColNoTypeCheck[uint8](vec)[row]
		ret = types.EncodeUint8(&val)

	case types.T_uint16:
		val := vector.MustFixedColNoTypeCheck[uint16](vec)[row]
		ret = types.EncodeUint16(&val)

	case types.T_uint32:
		val := vector.MustFixedColNoTypeCheck[uint32](vec)[row]
		ret = types.EncodeUint32(&val)

	case types.T_uint64:
		val := vector.MustFixedColNoTypeCheck[uint64](vec)[row]
		ret = types.EncodeUint64(&val)

	case types.T_float32:
		val := vector.MustFixedColNoTypeCheck[float32](vec)[row]
		ret = types.EncodeFloat32(&val)

	case types.T_float64:
		val := vector.MustFixedColNoTypeCheck[float64](vec)[row]
		ret = types.EncodeFloat64(&val)

	case types.T_varchar, types.T_char,
		types.T_binary, types.T_varbinary, types.T_text, types.T_blob, types.T_datalink:
		ret = []byte(vec.GetStringAt(int(row)))

	case types.T_json:
		if !transAll {
			can = false
			return
		}
		ret = []byte(vec.GetStringAt(int(row)))

	case types.T_timestamp:
		val := vector.MustFixedColNoTypeCheck[types.Timestamp](vec)[row]
		ret = types.EncodeTimestamp(&val)

	case types.T_date:
		val := vector.MustFixedColNoTypeCheck[types.Date](vec)[row]
		ret = types.EncodeDate(&val)

	case types.T_time:
		val := vector.MustFixedColNoTypeCheck[types.Time](vec)[row]
		ret = types.EncodeTime(&val)

	case types.T_datetime:
		val := vector.MustFixedColNoTypeCheck[types.Datetime](vec)[row]
		ret = types.EncodeDatetime(&val)

	case types.T_enum:
		if !transAll {
			can = false
			return
		}
		val := vector.MustFixedColNoTypeCheck[types.Enum](vec)[row]
		ret = types.EncodeEnum(&val)

	case types.T_decimal64:
		val := vector.MustFixedColNoTypeCheck[types.Decimal64](vec)[row]
		ret = types.EncodeDecimal64(&val)

	case types.T_decimal128:
		val := vector.MustFixedColNoTypeCheck[types.Decimal128](vec)[row]
		ret = types.EncodeDecimal128(&val)

	case types.T_uuid:
		val := vector.MustFixedColNoTypeCheck[types.Uuid](vec)[row]
		ret = types.EncodeUuid(&val)

	default:
		can = false
	}

	return
}

//func getOffsetFromUTC() string {
//	now := time.Now()
//	_, localOffset := now.Zone()
//	return offsetToString(localOffset)
//}
//
//func offsetToString(offset int) string {
//	hours := offset / 3600
//	minutes := (offset % 3600) / 60
//	if hours < 0 {
//		return fmt.Sprintf("-%02d:%02d", -hours, -minutes)
//	}
//	return fmt.Sprintf("+%02d:%02d", hours, minutes)
//}

// do not lock table if lock no rows now.
// if need to lock table, uncomment these codes
// func getLockTableAtTheEnd(tableDef *TableDef) bool {
// if tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName || //fake pk, skip
// 	tableDef.Partition != nil { // unsupport partition table
// 	return false
// }
// return !strings.HasPrefix(tableDef.Name, catalog.IndexTableNamePrefix)
// }

// DbNameOfObjRef return subscription name of ObjectRef if exists, to avoid the mismatching of account id and db name
func DbNameOfObjRef(objRef *ObjectRef) string {
	if objRef.SubscriptionName == "" {
		return objRef.SchemaName
	}
	return objRef.SubscriptionName
}
func doResolveTimeStamp(timeStamp string) (ts int64, err error) {
	loc, err := time.LoadLocation("Local")
	if err != nil {
		return 0, err
	}
	if len(timeStamp) == 0 {
		return 0, moerr.NewInvalidInputNoCtx("timestamp is empty")
	}
	t, err := time.ParseInLocation("2006-01-02 15:04:05", timeStamp, loc)
	if err != nil {
		return 0, moerr.NewInvalidInputNoCtxf("invalid timestamp format: %s", timeStamp)
	}
	ts = t.UTC().UnixNano()
	return ts, nil
}

func onlyHasHiddenPrimaryKey(tableDef *TableDef) bool {
	if tableDef == nil {
		return false
	}
	pk := tableDef.GetPkey()
	return pk != nil && pk.GetPkeyColName() == catalog.FakePrimaryKeyColName
}
