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
	"sort"
	"strconv"
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

func getJoinSideWithOuterScope(expr *plan.Expr, leftTags, rightTags map[int32]bool, markTag int32) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSideWithOuterScope(arg, leftTags, rightTags, markTag)
		}

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			side |= getJoinSideWithOuterScope(arg, leftTags, rightTags, markTag)
		}

	case *plan.Expr_Col:
		tag := exprImpl.Col.RelPos
		if leftTags[tag] {
			side = JoinSideLeft
		} else if rightTags[tag] {
			side = JoinSideRight
		} else if tag == markTag {
			side = JoinSideMark
		} else {
			side = JoinSideOuter
		}

	case *plan.Expr_Corr:
		side = JoinSideCorrelated
	}

	return
}

func containsTag(expr *plan.Expr, tag int32) bool {
	if expr == nil {
		return false
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if containsTag(arg, tag) {
				return true
			}
		}
	case *plan.Expr_W:
		if containsTag(exprImpl.W.WindowFunc, tag) {
			return true
		}
		for _, arg := range exprImpl.W.PartitionBy {
			if containsTag(arg, tag) {
				return true
			}
		}
		for _, order := range exprImpl.W.OrderBy {
			if containsTag(order.Expr, tag) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if containsTag(arg, tag) {
				return true
			}
		}
	case *plan.Expr_Sub:
		if exprImpl.Sub == nil {
			return false
		}
		return containsTag(exprImpl.Sub.Child, tag)
	case *plan.Expr_Col:
		return exprImpl.Col.RelPos == tag
	case *plan.Expr_Corr:
		return exprImpl.Corr.RelPos == tag
	}

	return false
}

func containsOnlyTags(expr *plan.Expr, tags map[int32]bool) bool {
	if expr == nil {
		return true
	}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if !containsOnlyTags(arg, tags) {
				return false
			}
		}

	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
			if !containsOnlyTags(arg, tags) {
				return false
			}
		}

	case *plan.Expr_Col:
		return tags[exprImpl.Col.RelPos]

	case *plan.Expr_Corr, *plan.Expr_Sub:
		return false
	}

	return true
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

func replaceColRefsIntroducesVolatile(expr *plan.Expr, tag int32, projects []*plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if replaceColRefsIntroducesVolatile(arg, tag, projects) {
				return true
			}
		}
	case *plan.Expr_Col:
		colRef := exprImpl.Col
		return colRef.RelPos == tag && ContainsVolatileFunction(projects[colRef.ColPos])
	case *plan.Expr_W:
		if replaceColRefsIntroducesVolatile(exprImpl.W.WindowFunc, tag, projects) {
			return true
		}
		for _, arg := range exprImpl.W.PartitionBy {
			if replaceColRefsIntroducesVolatile(arg, tag, projects) {
				return true
			}
		}
		for _, order := range exprImpl.W.OrderBy {
			if replaceColRefsIntroducesVolatile(order.Expr, tag, projects) {
				return true
			}
		}
	}

	return false
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
		// WHERE, HAVING and JOIN ON are executable scalar boundaries. Check
		// before boolean coercion so an interval pseudo-value reports the real
		// contract violation instead of an incidental INTERVAL-to-BOOL cast
		// overload error.
		if err = rejectStandaloneIntervalExpr(ctx.binder.GetContext(), expr, "predicate"); err != nil {
			return nil, err
		}
		needCast := true
		fn := expr.GetF()
		if fn != nil {
			// fulltext_match / bm25_match are rewritten to an index-scan join by the
			// optimizer; leave them un-cast so the rewrite can find them by name in the
			// filter list (a wrapping cast(... AS BOOL) would hide the function).
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
//
// Conjuncts are compared via a structural fingerprint (exprStructuralHash +
// exprStructuralEqual) rather than proto serialization. For deeply nested
// IN/OR trees the old Marshal path walked every expression twice (ProtoSize
// + writeTo) per lookup and dominated CPU; hashing traverses once with no
// allocation and collisions are rare enough that Equal rarely runs.
func applyDistributivity(ctx context.Context, expr *plan.Expr, exposeCrossTableKeys ...bool) *plan.Expr {
	exposeCrossTable := len(exposeCrossTableKeys) == 0 || exposeCrossTableKeys[0]
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = applyDistributivity(ctx, arg, exposeCrossTable)
		}

		if exprImpl.F.Func.ObjName != "or" {
			break
		}

		leftConds := splitPlanConjunction(exprImpl.F.Args[0])
		rightConds := splitPlanConjunction(exprImpl.F.Args[1])

		// Bucket right conjuncts by structural hash. Each bucket stores the
		// original expr + the per-bucket side state, so the left scan can
		// collision-check with exprStructuralEqual against the few conds
		// sharing a hash (normally 1).
		type rightEntry struct {
			cond *plan.Expr
			side int
		}
		rightBuckets := make(map[uint64][]*rightEntry, len(rightConds))
		rightEntries := make([]*rightEntry, len(rightConds))

		rightRelations := make(map[int32]struct{}, 2)
		rightRelationsKnown := true
		legacyRelPos := int32(-1)
		for i, cond := range rightConds {
			h := exprStructuralHash(cond)
			entry := &rightEntry{cond: cond, side: JoinSideRight}
			rightEntries[i] = entry
			rightBuckets[h] = append(rightBuckets[h], entry)
			rightRelationsKnown = collectExprRelations(cond, rightRelations) && rightRelationsKnown
			if !exposeCrossTable {
				args := cond.GetF().GetArgs()
				if len(args) == 2 {
					if col := args[0].GetCol(); col != nil {
						if legacyRelPos == -1 {
							legacyRelPos = col.RelPos
						} else if legacyRelPos != col.RelPos {
							legacyRelPos = -2
						}
					}
				}
			}
		}
		// Keep single-table DNF intact for composite-key range folding. The old
		// first-argument heuristic missed columns hidden in BETWEEN/IN and the
		// second side of equalities, so a cross-table DNF could be mistaken for
		// a single-table predicate and hide a common hash-join key.
		if exposeCrossTable && rightRelationsKnown && len(rightRelations) == 1 ||
			!exposeCrossTable && legacyRelPos >= 0 {
			return expr
		}

		var commonConds, leftOnlyConds, rightOnlyConds []*plan.Expr

		for _, cond := range leftConds {
			h := exprStructuralHash(cond)
			bucket := rightBuckets[h]
			var matched *rightEntry
			for _, entry := range bucket {
				if entry.side != JoinSideRight {
					continue
				}
				if exprStructuralEqual(entry.cond, cond) {
					matched = entry
					break
				}
			}
			if matched != nil {
				commonConds = append(commonConds, cond)
				matched.side = JoinSideBoth
			} else {
				leftOnlyConds = append(leftOnlyConds, cond)
			}
		}

		for i, cond := range rightConds {
			if rightEntries[i].side == JoinSideRight {
				rightOnlyConds = append(rightOnlyConds, cond)
			}
		}

		if len(commonConds) == 0 {
			return expr
		}
		// Factoring evaluates a common predicate before the residual OR. That is
		// only observationally equivalent when the common predicate is total and
		// side-effect-free; otherwise it can expose an error or volatile call on
		// rows for which the original expression short-circuited.
		if exposeCrossTable && !areTruncationSafePredicates(commonConds) {
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

func collectExprRelations(expr *plan.Expr, relations map[int32]struct{}) bool {
	if expr == nil {
		return true
	}
	switch item := expr.Expr.(type) {
	case *plan.Expr_Col:
		if item.Col == nil || item.Col.RelPos < 0 {
			return false
		}
		relations[item.Col.RelPos] = struct{}{}
	case *plan.Expr_Corr:
		if item.Corr == nil || item.Corr.RelPos < 0 {
			return false
		}
		relations[item.Corr.RelPos] = struct{}{}
	case *plan.Expr_F:
		if item.F != nil {
			for _, arg := range item.F.Args {
				if !collectExprRelations(arg, relations) {
					return false
				}
			}
		}
	case *plan.Expr_List:
		if item.List != nil {
			for _, arg := range item.List.List {
				if !collectExprRelations(arg, relations) {
					return false
				}
			}
		}
	case *plan.Expr_W:
		if item.W != nil {
			if !collectExprRelations(item.W.WindowFunc, relations) {
				return false
			}
			for _, arg := range item.W.PartitionBy {
				if !collectExprRelations(arg, relations) {
					return false
				}
			}
			for _, order := range item.W.OrderBy {
				if !collectExprRelations(order.Expr, relations) {
					return false
				}
			}
		}
	case *plan.Expr_Sub:
		if item.Sub != nil && !collectExprRelations(item.Sub.Child, relations) {
			return false
		}
	case *plan.Expr_Lit:
		if item.Lit != nil && !collectExprRelations(item.Lit.Src, relations) {
			return false
		}
	}
	return true
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
		if exprImpl.F.Func.ObjName == "and" && !conjunctionSharesMemoAcrossBranches(exprImpl.F.Args) {
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

// conjunctionSharesMemoAcrossBranches reports whether splitting an AND would
// separate occurrences that must share one volatile-expression memo cache.
func conjunctionSharesMemoAcrossBranches(args []*plan.Expr) bool {
	if len(args) != 2 {
		return false
	}
	left := make(map[int32]struct{})
	collectNegativeAuxIDs(args[0], left)
	if len(left) == 0 {
		return false
	}
	right := make(map[int32]struct{})
	collectNegativeAuxIDs(args[1], right)
	for id := range left {
		if _, ok := right[id]; ok {
			return true
		}
	}
	return false
}

func collectNegativeAuxIDs(expr *plan.Expr, ids map[int32]struct{}) {
	if expr == nil {
		return
	}
	if expr.AuxId < 0 {
		ids[expr.AuxId] = struct{}{}
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_F:
		if e.F != nil {
			for _, arg := range e.F.Args {
				collectNegativeAuxIDs(arg, ids)
			}
		}
	case *plan.Expr_List:
		if e.List != nil {
			for _, item := range e.List.List {
				collectNegativeAuxIDs(item, ids)
			}
		}
	case *plan.Expr_Lit:
		if e.Lit != nil {
			collectNegativeAuxIDs(e.Lit.Src, ids)
		}
	}
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

// PreparedPlanHasDeferredNumericFunction reports whether a prepared plan has
// an ABS argument whose overload was deferred until execution.  This is kept
// as a plan-introspection helper for tests and diagnostics; execute-time
// eligibility is cached on PrepareStmt and must not call this walker for every
// execution.
func PreparedPlanHasDeferredNumericFunction(preparePlan *Plan) bool {
	return len(PreparedPlanNumericFallbackParamPositions(preparePlan)) > 0
}

// PreparedPlanNumericFallbackParamPositions returns the parameter positions
// whose value supplies a deferred numeric ABS argument.  The result is plan
// metadata, not an execute-time decision: callers can compute it once when a
// prepared plan is built and use it to decide whether runtime values must be
// decoded.  In particular, this avoids scanning/deep-copying the entire plan
// on every ordinary execution.
func PreparedPlanNumericFallbackParamPositions(preparePlan *Plan) []int32 {
	if preparePlan == nil || preparePlan.GetQuery() == nil {
		return nil
	}
	positions := make(map[int32]struct{})
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		fn := expr.GetF()
		if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.GetObjName(), "abs") || len(fn.Args) != 1 {
			return nil
		}
		if !isPreparedNumericFallbackExpr(fn.Args[0]) {
			return nil
		}
		for pos := range preparedNumericValueParamPositions(fn.Args[0]) {
			positions[pos] = struct{}{}
		}
		return nil
	})
	if len(positions) == 0 {
		return nil
	}
	result := make([]int32, 0, len(positions))
	for pos := range positions {
		result = append(result, pos)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func isPreparedNumericFallbackExpr(expr *plan.Expr) bool {
	return expr != nil && expr.GetPreparedNumeric().GetFallback()
}

func ensurePreparedNumericMetadata(expr *plan.Expr) *plan.PreparedNumericMetadata {
	if expr == nil {
		return nil
	}
	if expr.PreparedNumeric == nil {
		expr.PreparedNumeric = &plan.PreparedNumericMetadata{}
	}
	return expr.PreparedNumeric
}

func copyPreparedNumericMetadata(metadata *plan.PreparedNumericMetadata) *plan.PreparedNumericMetadata {
	if metadata == nil {
		return nil
	}
	return &plan.PreparedNumericMetadata{
		Fallback:                    metadata.Fallback,
		ParamPos:                    metadata.ParamPos,
		FallbackSource:              metadata.FallbackSource,
		FallbackSourceNodeId:        metadata.FallbackSourceNodeId,
		FallbackSourceColPos:        metadata.FallbackSourceColPos,
		ProvisionalResultCast:       metadata.ProvisionalResultCast,
		ProvisionalResultPeer:       metadata.ProvisionalResultPeer,
		ProvisionalResultPeerTypeId: metadata.ProvisionalResultPeerTypeId,
		ProvisionalResultPeerWidth:  metadata.ProvisionalResultPeerWidth,
		ProvisionalResultPeerScale:  metadata.ProvisionalResultPeerScale,
	}
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
	case *plan.Expr_List:
		for _, arg := range exprImpl.List.List {
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
	case *tree.ValuesClause:
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
	case *tree.ValuesClause:
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
	// Column-free is not the same as scan-invariant. A volatile function can
	// produce a new value for every row and must not be evaluated once more by
	// block pruning before the row-level filter runs.
	if containsVolatileFunction(expr) {
		return false
	}
	return exprIsZonemappable(ctx, expr)
}

func exprIsZonemappable(ctx context.Context, expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		isConst := true
		for _, arg := range exprImpl.F.Args {
			if isRuntimeConstExpr(arg) {
				continue
			} else {
				isConst = false
			}
			isZonemappable := exprIsZonemappable(ctx, arg)
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
	return constantFoldWithPreparedExactSource(
		bat, expr, proc, varAndParamIsConst, foldInExpr, containsDynamicParam(expr))
}

func constantFoldWithPreparedExactSource(
	bat *batch.Batch,
	expr *plan.Expr,
	proc *process.Process,
	varAndParamIsConst bool,
	foldInExpr bool,
	preservePreparedExactSource bool,
) (*plan.Expr, error) {
	if expr.Typ.Id == int32(types.T_interval) {
		// INTERVAL is an executable argument type but has no standalone scalar
		// constant-fold representation. Keep it unchanged so callers can fold an
		// enclosing temporal expression or let a public scalar boundary reject it
		// without turning a bound expression into a planner panic.
		return expr, nil
	}

	// If it is Expr_List, perform constant folding on its elements
	if elist := expr.GetList(); elist != nil {
		exprList := elist.List
		cannotFold := false
		for i := range exprList {
			foldExpr, err := constantFoldWithPreparedExactSource(
				bat, exprList[i], proc, varAndParamIsConst, foldInExpr, preservePreparedExactSource)
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
		requiresStringProvenance, err := plan.RequiresMORPCVersion23StringProvenance(exprList)
		if err != nil {
			return nil, err
		}
		if requiresStringProvenance {
			// LiteralVec uses the stable Vector wire format, which cannot carry
			// per-item runtime string domains. Keep the literal list executable
			// and visible to the remote protocol capability analysis.
			return expr, nil
		}
		isSerialized := rule.ContainsSerializedLiteral(exprList)

		vec, err := colexec.GenerateConstListExpressionExecutor(proc, exprList)
		if err != nil {
			return nil, err
		}
		defer vec.Free(proc.Mp())
		if vec.GetStringSources() != nil {
			return expr, nil
		}

		// Nullable IN-lists must keep their null bitmap aligned with values.
		if !vec.IsConstNull() && !vec.GetNulls().Any() {
			vec.InplaceSortAndCompact()
		}
		data, err := vec.MarshalBinary()
		if err != nil {
			return nil, err
		}

		return &plan.Expr{
			Typ: expr.Typ,
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:          int32(vec.Length()),
					Data:         data,
					IsSerialized: isSerialized,
					StringSource: uint32(vec.GetStringSource()),
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
	if rule.IsLegacyTimeAssignmentOutsideInternalRange(fn) {
		return expr, nil
	}
	if f.IsRealTimeRelated() && !varAndParamIsConst {
		return expr, nil
	}
	if preservePreparedExactSource && rule.IsImplicitFloatCastOfExplicitDecimalConstant(expr) {
		// Statistics and binder helpers use this generic folder before the
		// prepare optimizer runs. Preserve the same exact source boundary here,
		// otherwise the later prepared-only fold cannot recover lost digits.
		return expr, nil
	}
	isVec := false
	for i := range fn.Args {
		foldExpr, errFold := constantFoldWithPreparedExactSource(
			bat, fn.Args[i], proc, varAndParamIsConst, foldInExpr, preservePreparedExactSource)
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
		if vec.GetStringSources() != nil {
			return expr, nil
		}
		data, err := vec.MarshalBinary()
		if err != nil {
			return expr, nil
		}

		return &plan.Expr{
			Typ: plan.Type{
				Id:      int32(vec.GetType().Oid),
				Scale:   vec.GetType().Scale,
				Width:   vec.GetType().Width,
				Charset: uint32(vec.GetType().Charset),
			},
			Expr: &plan.Expr_Vec{
				Vec: &plan.LiteralVec{
					Len:          int32(vec.Length()),
					Data:         data,
					StringSource: uint32(vec.GetStringSource()),
				},
			},
		}, nil
	}
	c := rule.GetConstantValue(vec, false, 0)
	if c == nil {
		return expr, nil
	}
	rule.PreserveFoldedLiteralStringDomain(expr, c)
	if source := vec.GetStringSource(); source != types.StringSourceLiteral {
		c.StringSource = uint32(source) + 1
	}
	rule.MarkFoldedLiteralSerialized(overloadID, fn.Args, c)
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
	} else if funcExpr := constExpr.GetF(); funcExpr != nil &&
		funcExpr.Func != nil && funcExpr.Func.GetObjName() == "cast" {
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
	// Runtime specialization materializes prepared values as typed constant
	// casts. When their domain already equals the IN left side, they are safe to
	// keep in the typed list just like a direct literal. Do not extend this to
	// row-dependent expressions merely because their declared types match.
	if constT.Eq(columnT) && (rule.IsConstant(constExpr, false) || isCastOfConstant(constExpr)) {
		return true
	}

	lit := constExpr.GetLit()
	if lit == nil {
		return false
	}

	//TODO: Check if T_array is required here?
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

// isCastOfConstant preserves same-type casts whose source is a constant even
// when the cast function itself is intentionally non-foldable (for example a
// VARCHAR literal explicitly cast to UUID). A row-dependent source still takes
// the regular cast path.
func isCastOfConstant(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || fn.Func.GetObjName() != "cast" || len(fn.Args) == 0 {
		return false
	}
	return rule.IsConstant(fn.Args[0], false) || isCastOfConstant(fn.Args[0])
}

// parseHiveOptionKV handles hive_partitioning / hive_partition_columns keys in
// Init*Param. It is defensive against legacy JSON where stripHiveOptionKeys
// (build_ddl.go) had not run; when the param already has values normalized
// during DDL, the legacy option is skipped to avoid case-flip or type drift.
//
// Each key's skip guard MUST inspect only its own field. An earlier version
// coupled the hive_partitioning guard to HivePartitionCols; for legacy option
// orders like "hive_partition_columns=year, hive_partitioning=true" that caused
// hive_partitioning to be silently skipped after cols was populated, leaving
// HivePartitioning=false and the table mis-classified as non-hive.
//
// Returns (handled, err):
//   - (false, nil)  : key is not a hive key; caller should fall through to its own switch
//   - (true, nil)   : key handled (either applied or intentionally skipped)
//   - (true, err)   : key handled but value invalid
func parseHiveOptionKV(param *tree.ExternParam, key, val string) (bool, error) {
	switch key {
	case "hive_partitioning":
		// Guard only on HivePartitioning itself — do NOT consult HivePartitionCols.
		if param.HivePartitioning {
			return true, nil
		}
		v := strings.ToLower(val)
		if v != "true" && v != "false" {
			return true, moerr.NewBadConfigf(param.Ctx, "hive_partitioning must be 'true' or 'false'")
		}
		param.HivePartitioning = (v == "true")
		return true, nil
	case "hive_partition_columns":
		if len(param.HivePartitionCols) > 0 {
			return true, nil
		}
		for _, p := range strings.Split(val, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				param.HivePartitionCols = append(param.HivePartitionCols, strings.ToLower(p))
			}
		}
		return true, nil
	}
	return false, nil
}

func validateHiveOptionConsistency(param *tree.ExternParam) error {
	if !param.HivePartitioning && len(param.HivePartitionCols) > 0 {
		return moerr.NewBadConfig(param.Ctx, "hive_partition_columns requires hive_partitioning='true'")
	}
	return nil
}

func InitInfileParam(param *tree.ExternParam) error {
	for i := 0; i < len(param.Option); i += 2 {
		key := strings.ToLower(param.Option[i])
		if handled, err := parseHiveOptionKV(param, key, param.Option[i+1]); handled {
			if err != nil {
				return err
			}
			continue
		}
		switch key {
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
		case ExternalWriteFilePatternKey, CSVCommentKey:
			// write_file_pattern is write-only; comment is read at parse time. Both
			// are kept in Option and consumed elsewhere, ignored here.
		default:
			return moerr.NewBadConfigf(param.Ctx, "the keyword '%s' is not support", key)
		}
	}
	if err := validateHiveOptionConsistency(param); err != nil {
		return err
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
		key := strings.ToLower(param.Option[i])
		if handled, err := parseHiveOptionKV(param, key, param.Option[i+1]); handled {
			if err != nil {
				return err
			}
			continue
		}
		switch key {
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
		case ExternalWriteFilePatternKey, CSVCommentKey:
			// write_file_pattern is write-only; comment is read at parse time. Both
			// are kept in Option and consumed elsewhere, ignored here.
		default:
			return moerr.NewBadConfigf(param.Ctx, "the keyword '%s' is not support", key)
		}
	}
	if err := validateHiveOptionConsistency(param); err != nil {
		return err
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

// ExternalWriteFilePatternKey is the external-table option that turns the table
// into a writable external table. Its value is a strftime template (with the
// %nN and %U MatrixOne extensions) that must resolve to a stage:// path.
const ExternalWriteFilePatternKey = "write_file_pattern"

// GetWriteFilePattern returns the WRITE_FILE_PATTERN option of an external table
// and whether it was set. An external table is writable iff this returns ok.
func GetWriteFilePattern(param *tree.ExternParam) (string, bool) {
	if param == nil {
		return "", false
	}
	for i := 0; i+1 < len(param.Option); i += 2 {
		if strings.ToLower(param.Option[i]) == ExternalWriteFilePatternKey {
			return param.Option[i+1], true
		}
	}
	return "", false
}

// CSVCommentKey is the external-table option that sets the CSV reader's comment
// marker: a line whose raw prefix (before unquoting) equals it is skipped on
// read. The default (option absent or empty) is no marker — every line is data.
const CSVCommentKey = "comment"

// GetCSVComment returns the COMMENT option of an external table (empty when
// unset, meaning no comment marker).
func GetCSVComment(param *tree.ExternParam) string {
	if param == nil {
		return ""
	}
	for i := 0; i+1 < len(param.Option); i += 2 {
		if strings.ToLower(param.Option[i]) == CSVCommentKey {
			return param.Option[i+1]
		}
	}
	return ""
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

	// Note: the parseHiveOptionKV call below is kept for parity with the other
	// two Init*Param functions, but hive_partitioning on a stage external table
	// is rejected at DDL (build_ddl.go validateAndSetHivePartitionOptions). The
	// hive branch here is therefore unreachable via normal DDL; it exists only
	// so every Init*Param follows the same shape and would tolerate legacy JSON
	// that snuck hive keys past validation.
	for i := 0; i < len(param.Option); i += 2 {
		key := strings.ToLower(param.Option[i])
		if handled, err := parseHiveOptionKV(param, key, param.Option[i+1]); handled {
			if err != nil {
				return err
			}
			continue
		}
		switch key {
		case "filepath":
			// stage:// paths have already been expanded to s.Url by
			// InitInfileOrStageParam. Keep the raw option for show/serialization
			// compatibility, but never let it override the resolved S3 prefix.
			continue
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
		case ExternalWriteFilePatternKey, CSVCommentKey:
			// write_file_pattern is write-only; comment is read at parse time. Both
			// are kept in Option and consumed elsewhere, ignored here.
		default:
			return moerr.NewBadConfigf(param.Ctx, "the keyword '%s' is not support", key)
		}
	}

	if err := validateHiveOptionConsistency(param); err != nil {
		return err
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
	return types.NewWithCharset(types.T(typ.Id), typ.Width, typ.Scale, uint8(typ.Charset))
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
	return resetPreparePlan(ctx, preparePlan, nil)
}

// NormalizePrepareParamRefs converts the parser's one-based parameter ordinals
// to execution-time zero-based positions without compacting gaps.
func NormalizePrepareParamRefs(ctx context.Context, preparePlan *Plan) error {
	if preparePlan == nil || preparePlan.GetQuery() == nil {
		return nil
	}
	rule := &decrementParamOrdinalRule{
		seen:         make(map[*plan.ParamRef]struct{}),
		seenFallback: make(map[*plan.Expr]struct{}),
	}
	visit := NewVisitPlan(preparePlan, []VisitPlanRule{rule})
	if err := visit.Visit(ctx); err != nil {
		return err
	}
	for i := range preparePlan.GetQuery().Params {
		var err error
		preparePlan.GetQuery().Params[i], err = rule.ApplyExpr(preparePlan.GetQuery().Params[i])
		if err != nil {
			return err
		}
	}
	return visitMissingNodeExprs(
		preparePlan.GetQuery(), preparePlan.GetQuery().Steps, []VisitPlanRule{rule})
}

func resetPreparePlan(
	ctx CompilerContext,
	preparePlan *Plan,
	transientQuery *Query,
) ([]*plan.ObjectRef, []int32, error) {
	// dcl tcl is not support
	var schemas []*plan.ObjectRef
	var paramTypes []int32
	resolveIndexDependencies := func(getParamRule *GetParamRule) ([]*plan.ObjectRef, error) {
		querySchemas := getParamRule.schemas
		for _, dependency := range getParamRule.indexDependencies {
			objRef, tableDef, err := ctx.ResolveIndexTableByRef(dependency.baseRef, dependency.tableName, dependency.snapshot)
			if err != nil {
				return nil, err
			}
			if objRef == nil || tableDef == nil {
				return nil, moerr.NewInternalErrorf(ctx.GetContext(), "resolved index table %q without catalog metadata", dependency.tableName)
			}
			querySchemas = appendPrepareSchemas(querySchemas,
				prepareSchemaRefWithSnapshot(objRef, tableDef, dependency.snapshot))
		}
		return querySchemas, nil
	}
	resetQuery := func(query *Query) ([]*plan.ObjectRef, []int32, error) {
		queryPlan := &Plan{Plan: &plan.Plan_Query{Query: query}}
		getParamRule := NewGetParamRule()
		visitQuery := NewVisitPlan(queryPlan, []VisitPlanRule{getParamRule})
		if err := visitQuery.Visit(ctx.GetContext()); err != nil {
			return nil, nil, err
		}
		for i := range query.Params {
			var err error
			query.Params[i], err = getParamRule.ApplyExpr(query.Params[i])
			if err != nil {
				return nil, nil, err
			}
		}

		getParamRule.SetParamOrder()
		args := getParamRule.params
		querySchemas, err := resolveIndexDependencies(getParamRule)
		if err != nil {
			return nil, nil, err
		}
		querySchemas = appendPrepareSchemas(querySchemas, query.GetCatalogDependencies()...)

		resetParamRule := NewResetParamOrderRule(args)
		visitQuery = NewVisitPlan(queryPlan, []VisitPlanRule{resetParamRule})
		if err := visitQuery.Visit(ctx.GetContext()); err != nil {
			return nil, nil, err
		}
		for i := range query.Params {
			var err error
			query.Params[i], err = resetParamRule.ApplyExpr(query.Params[i])
			if err != nil {
				return nil, nil, err
			}
		}
		return querySchemas, getParamRule.paramTypes, nil
	}
	resetSetVariables := func(setVars *plan.SetVariables) ([]*plan.ObjectRef, []int32, error) {
		getParamRule := NewGetParamRule()
		subqueryRoots := newSubqueryRootRule()
		for _, item := range setVars.Items {
			var err error
			item.Value, err = subqueryRoots.ApplyExpr(item.Value)
			if err != nil {
				return nil, nil, err
			}
			item.Value, err = getParamRule.ApplyExpr(item.Value)
			if err != nil {
				return nil, nil, err
			}
			if item.Reserved != nil {
				item.Reserved, err = subqueryRoots.ApplyExpr(item.Reserved)
				if err != nil {
					return nil, nil, err
				}
				item.Reserved, err = getParamRule.ApplyExpr(item.Reserved)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		visitedRoots := make(map[int32]struct{})
		for len(subqueryRoots.pending) > 0 {
			root := subqueryRoots.pending[0]
			subqueryRoots.pending = subqueryRoots.pending[1:]
			if _, ok := visitedRoots[root]; ok {
				continue
			}
			if transientQuery == nil || root < 0 || int(root) >= len(transientQuery.Nodes) {
				return nil, nil, moerr.NewInternalErrorf(
					ctx.GetContext(), "missing transient query root %d for prepared SET", root)
			}
			visitedRoots[root] = struct{}{}
			query := *transientQuery
			query.Steps = []int32{root}
			queryPlan := &Plan{Plan: &plan.Plan_Query{Query: &query}}
			visitQuery := NewVisitPlan(queryPlan, []VisitPlanRule{getParamRule, subqueryRoots})
			if err := visitQuery.Visit(ctx.GetContext()); err != nil {
				return nil, nil, err
			}
			if err := visitMissingNodeExprs(
				&query, query.Steps, []VisitPlanRule{getParamRule, subqueryRoots},
			); err != nil {
				return nil, nil, err
			}
		}
		getParamRule.SetParamOrder()
		resetRule := NewResetParamOrderRule(getParamRule.params)
		for _, item := range setVars.Items {
			var err error
			item.Value, err = resetRule.ApplyExpr(item.Value)
			if err != nil {
				return nil, nil, err
			}
			if item.Reserved != nil {
				item.Reserved, err = resetRule.ApplyExpr(item.Reserved)
				if err != nil {
					return nil, nil, err
				}
			}
		}
		querySchemas, err := resolveIndexDependencies(getParamRule)
		if err != nil {
			return nil, nil, err
		}
		if transientQuery != nil {
			querySchemas = appendPrepareSchemas(
				querySchemas, transientQuery.GetCatalogDependencies()...)
		}
		return querySchemas, getParamRule.paramTypes, nil
	}

	switch pp := preparePlan.Plan.(type) {
	case *plan.Plan_Tcl:
		return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare TCL and DCL statement")
	case *plan.Plan_Dcl:
		switch pp.Dcl.GetDclType() {
		case plan.DataControl_CREATE_ACCOUNT,
			plan.DataControl_ALTER_ACCOUNT,
			plan.DataControl_DROP_ACCOUNT:
			return nil, pp.Dcl.GetOther().GetParamTypes(), nil
		case plan.DataControl_SET_VARIABLES:
			schemas, paramTypes, err := resetSetVariables(pp.Dcl.GetSetVariables())
			if err != nil {
				return nil, nil, err
			}
			return schemas, paramTypes, nil
		default:
			return nil, nil, moerr.NewInvalidInput(ctx.GetContext(), "cannot prepare TCL and DCL statement")
		}
	case *plan.Plan_Ddl:
		if pp.Ddl.Query != nil {
			return resetQuery(pp.Ddl.Query)
		}

	case *plan.Plan_Query:
		return resetQuery(pp.Query)
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
	filled, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, paramVals)
	return filled, err
}

// ValidatePreparedLagLeadParams validates LAG/LEAD offset markers before the
// generic expression cast path can discard their protocol source type. This
// is also called by the cached prepared-execution path, which does not replace
// ParamRefs in the plan for each execution.
func ValidatePreparedLagLeadParams(ctx context.Context, preparePlan *Plan, paramVals []any) error {
	if preparePlan == nil || len(paramVals) == 0 {
		return nil
	}
	query := preparePlan.GetQuery()
	if query == nil && preparePlan.GetDdl() != nil {
		query = preparePlan.GetDdl().GetQuery()
	}
	if query == nil {
		return nil
	}

	for _, node := range query.GetNodes() {
		if node == nil {
			continue
		}
		for _, expr := range node.GetWinSpecList() {
			window := expr.GetW()
			if window == nil {
				continue
			}
			function := window.GetWindowFunc().GetF()
			if function == nil || len(function.Args) < 2 {
				continue
			}
			name := function.GetFunc().GetObjName()
			if name != "lag" && name != "lead" {
				continue
			}
			if position, ok := preparedWindowArgumentParamPosition(function.Args[1]); ok {
				if position < 0 || int(position) >= len(paramVals) {
					continue
				}
				if !isNonNegativePreparedInteger(paramVals[position]) {
					return moerr.NewWrongArguments(ctx, name)
				}
			}
		}
	}
	return nil
}

// PreparedLagLeadParamPositions returns the zero-based parameter positions
// used as prepared LAG/LEAD offsets.
func PreparedLagLeadParamPositions(preparePlan *Plan) []int32 {
	positions := make(map[int32]struct{})
	if preparePlan == nil {
		return nil
	}
	query := preparePlan.GetQuery()
	if query == nil && preparePlan.GetDdl() != nil {
		query = preparePlan.GetDdl().GetQuery()
	}
	if query == nil {
		return nil
	}

	for _, node := range query.GetNodes() {
		if node == nil {
			continue
		}
		for _, expr := range node.GetWinSpecList() {
			window := expr.GetW()
			if window == nil {
				continue
			}
			function := window.GetWindowFunc().GetF()
			if function == nil || len(function.Args) < 2 {
				continue
			}
			name := function.GetFunc().GetObjName()
			if name != "lag" && name != "lead" {
				continue
			}
			if position, ok := preparedWindowArgumentParamPosition(function.Args[1]); ok {
				positions[position] = struct{}{}
			}
		}
	}

	result := make([]int32, 0, len(positions))
	for position := range positions {
		result = append(result, position)
	}
	slices.Sort(result)
	return result
}

// PreparedPlanNeedsRuntimeSpecialization reports whether a binary prepared
// execution can change a result-column domain or an overloaded expression.
// Most prepared DML only needs the current parameter values.  Avoid copying
// and walking those plans on every execute, but retain the correctness path
// for predicates and expressions whose overload depends on the parameter
// domain (for example, `? = ?` in an UPDATE filter).
func PreparedPlanNeedsRuntimeSpecialization(preparePlan *Plan) bool {
	if preparePlan == nil {
		return false
	}

	scanPlan := DeepCopyPlan(preparePlan)
	if scanPlan == nil {
		return true
	}
	query := scanPlan.GetQuery()
	if query == nil && scanPlan.GetDdl() != nil {
		query = scanPlan.GetDdl().GetQuery()
	}
	if query == nil {
		return false
	}
	if scanPlan.GetQuery() == nil {
		scanPlan = &Plan{Plan: &plan.Plan_Query{Query: query}}
	}

	rule := &preparedRuntimeSpecializationScanRule{
		// A bare SELECT parameter keeps the cached result domain unless the
		// binary protocol path explicitly identifies it as a direct result
		// column.  Direct-result metadata is handled by
		// PreparedPlanDirectResultParamPositions; treating every root marker as
		// runtime-specialized here would put ordinary COM_STMT queries on the
		// per-execute deep-copy path.
		directResult: false,
		skipExprs:    preparedDMLWriteExpressions(query),
		seen:         make(map[*plan.Expr]struct{}),
	}
	if err := NewVisitPlan(scanPlan, []VisitPlanRule{rule}).Visit(context.Background()); err != nil {
		// The scan is an optimization only. Preserve correctness if a newly
		// added plan field cannot be visited here.
		return true
	}
	return rule.needs
}

// PreparedPlanNeedsRuntimeTextComparisonSpecialization reports whether the
// current binary-protocol parameter domains expose a text operand inside a
// numeric comparison. Prepare-time overload resolution may have wrapped that
// marker in a provisional integer cast; only text executions need to replace
// it with the engine's DOUBLE cast and its MySQL-compatible prefix, range, and
// warning semantics. Numeric executions can keep the cached compile.
func PreparedPlanNeedsRuntimeTextComparisonSpecialization(
	preparePlan *Plan,
	runtimeParamTypes []types.Type,
) bool {
	return len(preparedNumericComparisonTextParamPositions(preparePlan, runtimeParamTypes)) > 0
}

func preparedNumericComparisonTextParamPositions(
	preparePlan *Plan,
	runtimeParamTypes []types.Type,
) map[int]bool {
	positions := make(map[int]bool)
	if preparePlan == nil || len(runtimeParamTypes) == 0 {
		return positions
	}

	scanPlan := preparePlan
	query := scanPlan.GetQuery()
	if query == nil && scanPlan.GetDdl() != nil {
		query = scanPlan.GetDdl().GetQuery()
	}
	if query == nil {
		return positions
	}
	if scanPlan.GetQuery() == nil {
		scanPlan = &Plan{Plan: &plan.Plan_Query{Query: query}}
	}

	rule := &preparedRuntimeTextComparisonScanRule{
		runtimeParamTypes: runtimeParamTypes,
		positions:         positions,
		seen:              make(map[*plan.Expr]struct{}),
	}
	if err := NewVisitPlan(scanPlan, []VisitPlanRule{rule}).Visit(context.Background()); err != nil {
		// This scan only selects an additional specialization path. A visit
		// failure must not reinterpret parameters globally; the normal cached
		// execution remains the conservative fallback.
		return make(map[int]bool)
	}
	return positions
}

func isPreparedDMLStmt(stmtType plan.Query_StatementType) bool {
	switch stmtType {
	case plan.Query_INSERT, plan.Query_UPDATE, plan.Query_DELETE, plan.Query_MERGE:
		return true
	default:
		return false
	}
}

func preparedDMLWriteExpressions(query *plan.Query) map[*plan.Expr]struct{} {
	writeExprs := make(map[*plan.Expr]struct{})
	if query == nil || !isPreparedDMLStmt(query.StmtType) {
		return writeExprs
	}
	add := func(expr *plan.Expr) {
		if preparedExprContainsParam(expr) {
			writeExprs[expr] = struct{}{}
		}
	}

	// Follow only the writer's primary input path. ProjectList is used for
	// every relational projection, including private projects that implement
	// derived tables and scalar subqueries. Those secondary branches must stay
	// outside the preservation set so their marker comparisons can be rebound.
	// The final row-image projection and any unary projections below it are the
	// positional roots consumed by the writer.
	visited := make(map[int32]struct{})
	var visitWritePath func(int32)
	visitWritePath = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return
		}
		if _, ok := visited[nodeID]; ok {
			return
		}
		visited[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		if node.NodeType == plan.Node_PROJECT {
			for _, expr := range node.ProjectList {
				add(expr)
			}
		}
		if len(node.Children) > 0 {
			// A DML input can fan out through joins/applies. The final
			// positional projection is above that fan-out; once below it,
			// follow only the primary (left) path and leave secondary
			// subquery/derived-table projections to normal specialization.
			visitWritePath(node.Children[0])
		}
	}
	for _, node := range query.Nodes {
		if node == nil {
			continue
		}
		// LOCK_OP consumes the primary-key expression positionally from its
		// child batch. Multi-table INSERT builds each target as an independent
		// write step, so these row-image roots are not necessarily reachable
		// from a MULTI_UPDATE child edge.
		if node.NodeType == plan.Node_LOCK_OP && len(node.Children) == 1 {
			childID := node.Children[0]
			if childID >= 0 && int(childID) < len(query.Nodes) {
				input := query.Nodes[childID]
				if input != nil {
					for _, expr := range input.ProjectList {
						add(expr)
					}
				}
			}
		}
		// INSERT/MULTI_UPDATE may carry a writer projection directly on the
		// sink node.  DELETE has no value projection; its parameters belong to
		// filter expressions and must remain eligible for specialization.
		if node.NodeType == plan.Node_INSERT || node.NodeType == plan.Node_MULTI_UPDATE {
			for _, expr := range node.ProjectList {
				add(expr)
			}
			for _, childID := range node.Children {
				visitWritePath(childID)
			}
		}
		for _, expr := range node.OnUpdateExprs {
			add(expr)
		}
		if node.DedupJoinCtx != nil {
			for _, expr := range node.DedupJoinCtx.UpdateColExprList {
				add(expr)
			}
		}
		if node.RowsetData != nil {
			for _, col := range node.RowsetData.Cols {
				for _, row := range col.Data {
					if row != nil {
						add(row.Expr)
					}
				}
			}
		}
	}
	return writeExprs
}

type preparedRuntimeSpecializationScanRule struct {
	directResult bool
	needs        bool
	skipExprs    map[*plan.Expr]struct{}
	seen         map[*plan.Expr]struct{}
}

type preparedRuntimeTextComparisonScanRule struct {
	runtimeParamTypes []types.Type
	positions         map[int]bool
	seen              map[*plan.Expr]struct{}
}

func (rule *preparedRuntimeTextComparisonScanRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *preparedRuntimeTextComparisonScanRule) IsApplyExpr() bool {
	return true
}

func (rule *preparedRuntimeTextComparisonScanRule) ApplyNode(_ *Node) error {
	return nil
}

func (rule *preparedRuntimeTextComparisonScanRule) ApplyExpr(expr *plan.Expr) (*plan.Expr, error) {
	rule.scanExpr(expr)
	return expr, nil
}

func (rule *preparedRuntimeTextComparisonScanRule) scanExpr(expr *plan.Expr) {
	if expr == nil {
		return
	}
	if _, ok := rule.seen[expr]; ok {
		return
	}
	rule.seen[expr] = struct{}{}

	if function := expr.GetF(); function != nil {
		name := ""
		if function.Func != nil {
			name = strings.ToLower(function.Func.GetObjName())
		}
		if isPreparedNumericComparisonContext(name) && rule.argsHaveNumericDomain(function.Args) {
			for _, arg := range function.Args {
				rule.collectTextParams(arg)
			}
		}
		for _, arg := range function.Args {
			rule.scanExpr(arg)
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			rule.scanExpr(item)
		}
		return
	}
	if window := expr.GetW(); window != nil {
		rule.scanExpr(window.WindowFunc)
		for _, arg := range window.PartitionBy {
			rule.scanExpr(arg)
		}
		for _, order := range window.OrderBy {
			if order != nil {
				rule.scanExpr(order.Expr)
			}
		}
		if window.Frame != nil {
			if window.Frame.Start != nil {
				rule.scanExpr(window.Frame.Start.Val)
			}
			if window.Frame.End != nil {
				rule.scanExpr(window.Frame.End.Val)
			}
		}
	}
}

func (rule *preparedRuntimeTextComparisonScanRule) argsHaveNumericDomain(args []*plan.Expr) bool {
	for _, arg := range args {
		if rule.exprHasNumericDomain(arg) {
			return true
		}
	}
	return false
}

func (rule *preparedRuntimeTextComparisonScanRule) exprHasNumericDomain(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if param := expr.GetP(); param != nil {
		return rule.paramTypeIsNumeric(int(param.Pos))
	}
	if isImplicitPreparedParamCast(expr) {
		position, ok := implicitPreparedParamPosition(expr)
		return preparedComparisonTypeIsNumeric(types.T(expr.Typ.Id)) ||
			(ok && rule.paramTypeIsNumeric(position))
	}
	if expr.GetCol() != nil {
		// A numeric column is a numeric comparison domain too. Keep the column
		// expression itself unchanged; the text marker is rebound to the
		// engine's DOUBLE conversion so numeric-prefix and warning semantics are
		// preserved without relying on the stale prepare-time integer cast.
		return preparedComparisonTypeIsNumeric(types.T(expr.Typ.Id))
	}
	if preparedComparisonTypeIsNumeric(types.T(expr.Typ.Id)) {
		return true
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if rule.exprHasNumericDomain(item) {
				return true
			}
		}
	}
	return false
}

func (rule *preparedRuntimeTextComparisonScanRule) collectTextParams(expr *plan.Expr) {
	if expr == nil {
		return
	}
	if param := expr.GetP(); param != nil {
		position := int(param.Pos)
		if rule.paramTypeIsText(position) {
			rule.positions[position] = true
		}
		return
	}
	if function := expr.GetF(); function != nil {
		name := ""
		if function.Func != nil {
			name = strings.ToLower(function.Func.GetObjName())
			if name == "cast" && !isImplicitPreparedParamCast(expr) {
				// An explicit CAST owns a direct marker's conversion contract.
				// If its child is a domain-sensitive expression (for example
				// CAST(ABS(?) AS INT)), keep walking so the nested overload can
				// still receive the COM_STMT text-to-DOUBLE conversion.
				if len(function.Args) > 0 && preparedExprContainsParam(function.Args[0]) &&
					function.Args[0].GetP() == nil {
					for _, arg := range function.Args {
						rule.collectTextParams(arg)
					}
				}
				return
			}
		}
		if !isImplicitPreparedParamCast(expr) &&
			!isNumericContextFunction(name) && !supportsGenericNumericFunctionContext(name) {
			// Convert the result of a string-producing/consuming expression at
			// the outer comparison, not its parameter leaves. For example,
			// LENGTH(?) and CONCAT(?) must see the original string.
			return
		}
		for _, arg := range function.Args {
			rule.collectTextParams(arg)
		}
		return
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			rule.collectTextParams(item)
		}
		return
	}
	if window := expr.GetW(); window != nil {
		rule.collectTextParams(window.WindowFunc)
	}
}

func (rule *preparedRuntimeTextComparisonScanRule) paramTypeIsNumeric(position int) bool {
	return position >= 0 && position < len(rule.runtimeParamTypes) &&
		preparedComparisonTypeIsNumeric(rule.runtimeParamTypes[position].Oid)
}

func preparedComparisonTypeIsNumeric(typ types.T) bool {
	return typ == types.T_bit || (types.Type{Oid: typ}).IsNumeric()
}

func (rule *preparedRuntimeTextComparisonScanRule) paramTypeIsText(position int) bool {
	if position < 0 || position >= len(rule.runtimeParamTypes) {
		return false
	}
	switch rule.runtimeParamTypes[position].Oid {
	case types.T_char, types.T_varchar, types.T_text:
		return true
	default:
		return false
	}
}

func (rule *preparedRuntimeSpecializationScanRule) MatchNode(_ *Node) bool {
	return false
}

func (rule *preparedRuntimeSpecializationScanRule) IsApplyExpr() bool {
	return true
}

func (rule *preparedRuntimeSpecializationScanRule) ApplyNode(_ *Node) error {
	return nil
}

func (rule *preparedRuntimeSpecializationScanRule) ApplyExpr(expr *plan.Expr) (*plan.Expr, error) {
	if _, ok := rule.skipExprs[expr]; ok {
		// The outer expression is a positional write value and must not make
		// the write layout look like a different plan.  Its children are still
		// ordinary expressions, however: a comparison or CASE below the
		// assignment can have an execute-time parameter domain of its own.
		rule.scanExpr(expr, false)
		return expr, nil
	}
	rule.scanExpr(expr, true)
	return expr, nil
}

func (rule *preparedRuntimeSpecializationScanRule) scanExpr(expr *plan.Expr, root bool) {
	if expr == nil || rule.needs {
		return
	}
	if _, ok := rule.seen[expr]; ok {
		return
	}
	rule.seen[expr] = struct{}{}

	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P:
		if root && rule.directResult {
			rule.needs = true
		}
	case *plan.Expr_F:
		if exprImpl.F == nil || exprImpl.F.Func == nil {
			return
		}
		name := strings.ToLower(exprImpl.F.Func.GetObjName())
		if name == "cast" && isExplicitPreparedCast(expr) {
			// The user-selected cast owns the parameter domain. Its direct marker
			// does not require runtime specialization, but a nested expression can
			// still contain a genuinely deferred overload of its own.
			for _, arg := range exprImpl.F.Args {
				rule.scanExpr(arg, false)
			}
			return
		}
		if preparedRuntimeSpecializationFunction(name) || preparedFunctionResultDependsOnRuntimeParam(expr) {
			for argIndex, arg := range exprImpl.F.Args {
				if preparedExprRequiresRuntimeSpecializationAt(name, argIndex, arg) {
					rule.needs = true
					return
				}
			}
		}
		for _, arg := range exprImpl.F.Args {
			rule.scanExpr(arg, false)
		}
	case *plan.Expr_W:
		if exprImpl.W == nil {
			return
		}
		rule.scanExpr(exprImpl.W.WindowFunc, false)
		for _, arg := range exprImpl.W.PartitionBy {
			rule.scanExpr(arg, false)
		}
		for _, order := range exprImpl.W.OrderBy {
			if order != nil {
				rule.scanExpr(order.Expr, false)
			}
		}
		if exprImpl.W.Frame != nil {
			if exprImpl.W.Frame.Start != nil {
				rule.scanExpr(exprImpl.W.Frame.Start.Val, false)
			}
			if exprImpl.W.Frame.End != nil {
				rule.scanExpr(exprImpl.W.Frame.End.Val, false)
			}
		}
	case *plan.Expr_List:
		if exprImpl.List != nil {
			for _, item := range exprImpl.List.List {
				rule.scanExpr(item, false)
			}
		}
	}
}

func preparedExprRequiresRuntimeSpecialization(functionName string, expr *plan.Expr) bool {
	if !preparedExprContainsParam(expr) {
		return false
	}
	if isExplicitPreparedCast(expr) {
		return false
	}
	// A comparison against a table column already has a prepare-time cast to
	// the column's domain. Rebinding that cast on every execute is unnecessary
	// and would force otherwise cacheable DML writes down the fresh-compile
	// path. Marker-to-marker comparisons have no such domain owner and must
	// still be specialized.
	if isPreparedNumericComparison(functionName) {
		return preparedExprHasUnboundParam(expr)
	}
	return true
}

func preparedExprRequiresRuntimeSpecializationAt(functionName string, argIndex int, expr *plan.Expr) bool {
	// LAG/LEAD/NTH_VALUE offset markers affect row selection, not the result
	// value's type. Their value argument remains domain-sensitive, while the
	// cached compile can safely retain an offset parameter after validation.
	if (functionName == "lag" || functionName == "lead" || functionName == "nth_value") && argIndex == 1 {
		return false
	}
	return preparedExprRequiresRuntimeSpecialization(functionName, expr)
}

// preparedExprHasUnboundParam distinguishes a marker whose domain is still
// owned by the comparison from one already constrained by a prepare-time
// cast. Composite-key predicates may wrap several such casts in a serial()
// helper, so inspect the complete expression rather than only its root.
func preparedExprHasUnboundParam(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	if expr.GetP() != nil || expr.GetV() != nil {
		return true
	}
	if isImplicitPreparedParamCast(expr) {
		return false
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			if preparedExprHasUnboundParam(arg) {
				return true
			}
		}
		return false
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if preparedExprHasUnboundParam(item) {
				return true
			}
		}
	}
	return false
}

func preparedExprContainsParam(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P, *plan.Expr_V:
		return true
	case *plan.Expr_F:
		if exprImpl.F == nil {
			return false
		}
		for _, arg := range exprImpl.F.Args {
			if preparedExprContainsParam(arg) {
				return true
			}
		}
	case *plan.Expr_W:
		if exprImpl.W == nil {
			return false
		}
		if preparedExprContainsParam(exprImpl.W.WindowFunc) {
			return true
		}
		for _, arg := range exprImpl.W.PartitionBy {
			if preparedExprContainsParam(arg) {
				return true
			}
		}
		for _, order := range exprImpl.W.OrderBy {
			if order != nil && preparedExprContainsParam(order.Expr) {
				return true
			}
		}
	case *plan.Expr_List:
		if exprImpl.List == nil {
			return false
		}
		for _, item := range exprImpl.List.List {
			if preparedExprContainsParam(item) {
				return true
			}
		}
	}
	return false
}

func canonicalPreparedResultFunctionName(name string) string {
	if name == "iff" {
		return "if"
	}
	return name
}

func preparedNumericResultPolymorphicFunction(name string) bool {
	switch canonicalPreparedResultFunctionName(name) {
	case "case", "if", "coalesce", "ifnull", "nullif", "greatest", "least",
		"sum", "avg", "min", "max", "any_value",
		"first_value", "last_value", "lag", "lead", "nth_value", "max_by", "max_by_non_null":
		return true
	default:
		return false
	}
}

func preparedRuntimeSpecializationFunction(name string) bool {
	if isNumericContextFunction(name) || supportsGenericNumericFunctionContext(name) ||
		preparedNumericResultPolymorphicFunction(name) {
		return true
	}
	// Result-domain-polymorphic functions must stay on the specialization path
	// even when their overload itself is stable. For example, max_by returns
	// the type of its first argument, so a binary parameter can change the
	// result-column type from the prepare-time placeholder domain.
	switch name {
	case "ntile", "sleep",
		"date_add", "date_sub", "adddate", "subdate", "timestampadd", "timestampdiff",
		"=", "<=>", "!=", "<>", "<", "<=", ">", ">=",
		"like", "ilike", "regexp", "not_regexp", "between", "not_between",
		"in", "not_in", "partition_in":
		return true
	default:
		return false
	}
}

// preparedFunctionResultDependsOnRuntimeParam uses the function registry as a
// forward-compatible fallback for polymorphic functions.  The explicit list
// above covers functions whose executor semantics are known to depend on the
// parameter domain even when their return type is fixed.  For other functions,
// compare the selected overload and return type against the legal binary
// protocol parameter domains.  This keeps new first-argument-returning
// aggregates (for example max_by variants) from silently inheriting the
// prepare-time TEXT overload.
func preparedFunctionResultDependsOnRuntimeParam(expr *plan.Expr) bool {
	if expr == nil {
		return false
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
		return false
	}
	paramArgs := make([]int, 0, len(fn.Args))
	argTypes := make([]types.Type, len(fn.Args))
	for i, arg := range fn.Args {
		if arg == nil {
			return false
		}
		argTypes[i] = makeTypeByPlan2Expr(arg)
		if preparedExprContainsParam(arg) {
			paramArgs = append(paramArgs, i)
		}
	}
	if len(paramArgs) == 0 {
		return false
	}

	for _, paramArg := range paramArgs {
		for _, candidate := range preparedRuntimeParamTypeCandidates() {
			candidateArgs := append([]types.Type(nil), argTypes...)
			candidateArgs[paramArg] = candidate
			resolved, err := function.GetFunctionByName(context.Background(), fn.Func.GetObjName(), candidateArgs)
			if err != nil {
				continue
			}
			if resolved.GetEncodedOverloadID() != fn.Func.GetObj() || !resolved.GetReturnType().Eq(makeTypeByPlan2Expr(expr)) {
				return true
			}
		}
	}
	return false
}

var preparedRuntimeParamTypes = []types.Type{
	types.T_bool.ToType(),
	types.T_int8.ToType(), types.T_int16.ToType(), types.T_int32.ToType(), types.T_int64.ToType(),
	types.T_uint8.ToType(), types.T_uint16.ToType(), types.T_uint32.ToType(), types.T_uint64.ToType(),
	types.T_float32.ToType(), types.T_float64.ToType(),
	types.New(types.T_decimal64, 18, 2), types.New(types.T_decimal128, 38, 18),
	types.New(types.T_varchar, 64, 0), types.T_text.ToType(),
	types.T_date.ToType(), types.T_datetime.ToType(), types.T_timestamp.ToType(), types.T_time.ToType(),
	types.New(types.T_varbinary, 64, 0), types.T_json.ToType(), types.T_uuid.ToType(),
}

func preparedRuntimeParamTypeCandidates() []types.Type {
	return preparedRuntimeParamTypes
}

// FillValuesOfParamsInPlanWithSpecialization replaces parameters in an
// isolated plan copy and reports whether the replacement changed an overload
// or a result-column domain. Callers that already have a cached compile must
// only invalidate that compile when this flag is true; replacing a parameter
// with a same-domain literal is otherwise handled by the cached parameter
// executor.
func FillValuesOfParamsInPlanWithSpecialization(
	ctx context.Context,
	preparePlan *Plan,
	paramVals []any,
) (*Plan, bool, error) {
	return fillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, paramVals, false)
}

// FillValuesOfParamsInPlanWithSpecializationAtPositions limits execute-time
// rebinding to the supplied parameter positions. This is used when a binary
// protocol type only owns a direct result column: unrelated markers must
// remain ParamRefs so their expression-specific overloads and metadata are
// not changed while refreshing the visible result domain.
func FillValuesOfParamsInPlanWithSpecializationAtPositions(
	ctx context.Context,
	preparePlan *Plan,
	paramVals []any,
	positions []int32,
) (*Plan, bool, error) {
	selected := make([]bool, len(paramVals))
	for _, position := range positions {
		if position >= 0 && int(position) < len(selected) {
			selected[position] = true
		}
	}
	return fillValuesOfParamsInPlanWithSpecializationSelected(
		ctx, preparePlan, paramVals, false, selected)
}

// FillValuesOfParamsInPlanWithPreparedNumericOverload is the execute-time
// path for a prepared plan whose deferred numeric overload positions were
// computed when the plan was built. The caller has already made the
// eligibility decision, so this uses the normal specialization walk without
// changing the cached plan in place.
func FillValuesOfParamsInPlanWithPreparedNumericOverload(
	ctx context.Context,
	preparePlan *Plan,
	paramVals []any,
) (*Plan, bool, error) {
	return fillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, paramVals, false)
}

// FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites performs the
// same execute-time overload/result-type specialization as
// FillValuesOfParamsInPlanWithSpecialization, while preserving the outer
// expressions consumed positionally by INSERT/UPDATE/DELETE/MERGE writers.
// Parameters in predicates and other non-write expressions are still
// replaced and rebound normally.
func FillValuesOfParamsInPlanWithSpecializationPreservingDMLWrites(
	ctx context.Context,
	preparePlan *Plan,
	paramVals []any,
) (*Plan, bool, error) {
	return fillValuesOfParamsInPlanWithSpecialization(ctx, preparePlan, paramVals, true)
}

func fillValuesOfParamsInPlanWithSpecialization(
	ctx context.Context,
	preparePlan *Plan,
	paramVals []any,
	preserveDMLWrites bool,
) (*Plan, bool, error) {
	return fillValuesOfParamsInPlanWithSpecializationSelected(
		ctx, preparePlan, paramVals, preserveDMLWrites, nil)
}

func fillValuesOfParamsInPlanWithSpecializationSelected(
	ctx context.Context,
	preparePlan *Plan,
	paramVals []any,
	preserveDMLWrites bool,
	selected []bool,
) (*Plan, bool, error) {
	switch preparePlan.Plan.(type) {
	case *plan.Plan_Tcl:
		return nil, false, moerr.NewInvalidInput(ctx, "cannot prepare TCL statement")
	case *plan.Plan_Dcl:
		if preparePlan.GetDcl().GetSetVariables() == nil {
			return nil, false, moerr.NewInvalidInput(ctx, "cannot prepare this DCL statement")
		}
	}
	if err := ValidatePreparedLagLeadParams(ctx, preparePlan, paramVals); err != nil {
		return nil, false, err
	}
	if err := ValidatePreparedPaginationParams(ctx, preparePlan, paramVals); err != nil {
		return nil, false, err
	}
	effectiveParamVals := paramVals
	if selected != nil {
		effectiveParamVals = append([]any(nil), paramVals...)
		for i := range effectiveParamVals {
			if i >= len(selected) || !selected[i] {
				effectiveParamVals[i] = ParamValue{}
			}
		}
	}
	numericPrefixSpecialization := PreparedPlanNeedsNumericPrefixSpecialization(
		preparePlan, effectiveParamVals)
	copied := DeepCopyPlan(preparePlan)
	runtimeDecimalPrefix := hasRuntimeDecimalPrefixFilter(copied, effectiveParamVals)
	switch pp := copied.Plan.(type) {

	case *plan.Plan_Ddl:
		if pp.Ddl.Query != nil {
			queryPlan := &Plan{Plan: &plan.Plan_Query{Query: pp.Ddl.Query}}
			specialized, err := replaceParamValsWithSelection(
				ctx, queryPlan, effectiveParamVals, false, selected)
			if err != nil {
				return nil, false, err
			}
			return copied, specialized || numericPrefixSpecialization, nil
		}

	case *plan.Plan_Query, *plan.Plan_Dcl:
		specialized, err := replaceParamValsWithSelection(
			ctx, copied, effectiveParamVals, preserveDMLWrites, selected)
		if err != nil {
			return nil, false, err
		}
		return copied, specialized || runtimeDecimalPrefix, nil
	}
	return copied, false, nil
}

// ValidatePreparedPaginationParams validates parameter markers used by LIMIT
// and OFFSET before the values are converted through the generic expression
// cast path. MySQL accepts NULL and Boolean user variables here, but rejects
// string, floating-point, and decimal SQL-level sources even when their text
// is an integer. Binary-protocol integer text is accepted for clients such as
// Connector/ODBC that bind integer values as MYSQL_TYPE_STRING. Negative
// signed values use the unsigned-range error required by EXECUTE.
func ValidatePreparedPaginationParams(ctx context.Context, preparePlan *Plan, paramVals []any) error {
	for _, pos := range PreparedPaginationParamPositions(preparePlan) {
		if pos < 0 || int(pos) >= len(paramVals) {
			continue
		}
		valid, negative := validatePreparedPaginationValue(paramVals[pos])
		if negative {
			return moerr.NewPreparedParamOutOfRange(ctx, "unsigned integer", "EXECUTE")
		}
		if !valid {
			return moerr.NewWrongArguments(ctx, "EXECUTE")
		}
	}
	return nil
}

// PreparedPlanHasPaginationParams reports whether a prepared plan must bind
// LIMIT/OFFSET values for each execution instead of reusing a value-filled
// compile from an earlier execution.
func PreparedPlanHasPaginationParams(preparePlan *Plan) bool {
	return len(preparedPaginationParamPositions(preparePlan)) > 0
}

// PreparedPlanHasDirectResultParams reports whether a visible SELECT result
// column is ultimately sourced from a parameter marker.
func PreparedPlanHasDirectResultParams(preparePlan *Plan) bool {
	return len(PreparedPlanDirectResultParamPositions(preparePlan)) > 0
}

// PreparedPlanDirectResultParamPositions returns the zero-based parameter
// positions that flow directly into visible SELECT result columns. Optimizer
// passes can insert projection and pass-through nodes above the original
// marker, so this traces output ordinals and ColRefs back to the marker while
// deliberately excluding parameters nested in ordinary result functions.
//
// Callers must compute this once per prepared-plan generation. It is not an
// execute-time predicate: walking the plan on every COM_STMT_EXECUTE previously
// caused a material prepared-statement hot-path regression.
func PreparedPlanDirectResultParamPositions(preparePlan *Plan) []int32 {
	if preparePlan == nil {
		return nil
	}
	query := preparePlan.GetQuery()
	if query == nil || query.StmtType != plan.Query_SELECT || len(query.Steps) == 0 {
		return nil
	}
	rootStep := len(query.Steps) - 1
	if query.HasReturning {
		if query.ReturningStep < 0 || int(query.ReturningStep) >= len(query.Steps) {
			return nil
		}
		rootStep = int(query.ReturningStep)
	}
	rootID := query.Steps[rootStep]
	if rootID < 0 || int(rootID) >= len(query.Nodes) || query.Nodes[rootID] == nil {
		return nil
	}

	positions := make(map[int32]struct{})
	seen := make(map[directResultTraceKey]struct{})
	for colPos := range query.Nodes[rootID].ProjectList {
		collectDirectResultParamPositions(query, rootID, int32(colPos), positions, seen)
	}
	if len(positions) == 0 {
		return nil
	}
	result := make([]int32, 0, len(positions))
	for position := range positions {
		result = append(result, position)
	}
	slices.Sort(result)
	return result
}

type directResultTraceKey struct {
	nodeID int32
	colPos int32
}

func collectDirectResultParamPositions(
	query *plan.Query,
	nodeID, colPos int32,
	positions map[int32]struct{},
	seen map[directResultTraceKey]struct{},
) {
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) || colPos < 0 {
		return
	}
	node := query.Nodes[nodeID]
	if node == nil {
		return
	}
	key := directResultTraceKey{nodeID: nodeID, colPos: colPos}
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}

	if int(colPos) < len(node.ProjectList) {
		expr := node.ProjectList[colPos]
		if expr == nil {
			return
		}
		if collectDirectResultParamFromExpr(expr, positions) {
			return
		}
		col := expr.GetCol()
		if col == nil {
			return
		}
		childColPos := col.ColPos
		switch node.NodeType {
		case plan.Node_UNION, plan.Node_UNION_ALL,
			plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
			plan.Node_MINUS, plan.Node_MINUS_ALL:
			// A set operation owns a common-type result rather than transparently
			// forwarding one branch. PREPARE-time coercion can materialize the
			// other branches, so direct-result specialization must not guess a new
			// common domain from the surviving plan.
			return
		case plan.Node_AGG:
			// DISTINCT over a real row source is represented by PROJECT -> AGG.
			// The AGG projection references its grouping output with rel_pos=-1;
			// following child 0 at the same ordinal skips the owned group expression.
			if col.RelPos < 0 && col.ColPos >= 0 && int(col.ColPos) < len(node.GroupBy) {
				groupExpr := node.GroupBy[col.ColPos]
				if collectDirectResultParamFromExpr(groupExpr, positions) {
					return
				}
				if groupCol := groupExpr.GetCol(); groupCol != nil && len(node.Children) == 1 {
					collectDirectResultParamPositions(
						query, node.Children[0], groupCol.ColPos, positions, seen)
				}
				return
			}
		}
		if col.RelPos >= 0 && int(col.RelPos) < len(node.Children) {
			collectDirectResultParamPositions(query, node.Children[col.RelPos], childColPos, positions, seen)
			return
		}
		if len(node.Children) == 1 {
			collectDirectResultParamPositions(query, node.Children[0], childColPos, positions, seen)
		}
		return
	}

	// Some physical pass-through nodes have no projection. Preserve the output
	// ordinal through their sole child.
	if len(node.Children) == 1 {
		collectDirectResultParamPositions(query, node.Children[0], colPos, positions, seen)
	}
}

func collectDirectResultParamFromExpr(expr *Expr, positions map[int32]struct{}) bool {
	if expr == nil {
		return false
	}
	if param := expr.GetP(); param != nil {
		positions[param.Pos] = struct{}{}
		return true
	}
	// Implicit overload casts preserve marker provenance. Explicit CAST owns a
	// fixed result domain and must remain outside direct-result specialization.
	if isImplicitPreparedParamCast(expr) {
		fn := expr.GetF()
		return len(fn.Args) > 0 && collectDirectResultParamFromExpr(fn.Args[0], positions)
	}
	return false
}

// PreparedPaginationParamPositions returns the zero-based parameter positions
// used by LIMIT/OFFSET in a prepared plan.
func PreparedPaginationParamPositions(preparePlan *Plan) []int32 {
	positions := preparedPaginationParamPositions(preparePlan)
	result := make([]int32, 0, len(positions))
	for position := range positions {
		result = append(result, position)
	}
	slices.Sort(result)
	return result
}

// PreparedJSONComparisonParamPositions returns the direct parameter markers
// whose runtime SQL type controls a JSON equality comparison. The hidden
// adapter remains in a cacheable generic plan; execution metadata supplies the
// concrete type for only these positions.
func PreparedJSONComparisonParamPositions(preparePlan *Plan) []int32 {
	if preparePlan == nil {
		return nil
	}
	positions := make(map[int32]struct{})
	seen := make(map[*plan.Expr]struct{})
	// The protobuf owner walker covers every present and future plan field. The
	// expression collector below owns tree recursion because owner walking stops
	// at each expression root.
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		collectPreparedJSONComparisonParamPositions(expr, positions, seen)
		return nil
	})

	result := make([]int32, 0, len(positions))
	for position := range positions {
		result = append(result, position)
	}
	slices.Sort(result)
	return result
}

func collectPreparedJSONComparisonParamPositions(
	expr *plan.Expr,
	positions map[int32]struct{},
	seen map[*plan.Expr]struct{},
) {
	if expr == nil {
		return
	}
	if _, ok := seen[expr]; ok {
		return
	}
	seen[expr] = struct{}{}

	switch impl := expr.Expr.(type) {
	case *plan.Expr_F:
		if impl.F.GetFunc().GetObjName() == function.JsonComparisonParamFunctionName &&
			len(impl.F.Args) == 1 {
			if param := impl.F.Args[0].GetP(); param != nil {
				positions[param.Pos] = struct{}{}
			}
		}
		for _, arg := range impl.F.Args {
			collectPreparedJSONComparisonParamPositions(arg, positions, seen)
		}
	case *plan.Expr_W:
		window := impl.W
		collectPreparedJSONComparisonParamPositions(window.GetWindowFunc(), positions, seen)
		for _, item := range window.GetPartitionBy() {
			collectPreparedJSONComparisonParamPositions(item, positions, seen)
		}
		for _, order := range window.GetOrderBy() {
			collectPreparedJSONComparisonParamPositions(order.GetExpr(), positions, seen)
		}
		if frame := window.GetFrame(); frame != nil {
			collectPreparedJSONComparisonParamPositions(frame.GetStart().GetVal(), positions, seen)
			collectPreparedJSONComparisonParamPositions(frame.GetEnd().GetVal(), positions, seen)
		}
	case *plan.Expr_List:
		for _, item := range impl.List.List {
			collectPreparedJSONComparisonParamPositions(item, positions, seen)
		}
	case *plan.Expr_Sub:
		collectPreparedJSONComparisonParamPositions(impl.Sub.GetChild(), positions, seen)
	}
}

func preparedPaginationParamPositions(preparePlan *Plan) map[int32]struct{} {
	positions := make(map[int32]struct{})
	if preparePlan == nil {
		return positions
	}
	query := preparePlan.GetQuery()
	if query == nil && preparePlan.GetDdl() != nil {
		query = preparePlan.GetDdl().GetQuery()
	}
	if query == nil {
		return positions
	}

	for _, node := range query.GetNodes() {
		if node == nil {
			continue
		}
		collectPreparedParamPositions(node.GetLimit(), positions)
		collectPreparedParamPositions(node.GetOffset(), positions)
	}
	return positions
}

func collectPreparedParamPositions(expr *Expr, positions map[int32]struct{}) {
	if expr == nil {
		return
	}
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_P:
		positions[exprImpl.P.Pos] = struct{}{}
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			collectPreparedParamPositions(arg, positions)
		}
	case *plan.Expr_List:
		for _, item := range exprImpl.List.List {
			collectPreparedParamPositions(item, positions)
		}
	}
}

func validatePreparedPaginationValue(value any) (valid bool, negative bool) {
	kind := vector.PrepareParamNone
	isBinaryProtocol := false
	if param, ok := value.(ParamValue); ok {
		value = param.Value
		kind = param.PrepareParamKind
		isBinaryProtocol = param.IsBinaryProtocol
	}
	if value == nil {
		return true, false
	}
	if kind != vector.PrepareParamNone && kind != vector.PrepareParamInteger && kind != vector.PrepareParamBoolean {
		return false, false
	}

	switch value := value.(type) {
	case int:
		return true, value < 0
	case int8:
		return true, value < 0
	case int16:
		return true, value < 0
	case int32:
		return true, value < 0
	case int64:
		return true, value < 0
	case uint, uint8, uint16, uint32, uint64, types.MoYear, bool:
		return true, false
	case string:
		if kind == vector.PrepareParamBoolean {
			return value == "0" || value == "1", false
		}
		// Connector/ODBC converts SQL_INTEGER and SQL_BIGINT values to their
		// decimal text form and binds them as MYSQL_TYPE_STRING for server-side
		// prepared statements. Accept that wire representation only when the
		// value came from COM_STMT_EXECUTE. SQL PREPARE user variables and
		// other non-binary-protocol text values keep the stricter validation.
		if kind != vector.PrepareParamInteger && !isBinaryProtocol {
			return false, false
		}
		if strings.HasPrefix(value, "-") {
			digits := strings.TrimPrefix(value, "-")
			if digits == "" {
				return false, false
			}
			if _, err := strconv.ParseUint(digits, 10, 64); err != nil {
				return false, false
			}
			return true, strings.TrimLeft(digits, "0") != ""
		}
		_, err := strconv.ParseUint(value, 10, 64)
		return err == nil, false
	default:
		return false, false
	}
}

type ParamValue struct {
	Value any
	IsBin bool
	// IsBinaryProtocol records that the value came from COM_STMT_EXECUTE.
	// It is intentionally separate from IsBin: a VAR_STRING parameter is a
	// binary-protocol value without being a binary string literal.
	IsBinaryProtocol bool
	PrepareParamKind vector.PrepareParamKind
	// SourceType is the logical type of a SQL EXECUTE USING user variable. It
	// is deliberately separate from RuntimeType: SQL parameters are transported
	// through a text vector, and their source type is used only after an
	// arithmetic consumer establishes a numeric domain. Comparisons keep their
	// existing common-type and numeric-prefix contracts.
	SourceType    types.Type
	HasSourceType bool
	// RuntimeType is the type advertised by the binary-protocol parameter
	// binding.  Prepared plans deliberately keep parameter markers as TEXT
	// while they are cached, so the execute-time copy can use this optional
	// type to rebind overloaded functions and result metadata without mutating
	// the cached plan.
	RuntimeType    types.Type
	HasRuntimeType bool
	// DirectResultType is the wire-visible DECIMAL domain parsed from the same
	// binary-protocol lexeme as RuntimeType. RuntimeType keeps the normalized
	// numeric-prefix domain used by common-type consumers; a direct result keeps
	// the visible scale when representable and otherwise uses the normalized
	// domain for lexemes whose only excess digits are removable trailing zeroes.
	DirectResultType    types.Type
	HasDirectResultType bool
	// MaterializedValue is a bounded canonical DECIMAL lexeme produced by the
	// protocol scanner. Typed literal construction uses it instead of reparsing
	// the potentially max-packet-sized raw Value.
	MaterializedValue string
	// RetainParamRef records that a specialized query plan will be cached and
	// therefore must retain this parameter as runtime provenance even when the
	// parameter itself is unrelated to numeric-prefix specialization.
	RetainParamRef bool
	// EnableNumericPrefix records that the deployment-wide protocol version can
	// execute planner-injected MySQL numeric-prefix casts.  Keep the negotiated
	// capability on each value so execute-time plan specialization does not need
	// to guess a service identity from context.Context.
	EnableNumericPrefix bool
}

// PreparedRuntimeTypeFromString infers the narrowest numeric type needed by a
// textual value when it is used as an argument to a numeric overload.  A
// direct SELECT ? remains TEXT unless the protocol supplied an explicit
// numeric type; this helper is only used while rebinding a function argument.
func PreparedRuntimeTypeFromString(value string) (types.Type, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return types.Type{}, false
	}
	if strings.ContainsAny(value, ".eE") {
		return preparedDecimalType(value)
	}
	negative := strings.HasPrefix(value, "-")
	value = strings.TrimPrefix(value, "+")
	if negative {
		value = value[1:]
	}
	if value == "" {
		return types.Type{}, false
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return preparedDecimalType(value)
	}
	if negative {
		if parsed <= uint64(math.MaxInt64)+1 {
			return types.T_int64.ToType(), true
		}
		return types.Type{}, false
	}
	if parsed <= uint64(math.MaxInt64) {
		return types.T_int64.ToType(), true
	}
	return types.T_uint64.ToType(), true
}

// PreparedNumericPrefixTypeFromString derives the exact numeric domain of the
// prefix accepted by the CAST layer. It is intentionally separate from
// PreparedRuntimeTypeFromString: arbitrary text remains text outside a
// DECIMAL-aware common-type consumer, while that consumer follows MySQL and
// treats a missing numeric prefix as zero.
func PreparedNumericPrefixTypeFromString(value string) types.Type {
	prefix, ok := function.GetNumericStringPrefix(value)
	if !ok {
		return types.New(types.T_decimal64, 1, 0)
	}

	unsigned := prefix
	if unsigned[0] == '+' || unsigned[0] == '-' {
		unsigned = unsigned[1:]
	}
	mantissa := unsigned
	exponentText := ""
	if exponentAt := strings.IndexAny(unsigned, "eE"); exponentAt >= 0 {
		mantissa = unsigned[:exponentAt]
		exponentText = unsigned[exponentAt+1:]
	}

	digits := strings.ReplaceAll(mantissa, ".", "")
	nonZero := strings.TrimLeft(digits, "0")
	if nonZero == "" {
		return types.New(types.T_decimal64, 1, 0)
	}

	fractionalDigits := int64(0)
	if pointAt := strings.IndexByte(mantissa, '.'); pointAt >= 0 {
		fractionalDigits = int64(len(mantissa) - pointAt - 1)
	}
	trailingZeros := len(nonZero) - len(strings.TrimRight(nonZero, "0"))
	exponentCompensation := -fractionalDigits + int64(trailingZeros)
	exponent, bounded := preparedBoundedDecimalExponent(exponentText, exponentCompensation)
	if !bounded {
		return types.T_float64.ToType()
	}

	coefficient := nonZero[:len(nonZero)-trailingZeros]
	decimalExponent := exponent

	integralWidth := int64(0)
	scale := int64(0)
	if decimalExponent >= 0 {
		integralWidth = int64(len(coefficient)) + decimalExponent
	} else {
		scale = -decimalExponent
		integralWidth = int64(len(coefficient)) - scale
		if integralWidth < 0 {
			integralWidth = 0
		}
	}
	width := integralWidth + scale
	if width < 1 {
		width = 1
	}
	if width > int64(types.T_decimal256.ToType().Width) || scale > int64(types.T_decimal256.ToType().Width) {
		return types.T_float64.ToType()
	}

	w, s := int32(width), int32(scale)
	switch {
	case w <= types.T_decimal64.ToType().Width:
		return types.New(types.T_decimal64, w, s)
	case w <= types.T_decimal128.ToType().Width:
		return types.New(types.T_decimal128, w, s)
	default:
		return types.New(types.T_decimal256, w, s)
	}
}

func preparedBoundedDecimalExponent(value string, compensation int64) (int64, bool) {
	if value == "" {
		return compensation, absInt64Within(compensation, int64(types.T_decimal256.ToType().Width))
	}
	negative := value[0] == '-'
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	value = strings.TrimLeft(value, "0")
	if value == "" {
		return compensation, absInt64Within(compensation, int64(types.T_decimal256.ToType().Width))
	}
	// Parse at most an int64-sized exponent after discarding leading zeroes.
	// This keeps attacker-sized inputs O(n) and avoids big integers or
	// input-length allocations. Any larger magnitude cannot be compensated by
	// the bounded mantissa length into a Decimal256 domain.
	if len(value) > 19 {
		return 0, false
	}
	exponent, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, false
	}
	if negative {
		exponent = -exponent
	}
	if compensation > 0 && exponent > math.MaxInt64-compensation ||
		compensation < 0 && exponent < math.MinInt64-compensation {
		return 0, false
	}
	netExponent := exponent + compensation
	return netExponent, absInt64Within(netExponent, int64(types.T_decimal256.ToType().Width))
}

func absInt64Within(value, limit int64) bool {
	return value >= -limit && value <= limit
}

// PreparedDecimalRuntimeTypes parses one complete binary-protocol DECIMAL
// lexeme and returns both domains needed by prepared execution. normalized is
// the trailing-zero-free domain used by numeric-prefix/common-type consumers;
// visible preserves the lexeme's effective scale when representable and falls
// back to normalized when redundant trailing zeroes alone exceed DECIMAL256.
// The scan performs no input-length allocation, even for a
// max_allowed_packet-sized value.
func PreparedDecimalRuntimeTypes(value string) (normalized, visible types.Type, ok bool) {
	normalized, visible, _, ok = preparedDecimalRuntimeDomains(value, false)
	return normalized, visible, ok
}

// PreparedDecimalRuntimeDomains additionally returns a bounded canonical
// lexeme suitable for typed literal materialization.
func PreparedDecimalRuntimeDomains(value string) (normalized, visible types.Type, canonical string, ok bool) {
	return preparedDecimalRuntimeDomains(value, true)
}

func preparedDecimalRuntimeDomains(
	value string,
	materialize bool,
) (normalized, visible types.Type, canonical string, ok bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return types.Type{}, types.Type{}, "", false
	}

	pos := 0
	negative := value[pos] == '-'
	if value[pos] == '+' || value[pos] == '-' {
		pos++
	}
	if pos == len(value) {
		return types.Type{}, types.Type{}, "", false
	}

	var digitCount, leadingZeros, fractionalDigits, trailingZeros int64
	var coefficient [76]byte
	coefficientStored := 0
	seenDigit, seenPoint, seenNonZero := false, false, false
	for pos < len(value) && value[pos] != 'e' && value[pos] != 'E' {
		ch := value[pos]
		switch {
		case ch >= '0' && ch <= '9':
			seenDigit = true
			digitCount++
			if seenPoint {
				fractionalDigits++
			}
			if !seenNonZero {
				if ch == '0' {
					leadingZeros++
				} else {
					seenNonZero = true
				}
			} else if ch == '0' {
				trailingZeros++
			} else {
				trailingZeros = 0
			}
			if seenNonZero && coefficientStored < len(coefficient) {
				coefficient[coefficientStored] = ch
				coefficientStored++
			}
		case ch == '.' && !seenPoint:
			seenPoint = true
		default:
			return types.Type{}, types.Type{}, "", false
		}
		pos++
	}
	if !seenDigit {
		return types.Type{}, types.Type{}, "", false
	}

	exponent, exponentState, valid := scanPreparedDecimalExponent(value[pos:])
	if !valid {
		return types.Type{}, types.Type{}, "", false
	}
	coefficientDigits := digitCount - leadingZeros
	if coefficientDigits == 0 {
		// Zero is representable independently of a positive exponent. Only the
		// effective negative scale can exceed Decimal256.
		var scale int64
		switch exponentState {
		case preparedExponentHugePositive:
			scale = 0
		case preparedExponentHugeNegative:
			return types.Type{}, types.Type{}, "", false
		default:
			netExponent, bounded := addPreparedDecimalExponent(exponent, -fractionalDigits)
			if !bounded || netExponent < -int64(types.T_decimal256.ToType().Width) {
				return types.Type{}, types.Type{}, "", false
			}
			if netExponent < 0 {
				scale = -netExponent
			}
		}
		visible, ok = preparedDecimalTypeForWidth(max(int64(1), scale), scale)
		if !ok {
			return types.Type{}, types.Type{}, "", false
		}
		canonical := ""
		if materialize {
			canonical = "0"
		}
		return types.New(types.T_decimal64, 1, 0), visible, canonical, true
	}
	if exponentState != preparedExponentFinite {
		return types.Type{}, types.Type{}, "", false
	}

	visibleExponent, visibleExponentBounded := addPreparedDecimalExponent(exponent, -fractionalDigits)
	normalizedExponent, bounded := addPreparedDecimalExponent(
		exponent, -fractionalDigits+trailingZeros)
	if !bounded {
		return types.Type{}, types.Type{}, "", false
	}
	normalizedCoefficientDigits := coefficientDigits - trailingZeros
	normalized, ok = preparedDecimalTypeFromCoefficient(normalizedCoefficientDigits, normalizedExponent)
	if !ok || normalizedCoefficientDigits > int64(len(coefficient)) {
		return types.Type{}, types.Type{}, "", false
	}

	canonicalCoefficientDigits := coefficientDigits
	canonicalExponent := visibleExponent
	if visibleExponentBounded {
		visible, ok = preparedDecimalTypeFromCoefficient(coefficientDigits, visibleExponent)
	}
	if !visibleExponentBounded || !ok {
		// A DECIMAL transport lexeme can expose more than 76 coefficient digits
		// solely through removable trailing zeroes. Preserve the exact value by
		// falling back to its normalized domain instead of rejecting it before
		// normalization or materializing the unbounded visible spelling.
		visible = normalized
		canonicalCoefficientDigits = normalizedCoefficientDigits
		canonicalExponent = normalizedExponent
	}
	if canonicalCoefficientDigits > int64(len(coefficient)) {
		return types.Type{}, types.Type{}, "", false
	}
	if !materialize {
		return normalized, visible, "", true
	}
	var canonicalBuilder strings.Builder
	canonicalBuilder.Grow(int(canonicalCoefficientDigits) + 21)
	if negative {
		canonicalBuilder.WriteByte('-')
	}
	canonicalBuilder.Write(coefficient[:int(canonicalCoefficientDigits)])
	if canonicalExponent != 0 {
		canonicalBuilder.WriteByte('e')
		canonicalBuilder.WriteString(strconv.FormatInt(canonicalExponent, 10))
	}
	return normalized, visible, canonicalBuilder.String(), true
}

type preparedExponentState uint8

const (
	preparedExponentFinite preparedExponentState = iota
	preparedExponentHugePositive
	preparedExponentHugeNegative
)

func scanPreparedDecimalExponent(value string) (int64, preparedExponentState, bool) {
	if value == "" {
		return 0, preparedExponentFinite, true
	}
	if value[0] != 'e' && value[0] != 'E' {
		return 0, preparedExponentFinite, false
	}
	value = value[1:]
	if value == "" {
		return 0, preparedExponentFinite, false
	}
	negative := false
	if value[0] == '+' || value[0] == '-' {
		negative = value[0] == '-'
		value = value[1:]
	}
	if value == "" {
		return 0, preparedExponentFinite, false
	}
	for len(value) > 0 && value[0] == '0' {
		value = value[1:]
	}
	if value == "" {
		return 0, preparedExponentFinite, true
	}
	for i := range len(value) {
		if value[i] < '0' || value[i] > '9' {
			return 0, preparedExponentFinite, false
		}
	}
	if len(value) > 19 {
		if negative {
			return 0, preparedExponentHugeNegative, true
		}
		return 0, preparedExponentHugePositive, true
	}
	magnitude, err := strconv.ParseUint(value, 10, 64)
	if err != nil || magnitude > math.MaxInt64 {
		if negative {
			return 0, preparedExponentHugeNegative, true
		}
		return 0, preparedExponentHugePositive, true
	}
	exponent := int64(magnitude)
	if negative {
		exponent = -exponent
	}
	return exponent, preparedExponentFinite, true
}

func addPreparedDecimalExponent(exponent, compensation int64) (int64, bool) {
	if compensation > 0 && exponent > math.MaxInt64-compensation ||
		compensation < 0 && exponent < math.MinInt64-compensation {
		return 0, false
	}
	return exponent + compensation, true
}

func preparedDecimalTypeFromCoefficient(coefficientDigits, exponent int64) (types.Type, bool) {
	maxWidth := int64(types.T_decimal256.ToType().Width)
	if coefficientDigits < 1 || exponent < -maxWidth || exponent > maxWidth {
		return types.Type{}, false
	}
	if exponent >= 0 {
		return preparedDecimalTypeForWidth(coefficientDigits+exponent, 0)
	}
	scale := -exponent
	return preparedDecimalTypeForWidth(max(coefficientDigits, scale), scale)
}

func preparedDecimalTypeForWidth(width, scale int64) (types.Type, bool) {
	maxWidth := int64(types.T_decimal256.ToType().Width)
	if width < 1 || width > maxWidth || scale < 0 || scale > maxWidth || scale > width {
		return types.Type{}, false
	}
	w, s := int32(width), int32(scale)
	switch {
	case w <= types.T_decimal64.ToType().Width:
		return types.New(types.T_decimal64, w, s), true
	case w <= types.T_decimal128.ToType().Width:
		return types.New(types.T_decimal128, w, s), true
	default:
		return types.New(types.T_decimal256, w, s), true
	}
}

// PreparedDecimalRuntimeType derives the scale-preserving visible domain of a
// complete binary-protocol DECIMAL lexeme.
func PreparedDecimalRuntimeType(value string) (types.Type, bool) {
	_, visible, ok := PreparedDecimalRuntimeTypes(value)
	return visible, ok
}

func preparedDecimalType(value string) (types.Type, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return types.Type{}, false
	}
	unsigned := value
	if unsigned[0] == '+' || unsigned[0] == '-' {
		unsigned = unsigned[1:]
	}
	if unsigned == "" {
		return types.Type{}, false
	}
	mantissa := unsigned
	if exponentAt := strings.IndexAny(unsigned, "eE"); exponentAt >= 0 {
		if strings.ContainsAny(unsigned[exponentAt+1:], "eE") ||
			!isDecimalExponent(unsigned[exponentAt+1:]) {
			return types.Type{}, false
		}
		mantissa = unsigned[:exponentAt]
	}
	if !isDecimalMantissa(mantissa) {
		return types.Type{}, false
	}
	typ := PreparedNumericPrefixTypeFromString(value)
	if !typ.IsDecimal() {
		return types.Type{}, false
	}
	return typ, true
}

// preparedNumericPrefixParamPositions returns the parameter positions that
// independently satisfy the decimal-aware common-type eligibility check. The
// frontend normally computes this one-position-at-a-time before calling the
// replacement path; doing the same here keeps direct callers (and tests) from
// combining the numeric-prefix and COM_STMT text-comparison conversions for an
// unrelated marker in the same plan.
func preparedNumericPrefixParamPositions(preparePlan *Plan, paramVals []any) map[int]bool {
	positions := make(map[int]bool)
	candidates := make([]any, len(paramVals))
	for i, value := range paramVals {
		param, ok := value.(ParamValue)
		if !ok {
			candidates[i] = value
			continue
		}
		// A text COM_STMT value must use the common DOUBLE comparison domain
		// whenever its numeric prefix is absent, out of range, or loses DOUBLE
		// precision for the target column. Numeric-prefix casts are intended for
		// index keys and would otherwise narrow the value to DECIMAL64(1,0) (for
		// example, `foo`) or round a >2^53 integer before the filter sees it.
		// This guard is specific to COM_STMT text packets. SQL EXECUTE and direct
		// unit-test callers use EnableNumericPrefix for the decimal-aware
		// common-type path, where an implicit DECIMAL cast is the intended domain
		// rather than a text-comparison fallback. Keep all remaining eligible
		// markers together: a common-value expression such as COALESCE can need
		// more than one marker to establish its numeric context.
		if param.IsBinaryProtocol &&
			preparedTextParamNeedsDoubleComparison(preparePlan, i, param.Value) {
			param.EnableNumericPrefix = false
		}
		candidates[i] = param
	}
	if !PreparedPlanNeedsNumericPrefixSpecialization(preparePlan, candidates) {
		return positions
	}
	for i, value := range candidates {
		param, ok := value.(ParamValue)
		if ok && param.EnableNumericPrefix {
			positions[i] = true
		}
	}
	return positions
}

func preparedTextParamNeedsDoubleComparison(preparePlan *Plan, position int, value any) bool {
	if preparePlan == nil || value == nil {
		return false
	}
	text, ok := value.(string)
	if !ok {
		text = fmt.Sprint(value)
	}
	foundTarget := false
	needsFallback := false
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		if needsFallback {
			return nil
		}
		if expr.GetF() == nil || expr.GetF().Func == nil ||
			strings.ToLower(expr.GetF().Func.GetObjName()) != "cast" ||
			!isImplicitPreparedParamCast(expr) {
			return nil
		}
		paramPosition, ok := implicitPreparedParamPosition(expr)
		if !ok || paramPosition != position {
			return nil
		}
		foundTarget = true
		needsFallback = preparedComparisonTextNeedsDoubleFallback(text, expr.Typ)
		return nil
	})
	if needsFallback {
		return true
	}
	// A marker without an implicit target cast can only be admitted by a
	// decimal-aware context after its string has been parsed.  Keep nonnumeric
	// strings in the generic DOUBLE path rather than synthesizing a decimal
	// prefix cast.
	return !foundTarget && func() bool {
		_, ok := function.GetNumericStringPrefix(text)
		return !ok
	}()
}

func exprContainsPreparedPosition(expr *plan.Expr, position int) bool {
	if expr == nil {
		return false
	}
	if param := expr.GetP(); param != nil && param.Pos == int32(position) {
		return true
	}
	if literal := expr.GetLit(); literal != nil && exprContainsPreparedPosition(literal.Src, position) {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprContainsPreparedPosition(arg, position) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsPreparedPosition(item, position) {
				return true
			}
		}
	}
	return false
}

// preparedPlanHasStaticDecimalPeer reports whether a marker participates in a
// function whose other operand contains a real DECIMAL expression. An integer
// column may cause the frontend to tentatively classify a text packet as a
// possible DECIMAL candidate, but that must not suppress the COM_STMT
// text-to-DOUBLE comparison path (for example, an indexed `id = ?` filter).
func preparedPlanHasStaticDecimalPeer(preparePlan *Plan, position int) bool {
	found := false
	_ = plan.VisitExpressionsInOwner(preparePlan, func(expr *plan.Expr) error {
		if found {
			return nil
		}
		fn := expr.GetF()
		if fn == nil {
			return nil
		}
		for _, arg := range fn.Args {
			if !exprContainsPreparedPosition(arg, position) {
				continue
			}
			for _, peer := range fn.Args {
				if peer != arg && exprHasStaticDecimalOperand(peer, position) {
					found = true
					return nil
				}
			}
		}
		return nil
	})
	return found
}

func exprHasStaticDecimalOperand(expr *plan.Expr, position int) bool {
	if expr == nil || exprContainsPreparedPosition(expr, position) {
		return false
	}
	if types.T(expr.Typ.Id).IsDecimal() {
		return true
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprHasStaticDecimalOperand(arg, position) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprHasStaticDecimalOperand(item, position) {
				return true
			}
		}
	}
	return false
}

// PreparedRuntimeDecimalTypeFromString infers metadata for a parameter whose
// binary protocol type is explicitly DECIMAL/NEWDECIMAL.
func PreparedRuntimeDecimalTypeFromString(value string) (types.Type, bool) {
	return preparedDecimalType(value)
}

// preparedExponentType validates a finite floating-point exponent form. It is
// retained for callers that need the approximate domain.
func preparedExponentType(value string) (types.Type, bool) {
	value = strings.TrimSpace(value)
	if value == "" || !strings.ContainsAny(value, "eE") {
		return types.Type{}, false
	}
	unsigned := value
	if unsigned[0] == '+' || unsigned[0] == '-' {
		unsigned = unsigned[1:]
	}
	exponentAt := strings.IndexAny(unsigned, "eE")
	if exponentAt <= 0 || strings.ContainsAny(unsigned[exponentAt+1:], "eE") ||
		!isDecimalMantissa(unsigned[:exponentAt]) || !isDecimalExponent(unsigned[exponentAt+1:]) {
		return types.Type{}, false
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsInf(parsed, 0) {
		return types.Type{}, false
	}
	return types.T_float64.ToType(), true
}

// preparedNumericComparisonTextType selects the DOUBLE comparison domain for
// textual operands in numeric comparisons.
func preparedNumericComparisonTextType() types.Type {
	return types.T_float64.ToType()
}

func isDecimalMantissa(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) > 2 || len(parts) == 0 || (parts[0] == "" && (len(parts) == 1 || parts[1] == "")) {
		return false
	}
	for _, part := range parts {
		for i := 0; i < len(part); i++ {
			if part[i] < '0' || part[i] > '9' {
				return false
			}
		}
	}
	return parts[0] != "" || (len(parts) == 2 && parts[1] != "")
}

func isDecimalExponent(value string) bool {
	if value == "" {
		return false
	}
	if value[0] == '+' || value[0] == '-' {
		value = value[1:]
	}
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func preparedWindowArgumentParamPosition(expr *Expr) (int32, bool) {
	if param := expr.GetP(); param != nil {
		return param.Pos, true
	}
	fn := expr.GetF()
	if fn == nil || fn.GetFunc().GetObjName() != "cast" || len(fn.Args) == 0 {
		return 0, false
	}
	param := fn.Args[0].GetP()
	if param == nil {
		return 0, false
	}
	return param.Pos, true
}

func isPositivePreparedInteger(value any) bool {
	kind := vector.PrepareParamNone
	if paramValue, ok := value.(ParamValue); ok {
		value = paramValue.Value
		kind = paramValue.PrepareParamKind
	}
	if value == nil || kind != vector.PrepareParamNone && kind != vector.PrepareParamInteger {
		return false
	}

	switch value := value.(type) {
	case int:
		return value > 0
	case int8:
		return value > 0
	case int16:
		return value > 0
	case int32:
		return value > 0
	case int64:
		return value > 0
	case uint:
		return value > 0 && uint64(value) <= uint64(math.MaxInt64)
	case uint8:
		return value > 0
	case uint16:
		return value > 0
	case uint32:
		return value > 0
	case uint64:
		return value > 0 && value <= uint64(math.MaxInt64)
	case types.MoYear:
		return value > 0
	case string:
		if kind != vector.PrepareParamInteger {
			return false
		}
		parsed, err := strconv.ParseUint(value, 10, 63)
		return err == nil && parsed > 0
	default:
		return false
	}
}

// preparedRuntimeParamExpr materializes a binary-protocol parameter using the
// same literal representation that the expression executor uses for a value of
// runtimeType.  Keeping only a numeric Expr.Typ is not sufficient: the
// executor dispatches on Literal.Value, and a Sval always produces a VARCHAR
// vector even when the surrounding expression advertises a numeric type.
// PreparedRuntimeParamExpr materializes value as a typed prepared-parameter
// expression. Frontend query-aware SET evaluation uses it to preserve the
// specialized result domain after a scalar subquery is executed separately.
func PreparedRuntimeParamExpr(ctx context.Context, value any, isBin bool, runtimeType types.Type) (*Expr, error) {
	return preparedRuntimeParamExpr(ctx, value, isBin, runtimeType)
}

func preparedSQLExecuteNumericParamExpr(
	ctx context.Context,
	value any,
	isBin bool,
	sourceType types.Type,
) (*Expr, error) {
	source, err := preparedRuntimeParamExpr(ctx, value, isBin, sourceType)
	if err != nil {
		return nil, err
	}
	if isStringBackedType(sourceType) {
		if _, ok := function.GetNumericStringPrefix(fmt.Sprintf("%v", value)); !ok {
			// An entirely non-numeric string must retain the existing cast/error
			// contract of the prepared expression. The approximate arithmetic
			// source path only owns strings with a MySQL numeric prefix.
			return nil, nil
		}
		// A SQL string user variable enters arithmetic through MySQL's
		// approximate numeric-prefix domain. Keep that distinct from a DECIMAL
		// user variable, even though both arrive in the frontend's text vector.
		return appendExplicitCastBeforeExpr(ctx, source, makeSimplePlan2Type(types.T_float64))
	}
	if sourceType.Oid == types.T_bool {
		return makePlan2CastExpr(ctx, source, makeSimplePlan2Type(types.T_int64))
	}
	if sourceType.Oid == types.T_bit {
		return makePlan2CastExpr(ctx, source, makeSimplePlan2Type(types.T_uint64))
	}
	if sourceType.IsNumeric() || sourceType.Oid == types.T_year {
		return source, nil
	}
	return nil, nil
}

func preparedRuntimeParamExpr(ctx context.Context, value any, isBin bool, runtimeType types.Type) (*Expr, error) {
	rawText := fmt.Sprintf("%v", value)
	text := strings.TrimSpace(rawText)
	paramType := makePlan2Type(&runtimeType)
	makeLiteral := func(literal any) *Expr {
		lit := &plan.Literal{IsBin: isBin}
		switch value := literal.(type) {
		case *plan.Literal_Bval:
			lit.Value = value
		case *plan.Literal_I8Val:
			lit.Value = value
		case *plan.Literal_I16Val:
			lit.Value = value
		case *plan.Literal_I32Val:
			lit.Value = value
		case *plan.Literal_I64Val:
			lit.Value = value
		case *plan.Literal_U8Val:
			lit.Value = value
		case *plan.Literal_U16Val:
			lit.Value = value
		case *plan.Literal_U32Val:
			lit.Value = value
		case *plan.Literal_U64Val:
			lit.Value = value
		case *plan.Literal_Fval:
			lit.Value = value
		case *plan.Literal_Dval:
			lit.Value = value
		case *plan.Literal_Sval:
			lit.Value = value
		default:
			lit.Value = &plan.Literal_Sval{Sval: fmt.Sprintf("%v", literal)}
		}
		return &Expr{Typ: paramType, Expr: &plan.Expr_Lit{Lit: lit}}
	}
	castText := func() (*Expr, error) {
		return makePlan2CastExpr(
			ctx,
			makePlan2StringConstExprWithType(rawText, isBin),
			paramType,
		)
	}

	switch runtimeType.Oid {
	case types.T_bool:
		value, err := strconv.ParseBool(text)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_Bval{Bval: value}), nil
	case types.T_int8:
		value, err := strconv.ParseInt(text, 10, 8)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_I8Val{I8Val: int32(value)}), nil
	case types.T_int16:
		value, err := strconv.ParseInt(text, 10, 16)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_I16Val{I16Val: int32(value)}), nil
	case types.T_int32:
		value, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_I32Val{I32Val: int32(value)}), nil
	case types.T_int64:
		value, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_I64Val{I64Val: value}), nil
	case types.T_uint8:
		value, err := strconv.ParseUint(text, 10, 8)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_U8Val{U8Val: uint32(value)}), nil
	case types.T_uint16:
		value, err := strconv.ParseUint(text, 10, 16)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_U16Val{U16Val: uint32(value)}), nil
	case types.T_uint32:
		value, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_U32Val{U32Val: uint32(value)}), nil
	case types.T_uint64, types.T_bit:
		value, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_U64Val{U64Val: value}), nil
	case types.T_year:
		value, err := strconv.ParseInt(text, 10, 32)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_I32Val{I32Val: int32(value)}), nil
	case types.T_float32:
		value, err := strconv.ParseFloat(text, 32)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_Fval{Fval: float32(value)}), nil
	case types.T_float64:
		value, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return castText()
		}
		return makeLiteral(&plan.Literal_Dval{Dval: value}), nil
	case types.T_decimal64:
		width, scale := runtimeType.Width, runtimeType.Scale
		if width <= 0 || scale < 0 || scale > width {
			if inferred, ok := preparedDecimalType(text); ok && inferred.Oid == types.T_decimal64 {
				width, scale = inferred.Width, inferred.Scale
			}
		}
		value, err := types.ParseDecimal64(text, width, scale)
		if err != nil {
			return castText()
		}
		paramType.Width, paramType.Scale = width, scale
		return &Expr{
			Typ: paramType,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				IsBin: isBin,
				Value: &plan.Literal_Decimal64Val{Decimal64Val: &plan.Decimal64{A: int64(value)}},
			}},
		}, nil
	case types.T_decimal128:
		width, scale := runtimeType.Width, runtimeType.Scale
		if width <= 0 || scale < 0 || scale > width {
			if inferred, ok := preparedDecimalType(text); ok && inferred.Oid == types.T_decimal128 {
				width, scale = inferred.Width, inferred.Scale
			}
		}
		value, err := types.ParseDecimal128(text, width, scale)
		if err != nil {
			return castText()
		}
		paramType.Width, paramType.Scale = width, scale
		return &Expr{
			Typ: paramType,
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				IsBin: isBin,
				Value: &plan.Literal_Decimal128Val{Decimal128Val: &plan.Decimal128{
					A: int64(value.B0_63), B: int64(value.B64_127),
				}},
			}},
		}, nil
	case types.T_decimal256:
		// The plan literal protocol has no Decimal256 oneof.  Keep the
		// conversion explicit so execution still materializes a Decimal256
		// vector instead of treating the value as VARCHAR.
		return castText()
	default:
		return makeLiteral(&plan.Literal_Sval{Sval: rawText}), nil
	}
}

func isNonNegativePreparedInteger(value any) bool {
	kind := vector.PrepareParamNone
	if paramValue, ok := value.(ParamValue); ok {
		value = paramValue.Value
		kind = paramValue.PrepareParamKind
	}
	if value == nil || kind == vector.PrepareParamFloat || kind == vector.PrepareParamDecimal {
		return false
	}
	if kind == vector.PrepareParamBoolean {
		switch value := value.(type) {
		case bool:
			return true
		case string:
			return value == "0" || value == "1"
		default:
			return false
		}
	}
	if _, ok := value.(bool); ok {
		return true
	}
	valid, negative := validatePreparedPaginationValue(ParamValue{
		Value:            value,
		PrepareParamKind: kind,
	})
	return valid && !negative
}

func replaceParamVals(
	ctx context.Context,
	plan0 *Plan,
	paramVals []any,
	preserveDMLWriteArgs ...bool,
) (bool, error) {
	preserveDMLWrites := len(preserveDMLWriteArgs) > 0 && preserveDMLWriteArgs[0]
	return replaceParamValsWithSelection(ctx, plan0, paramVals, preserveDMLWrites, nil)
}

func replaceParamValsWithSelection(
	ctx context.Context,
	plan0 *Plan,
	paramVals []any,
	preserveDMLWrites bool,
	selected []bool,
) (bool, error) {
	directResultPositions := PreparedPlanDirectResultParamPositions(plan0)
	params := make([]*Expr, len(paramVals))
	sqlExecuteNumericParams := make([]*Expr, len(paramVals))
	sqlExecuteStringBackedParams := make([]bool, len(paramVals))
	var err error
	for i, val := range paramVals {
		if selected != nil && (i >= len(selected) || !selected[i]) {
			continue
		}
		isBin := false
		runtimeType := types.T_text.ToType()
		hasRuntimeType := false
		numericPrefixSource := false
		retainParamRef := false
		if param, ok := val.(ParamValue); ok {
			val = param.Value
			if param.MaterializedValue != "" {
				val = param.MaterializedValue
			}
			isBin = param.IsBin
			runtimeType = param.RuntimeType
			hasRuntimeType = param.HasRuntimeType
			numericPrefixSource = param.EnableNumericPrefix
			retainParamRef = param.RetainParamRef
			if param.HasSourceType && param.Value != nil {
				sqlExecuteStringBackedParams[i] = isStringBackedType(param.SourceType)
				sqlExecuteNumericParams[i], err = preparedSQLExecuteNumericParamExpr(
					ctx, param.Value, param.IsBin, param.SourceType)
				if err != nil {
					return false, err
				}
				if sqlExecuteNumericParams[i] != nil && (numericPrefixSource || retainParamRef) {
					attachPreparedRuntimeParamSource(sqlExecuteNumericParams[i], &plan.Expr{
						Typ:  makePlan2Type(&param.SourceType),
						Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: int32(i)}},
					})
				}
			}
		}
		paramType := plan.Type{Id: int32(types.T_text)}
		if hasRuntimeType {
			paramType = makePlan2Type(&runtimeType)
		}
		_, directRuntimeResult := slices.BinarySearch(directResultPositions, int32(i))
		directRuntimeResult = directRuntimeResult && hasRuntimeType
		if val == nil {
			pc := &plan.Literal{
				Isnull: true,
				Value:  &plan.Literal_Sval{Sval: ""},
			}
			params[i] = &plan.Expr{
				Typ: paramType,
				Expr: &plan.Expr_Lit{
					Lit: pc,
				},
			}
		} else {
			if hasRuntimeType {
				params[i], err = preparedRuntimeParamExpr(ctx, val, isBin, runtimeType)
				if err != nil {
					return false, err
				}
				if numericPrefixSource || retainParamRef || directRuntimeResult {
					attachPreparedRuntimeParamSource(params[i], &plan.Expr{
						Typ: paramType, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: int32(i)}},
					})
				}
				continue
			}
			pc := &plan.Literal{IsBin: isBin}
			pc.Value = &plan.Literal_Sval{Sval: fmt.Sprintf("%v", val)}
			params[i] = &plan.Expr{
				Typ: paramType,
				Expr: &plan.Expr_Lit{
					Lit: pc,
				},
			}
		}
		if (numericPrefixSource || retainParamRef || directRuntimeResult) && params[i].GetLit() != nil {
			params[i].GetLit().Src = &plan.Expr{
				Typ: paramType, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: int32(i)}},
			}
		}
	}
	// LIMIT/OFFSET and LAG/LEAD offset markers have fixed unsigned-integer
	// contracts. Their assignment-time SQL SourceType must not participate in
	// result-domain specialization (for example SET @k = 2 may carry DECIMAL
	// metadata while LIMIT still requires UINT64).
	fixedIntegerPositions := PreparedPaginationParamPositions(plan0)
	fixedIntegerPositions = append(fixedIntegerPositions, PreparedLagLeadParamPositions(plan0)...)
	for _, position := range fixedIntegerPositions {
		if position >= 0 && int(position) < len(sqlExecuteNumericParams) {
			sqlExecuteNumericParams[position] = nil
			sqlExecuteStringBackedParams[position] = false
		}
	}

	paramRule := NewResetParamRefRule(ctx, params)
	paramRule.sqlExecuteNumericParams = sqlExecuteNumericParams
	paramRule.sqlExecuteStringBackedParams = sqlExecuteStringBackedParams
	paramRule.setPreparedPlan(plan0)
	// Keep the original execute-time values and protocol categories on the
	// rebinding rule.  The plan parameters above intentionally use their
	// prepare-time transport literals; overloaded functions such as ABS need
	// the runtime category to select an exact integer/decimal overload.
	paramRule.SetParamValues(paramVals)
	if preserveDMLWrites {
		paramRule.preserveRoots = preparedDMLWriteExpressions(plan0.GetQuery())
	}
	runtimeParamTypes := make([]types.Type, len(paramVals))
	for i, val := range paramVals {
		if selected != nil && (i >= len(selected) || !selected[i]) {
			continue
		}
		param, ok := val.(ParamValue)
		if !ok {
			continue
		}
		if !param.IsBinaryProtocol {
			if param.HasSourceType && isStringBackedType(param.SourceType) {
				runtimeParamTypes[i] = types.T_text.ToType()
			}
			continue
		}
		if param.PrepareParamKind == vector.PrepareParamBoolean {
			// database/sql encodes bool as MYSQL_TYPE_TINY. Keep the Boolean
			// literal for functions such as JSON_SET, but retain its numeric
			// protocol domain while selecting comparison coercion.
			runtimeParamTypes[i] = types.T_int8.ToType()
		} else if param.HasRuntimeType {
			runtimeParamTypes[i] = param.RuntimeType
		} else if param.IsBinaryProtocol && param.Value != nil {
			runtimeParamTypes[i] = types.T_text.ToType()
		}
	}
	paramRule.numericComparisonTextParamPositions =
		preparedNumericComparisonTextParamPositions(plan0, runtimeParamTypes)
	numericPrefixPositions := preparedNumericPrefixParamPositions(plan0, paramVals)
	paramRule.inferTextParamPositions = make(map[int]bool)
	paramRule.numericPrefixParamPositions = make(map[int]bool)
	paramRule.numericPrefixParamKinds = make(map[int]types.StringConversionKind)
	for i, val := range paramVals {
		if selected != nil && (i >= len(selected) || !selected[i]) {
			continue
		}
		if param, ok := val.(ParamValue); ok {
			if param.IsBinaryProtocol {
				paramRule.inferTextParamPositions[i] = true
			}
			if param.HasRuntimeType && param.RuntimeType.Oid == types.T_text {
				paramRule.inferTextParamTypes = true
			}
			if param.EnableNumericPrefix && numericPrefixPositions[i] {
				paramRule.numericPrefixParamPositions[i] = true
				paramRule.numericPrefixParamKinds[i] = param.PrepareParamKind
				// The numeric-prefix capability is the stronger execute-time
				// contract only when this position is selected by the
				// decimal-aware common-type path. Do not also route that marker
				// through the generic COM_STMT text-vs-numeric DOUBLE fallback;
				// the two conversions have different result domains (notably for
				// nested COALESCE). Marker-to-marker comparisons keep the DOUBLE
				// path because they have no exact numeric peer.
				if numericPrefixPositions[i] && preparedPlanHasStaticDecimalPeer(plan0, i) {
					delete(paramRule.numericComparisonTextParamPositions, i)
				}
			}
		}
	}
	paramRule.validateFunctionArgs = func(name string, args []*Expr) error {
		if name != "nth_value" || len(args) != 2 {
			return nil
		}
		pos, ok := preparedWindowArgumentParamPosition(args[1])
		if !ok {
			return nil
		}
		if pos < 0 || int(pos) >= len(paramVals) {
			return moerr.NewInternalErrorf(ctx, "get prepare params error, index %d not exists", pos)
		}
		if !isPositivePreparedInteger(paramVals[pos]) {
			return moerr.NewWrongArguments(ctx, name)
		}
		return nil
	}
	if setVariables := plan0.GetDcl().GetSetVariables(); setVariables != nil {
		for _, item := range setVariables.Items {
			if item.Value != nil {
				item.Value, err = paramRule.ApplyExpr(item.Value)
				if err != nil {
					return false, err
				}
			}
			if item.Reserved != nil {
				item.Reserved, err = paramRule.ApplyExpr(item.Reserved)
				if err != nil {
					return false, err
				}
			}
		}
	} else {
		visitPlan := NewVisitPlan(plan0, []VisitPlanRule{paramRule})
		err = visitPlan.Visit(ctx)
		if err != nil {
			return false, err
		}
	}
	refreshPreparedPlanProjectionTypes(plan0)

	// A direct SELECT parameter is part of the result-column contract. Propagate
	// its execute-time type through transparent projection/sort/distinct nodes
	// so the final visible ColDef agrees with the rewritten source expression.
	directResultSpecialized := propagatePreparedDirectResultTypes(plan0, paramVals)
	return paramRule.specialized || directResultSpecialized, nil
}

func propagatePreparedDirectResultTypes(plan0 *Plan, paramVals []any) bool {
	query := plan0.GetQuery()
	if query == nil || query.StmtType != plan.Query_SELECT || len(query.Steps) == 0 {
		return false
	}
	rootID := query.Steps[len(query.Steps)-1]
	if rootID < 0 || int(rootID) >= len(query.Nodes) || query.Nodes[rootID] == nil {
		return false
	}
	memo := make(map[directResultTraceKey]plan.Type)
	found := make(map[directResultTraceKey]bool)
	specialized := false
	for colPos := range query.Nodes[rootID].ProjectList {
		_, direct := propagatePreparedDirectResultTypeAt(
			query, rootID, int32(colPos), paramVals, memo, found)
		specialized = specialized || direct
	}
	return specialized
}

func propagatePreparedDirectResultTypeAt(
	query *plan.Query,
	nodeID, colPos int32,
	paramVals []any,
	memo map[directResultTraceKey]plan.Type,
	found map[directResultTraceKey]bool,
) (plan.Type, bool) {
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) || colPos < 0 {
		return plan.Type{}, false
	}
	key := directResultTraceKey{nodeID: nodeID, colPos: colPos}
	if direct, ok := found[key]; ok {
		return memo[key], direct
	}
	// Publish a negative entry before recursion so malformed cyclic plans stay
	// total instead of recursing indefinitely.
	found[key] = false
	node := query.Nodes[nodeID]
	if node == nil {
		return plan.Type{}, false
	}
	switch node.NodeType {
	case plan.Node_UNION, plan.Node_UNION_ALL,
		plan.Node_INTERSECT, plan.Node_INTERSECT_ALL,
		plan.Node_MINUS, plan.Node_MINUS_ALL:
		return plan.Type{}, false
	}

	if int(colPos) >= len(node.ProjectList) {
		if len(node.Children) != 1 {
			return plan.Type{}, false
		}
		typ, direct := propagatePreparedDirectResultTypeAt(
			query, node.Children[0], colPos, paramVals, memo, found)
		if direct {
			memo[key], found[key] = typ, true
		}
		return typ, direct
	}
	expr := node.ProjectList[colPos]
	if expr == nil {
		return plan.Type{}, false
	}
	if position, ok := preparedRuntimeSourceParamPosition(expr); ok &&
		position >= 0 && position < len(paramVals) && runtimeParamHasExplicitType(paramVals[position]) {
		memo[key], found[key] = expr.Typ, true
		return expr.Typ, true
	}
	col := expr.GetCol()
	if col == nil {
		return plan.Type{}, false
	}
	if node.NodeType == plan.Node_AGG && col.RelPos < 0 &&
		col.ColPos >= 0 && int(col.ColPos) < len(node.GroupBy) {
		groupExpr := node.GroupBy[col.ColPos]
		if position, ok := preparedRuntimeSourceParamPosition(groupExpr); ok &&
			position >= 0 && position < len(paramVals) && runtimeParamHasExplicitType(paramVals[position]) {
			expr.Typ = groupExpr.Typ
			memo[key], found[key] = groupExpr.Typ, true
			return groupExpr.Typ, true
		}
		groupCol := groupExpr.GetCol()
		if groupCol == nil || len(node.Children) != 1 {
			return plan.Type{}, false
		}
		typ, direct := propagatePreparedDirectResultTypeAt(
			query, node.Children[0], groupCol.ColPos, paramVals, memo, found)
		if !direct {
			return plan.Type{}, false
		}
		groupExpr.Typ = typ
		expr.Typ = typ
		memo[key], found[key] = typ, true
		return typ, true
	}
	childID := int32(-1)
	if col.RelPos >= 0 && int(col.RelPos) < len(node.Children) {
		childID = node.Children[col.RelPos]
	} else if len(node.Children) == 1 {
		childID = node.Children[0]
	}
	if childID < 0 {
		return plan.Type{}, false
	}
	typ, direct := propagatePreparedDirectResultTypeAt(
		query, childID, col.ColPos, paramVals, memo, found)
	if !direct {
		return plan.Type{}, false
	}
	expr.Typ = typ
	memo[key], found[key] = typ, true
	return typ, true
}

func preparedRuntimeSourceParamPosition(expr *Expr) (int, bool) {
	if expr == nil {
		return 0, false
	}
	if param := expr.GetP(); param != nil {
		return int(param.Pos), true
	}
	literal := expr.GetLit()
	if literal != nil && literal.Src != nil && literal.Src.GetP() != nil {
		return int(literal.Src.GetP().Pos), true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.GetObjName(), "cast") || len(fn.Args) == 0 {
		return 0, false
	}
	return preparedRuntimeSourceParamPosition(fn.Args[0])
}

func attachPreparedRuntimeParamSource(expr, source *Expr) bool {
	if expr == nil || source == nil {
		return false
	}
	if literal := expr.GetLit(); literal != nil {
		literal.Src = source
		return true
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || !strings.EqualFold(fn.Func.GetObjName(), "cast") || len(fn.Args) == 0 {
		return false
	}
	literal := fn.Args[0].GetLit()
	if literal == nil {
		return false
	}
	literal.Src = source
	return true
}

// refreshPreparedPlanProjectionTypes repairs synthetic column expressions
// whose source expression changed type while a prepared plan was rebound.
// Aggregate and window nodes expose their computed values through positional
// column references; the expression visitor updates the source expression but
// cannot infer the cached type on those references. Downstream PROJECT nodes
// must be refreshed as well so result metadata and vector decoding agree.
func refreshPreparedPlanProjectionTypes(plan0 *Plan) {
	query := plan0.GetQuery()
	if query == nil {
		return
	}

	visited := make(map[int32]bool)
	var refreshNode func(int32)
	refreshNode = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || visited[nodeID] {
			return
		}
		visited[nodeID] = true
		node := query.Nodes[nodeID]
		if node == nil {
			return
		}
		for _, childID := range node.Children {
			refreshNode(childID)
		}

		switch node.NodeType {
		case plan.Node_AGG:
			groupSize := int32(len(node.GroupBy))
			for _, expr := range node.ProjectList {
				if expr == nil {
					continue
				}
				col := expr.GetCol()
				if col == nil {
					continue
				}
				switch col.RelPos {
				case -1:
					if col.ColPos >= 0 && int(col.ColPos) < len(node.GroupBy) {
						expr.Typ = node.GroupBy[col.ColPos].Typ
					}
				case -2:
					aggPos := col.ColPos - groupSize
					if aggPos >= 0 && int(aggPos) < len(node.AggList) {
						expr.Typ = node.AggList[aggPos].Typ
					}
				}
			}
		case plan.Node_WINDOW:
			// The outer W expression retains the type used by the original
			// window function unless it is explicitly synchronized here.
			for _, expr := range node.WinSpecList {
				if expr == nil || expr.GetW() == nil || expr.GetW().WindowFunc == nil {
					continue
				}
				expr.Typ = expr.GetW().WindowFunc.Typ
			}
			var child *plan.Node
			if len(node.Children) == 1 {
				childID := node.Children[0]
				if childID >= 0 && int(childID) < len(query.Nodes) {
					child = query.Nodes[childID]
				}
			}
			childProjectSize := int32(0)
			if child != nil {
				childProjectSize = int32(len(child.ProjectList))
			}
			for _, expr := range node.ProjectList {
				if expr == nil {
					continue
				}
				col := expr.GetCol()
				if col == nil || col.RelPos != -1 {
					continue
				}
				winPos := col.ColPos - childProjectSize
				if winPos >= 0 && int(winPos) < len(node.WinSpecList) {
					expr.Typ = node.WinSpecList[winPos].Typ
				}
			}
		}

		// A projection node exposes its child columns through RelPos == 0.
		// Propagate the child's refreshed type so final result metadata and the
		// vector decoder agree with the runtime expression domain.
		if node.NodeType == plan.Node_PROJECT && len(node.Children) == 1 {
			childID := node.Children[0]
			if childID >= 0 && int(childID) < len(query.Nodes) {
				child := query.Nodes[childID]
				if child != nil {
					for _, expr := range node.ProjectList {
						if expr == nil {
							continue
						}
						col := expr.GetCol()
						if col == nil || col.RelPos != 0 || col.ColPos < 0 || int(col.ColPos) >= len(child.ProjectList) {
							continue
						}
						expr.Typ = child.ProjectList[col.ColPos].Typ
					}
				}
			}
		}
	}

	for _, step := range query.Steps {
		refreshNode(step)
	}
}

func runtimeParamHasExplicitType(value any) bool {
	param, ok := value.(ParamValue)
	return ok && param.HasRuntimeType
}

// XXX: Any code relying on Name in ColRef, except for "explain", is bad design and practically buggy.
func (builder *QueryBuilder) addNameByColRef(tag int32, tableDef *plan.TableDef) {
	for i, col := range tableDef.Cols {
		builder.nameByColRef[[2]int32{tag, int32(i)}] = tableDef.Name + "." + col.Name
	}
}

func GetRowSizeFromTableDef(tableDef *TableDef, ignoreHiddenKey bool) float64 {
	// Column widths are protocol capacities and may use MaxLongTextLen
	// (math.MaxInt32). Accumulate in float64 and cap the planner estimate so
	// adding an ordinary column cannot wrap the old int32 accumulator negative.
	const maxPlanningRowSize = float64(math.MaxInt32)
	size := float64(0)
	for _, col := range tableDef.Cols {
		if col.Hidden && ignoreHiddenKey {
			continue
		}
		if col.Typ.Width > 0 {
			size += float64(col.Typ.Width)
		} else {
			typ := types.T(col.Typ.Id).ToType()
			if typ.Width > 0 {
				size += float64(typ.Width)
			} else {
				size += float64(typ.Size)
			}
		}
		if size >= maxPlanningRowSize {
			return maxPlanningRowSize
		}
	}
	return size
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
			// Nullable folded lists must keep their null bitmap aligned with values.
			if !vec.IsConstNull() && !vec.GetNulls().Any() {
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

// isExecutionConstantExpr reports whether an expression is a value that is unknown at
// plan time but CONSTANT for the whole execution: a prepared parameter marker,
// optionally wrapped in monotone casts (`CAST(? AS DOUBLE)`).
//
// The distinction that matters is against a per-ROW expression such as a column
// reference. Both are "not a literal", but only this kind can be constant-folded once
// before a scan and used as a fixed bound; a column reference varies per row and must
// stay a residual filter. Optimizer rules that want to admit `?` where they previously
// demanded a literal should test this, never merely `GetLit() == nil`.
//
// EVERY argument of a wrapper is checked, not only the first. Some wrappers have a
// value-affecting second argument -- `round(?, digits)` -- so `round(?, per_row_col)`
// is a different value on every row even though argument 0 is a parameter. Following
// argument 0 alone reports that as constant, and the bound is then peeled into one
// scan-wide range that cannot be folded, failing the read.
//
// The expression must also actually CONTAIN a parameter: an all-literal expression is
// handled by the literal path, and reporting it here would route it down the runtime
// branch instead.
func isExecutionConstantExpr(expr *plan.Expr) bool {
	constant, hasParam := execConstantExpr(expr, 0)
	return constant && hasParam
}

// execConstantExpr returns whether expr is constant for the execution, and whether it
// contains a parameter marker at all.
func execConstantExpr(expr *plan.Expr, depth int) (constant, hasParam bool) {
	if expr == nil || depth > 8 {
		return false, false
	}
	if expr.GetP() != nil {
		return true, true
	}
	if lit := expr.GetLit(); lit != nil {
		return true, false // fixed for the execution, but not a parameter
	}
	fn := expr.GetF()
	if fn == nil || fn.Func == nil || len(fn.Args) == 0 {
		return false, false
	}
	switch fn.Func.ObjName {
	case "cast":
		// Argument 1 is the TARGET TYPE, not a value, so only argument 0 carries data.
		return execConstantExpr(fn.Args[0], depth+1)
	case "round", "floor", "ceil":
		// Every argument here is a value, and every one of them affects the result:
		// round(x, digits) moves with digits. Checking argument 0 alone would accept
		// round(?, per_row_col).
		for _, arg := range fn.Args {
			argConst, argParam := execConstantExpr(arg, depth+1)
			if !argConst {
				return false, false
			}
			hasParam = hasParam || argParam
		}
		return true, hasParam
	default:
		// An unlisted function may have any arity or any per-row argument; refuse
		// rather than guess which of its arguments are value-affecting.
		return false, false
	}
}
