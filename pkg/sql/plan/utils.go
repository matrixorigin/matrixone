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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
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
		ret := false
		for _, arg := range exprImpl.F.Args {
			ret = ret || hasCorrCol(arg)
		}
		return ret

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

func getJoinSide(expr *plan.Expr, leftTags, rightTags map[int32]*Binding) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSide(arg, leftTags, rightTags)
		}

	case *plan.Expr_Col:
		if _, ok := leftTags[exprImpl.Col.RelPos]; ok {
			side = JoinSideLeft
		} else if _, ok := rightTags[exprImpl.Col.RelPos]; ok {
			side = JoinSideRight
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
			expr, err = makePlan2CastExpr(expr, &plan.Type{Id: int32(types.T_bool)})
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
func applyDistributivity(expr *plan.Expr) *plan.Expr {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for i, arg := range exprImpl.F.Args {
			exprImpl.F.Args[i] = applyDistributivity(arg)
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

		expr, _ = combinePlanConjunction(commonConds)

		if len(leftOnlyConds) == 0 || len(rightOnlyConds) == 0 {
			return expr
		}

		leftExpr, _ := combinePlanConjunction(leftOnlyConds)
		rightExpr, _ := combinePlanConjunction(rightOnlyConds)

		leftExpr, _ = bindFuncExprImplByPlanExpr("or", []*plan.Expr{leftExpr, rightExpr})

		expr, _ = bindFuncExprImplByPlanExpr("and", []*plan.Expr{expr, leftExpr})
	}

	return expr
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

func combinePlanConjunction(exprs []*plan.Expr) (expr *plan.Expr, err error) {
	expr = exprs[0]

	for i := 1; i < len(exprs); i++ {
		expr, err = bindFuncExprImplByPlanExpr("and", []*plan.Expr{expr, exprs[i]})

		if err != nil {
			break
		}
	}

	return
}

func rejectsNull(filter *plan.Expr) bool {
	filter = replaceColRefWithNull(DeepCopyExpr(filter))

	bat := batch.NewWithSize(0)
	bat.Zs = []int64{1}
	filter, err := ConstantFold(bat, filter)
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

func increaseRefCnt(expr *plan.Expr, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]++

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			increaseRefCnt(arg, colRefCnt)
		}
	}
}

func decreaseRefCnt(expr *plan.Expr, colRefCnt map[[2]int32]int) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colRefCnt[[2]int32{exprImpl.Col.RelPos, exprImpl.Col.ColPos}]--

	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			decreaseRefCnt(arg, colRefCnt)
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

func getUnionSelects(stmt *tree.UnionClause, selects *[]tree.Statement, unionTypes *[]plan.Node_NodeType) error {
	switch leftStmt := stmt.Left.(type) {
	case *tree.UnionClause:
		err := getUnionSelects(leftStmt, selects, unionTypes)
		if err != nil {
			return err
		}
	case *tree.SelectClause:
		*selects = append(*selects, leftStmt)
	case *tree.ParenSelect:
		*selects = append(*selects, leftStmt.Select)
	default:
		return moerr.NewParseError("unexpected statement in union: '%v'", tree.String(leftStmt, dialect.MYSQL))
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
		return moerr.NewParseError("unexpected statement in union2: '%v'", tree.String(rightStmt, dialect.MYSQL))
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
			return moerr.NewNYI("EXCEPT/MINUS ALL clause")
		} else {
			*unionTypes = append(*unionTypes, plan.Node_MINUS)
		}
	}
	return nil
}

func ConstantFold(bat *batch.Batch, e *plan.Expr) (*plan.Expr, error) {
	var err error

	ef, ok := e.Expr.(*plan.Expr_F)
	if !ok {
		return e, nil
	}
	overloadID := ef.F.Func.GetObj()
	f, err := function.GetFunctionByID(overloadID)
	if err != nil {
		return nil, err
	}
	if f.Volatile { // function cannot be fold
		return e, nil
	}
	for i := range ef.F.Args {
		ef.F.Args[i], err = ConstantFold(bat, ef.F.Args[i])
		if err != nil {
			return nil, err
		}
	}
	if !isConstant(e) {
		return e, nil
	}
	vec, err := colexec.EvalExpr(bat, nil, e)
	if err != nil {
		return nil, err
	}
	c := getConstantValue(vec)
	if c == nil {
		return e, nil
	}
	ec := &plan.Expr_C{
		C: c,
	}
	e.Expr = ec
	return e, nil
}

func getConstantValue(vec *vector.Vector) *plan.Const {
	if nulls.Any(vec.Nsp) {
		return &plan.Const{Isnull: true}
	}
	switch vec.Typ.Oid {
	case types.T_bool:
		return &plan.Const{
			Value: &plan.Const_Bval{
				Bval: vec.Col.([]bool)[0],
			},
		}
	case types.T_int64:
		return &plan.Const{
			Value: &plan.Const_Ival{
				Ival: vec.Col.([]int64)[0],
			},
		}
	case types.T_float64:
		return &plan.Const{
			Value: &plan.Const_Dval{
				Dval: vec.Col.([]float64)[0],
			},
		}
	case types.T_varchar:
		return &plan.Const{
			Value: &plan.Const_Sval{
				Sval: vec.GetString(0),
			},
		}
	default:
		return nil
	}
}

func isConstant(e *plan.Expr) bool {
	switch ef := e.Expr.(type) {
	case *plan.Expr_C, *plan.Expr_T:
		return true
	case *plan.Expr_F:
		overloadID := ef.F.Func.GetObj()
		f, err := function.GetFunctionByID(overloadID)
		if err != nil {
			return false
		}
		if f.Volatile { // function cannot be fold
			return false
		}
		for i := range ef.F.Args {
			if !isConstant(ef.F.Args[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
