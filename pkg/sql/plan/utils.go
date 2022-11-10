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
	"math"

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
	// XXX MPOOL
	// This is a bug -- colexec EvalExpr need to eval, therefore, could potentially need
	// a mpool.  proc is passed in a nil, where do I get a mpool?   Session?
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
	case types.T_int8:
		return &plan.Const{
			Value: &plan.Const_I8Val{
				I8Val: int32(vec.Col.([]int8)[0]),
			},
		}
	case types.T_int16:
		return &plan.Const{
			Value: &plan.Const_I16Val{
				I16Val: int32(vec.Col.([]int16)[0]),
			},
		}
	case types.T_int32:
		return &plan.Const{
			Value: &plan.Const_I32Val{
				I32Val: vec.Col.([]int32)[0],
			},
		}
	case types.T_int64:
		return &plan.Const{
			Value: &plan.Const_I64Val{
				I64Val: vec.Col.([]int64)[0],
			},
		}
	case types.T_uint8:
		return &plan.Const{
			Value: &plan.Const_U8Val{
				U8Val: uint32(vec.Col.([]uint8)[0]),
			},
		}
	case types.T_uint16:
		return &plan.Const{
			Value: &plan.Const_U16Val{
				U16Val: uint32(vec.Col.([]uint16)[0]),
			},
		}
	case types.T_uint32:
		return &plan.Const{
			Value: &plan.Const_U32Val{
				U32Val: vec.Col.([]uint32)[0],
			},
		}
	case types.T_uint64:
		return &plan.Const{
			Value: &plan.Const_U64Val{
				U64Val: vec.Col.([]uint64)[0],
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

func rewriteTableFunction(tblFunc *tree.TableFunction, leftCtx *BindContext) error {
	//var err error
	//newTableAliasMap := make(map[string]string)
	//newColAliasMap := make(map[string]string)
	//col2Table := make(map[string]string)
	//for i := range tblFunc.SelectStmt.Select.(*tree.SelectClause).From.Tables {
	//	alias := string(tblFunc.SelectStmt.Select.(*tree.SelectClause).From.Tables[i].(*tree.AliasedTableExpr).As.Alias)
	//	if len(alias) == 0 {
	//		alias = string(tblFunc.SelectStmt.Select.(*tree.SelectClause).From.Tables[i].(*tree.AliasedTableExpr).Expr.(*tree.TableName).ObjectName)
	//	}
	//	newAlias := fmt.Sprintf("%s_tbl_%d", alias, i)
	//	tblFunc.SelectStmt.Select.(*tree.SelectClause).From.Tables[i].(*tree.AliasedTableExpr).As.Alias = tree.Identifier(newAlias)
	//	//newTableAliasMap[alias] = newAlias
	//}
	for i := range tblFunc.SelectStmt.Select.(*tree.SelectClause).Exprs {
		selectExpr := tblFunc.SelectStmt.Select.(*tree.SelectClause).Exprs[i] //take care, this is not a pointer
		expr := selectExpr.Expr.(*tree.UnresolvedName)
		_, tableName, colName := expr.GetNames()
		if len(tableName) == 0 {
			if binding, ok := leftCtx.bindingByCol[colName]; ok {
				tableName = binding.table
				expr.Parts[1] = tableName
			} else {
				return moerr.NewInternalError("cannot find column '%s'", colName)
			}
		}
		//newTableName = newTableAliasMap[tableName]
		//newColAlias = fmt.Sprintf("%s_%d", colName, i)
		//newColAliasMap[colName] = newColAlias
		//col2Table[newColAlias] = newTableName
		//newName, err := tree.NewUnresolvedName(newTableName, colName)
		//if err != nil {
		//	return err
		//}
		//tblFunc.SelectStmt.Select.(*tree.SelectClause).Exprs[i].Expr = newName
		//tblFunc.SelectStmt.Select.(*tree.SelectClause).Exprs[i].As = tree.UnrestrictedIdentifier(newColAlias)
	}

	//for i, _ := range tblFunc.Func.Exprs {
	//	tblFunc.Func.Exprs[i], err = rewriteTableFunctionExpr(tblFunc.Func.Exprs[i], newTableAliasMap, newColAliasMap, col2Table)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

//
//func rewriteTableFunctionExpr(ast tree.Expr, tableAlias map[string]string, colAlias map[string]string, col2Table map[string]string) (tree.Expr, error) {
//	var err error
//	switch item := ast.(type) {
//	case *tree.UnresolvedName:
//		_, tblName, colName := item.GetNames()
//		if len(tblName) > 0 {
//			if alias, ok := tableAlias[tblName]; ok {
//				item.Parts[1] = alias
//			}
//		} else {
//			newColName := colAlias[colName]
//			newTblName := col2Table[newColName]
//			item.Parts[1] = newTblName
//		}
//	case *tree.FuncExpr:
//		for i, _ := range item.Exprs {
//			item.Exprs[i], err = rewriteTableFunctionExpr(item.Exprs[i], tableAlias, colAlias, col2Table)
//			if err != nil {
//				return nil, err
//			}
//		}
//	case *tree.NumVal:
//		break
//	default:
//		return nil, moerr.NewNotSupported("table function expr '%s' not supported", item)
//	}
//	return ast, nil
//}

// lookUpFnCols looks up the columns in the function expression
func lookUpFnCols(ret tree.SelectExprs, fn interface{}) tree.SelectExprs {
	switch fnExpr := fn.(type) { //TODO add more cases
	case *tree.UnresolvedName:
		ret = append(ret, tree.SelectExpr{Expr: fnExpr})
	case *tree.FuncExpr:
		for _, arg := range fnExpr.Exprs {
			ret = lookUpFnCols(ret, arg)
		}
	case *tree.BinaryExpr:
		ret = lookUpFnCols(ret, fnExpr.Left)
		ret = lookUpFnCols(ret, fnExpr.Right)
	case *tree.UnaryExpr:
		ret = lookUpFnCols(ret, fnExpr.Expr)
	}
	return ret
}
func buildTableFunctionStmt(tbl *tree.TableFunction, left tree.TableExpr, leftCtx *BindContext) error {
	var selectExprs tree.SelectExprs
	selectExprs = lookUpFnCols(selectExprs, tbl.Func)
	tbl.SelectStmt = &tree.Select{
		Select: &tree.SelectClause{
			From: &tree.From{
				Tables: []tree.TableExpr{left},
			},
			Exprs: selectExprs,
		},
	}
	return rewriteTableFunction(tbl, leftCtx)
}

func clearBinding(ctx *BindContext) {
	ctx.bindingByCol = make(map[string]*Binding)
	ctx.bindingByTable = make(map[string]*Binding)
	ctx.bindingByTag = make(map[int32]*Binding)
	ctx.bindingTree = &BindingTreeNode{}
	ctx.bindings = make([]*Binding, 0)
}

func unwindTupleComparison(nonEqOp, op string, leftExprs, rightExprs []*plan.Expr, idx int) (*plan.Expr, error) {
	if idx == len(leftExprs)-1 {
		return bindFuncExprImplByPlanExpr(op, []*plan.Expr{
			leftExprs[idx],
			rightExprs[idx],
		})
	}

	expr, err := bindFuncExprImplByPlanExpr(nonEqOp, []*plan.Expr{
		DeepCopyExpr(leftExprs[idx]),
		DeepCopyExpr(rightExprs[idx]),
	})
	if err != nil {
		return nil, err
	}

	eqExpr, err := bindFuncExprImplByPlanExpr("=", []*plan.Expr{
		leftExprs[idx],
		rightExprs[idx],
	})
	if err != nil {
		return nil, err
	}

	tailExpr, err := unwindTupleComparison(nonEqOp, op, leftExprs, rightExprs, idx+1)
	if err != nil {
		return nil, err
	}

	tailExpr, err = bindFuncExprImplByPlanExpr("and", []*plan.Expr{eqExpr, tailExpr})
	if err != nil {
		return nil, err
	}

	return bindFuncExprImplByPlanExpr("or", []*plan.Expr{expr, tailExpr})
}

// checkNoNeedCast
// if constant's type higher than column's type
// and constant's value in range of column's type, then no cast was needed
func checkNoNeedCast(constT, columnT types.T, constExpr *plan.Expr_C) bool {
	key := [2]types.T{columnT, constT}
	// lowIntCol > highIntConst
	if _, ok := intCastTableForRewrite[key]; ok {
		val, valOk := constExpr.C.Value.(*plan.Const_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val

		switch columnT {
		case types.T_int8:
			return constVal <= int64(math.MaxInt8) && constVal >= int64(math.MinInt8)
		case types.T_int16:
			return constVal <= int64(math.MaxInt16) && constVal >= int64(math.MinInt16)
		case types.T_int32:
			return constVal <= int64(math.MaxInt32) && constVal >= int64(math.MinInt32)
		}
	}

	// lowUIntCol > highUIntConst
	if _, ok := uintCastTableForRewrite[key]; ok {
		val, valOk := constExpr.C.Value.(*plan.Const_U64Val)
		if !valOk {
			return false
		}
		constVal := val.U64Val

		switch columnT {
		case types.T_uint8:
			return constVal <= uint64(math.MaxUint8)
		case types.T_uint16:
			return constVal <= uint64(math.MaxUint16)
		case types.T_uint32:
			return constVal <= uint64(math.MaxUint32)
		}
	}

	// lowUIntCol > highIntConst
	if _, ok := uint2intCastTableForRewrite[key]; ok {
		val, valOk := constExpr.C.Value.(*plan.Const_I64Val)
		if !valOk {
			return false
		}
		constVal := val.I64Val

		switch columnT {
		case types.T_uint8:
			return constVal <= int64(math.MaxUint8)
		case types.T_uint16:
			return constVal <= int64(math.MaxUint16)
		case types.T_uint32:
			return constVal <= int64(math.MaxUint32)
		}
	}

	return false
}
