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

package plan2

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
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

func hasSubquery(expr *plan.Expr) bool {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Sub:
		return true

	case *plan.Expr_F:
		ret := false
		for _, arg := range exprImpl.F.Args {
			ret = ret || hasSubquery(arg)
		}
		return ret

	default:
		return false
	}
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
	var filterPreds, joinPreds []*plan.Expr
	for _, pred := range preds {
		newPred, correlated := decreaseDepth(pred)
		if f, ok := pred.Expr.(*plan.Expr_F); ok && !correlated {
			if f.F.Func.ObjName == "=" {
				joinPreds = append(joinPreds, newPred)
				continue
			}
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

func DeepCopyExpr(expr *Expr) *Expr {
	new_expr := &Expr{
		Typ: &plan.Type{
			Id:        expr.Typ.GetId(),
			Nullable:  expr.Typ.GetNullable(),
			Width:     expr.Typ.GetWidth(),
			Precision: expr.Typ.GetPrecision(),
			Size:      expr.Typ.GetSize(),
			Scale:     expr.Typ.GetScale(),
		},
	}

	switch item := expr.Expr.(type) {
	case *plan.Expr_C:
		new_expr.Expr = &plan.Expr_C{
			C: &plan.Const{
				Isnull: item.C.GetIsnull(),
				Value:  item.C.GetValue(),
			},
		}

	case *plan.Expr_P:
		new_expr.Expr = &plan.Expr_P{
			P: &plan.ParamRef{
				Pos: item.P.GetPos(),
			},
		}

	case *plan.Expr_V:
		new_expr.Expr = &plan.Expr_V{
			V: &plan.VarRef{
				Name: item.V.GetName(),
			},
		}

	case *plan.Expr_Col:
		new_expr.Expr = &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: item.Col.GetRelPos(),
				ColPos: item.Col.GetColPos(),
			},
		}

	case *plan.Expr_F:
		new_args := make([]*Expr, len(item.F.Args))
		for idx, arg := range item.F.Args {
			new_args[idx] = DeepCopyExpr(arg)
		}
		new_expr.Expr = &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Server:     item.F.Func.GetServer(),
					Db:         item.F.Func.GetDb(),
					Schema:     item.F.Func.GetSchema(),
					Obj:        item.F.Func.GetObj(),
					ServerName: item.F.Func.GetServerName(),
					DbName:     item.F.Func.GetDbName(),
					SchemaName: item.F.Func.GetSchemaName(),
					ObjName:    item.F.Func.GetObjName(),
				},
				Args: new_args,
			},
		}

	case *plan.Expr_Sub:
		new_expr.Expr = &plan.Expr_Sub{
			Sub: &plan.SubqueryRef{
				NodeId: item.Sub.GetNodeId(),
			},
		}

	case *plan.Expr_Corr:
		new_expr.Expr = &plan.Expr_Corr{
			Corr: &plan.CorrColRef{
				ColPos: item.Corr.GetColPos(),
				RelPos: item.Corr.GetRelPos(),
				Depth:  item.Corr.GetDepth(),
			},
		}

	case *plan.Expr_T:
		new_expr.Expr = &plan.Expr_T{
			T: &plan.TargetType{
				Typ: &plan.Type{
					Id:        item.T.Typ.GetId(),
					Nullable:  item.T.Typ.GetNullable(),
					Width:     item.T.Typ.GetWidth(),
					Precision: item.T.Typ.GetPrecision(),
					Size:      item.T.Typ.GetSize(),
					Scale:     item.T.Typ.GetScale(),
				},
			},
		}
	}

	return new_expr
}

func getJoinSide(expr *plan.Expr, leftTags map[int32]*Binding) (side int8) {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			side |= getJoinSide(arg, leftTags)
		}

	case *plan.Expr_Col:
		if _, ok := leftTags[exprImpl.Col.RelPos]; ok {
			side = 0b01
		} else {
			side = 0b10
		}

	case *plan.Expr_Corr:
		side = 0b100
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
