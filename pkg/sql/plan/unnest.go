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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	defaultColDefs = []*plan.ColDef{
		{
			Name: "col",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    4,
			},
		},
		{
			Name: "seq",
			Typ: &plan.Type{
				Id:       int32(types.T_int32),
				Nullable: true,
			},
		},
		{
			Name: "key",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    256,
			},
		},
		{
			Name: "path",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    256,
			},
		},
		{
			Name: "index",
			Typ: &plan.Type{
				Id:       int32(types.T_int32),
				Nullable: true,
				Width:    4,
			},
		},
		{
			Name: "value",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    1024,
			},
		},
		{
			Name: "this",
			Typ: &plan.Type{
				Id:       int32(types.T_varchar),
				Nullable: true,
				Width:    1024,
			},
		},
	}
)

func _dupType(typ *plan.Type) *plan.Type {
	return &plan.Type{
		Id:        typ.Id,
		Nullable:  typ.Nullable,
		Size:      typ.Size,
		Width:     typ.Width,
		Scale:     typ.Scale,
		Precision: typ.Precision,
	}
}

func _dupColDef(src *plan.ColDef) *plan.ColDef {
	return &plan.ColDef{
		Name: src.Name,
		Typ:  _dupType(src.Typ),
	}
}

func _getDefaultColDefs() []*plan.ColDef {
	ret := make([]*plan.ColDef, 0, len(defaultColDefs))
	for _, v := range defaultColDefs {
		ret = append(ret, _dupColDef(v))
	}
	return ret
}

func (builder *QueryBuilder) buildUnnest(tbl *tree.TableFunction, ctx *BindContext, childId int32) (int32, error) {
	ctx.binder = NewTableBinder(builder, ctx)
	exprs := make([]*plan.Expr, 0, len(tbl.Func.Exprs))
	for _, v := range tbl.Func.Exprs {
		curExpr, err := ctx.binder.BindExpr(v, 0, false)
		if err != nil {
			return 0, err
		}
		exprs = append(exprs, curExpr)
	}
	colDefs := _getDefaultColDefs()
	node := &plan.Node{
		NodeType: plan.Node_TABLE_FUNCTION,
		Cost:     &plan.Cost{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name: "unnest",
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs, // now only support one func expr in unnest
	}
	if childId == -1 {
		scanNode := &plan.Node{
			NodeType: plan.Node_VALUE_SCAN,
		}
		childId = builder.appendNode(scanNode, ctx)
	}
	node.Children = []int32{childId}
	nodeID := builder.appendNode(node, ctx)
	return nodeID, nil
}

//func genTblParam(tbl *tree.TableFunction) (*unnest.ExternalParam, error) {
//	if len(tbl.Func.Exprs) > 3 || len(tbl.Func.Exprs) < 1 {
//		return nil, moerr.NewInvalidInput("the number of unnest param is not valid")
//	}
//	var err error
//	uParam := &unnest.ExternalParam{}
//	switch o := tbl.Func.Exprs[0].(type) {
//	case *tree.UnresolvedName:
//		_, _, uParam.ColName = o.GetNames()
//		uParam.Typ = "col"
//	case *tree.FuncExpr:
//		uParam.Typ = "func"
//	case *tree.NumVal:
//		uParam.Typ = "str"
//	default:
//		return nil, moerr.NewNotSupported("unsupported unnest param type: %T", o)
//	}
//	if len(tbl.Func.Exprs) > 1 {
//		uParam.Path = tbl.Func.Exprs[1].String()
//	} else {
//		uParam.Path = "$"
//	}
//	if len(tbl.Func.Exprs) > 2 {
//		uParam.Outer = strings.ToLower(tbl.Func.Exprs[2].String()) == "true"
//	} else {
//		uParam.Outer = false
//	}
//	return uParam, err
//}
