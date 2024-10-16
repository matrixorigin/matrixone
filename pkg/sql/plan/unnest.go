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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var (
	defaultColDefs = []*plan.ColDef{
		{
			Name: "col",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       types.MaxVarcharLen,
			},
		},
		{
			Name: "seq",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
		{
			Name: "key",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       types.MaxVarcharLen,
			},
		},
		{
			Name: "path",
			Typ: plan.Type{
				Id:          int32(types.T_varchar),
				NotNullable: false,
				Width:       types.MaxVarcharLen,
			},
		},
		{
			Name: "index",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
		{
			Name: "value",
			Typ: plan.Type{
				Id:          int32(types.T_json),
				NotNullable: false,
			},
		},
		{
			Name: "this",
			Typ: plan.Type{
				Id:          int32(types.T_json),
				NotNullable: false,
			},
		},
	}
)

func _dupType(typ *plan.Type) *plan.Type {
	return &plan.Type{
		Id:          typ.Id,
		NotNullable: typ.NotNullable,
		Width:       typ.Width,
		Scale:       typ.Scale,
	}
}

func _dupColDef(src *plan.ColDef) *plan.ColDef {
	return &plan.ColDef{
		Name:       src.Name,
		OriginName: src.OriginName,
		Typ:        *_dupType(&src.Typ),
	}
}

func _getColDefs(colDefs []*plan.ColDef) []*plan.ColDef {
	ret := make([]*plan.ColDef, 0, len(colDefs))
	for _, v := range colDefs {
		ret = append(ret, _dupColDef(v))
	}
	return ret
}

func (builder *QueryBuilder) buildUnnest(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {
	colDefs := _getColDefs(defaultColDefs)
	colName := findColName(tbl.Func)
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  "unnest",
				Param: []byte(colName),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		Children:        []int32{childId},
		TblFuncExprList: exprs,
	}
	return builder.appendNode(node, ctx), nil
}

func findColName(fn *tree.FuncExpr) string {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return ""
	}
	return tree.String(fn.Exprs[0], dialect.MYSQL)
}
