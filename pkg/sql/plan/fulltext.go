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
	"go/constant"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// coldef shall copy index type
var (
	fulltext_index_scan_func_name = "fulltext_index_scan"

	ftIndexColdefs = []*plan.ColDef{
		// row_id type should be same as index type
		{
			Name: "doc_id",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
			},
		},
		{
			Name: "score",
			Typ: plan.Type{
				Id:          int32(types.T_float32),
				NotNullable: false,
				Width:       4,
			},
		},
	}
)

// arg list [index_table_name, search_against, mode]
func (builder *QueryBuilder) buildFullTextIndexScan(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, childId int32) (int32, error) {

	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (%d != 3). fulltext_index_scan(table_name, search_pattern, mode)", len(exprs))
	}

	colDefs := _getColDefs(ftIndexColdefs)

	/*
		val, err := builder.compCtx.ResolveVariable("save_query_result", true, false)
		if err == nil {
			if v, _ := val.(int8); v == 0 {
				return 0, moerr.NewNoConfig(builder.GetContext(), "save query result")
			} else {
				logutil.Infof("buildMetaScan : save query result: %v", v)
			}
		} else {
			return 0, err
		}
	*/

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  fulltext_index_scan_func_name,
				Param: []byte(""),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        []int32{childId},
	}
	return builder.appendNode(node, ctx), nil
}

// seems coldef just check the name but not type.  don't need to change primary key type to match with source table.
func (builder *QueryBuilder) getFullTextColDefs(fn *tree.FuncExpr) ([]*plan.ColDef, error) {

	coldefs := _getColDefs(ftIndexColdefs)

	if val, ok := fn.Exprs[3].(*tree.NumVal); ok {
		oid, ok := constant.Int64Val(val.Value)
		if !ok {
			return nil, moerr.NewNoConfig(builder.GetContext(), "invalid primary key type (not int64)")
		}

		coldefs[0].Typ.Id = int32(oid)
	} else {
		return nil, moerr.NewNoConfig(builder.GetContext(), "invalid primary key type (not NumVal)")
	}

	return coldefs, nil
}
