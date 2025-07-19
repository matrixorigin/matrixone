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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// coldef shall copy index type
var (
	hnsw_create_func_name = "hnsw_create"
	hnsw_search_func_name = "hnsw_search"

	hnswBuildIndexColDefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	hnswSearchColDefs = []*plan.ColDef{
		{
			Name: "pkid",
			Typ: plan.Type{
				Id:          int32(types.T_int64),
				NotNullable: false,
				Width:       8,
			},
		},
		{
			Name: "score",
			Typ: plan.Type{
				Id:          int32(types.T_float64),
				NotNullable: false,
				Width:       8,
			},
		},
	}
)

// arg list [param, hnsw.IndexTableConfig (JSON), pkid, vec]
func (builder *QueryBuilder) buildHnswCreate(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 4 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 4).")
	}

	colDefs := _getColDefs(hnswBuildIndexColDefs)
	params, err := builder.getHnswParams(tbl.Func)
	if err != nil {
		return 0, err
	}

	scanNode := builder.qry.Nodes[children[0]]
	if scanNode.NodeType != plan.Node_TABLE_SCAN {
		return 0, moerr.NewNoConfig(builder.GetContext(), "child node is not a TABLE SCAN")
	}

	// remove the first argment and put the first argument to Param
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:     hnsw_create_func_name,
				Param:    []byte(params),
				IsSingle: true, // model building require single thread mode so set IsSingle to true
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

// arg list [param, hnsw.IndexTableconfig (JSON), search_vec]
func (builder *QueryBuilder) buildHnswSearch(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 3).")
	}

	colDefs := _getColDefs(hnswSearchColDefs)

	params, err := builder.getHnswParams(tbl.Func)
	if err != nil {
		return 0, err
	}
	// remove the first argment and put the first argument to Param
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:  hnsw_search_func_name,
				Param: []byte(params),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

func (builder *QueryBuilder) getHnswParams(fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(builder.GetContext(), "first parameter must be string")
}
