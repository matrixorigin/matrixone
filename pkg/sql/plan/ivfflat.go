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
	ivf_create_func_name = "ivf_create"
	ivf_search_func_name = "ivf_search"

	ivfBuildIndexColDefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	ivfSearchColDefs = []*plan.ColDef{
		{
			Name: "pkid",
			Typ: plan.Type{
				Id:          int32(types.T_any),
				NotNullable: false,
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

// arg list [param, ivf.IndexTableConfig (JSON), vec]
func (builder *QueryBuilder) buildIvfCreate(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 3).")
	}

	colDefs := _getColDefs(ivfBuildIndexColDefs)
	params, err := builder.getIvfParams(tbl.Func)
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
				Name:     ivf_create_func_name,
				Param:    []byte(params),
				IsSingle: true, // centroid computation require single thread mode so set IsSingle to true
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.genNewTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

// arg list [param, ivf.IndexTableconfig (JSON), search_vec]
func (builder *QueryBuilder) buildIvfSearch(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS != 3).")
	}

	colDefs := _getColDefs(ivfSearchColDefs)

	params, err := builder.getIvfParams(tbl.Func)
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
				Name:  ivf_search_func_name,
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

func (builder *QueryBuilder) getIvfParams(fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(builder.GetContext(), "first parameter must be string")
}
