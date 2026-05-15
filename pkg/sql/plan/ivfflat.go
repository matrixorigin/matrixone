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
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
)

// IVF-FLAT search-side constants now live in
// pkg/sql/plan/vectorplan/ivfflat.go (Phase 4e) so the lifted plan body
// in pkg/vectorindex/ivfflat/plugin/plan can reference them. These
// aliases keep existing callers in this file compiling unchanged.
var (
	kIVFCreateFuncName = "ivf_create"
	kIVFSearchFuncName = vectorplan.IVFFLATSearchFuncName

	kIVFBuildIndexColDefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	kIVFSearchColDefs = vectorplan.IVFFLATSearchColDefs
)

// arg list [param, ivf.IndexTableConfig (JSON), vec]
func (builder *QueryBuilder) buildIvfCreate(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 2 {
		return 0, moerr.NewInvalidInput(builder.GetContext(), "Invalid number of arguments (NARGS < 2).")
	}

	colDefs := DeepCopyColDefList(kIVFBuildIndexColDefs)
	params, err := builder.getIvfParams(tbl.Func)
	if err != nil {
		return 0, err
	}

	/*
		scanNode := builder.qry.Nodes[children[0]]
		if scanNode.NodeType != plan.Node_TABLE_SCAN {
			return 0, moerr.NewNoConfig(builder.GetContext(), "child node is not a TABLE SCAN")
		}
	*/

	// remove the first argment and put the first argument to Param
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table", //test if ok
			//Name:               tbl.String(),
			TblFunc: &plan.TableFunction{
				Name:     kIVFCreateFuncName,
				Param:    []byte(params),
				IsSingle: true, // centroid computation require single thread mode so set IsSingle to true
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.GenNewBindTag()},
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

	colDefs := DeepCopyColDefList(kIVFSearchColDefs)

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
				Name:  kIVFSearchFuncName,
				Param: []byte(params),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{builder.GenNewBindTag()},
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
