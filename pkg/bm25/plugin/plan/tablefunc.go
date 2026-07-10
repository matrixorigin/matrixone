// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
	planplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// bm25 table functions: build (source rows -> chunk store) and search
// (query -> ranked doc_ids). Registered into the plan-side TVF dispatch the
// same way the vector plugins register hnsw_create / hnsw_search.

const (
	Bm25CreateFuncName = "bm25_create"
	Bm25SearchFuncName = "bm25_search"
)

var (
	bm25CreateColDefs = []*plan.ColDef{
		{Name: "status", Typ: plan.Type{Id: int32(types.T_int32), Width: 4}},
	}

	bm25SearchColDefs = []*plan.ColDef{
		{Name: "doc_id", Typ: plan.Type{Id: int32(types.T_int64), Width: 8}},
		{Name: "score", Typ: plan.Type{Id: int32(types.T_float64), Width: 8}},
	}
)

func init() {
	planplugin.RegisterTableFunc(Bm25CreateFuncName, buildBm25Create)
	planplugin.RegisterTableFunc(Bm25SearchFuncName, buildBm25Search)
}

// getBm25Params extracts the leading param constant (arg 0). Mirrors the vector
// plugins' getVectorParams: the first arg is a string const carrying the build
// params; it is stripped from the TblFuncExprList and stored on the node.
func getBm25Params(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}

// buildBm25Create — arg list: [param, TableConfig(JSON), pk, cols...].
func buildBm25Create(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "bm25_create: invalid number of arguments (NARGS < 4)")
	}
	colDefs := planplugin.DeepCopyColDefList(bm25CreateColDefs)
	params, err := getBm25Params(pb, tbl.Func)
	if err != nil {
		return 0, err
	}
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:     Bm25CreateFuncName,
				Param:    []byte(params),
				IsSingle: true,
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{pb.GenNewBindTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return pb.AppendNode(node, ctx), nil
}

// buildBm25Search — arg list: [param, TableConfig(JSON), pattern].
func buildBm25Search(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "bm25_search: invalid number of arguments (NARGS != 3)")
	}
	colDefs := planplugin.DeepCopyColDefList(bm25SearchColDefs)
	params, err := getBm25Params(pb, tbl.Func)
	if err != nil {
		return 0, err
	}
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  Bm25SearchFuncName,
				Param: []byte(params),
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{pb.GenNewBindTag()},
		TblFuncExprList: exprs,
		Children:        children,
	}
	return pb.AppendNode(node, ctx), nil
}
