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

// FullText2SearchFuncName is the search TVF: MATCH over a fulltext2 index →
// ranked (doc_id, score). Registered into the plan-side TVF dispatch like the
// vector plugins' *_search. Args: [param, TableConfig(JSON), pattern].
const FullText2SearchFuncName = "fulltext2_search"

var fulltext2SearchColDefs = []*plan.ColDef{
	{Name: "doc_id", Typ: plan.Type{Id: int32(types.T_int64), Width: 8}},
	{Name: "score", Typ: plan.Type{Id: int32(types.T_float64), Width: 8}},
}

func init() {
	planplugin.RegisterTableFunc(FullText2SearchFuncName, buildFullText2Search)
}

func getFullText2Params(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}

// buildFullText2Search — arg list: [param, TableConfig(JSON), pattern]. param is
// stripped; the exec side (fulltext2_search.go) reads [cfg, pattern].
func buildFullText2Search(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "fulltext2_search: invalid number of arguments (NARGS != 3)")
	}
	colDefs := planplugin.DeepCopyColDefList(fulltext2SearchColDefs)
	params, err := getFullText2Params(pb, tbl.Func)
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
				Name:  FullText2SearchFuncName,
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
