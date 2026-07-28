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

// IVF-FLAT table-function plumbing — the build*/search* node constructors
// invoked when the planner sees `ivf_create(...)` / `ivf_search(...)` in
// SQL. Lifted from pkg/sql/plan/ivfflat.go (Phase 4g, now deleted).
//
// The plugin's init() registers these with planplugin.RegisterTableFunc;
// pkg/sql/plan/query_builder.go's table-function dispatch falls through
// to the registry in its default arm.

const (
	IVFFLATCreateFuncName = "ivf_create"
	IVFFLATSearchFuncName = "ivf_search"
)

var (
	ivfflatBuildIndexColDefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	// IVFFLATSearchColDefs is the (pkid, score) schema the ivf_search
	// table function returns. Exported so the ApplyForSort body in
	// plan.go can reference it. pkid type gets rewritten at plan time
	// to the parent table's actual PK type.
	IVFFLATSearchColDefs = []*plan.ColDef{
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

func init() {
	planplugin.RegisterTableFunc(IVFFLATCreateFuncName, buildIvfflatCreate)
	planplugin.RegisterTableFunc(IVFFLATSearchFuncName, buildIvfflatSearch)
}

// buildIvfflatCreate constructs a FUNCTION_SCAN node for `ivf_create`.
// arg list: [param, ivf.IndexTableConfig (JSON), vec].
//
// IsSingle is set on the TblFunc because centroid computation requires
// single-threaded execution. Lifted from (*QueryBuilder).buildIvfCreate.
func buildIvfflatCreate(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 2 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS < 2).")
	}

	colDefs := planplugin.DeepCopyColDefList(ivfflatBuildIndexColDefs)
	params, err := getIvfflatTblFuncParams(pb, tbl.Func)
	if err != nil {
		return 0, err
	}

	// remove the first argument and put it to Param
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:     IVFFLATCreateFuncName,
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

// buildIvfflatSearch constructs a FUNCTION_SCAN node for `ivf_search`.
// arg list: [param, IndexTableConfig (JSON), search_vec].
//
// Lifted from (*QueryBuilder).buildIvfSearch.
func buildIvfflatSearch(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS != 3).")
	}

	colDefs := planplugin.DeepCopyColDefList(IVFFLATSearchColDefs)
	params, err := getIvfflatTblFuncParams(pb, tbl.Func)
	if err != nil {
		return 0, err
	}
	// remove the first argument and put it to Param
	exprs = exprs[1:]

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:  IVFFLATSearchFuncName,
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

// getIvfflatTblFuncParams extracts the first argument (a string literal)
// from the user's `ivf_create(...)` / `ivf_search(...)` call.
func getIvfflatTblFuncParams(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}
