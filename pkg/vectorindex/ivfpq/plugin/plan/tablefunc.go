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

// IVF-PQ table-function plumbing — the build*/search* node constructors
// invoked when the planner sees `ivfpq_create(...)` / `ivfpq_search(...)`
// in SQL. Lifted from pkg/sql/plan/ivfpq.go (now deleted).
//
// The plugin's init() registers these with planplugin.RegisterTableFunc;
// pkg/sql/plan/query_builder.go's table-function dispatch falls through to
// the registry in its default arm.

const (
	IVFPQCreateFuncName = "ivfpq_create"
	IVFPQSearchFuncName = "ivfpq_search"
)

var (
	ivfpqBuildIndexColDefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	// IVFPQSearchColDefs is the (pkid, score) schema the ivfpq_search
	// table function returns. Exported so the ApplyForSort body in
	// plan.go can reference it.
	IVFPQSearchColDefs = []*plan.ColDef{
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

func init() {
	planplugin.RegisterTableFunc(IVFPQCreateFuncName, buildIvfpqCreate)
	planplugin.RegisterTableFunc(IVFPQSearchFuncName, buildIvfpqSearch)
}

// buildIvfpqCreate constructs a FUNCTION_SCAN node for `ivfpq_create`.
// arg list: [param, ivfpq.IndexTableConfig (JSON), pkid, vec].
//
// Lifted from (*QueryBuilder).buildIvfpqCreate (was pkg/sql/plan/ivfpq.go).
func buildIvfpqCreate(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS < 4).")
	}

	colDefs := planplugin.DeepCopyColDefList(ivfpqBuildIndexColDefs)
	params, err := getIvfpqParams(pb, tbl.Func)
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
				Name:     IVFPQCreateFuncName,
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

// buildIvfpqSearch constructs a FUNCTION_SCAN node for `ivfpq_search`.
// arg list: [param, IndexTableConfig (JSON), search_vec, filter_predicates_json?].
// The trailing filter_predicates_json is optional — omitted for unfiltered
// search.
//
// Lifted from (*QueryBuilder).buildIvfpqSearch (was pkg/sql/plan/ivfpq.go).
func buildIvfpqSearch(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 && len(exprs) != 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS must be 3 or 4).")
	}

	colDefs := planplugin.DeepCopyColDefList(IVFPQSearchColDefs)

	params, err := getIvfpqParams(pb, tbl.Func)
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
				Name:  IVFPQSearchFuncName,
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

func getIvfpqParams(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}
