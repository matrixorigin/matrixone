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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/vectorplan"
)

// CAGRA table-function plumbing — the build*/search* node constructors
// invoked when the planner sees `cagra_create(...)` / `cagra_search(...)`
// in SQL. Lifted from pkg/sql/plan/cagra.go (now deleted).

const (
	CAGRACreateFuncName = "cagra_create"
	CAGRASearchFuncName = "cagra_search"
)

var (
	cagraBuildIndexColDefs = []*plan.ColDef{
		{
			Name: "status",
			Typ: plan.Type{
				Id:          int32(types.T_int32),
				NotNullable: false,
				Width:       4,
			},
		},
	}

	CAGRASearchColDefs = []*plan.ColDef{
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
	vectorplan.RegisterTableFunc(CAGRACreateFuncName, buildCagraCreate)
	vectorplan.RegisterTableFunc(CAGRASearchFuncName, buildCagraSearch)
}

func buildCagraCreate(pb vectorplan.PlanBuilder, tbl *tree.TableFunction, ctx vectorplan.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS < 4).")
	}

	colDefs := vectorplan.DeepCopyColDefList(cagraBuildIndexColDefs)
	params, err := getCagraParams(pb, tbl.Func)
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
				Name:     CAGRACreateFuncName,
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

func buildCagraSearch(pb vectorplan.PlanBuilder, tbl *tree.TableFunction, ctx vectorplan.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 3 && len(exprs) != 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "Invalid number of arguments (NARGS must be 3 or 4).")
	}

	colDefs := vectorplan.DeepCopyColDefList(CAGRASearchColDefs)

	params, err := getCagraParams(pb, tbl.Func)
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
				Name:  CAGRASearchFuncName,
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

func getCagraParams(pb vectorplan.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}
