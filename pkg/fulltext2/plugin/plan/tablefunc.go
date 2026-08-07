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
	"github.com/matrixorigin/matrixone/pkg/catalog"
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

// Must match the executor (fulltext2_search.go): doc_id via AppendAny (the pk's own
// type — int/varchar/uuid/…, so T_any, NOT T_int64), score via AppendFixed[float32]
// (T_float32/Width 4, NOT T_float64 — an 8-byte score column read 4-byte writes as
// garbage). Same shape as the MATCH-rewrite path's ftIndexColdefs; a direct
// `FROM fulltext2_search(...)` call uses THESE defs, so they must be correct too.
var fulltext2SearchColDefs = []*plan.ColDef{
	{Name: catalog.FullText2Search_OutCol_DocId, Typ: plan.Type{Id: int32(types.T_any)}},
	{Name: catalog.FullText2Search_OutCol_Score, Typ: plan.Type{Id: int32(types.T_float32), Width: 4}},
}

// FullText2CreateFuncName is the build TVF: CROSS APPLY'd over the source table
// at CREATE INDEX / REINDEX time, it tokenizes each row (datalink/json/parser
// resolved in execution), builds capacity-bounded tag=0 base segments, and
// persists them. Args: [param, TableConfig(JSON), pk, cols...]. Output: a single
// discarded status row.
const FullText2CreateFuncName = "fulltext2_create"

var fulltext2CreateColDefs = []*plan.ColDef{
	{Name: "status", Typ: plan.Type{Id: int32(types.T_int32), Width: 4}},
}

// FullText2CompactFuncName is the standalone MERGE-compaction TVF:
// `fulltext2_compact(db, store, meta, capacity[, position_free[, posting_capacity]])`.
// No driving table; args are passed as-is (no leading param strip, unlike
// search/create). Output: a single `live_docs` count row.
const FullText2CompactFuncName = "fulltext2_compact"

var fulltext2CompactColDefs = []*plan.ColDef{
	{Name: "live_docs", Typ: plan.Type{Id: int32(types.T_int64), NotNullable: true}},
}

func init() {
	planplugin.RegisterTableFunc(FullText2SearchFuncName, buildFullText2Search)
	planplugin.RegisterTableFunc(FullText2CreateFuncName, buildFullText2Create)
	planplugin.RegisterTableFunc(FullText2CompactFuncName, buildFullText2Compact)
}

// buildFullText2Create — arg list: [param, TableConfig(JSON), pk, cols...].
func buildFullText2Create(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "fulltext2_create: invalid number of arguments (NARGS < 4)")
	}
	colDefs := planplugin.DeepCopyColDefList(fulltext2CreateColDefs)
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
				Name:     FullText2CreateFuncName,
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

// buildFullText2Compact — the standalone `fulltext2_compact(db, store, meta,
// capacity, ...)` MERGE table function. No driving table; the varchar args are
// passed through as-is (no leading param strip). Mirrors the search/create
// registration so the plan-side dispatch routes through the plugin registry
// (query_builder.go) instead of a hardcoded switch case.
func buildFullText2Compact(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	colDefs := planplugin.DeepCopyColDefList(fulltext2CompactColDefs)
	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name: FullText2CompactFuncName,
			},
			Cols: colDefs,
		},
		BindingTags:     []int32{pb.GenNewBindTag()},
		Children:        children,
		TblFuncExprList: exprs,
	}
	return pb.AppendNode(node, ctx), nil
}

func getFullText2Params(pb planplugin.PlanBuilder, fn *tree.FuncExpr) (string, error) {
	if _, ok := fn.Exprs[0].(*tree.NumVal); ok {
		return fn.Exprs[0].String(), nil
	}
	return "", moerr.NewNoConfig(pb.GetContext(), "first parameter must be string")
}

// buildFullText2Search — arg list: [param, TableConfig(JSON), pattern, mode].
// param is stripped; the exec side (fulltext2_search.go) reads [cfg, pattern, mode].
func buildFullText2Search(pb planplugin.PlanBuilder, tbl *tree.TableFunction, ctx planplugin.BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) != 4 {
		return 0, moerr.NewInvalidInput(pb.GetContext(), "fulltext2_search: invalid number of arguments (NARGS != 4)")
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
