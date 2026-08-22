// Copyright 2026 Matrix Origin
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
	"encoding/json"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// ForeignTVFParam is the plan->operator parameter for esql_tvf / sql_tvf. It
// carries the source kind, whether the caller supplied a schema, and the FULL
// declared column order. The full order is needed because the optimizer may
// prune or reorder the FUNCTION_SCAN's output columns, while the CSV field
// positions are fixed by the (opaque) foreign query text; the operator maps
// each surviving output column back to its original field position by name.
type ForeignTVFParam struct {
	Kind     string                 `json:"kind"`      // "esql" | "sql"
	NoSchema bool                   `json:"no_schema"` // true => single JSON-array "result" column
	Cols     []ParseJsonlOptionsCol `json:"cols,omitempty"`
}

const (
	ForeignTVFKindESQL = "esql"
	ForeignTVFKindSQL  = "sql"
)

// syntax:
//
//	esql_tvf(esql [, schema [, conn]])
//	sql_tvf(sql   [, schema [, conn]])
//
// schema reuses the parse_jsonl_data column spec and, when present, must be a
// constant string literal (it decides the output columns at plan time). When
// omitted or a NULL literal, the result is a single JSON column whose rows are
// arrays of the source's string fields. conn is a runtime value (typically a
// session variable holding a handle from esql_tvf_connect / sql_tvf_connect);
// when omitted or NULL the default @esql_tvf_config / @sql_tvf_config is used.
func (builder *QueryBuilder) buildEsqlTvf(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	return builder.buildForeignTVF("esql_tvf", ForeignTVFKindESQL, tbl, ctx, exprs, children)
}

func (builder *QueryBuilder) buildSqlTvf(tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	return builder.buildForeignTVF("sql_tvf", ForeignTVFKindSQL, tbl, ctx, exprs, children)
}

func (builder *QueryBuilder) buildForeignTVF(tvfName, kind string, tbl *tree.TableFunction, ctx *BindContext, exprs []*plan.Expr, children []int32) (int32, error) {
	if len(exprs) < 1 || len(exprs) > 3 {
		return 0, moerr.NewInvalidInputf(builder.GetContext(), "%s requires 1 to 3 arguments: (query [, schema [, conn]])", tvfName)
	}

	// Resolve the output schema at plan time. A present, non-NULL schema
	// argument must be a constant string literal.
	var cols []*plan.ColDef
	var schemaCols []ParseJsonlOptionsCol
	noSchema := true
	if len(exprs) >= 2 && !isNullConstExpr(exprs[1]) {
		param2, ok := tbl.Func.Exprs[1].(*tree.NumVal)
		if !ok {
			return 0, moerr.NewInvalidInputf(builder.GetContext(), "the schema (2nd) argument of %s must be a constant string or NULL", tvfName)
		}
		opts, err := parseTVFColumnSchema(builder.GetContext(), param2.String())
		if err != nil {
			return 0, err
		}
		cols, err = buildTVFColDefs(builder.GetContext(), opts)
		if err != nil {
			return 0, err
		}
		if len(cols) == 0 {
			return 0, moerr.NewInvalidInputf(builder.GetContext(), "the schema argument of %s defines no columns", tvfName)
		}
		schemaCols = opts.Cols
		noSchema = false
	}
	if noSchema {
		cols = []*plan.ColDef{{
			Name: "result",
			Typ:  makeSimplePlan2Type(types.T_json),
		}}
	}

	// Runtime arguments the operator evaluates: the query (0) and, if present,
	// the conn handle (2). The schema literal is consumed here, not at runtime.
	runtimeArgs := []*plan.Expr{exprs[0]}
	if len(exprs) >= 3 {
		runtimeArgs = append(runtimeArgs, exprs[2])
	}

	paramData, err := json.Marshal(ForeignTVFParam{Kind: kind, NoSchema: noSchema, Cols: schemaCols})
	if err != nil {
		return 0, err
	}

	node := &plan.Node{
		NodeType: plan.Node_FUNCTION_SCAN,
		Stats:    &plan.Stats{},
		TableDef: &plan.TableDef{
			TableType: "func_table",
			TblFunc: &plan.TableFunction{
				Name:     tvfName,
				Param:    paramData,
				IsSingle: true, // one instance per call; the connection is session-local
			},
			Cols: cols,
		},
		BindingTags:     []int32{builder.genNewBindTag()},
		TblFuncExprList: runtimeArgs,
		Children:        children,
	}
	return builder.appendNode(node, ctx), nil
}

// isNullConstExpr reports whether e is a NULL constant literal.
func isNullConstExpr(e *plan.Expr) bool {
	if lit := e.GetLit(); lit != nil {
		return lit.Isnull
	}
	return false
}
