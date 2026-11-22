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
)

const moChangedTablesName = "mo_changed_tables"

func (builder *QueryBuilder) buildMoChangedTables(ctx *BindContext, exprs []*plan.Expr) (int32, error) {
	if len(exprs) < 1 || len(exprs) > 2 {
		return 0, moerr.NewInvalidArg(builder.GetContext(), "mo_changed_tables argument count", len(exprs))
	}

	markerType := makeGeneratedPlan2Type(types.T_text, types.MaxMediumTextLen, 0, false)
	for i, target := range []plan.Type{markerType, makeGeneratedPlan2Type(types.T_varchar, 300, 0, false)}[:len(exprs)] {
		arg := exprs[i]
		if err := plan.VisitExprTree(arg, func(expr *plan.Expr) error {
			if expr.GetCol() != nil || expr.GetCorr() != nil || expr.GetSub() != nil {
				return moerr.NewInvalidInput(builder.GetContext(), "mo_changed_tables arguments cannot depend on table rows or subqueries")
			}
			return nil
		}); err != nil {
			return 0, err
		}
		if arg.GetP() != nil {
			// The prepared-parameter transport carries the full string. Give
			// its executor the API domain without a narrower intermediate cast.
			arg.Typ = target
			continue
		}
		if !types.T(arg.Typ.Id).IsMySQLString() && arg.Typ.Id != int32(types.T_any) {
			return 0, moerr.NewInvalidInput(builder.GetContext(), "mo_changed_tables arguments must be strings or NULL")
		}
		// Keep the value intact for validation in the operator. In particular,
		// a cast to VARCHAR(256) could truncate an invalid observer name.
		if arg.GetLit() != nil && arg.GetLit().Isnull {
			arg.Typ = target
		}
	}

	varchar := func(name string, width int32, notNull bool) *plan.ColDef {
		return &plan.ColDef{Name: name, Typ: makeGeneratedPlan2Type(types.T_varchar, width, 0, notNull)}
	}
	id := func(name string, typ types.T) *plan.ColDef {
		return &plan.ColDef{Name: name, Typ: typ.ToType().PlanType()}
	}
	columns := []*plan.ColDef{
		varchar("kind", 8, true),
		varchar("reason", 64, false),
		id("account_id", types.T_uint32),
		varchar("account_name", 300, false),
		id("database_id", types.T_uint64),
		varchar("database_name", 256, false),
		id("table_id", types.T_uint64),
		varchar("table_name", 256, false),
		{Name: "marker", Typ: markerType},
		varchar("from_ts", 32, false),
		varchar("to_ts", 32, false),
		id("source_count", types.T_uint64),
		id("baseline_count", types.T_uint64),
		id("changed_count", types.T_uint64),
	}
	node := newChangeFunctionScan(builder, moChangedTablesName, nil, exprs, columns)
	node.TableDef.TblFunc.IsSingle = true
	return builder.appendNode(node, ctx), nil
}
