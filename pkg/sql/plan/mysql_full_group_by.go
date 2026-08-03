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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// mysqlFullGroupByAllowsColumns implements the MySQL ONLY_FULL_GROUP_BY
// exceptions that are local to one query block: a projected column is valid
// when it is constrained to one statement-stable value by WHERE, or when its
// table's complete declared primary key is present in GROUP BY.
func (builder *QueryBuilder) mysqlFullGroupByAllowsColumns(ctx *BindContext, columns []string) bool {
	for _, name := range columns {
		separator := strings.LastIndex(name, ".")
		if separator < 1 || separator == len(name)-1 {
			return false
		}
		table, column := name[:separator], name[separator+1:]
		binding := ctx.bindingByTable[table]
		if binding == nil {
			return false
		}
		columnPos := binding.FindColumn(column)
		if columnPos == NotFound || columnPos == AmbiguousName {
			return false
		}
		if builder.mysqlFullGroupByAllowsColumn(ctx, binding, columnPos) {
			continue
		}
		return false
	}
	return true
}

func (builder *QueryBuilder) mysqlFullGroupByAllowsColRef(ctx *BindContext, expr *Expr) bool {
	col := expr.GetCol()
	if col == nil {
		return false
	}
	binding := ctx.bindingByTag[col.RelPos]
	return binding != nil && builder.mysqlFullGroupByAllowsColumn(ctx, binding, col.ColPos)
}

func (builder *QueryBuilder) mysqlFullGroupByAllowsColumn(ctx *BindContext, binding *Binding, columnPos int32) bool {
	return filterListHasSingleValueEqualityOnCol(ctx.whereFilters, binding.tag, columnPos) ||
		builder.groupByIncludesPrimaryKey(ctx, binding)
}

func (builder *QueryBuilder) groupByIncludesPrimaryKey(ctx *BindContext, binding *Binding) bool {
	if binding.nodeId < 0 || int(binding.nodeId) >= len(builder.qry.Nodes) {
		return false
	}
	tableDef := builder.qry.Nodes[binding.nodeId].TableDef
	if tableDef == nil || tableDef.Pkey == nil || tableDef.Pkey.PkeyColName == catalog.FakePrimaryKeyColName {
		return false
	}

	primaryKeyNames := tableDef.Pkey.Names
	if len(primaryKeyNames) > 0 {
		for _, name := range primaryKeyNames {
			colPos := binding.FindColumn(strings.ToLower(name))
			if colPos == NotFound || colPos == AmbiguousName || !groupByContainsColumn(ctx.groups, binding.tag, colPos) {
				return false
			}
		}
		return len(primaryKeyNames) > 0
	}

	primaryKeyCols := tableDef.Pkey.Cols
	if len(primaryKeyCols) == 0 && tableDef.Pkey.PkeyColName != "" {
		colPos := binding.FindColumn(strings.ToLower(tableDef.Pkey.PkeyColName))
		return colPos != NotFound && colPos != AmbiguousName && groupByContainsColumn(ctx.groups, binding.tag, colPos)
	}
	if len(primaryKeyCols) == 0 {
		return false
	}
	for _, colPos := range primaryKeyCols {
		if colPos >= uint64(len(binding.cols)) || !groupByContainsColumn(ctx.groups, binding.tag, int32(colPos)) {
			return false
		}
	}
	return true
}

func filterListHasSingleValueEqualityOnCol(filters []*Expr, tag, columnPos int32) bool {
	for _, filter := range filters {
		fn := filter.GetF()
		if fn == nil || fn.Func.ObjName != "=" || len(fn.Args) != 2 {
			continue
		}
		if exprIsCol(fn.Args[0], tag, columnPos) && isMySQLFullGroupBySingleValueExpr(fn.Args[1]) {
			return true
		}
		if exprIsCol(fn.Args[1], tag, columnPos) && isMySQLFullGroupBySingleValueExpr(fn.Args[0]) {
			return true
		}
	}
	return false
}

func isMySQLFullGroupBySingleValueExpr(expr *Expr) bool {
	switch e := expr.Expr.(type) {
	case *pbplan.Expr_Lit, *pbplan.Expr_P, *pbplan.Expr_V, *pbplan.Expr_Vec, *pbplan.Expr_T:
		return true
	case *pbplan.Expr_F:
		overload, ok := function.GetFunctionByIdWithoutError(e.F.Func.Obj)
		if !ok || overload.CannotFold() {
			return false
		}
		for _, arg := range e.F.Args {
			if !isMySQLFullGroupBySingleValueExpr(arg) {
				return false
			}
		}
		return true
	case *pbplan.Expr_List:
		for _, item := range e.List.List {
			if !isMySQLFullGroupBySingleValueExpr(item) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func groupByContainsColumn(groups []*Expr, tag, columnPos int32) bool {
	for _, group := range groups {
		if col := group.GetCol(); col != nil && col.RelPos == tag && col.ColPos == columnPos {
			return true
		}
	}
	return false
}
