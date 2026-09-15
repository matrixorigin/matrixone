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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

// mysqlFullGroupByRejectedColumn implements the MySQL ONLY_FULL_GROUP_BY
// exceptions that are local to one query block: a projected column is valid
// when it is constrained to one statement-stable value by WHERE, or when its
// table's complete primary or eligible NOT NULL UNIQUE key is grouped.
// It returns the first column that does not satisfy either exception.
func (builder *QueryBuilder) mysqlFullGroupByRejectedColumn(ctx *BindContext, columns []boundColumn) (string, bool) {
	for _, column := range columns {
		binding := ctx.bindingByTag[column.relation]
		if binding != nil && column.columnPos >= 0 && int(column.columnPos) < len(binding.cols) &&
			builder.mysqlFullGroupByAllowsColumn(ctx, binding, column.columnPos) {
			continue
		}
		return column.name, true
	}
	return "", false
}

func (builder *QueryBuilder) mysqlFullGroupByAllowsColRef(ctx *BindContext, expr *Expr) bool {
	allowed, found := builder.mysqlFullGroupByAllowsExprColumns(ctx, expr)
	return found && allowed
}

// mysqlFullGroupByAllowsExprColumns handles wrappers introduced while binding
// a column, notably ENUM/SET index-to-value conversion. Every source column in
// the wrapper must independently satisfy an ONLY_FULL_GROUP_BY exception.
func (builder *QueryBuilder) mysqlFullGroupByAllowsExprColumns(ctx *BindContext, expr *Expr) (allowed, found bool) {
	if col := expr.GetCol(); col != nil {
		binding := ctx.bindingByTag[col.RelPos]
		return binding != nil && builder.mysqlFullGroupByAllowsColumn(ctx, binding, col.ColPos), true
	}

	fn := expr.GetF()
	if fn == nil {
		return true, false
	}
	for _, arg := range fn.Args {
		argAllowed, argFound := builder.mysqlFullGroupByAllowsExprColumns(ctx, arg)
		if argFound {
			found = true
			if !argAllowed {
				return false, true
			}
		}
	}
	return true, found
}

func (builder *QueryBuilder) mysqlFullGroupByAllowsColumn(ctx *BindContext, binding *Binding, columnPos int32) bool {
	return filterListHasSingleValueEqualityOnCol(ctx.whereFilters, binding.tag, columnPos) ||
		builder.groupByIncludesPrimaryKey(ctx, binding) ||
		builder.groupByIncludesNotNullUniqueKey(ctx, binding)
}

// This is a binding-local acceptance proof, not a uniqueness property for
// optimizer rewrites. In particular, it does not infer keys through joins or
// derived relations, or turn filtered nullable keys into NOT NULL keys.
func (builder *QueryBuilder) groupByIncludesNotNullUniqueKey(ctx *BindContext, binding *Binding) bool {
	if binding.nodeId < 0 || int(binding.nodeId) >= len(builder.qry.Nodes) {
		return false
	}
	node := builder.qry.Nodes[binding.nodeId]
	if node == nil || node.NodeType != pbplan.Node_TABLE_SCAN || node.TableDef == nil {
		return false
	}
	for _, index := range node.TableDef.Indexes {
		if index == nil || !index.Unique || !index.TableExist || index.IndexTableName == "" || len(index.Parts) == 0 ||
			!catalog.IsRegularIndexAlgo(index.IndexAlgo) || catalog.IsRTreeIndexAlgo(index.IndexAlgo) {
			continue
		}
		prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(index.IndexAlgoParams)
		if err != nil || len(prefixes) != 0 {
			continue
		}
		if groupByIncludesNotNullUniqueParts(ctx, binding, node.TableDef, index.Parts) {
			return true
		}
	}
	return false
}

func groupByIncludesNotNullUniqueParts(ctx *BindContext, binding *Binding, table *pbplan.TableDef, parts []string) bool {
	seen := make(map[int32]struct{}, len(parts))
	for _, name := range parts {
		pos, ok := tableColumnPosition(table, name)
		if !ok || pos < 0 || int(pos) >= len(binding.cols) {
			return false
		}
		if _, duplicate := seen[pos]; duplicate {
			return false
		}
		seen[pos] = struct{}{}
		col := table.Cols[pos]
		if col.Hidden || col.GeneratedCol != nil || col.Default == nil || col.Default.NullAbility ||
			!primaryKeyColumnTypeSupportsSQLEqualityProof(col.Typ) || !groupByContainsColumn(ctx, binding.tag, pos) {
			return false
		}
	}
	return true
}

func (bc *BindContext) aggregateQueryForFullGroupBy() bool {
	return bc != nil &&
		(len(bc.groups) > 0 ||
			len(bc.times) > 0 ||
			len(bc.aggregates) > 0 ||
			bc.pendingAggregateQuery)
}

func (builder *QueryBuilder) groupByIncludesPrimaryKey(ctx *BindContext, binding *Binding) bool {
	if binding.nodeId < 0 || int(binding.nodeId) >= len(builder.qry.Nodes) {
		return false
	}
	tableDef := builder.qry.Nodes[binding.nodeId].TableDef
	primaryKeyPositions, ok := sqlEqualityCompatiblePrimaryKeyColumnPositions(tableDef)
	if !ok {
		return false
	}
	for _, colPos := range primaryKeyPositions {
		if colPos < 0 || int(colPos) >= len(binding.cols) ||
			!groupByContainsColumn(ctx, binding.tag, colPos) {
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

func groupByContainsColumn(ctx *BindContext, tag, columnPos int32) bool {
	for pos, group := range ctx.groups {
		if col := group.GetCol(); col != nil && col.RelPos == tag && col.ColPos == columnPos {
			// ROLLUP and CUBE share the complete group expression list across
			// their grouping-set branches. A key column only establishes a
			// functional dependency in branches where that group is active.
			return len(ctx.groupingFlag) == 0 || pos < len(ctx.groupingFlag) && ctx.groupingFlag[pos]
		}
	}
	return false
}
