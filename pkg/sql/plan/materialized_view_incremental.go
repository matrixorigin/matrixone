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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// buildMaterializedViewIncrementalPlan recognizes the row-local subset that can
// be evaluated over a bounded VALUES batch by the normal SQL engine. Expressions
// outside this subset deliberately leave IncrementalSpec empty so the consumer
// performs a snapshot-consistent full refresh instead.
func buildMaterializedViewIncrementalPlan(stmt *tree.Select, outputCols []*ColDef, stateTable ...string) (string, []*ColDef, string) {
	if stmt == nil || stmt.With != nil || stmt.TimeWindow != nil || stmt.Limit != nil || stmt.RankOption != nil {
		return "", nil, ""
	}
	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || clause.Having != nil ||
		clause.From == nil || len(clause.From.Tables) != 1 || len(clause.Exprs) != len(outputCols) {
		return "", nil, ""
	}
	selectDistinct := clause.Distinct
	if selectDistinct {
		if clause.GroupBy != nil {
			return "", nil, ""
		}
	} else if clause.GroupBy == nil || clause.GroupBy.Cube || clause.GroupBy.Rollup ||
		clause.GroupBy.GroupingSets || clause.GroupBy.Apart {
		return "", nil, ""
	}
	source := materializedViewSourceTable(clause.From.Tables[0])
	if source == nil {
		return "", nil, ""
	}
	sourceAlias := string(source.ObjectName)
	if aliased, ok := clause.From.Tables[0].(*tree.AliasedTableExpr); ok && aliased.As.Alias != "" {
		sourceAlias = string(aliased.As.Alias)
	}

	collector := &materializedViewIncrementalColumnCollector{}
	groups := make([]materializedViewIncrementalGroup, 0)
	groupBySQL := make(map[string]int)
	groupExprLists := []tree.Exprs(nil)
	if selectDistinct {
		distinctExprs := make(tree.Exprs, len(clause.Exprs))
		for i := range clause.Exprs {
			distinctExprs[i] = clause.Exprs[i].Expr
		}
		groupExprLists = []tree.Exprs{distinctExprs}
	} else {
		groupExprLists = clause.GroupBy.GroupByExprsList
	}
	for _, exprs := range groupExprLists {
		for _, expr := range exprs {
			if !materializedViewIncrementalScalarSupported(expr) || !collector.collect(expr) {
				return "", nil, ""
			}
			sql := materializedViewIncrementalExprSQL(expr)
			groupBySQL[strings.ToLower(sql)] = len(groups)
			groups = append(groups, materializedViewIncrementalGroup{Expression: sql})
		}
	}
	if len(groups) == 0 {
		return "", nil, ""
	}

	spec := materializedViewIncrementalDescription{
		Version:        2,
		Strategy:       "direct-delta",
		SourceAlias:    sourceAlias,
		Groups:         groups,
		RowCountColumn: materializedViewUniqueStateColumn(outputCols, "__mo_mv_row_count"),
	}
	needsAuxiliaryState := false
	if clause.Where != nil {
		if !materializedViewIncrementalScalarSupported(clause.Where.Expr) || !collector.collect(clause.Where.Expr) {
			return "", nil, ""
		}
		spec.Filter = materializedViewIncrementalExprSQL(clause.Where.Expr)
	}

	stateCols := []*ColDef{materializedViewStateColumn(spec.RowCountColumn, Type{Id: int32(types.T_int64)}, false)}
	stateExprs := []string{"count(*)"}
	for i, selectExpr := range clause.Exprs {
		outputName := outputCols[i].Name
		if selectExpr.As != nil && !selectExpr.As.Empty() {
			outputName = selectExpr.As.Origin()
		}
		if groupIdx, found := groupBySQL[strings.ToLower(materializedViewIncrementalExprSQL(selectExpr.Expr))]; found {
			if spec.Groups[groupIdx].OutputColumn != "" {
				return "", nil, ""
			}
			spec.Groups[groupIdx].OutputColumn = outputName
			spec.Groups[groupIdx].NotNullable = outputCols[i].Typ.NotNullable
			continue
		}
		if selectDistinct {
			return "", nil, ""
		}
		fn, ok := selectExpr.Expr.(*tree.FuncExpr)
		if !ok || fn.WindowSpec != nil || len(fn.OrderBy) != 0 {
			return "", nil, ""
		}
		name := materializedViewIncrementalFunctionName(fn)
		if name != "count" && name != "sum" && name != "avg" && name != "min" && name != "max" {
			return "", nil, ""
		}
		agg := materializedViewIncrementalAggregate{Kind: name, OutputColumn: outputName}
		if fn.Type == tree.FUNC_TYPE_DISTINCT {
			if name != "count" || len(fn.Exprs) != 1 || !materializedViewIncrementalScalarSupported(fn.Exprs[0]) || !collector.collect(fn.Exprs[0]) {
				return "", nil, ""
			}
			agg.Kind = "count_distinct"
			agg.StateIndex = len(spec.Aggregates) + 1
			agg.InputExpression = materializedViewIncrementalExprSQL(fn.Exprs[0])
			spec.Strategy = "hybrid-state"
			needsAuxiliaryState = true
		} else if name == "count" && (len(fn.Exprs) == 0 || len(fn.Exprs) == 1 && isMaterializedViewStar(fn.Exprs[0])) {
			agg.Kind = "count_star"
		} else {
			if len(fn.Exprs) != 1 || !materializedViewIncrementalScalarSupported(fn.Exprs[0]) || !collector.collect(fn.Exprs[0]) {
				return "", nil, ""
			}
			agg.InputExpression = materializedViewIncrementalExprSQL(fn.Exprs[0])
			if name == "count" {
				agg.Kind = "count_column"
			}
		}
		if agg.Kind == "count_distinct" {
			// The visible value is initialized by the normal snapshot query. Tail
			// maintenance stores exact value multiplicities in the auxiliary state
			// table and updates this column only on 0<->1 transitions.
		} else if name == "avg" {
			agg.StateSumColumn = materializedViewUniqueStateColumn(outputCols, fmt.Sprintf("__mo_mv_avg_sum_%d", i))
			agg.StateCountColumn = materializedViewUniqueStateColumn(outputCols, fmt.Sprintf("__mo_mv_avg_count_%d", i))
			stateCols = append(stateCols,
				materializedViewStateColumn(agg.StateSumColumn, outputCols[i].Typ, true),
				materializedViewStateColumn(agg.StateCountColumn, Type{Id: int32(types.T_int64)}, false))
			stateExprs = append(stateExprs, "sum("+agg.InputExpression+")", "count("+agg.InputExpression+")")
		} else if name == "sum" {
			agg.StateSumColumn = materializedViewUniqueStateColumn(outputCols, fmt.Sprintf("__mo_mv_sum_sum_%d", i))
			agg.StateCountColumn = materializedViewUniqueStateColumn(outputCols, fmt.Sprintf("__mo_mv_sum_count_%d", i))
			stateCols = append(stateCols,
				materializedViewStateColumn(agg.StateSumColumn, outputCols[i].Typ, true),
				materializedViewStateColumn(agg.StateCountColumn, Type{Id: int32(types.T_int64)}, false))
			stateExprs = append(stateExprs, "sum("+agg.InputExpression+")", "count("+agg.InputExpression+")")
		}
		if name == "min" || name == "max" {
			spec.Strategy = "hybrid-affected-group"
			needsAuxiliaryState = true
		}
		spec.Aggregates = append(spec.Aggregates, agg)
	}
	for _, group := range spec.Groups {
		if group.OutputColumn == "" {
			return "", nil, ""
		}
	}
	if len(spec.Aggregates) == 0 && !selectDistinct {
		return "", nil, ""
	}
	if needsAuxiliaryState && len(stateTable) > 0 {
		spec.StateTable = stateTable[0]
	}
	spec.GroupKeyColumn = materializedViewUniqueStateColumn(outputCols, "__mo_mv_group_key")
	stateCols = append(stateCols, materializedViewStateColumn(spec.GroupKeyColumn, Type{
		Id: int32(types.T_varchar), Width: types.MaxVarcharLen, Charset: uint32(types.CharsetBinary),
	}, false))
	groupKeyArgs := make([]string, len(spec.Groups))
	for i := range spec.Groups {
		groupKeyArgs[i] = spec.Groups[i].Expression
	}
	stateExprs = append(stateExprs, "serial_full("+strings.Join(groupKeyArgs, ",")+")")

	spec.SourceColumns = collector.columns()
	spec.StateColumns = make([]string, len(stateCols))
	for i := range stateCols {
		spec.StateColumns[i] = stateCols[i].Name
	}
	stateRefreshSQL, ok := materializedViewRefreshSQLWithStateForMode(stmt, stateExprs, spec.StateColumns, selectDistinct, groupExprLists)
	if !ok {
		return "", nil, ""
	}
	b, err := json.Marshal(spec)
	if err != nil {
		return "", nil, ""
	}
	return base64.StdEncoding.EncodeToString(b), stateCols, stateRefreshSQL
}

func materializedViewRefreshSQLWithStateForMode(
	stmt *tree.Select,
	expressions, aliases []string,
	selectDistinct bool,
	groupExprLists []tree.Exprs,
) (string, bool) {
	if !selectDistinct {
		return materializedViewRefreshSQLWithState(stmt, expressions, aliases)
	}
	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok {
		return "", false
	}
	originalDistinct, originalGroupBy := clause.Distinct, clause.GroupBy
	clause.Distinct = false
	clause.GroupBy = &tree.GroupByClause{GroupByExprsList: groupExprLists}
	defer func() {
		clause.Distinct = originalDistinct
		clause.GroupBy = originalGroupBy
	}()
	return materializedViewRefreshSQLWithState(stmt, expressions, aliases)
}

func materializedViewIncrementalExprSQL(expr tree.Expr) string {
	return tree.StringWithOpts(expr, dialect.MYSQL, tree.WithSingleQuoteString())
}

func materializedViewIncrementalFunctionName(fn *tree.FuncExpr) string {
	if fn == nil {
		return ""
	}
	if fn.FuncName != nil {
		return strings.ToLower(fn.FuncName.Origin())
	}
	if fn.Func.FunctionReference != nil {
		if name, ok := fn.Func.FunctionReference.(*tree.UnresolvedName); ok {
			return strings.ToLower(name.ColName())
		}
	}
	return ""
}

func materializedViewIncrementalScalarSupported(expr tree.Expr) bool {
	switch node := expr.(type) {
	case *tree.UnresolvedName:
		return !node.Star && node.NumParts > 0 && node.NumParts <= 2
	case *tree.NumVal:
		return true
	case *tree.BinaryExpr:
		return materializedViewIncrementalScalarSupported(node.Left) && materializedViewIncrementalScalarSupported(node.Right)
	case *tree.UnaryExpr:
		return materializedViewIncrementalScalarSupported(node.Expr)
	case *tree.ComparisonExpr:
		return node.SubOp == 0 && node.Escape == nil && materializedViewIncrementalScalarSupported(node.Left) && materializedViewIncrementalScalarSupported(node.Right)
	case *tree.AndExpr:
		return materializedViewIncrementalScalarSupported(node.Left) && materializedViewIncrementalScalarSupported(node.Right)
	case *tree.OrExpr:
		return materializedViewIncrementalScalarSupported(node.Left) && materializedViewIncrementalScalarSupported(node.Right)
	case *tree.XorExpr:
		return materializedViewIncrementalScalarSupported(node.Left) && materializedViewIncrementalScalarSupported(node.Right)
	case *tree.NotExpr:
		return materializedViewIncrementalScalarSupported(node.Expr)
	case *tree.IsNullExpr:
		return materializedViewIncrementalScalarSupported(node.Expr)
	case *tree.IsNotNullExpr:
		return materializedViewIncrementalScalarSupported(node.Expr)
	case *tree.RangeCond:
		return materializedViewIncrementalScalarSupported(node.Left) && materializedViewIncrementalScalarSupported(node.From) && materializedViewIncrementalScalarSupported(node.To)
	case *tree.CastExpr:
		return materializedViewIncrementalScalarSupported(node.Expr)
	case *tree.CaseExpr:
		if node.Expr != nil && !materializedViewIncrementalScalarSupported(node.Expr) {
			return false
		}
		for _, when := range node.Whens {
			if when == nil || !materializedViewIncrementalScalarSupported(when.Cond) || !materializedViewIncrementalScalarSupported(when.Val) {
				return false
			}
		}
		return node.Else == nil || materializedViewIncrementalScalarSupported(node.Else)
	case *tree.FuncExpr:
		name := materializedViewIncrementalFunctionName(node)
		if name != "date_trunc" && name != "coalesce" && name != "ifnull" && name != "abs" && name != "floor" && name != "ceil" {
			return false
		}
		if node.WindowSpec != nil || len(node.OrderBy) != 0 || node.Type == tree.FUNC_TYPE_DISTINCT {
			return false
		}
		for _, arg := range node.Exprs {
			if !materializedViewIncrementalScalarSupported(arg) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

type materializedViewIncrementalColumnCollector struct {
	set map[string]struct{}
}

func (c *materializedViewIncrementalColumnCollector) collect(expr tree.Expr) (ok bool) {
	if c.set == nil {
		c.set = make(map[string]struct{})
	}
	defer func() { ok = ok && recover() == nil }()
	_, ok = expr.Accept(c)
	return ok
}

func (c *materializedViewIncrementalColumnCollector) Enter(expr tree.Expr) (tree.Expr, bool) {
	if name, ok := expr.(*tree.UnresolvedName); ok && !name.Star {
		c.set[strings.ToLower(name.ColName())] = struct{}{}
	}
	return expr, false
}

func (c *materializedViewIncrementalColumnCollector) Exit(expr tree.Expr) (tree.Expr, bool) {
	return expr, true
}

func (c *materializedViewIncrementalColumnCollector) columns() []string {
	columns := make([]string, 0, len(c.set))
	for name := range c.set {
		columns = append(columns, name)
	}
	sort.Strings(columns)
	return columns
}

func materializedViewUniqueStateColumn(outputCols []*ColDef, base string) string {
	used := make(map[string]struct{}, len(outputCols))
	for _, col := range outputCols {
		used[strings.ToLower(col.Name)] = struct{}{}
	}
	name := base
	for suffix := 1; ; suffix++ {
		if _, exists := used[strings.ToLower(name)]; !exists {
			return name
		}
		name = fmt.Sprintf("%s_%d", base, suffix)
	}
}

func materializedViewStateColumn(name string, typ Type, nullable bool) *ColDef {
	typ.NotNullable = !nullable
	return &ColDef{
		Name: name, Hidden: true, Typ: typ,
		Default: &planpb.Default{NullAbility: nullable}, NotNull: !nullable,
	}
}

func materializedViewRefreshSQLWithState(stmt *tree.Select, expressions, aliases []string) (string, bool) {
	clause, ok := stmt.Select.(*tree.SelectClause)
	if !ok || len(expressions) != len(aliases) {
		return "", false
	}
	originalLen := len(clause.Exprs)
	parsed := make([]tree.Statement, 0, len(expressions))
	defer func() {
		clause.Exprs = clause.Exprs[:originalLen]
		for _, statement := range parsed {
			statement.Free()
		}
	}()
	for i, expression := range expressions {
		statement, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
			"select "+expression+" as `"+strings.ReplaceAll(aliases[i], "`", "``")+"`", 1)
		if err != nil {
			return "", false
		}
		parsed = append(parsed, statement)
		selectStmt, ok := statement.(*tree.Select)
		if !ok {
			return "", false
		}
		selectClause, ok := selectStmt.Select.(*tree.SelectClause)
		if !ok || len(selectClause.Exprs) != 1 {
			return "", false
		}
		clause.Exprs = append(clause.Exprs, selectClause.Exprs[0])
	}
	return materializedViewRefreshSQL(stmt), true
}
