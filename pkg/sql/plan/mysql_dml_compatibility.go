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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func rejectWindowFunctionUnlessMatrixOneNative(ctx CompilerContext, expr tree.Expr) error {
	mode, err := ctx.ResolveVariable("sql_mode", true, false)
	if err == nil {
		if modeStr, ok := mode.(string); ok && mysql.HasMatrixOneNativeSQLMode(modeStr) {
			return nil
		}
	}

	return rejectWindowFunction(ctx, expr)
}

func rejectWindowFunction(ctx CompilerContext, expr tree.Expr) error {
	if name, ok := findNestedWindowFuncName(expr); ok {
		return moerr.NewWindowInvalidUse(ctx.GetContext(), name)
	}
	return nil
}

// validateMultiTableUpdateClauses enforces the MySQL multiple-table UPDATE
// grammar. ORDER BY and LIMIT belong to the single-table form only.
func validateMultiTableUpdateClauses(ctx CompilerContext, stmt *tree.Update) error {
	if !updateHasMultiTableTargetShape(stmt) {
		return nil
	}
	if len(stmt.OrderBy) > 0 {
		return moerr.NewWrongUsage(ctx.GetContext(), "UPDATE", "ORDER BY")
	}
	if stmt.Limit != nil {
		return moerr.NewWrongUsage(ctx.GetContext(), "UPDATE", "LIMIT")
	}
	return nil
}

func validateUpdateWindowFunctions(ctx CompilerContext, stmt *tree.Update) error {
	for _, updateExpr := range stmt.Exprs {
		if err := rejectWindowFunctionUnlessMatrixOneNative(ctx, updateExpr.Expr); err != nil {
			return err
		}
	}
	return nil
}

func tableExprContainsJoin(expr tree.TableExpr) bool {
	switch tbl := expr.(type) {
	case *tree.JoinTableExpr:
		return true
	case *tree.ParenTableExpr:
		return tableExprContainsJoin(tbl.Expr)
	case *tree.AliasedTableExpr:
		return tableExprContainsJoin(tbl.Expr)
	default:
		return false
	}
}

type mysqlDMLTarget struct {
	objID   int64
	tableID uint64
	schema  string
	name    string
}

func makeMySQLDMLTargets(objRefs []*ObjectRef, tableDefs []*TableDef) []mysqlDMLTarget {
	targets := make([]mysqlDMLTarget, 0, len(tableDefs))
	for i, tableDef := range tableDefs {
		if tableDef == nil {
			continue
		}
		target := mysqlDMLTarget{tableID: tableDef.TblId, schema: tableDef.DbName, name: tableDef.Name}
		if i < len(objRefs) && objRefs[i] != nil {
			target.objID = objRefs[i].Obj
			if objRefs[i].SchemaName != "" {
				target.schema = objRefs[i].SchemaName
			}
			if objRefs[i].ObjName != "" {
				target.name = objRefs[i].ObjName
			}
		}
		targets = append(targets, target)
	}
	return targets
}

// validateUpdateTargetSubqueries and validateDeleteTargetSubqueries implement
// ER_UPDATE_TABLE_USED (1093). A direct read of a modified table from a scalar,
// IN, or EXISTS subquery is rejected. A read behind a derived-table boundary is
// deliberately allowed: MySQL permits that form when the derived table is
// materialized, and MatrixOne's derived-table plan supplies the same statement
// snapshot boundary.
func validateUpdateTargetSubqueries(
	ctx CompilerContext,
	stmt *tree.Update,
	objRefs []*ObjectRef,
	tableDefs []*TableDef,
) error {
	targets := makeMySQLDMLTargets(objRefs, tableDefs)
	visibleCTEs := mysqlCTENames(stmt.With, nil)
	outerTargetQualifiers := mysqlUpdateTargetQualifiers(ctx, stmt.Tables, targets)

	for _, updateExpr := range stmt.Exprs {
		if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
			ctx, updateExpr.Expr, targets, visibleCTEs, outerTargetQualifiers,
		); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	if stmt.Where != nil {
		if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
			ctx, stmt.Where.Expr, targets, visibleCTEs, outerTargetQualifiers,
		); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	for _, order := range stmt.OrderBy {
		if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
			ctx, order.Expr, targets, visibleCTEs, outerTargetQualifiers,
		); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	if target, ok := findMySQLDMLTargetInLimitWithOuterTargets(
		ctx, stmt.Limit, targets, visibleCTEs, outerTargetQualifiers,
	); ok {
		return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
	}
	for _, tableExpr := range stmt.Tables {
		if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr, targets, visibleCTEs); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	if stmt.From != nil {
		for _, tableExpr := range stmt.From.Tables {
			if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr, targets, visibleCTEs); ok {
				return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
			}
		}
	}
	return nil
}

func validateDeleteTargetSubqueries(
	ctx CompilerContext,
	stmt *tree.Delete,
	objRefs []*ObjectRef,
	tableDefs []*TableDef,
) error {
	targets := makeMySQLDMLTargets(objRefs, tableDefs)
	visibleCTEs := mysqlCTENames(stmt.With, nil)

	if stmt.Where != nil {
		if target, ok := findMySQLDMLTargetInExpr(ctx, stmt.Where.Expr, targets, visibleCTEs); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	for _, order := range stmt.OrderBy {
		if target, ok := findMySQLDMLTargetInExpr(ctx, order.Expr, targets, visibleCTEs); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	if target, ok := findMySQLDMLTargetInLimit(ctx, stmt.Limit, targets, visibleCTEs); ok {
		return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
	}
	for _, tableExpr := range stmt.Tables {
		if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr, targets, visibleCTEs); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	for _, tableExpr := range stmt.TableRefs {
		if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr, targets, visibleCTEs); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
	return nil
}

func mysqlCTENames(with *tree.With, inherited map[string]struct{}) map[string]struct{} {
	if with == nil || len(with.CTEs) == 0 {
		return inherited
	}
	result := make(map[string]struct{}, len(inherited)+len(with.CTEs))
	for name := range inherited {
		result[name] = struct{}{}
	}
	for _, cte := range with.CTEs {
		if cte != nil && cte.Name != nil {
			result[strings.ToLower(string(cte.Name.Alias))] = struct{}{}
		}
	}
	return result
}

func findMySQLDMLTargetInLimit(
	ctx CompilerContext,
	limit *tree.Limit,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
) (string, bool) {
	return findMySQLDMLTargetInLimitWithOuterTargets(ctx, limit, targets, visibleCTEs, nil)
}

func findMySQLDMLTargetInLimitWithOuterTargets(
	ctx CompilerContext,
	limit *tree.Limit,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
) (string, bool) {
	if limit == nil {
		return "", false
	}
	if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
		ctx, limit.Count, targets, visibleCTEs, outerTargetQualifiers,
	); ok {
		return target, true
	}
	return findMySQLDMLTargetInExprWithOuterTargets(
		ctx, limit.Offset, targets, visibleCTEs, outerTargetQualifiers,
	)
}

func findMySQLDMLTargetInExpr(
	ctx CompilerContext,
	expr tree.Expr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
) (string, bool) {
	return findMySQLDMLTargetInExprWithOuterTargets(ctx, expr, targets, visibleCTEs, nil)
}

func findMySQLDMLTargetInExprWithOuterTargets(
	ctx CompilerContext,
	expr tree.Expr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
) (string, bool) {
	if expr == nil || len(targets) == 0 {
		return "", false
	}
	var found string
	walkGroupingSetOrderByExpr(expr, func(node tree.Expr) bool {
		if found != "" {
			return false
		}
		subquery, ok := node.(*tree.Subquery)
		if !ok {
			return true
		}
		found, _ = findMySQLDMLTargetInSelectWithOuterTargets(
			ctx, subquery.Select, targets, visibleCTEs, outerTargetQualifiers,
		)
		// findMySQLDMLTargetInSelect owns traversal below this subquery. Do not
		// let the reflection walker enter it a second time.
		return false
	})
	return found, found != ""
}

func findMySQLDMLTargetInSelect(
	ctx CompilerContext,
	stmt tree.SelectStatement,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
) (string, bool) {
	return findMySQLDMLTargetInSelectWithOuterTargets(ctx, stmt, targets, visibleCTEs, nil)
}

func findMySQLDMLTargetInSelectWithOuterTargets(
	ctx CompilerContext,
	stmt tree.SelectStatement,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
) (string, bool) {
	return findMySQLDMLTargetInSelectWithQueryTargets(
		ctx, stmt, targets, visibleCTEs, outerTargetQualifiers, nil,
	)
}

func findMySQLDMLTargetInSelectWithQueryTargets(
	ctx CompilerContext,
	stmt tree.SelectStatement,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
	queryCorrelatedTargets map[mysqlDMLTarget]struct{},
) (string, bool) {
	switch selectStmt := stmt.(type) {
	case *tree.Select:
		visibleCTEs = mysqlCTENames(selectStmt.With, visibleCTEs)
		queryCorrelatedTargets = mysqlMergeDMLTargets(
			queryCorrelatedTargets,
			mysqlCorrelatedUpdateTargetsInSelect(selectStmt, outerTargetQualifiers),
		)
		if target, ok := findMySQLDMLTargetInSelectWithQueryTargets(
			ctx, selectStmt.Select, targets, visibleCTEs, outerTargetQualifiers, queryCorrelatedTargets,
		); ok {
			return target, true
		}
		for _, order := range selectStmt.OrderBy {
			if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
				ctx, order.Expr, targets, visibleCTEs, outerTargetQualifiers,
			); ok {
				return target, true
			}
		}
		if target, ok := findMySQLDMLTargetInLimitWithOuterTargets(
			ctx, selectStmt.Limit, targets, visibleCTEs, outerTargetQualifiers,
		); ok {
			return target, true
		}
		if selectStmt.TimeWindow != nil {
			if selectStmt.TimeWindow.Interval != nil {
				if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
					ctx, selectStmt.TimeWindow.Interval.Val, targets, visibleCTEs, outerTargetQualifiers,
				); ok {
					return target, true
				}
			}
			if selectStmt.TimeWindow.Sliding != nil {
				if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
					ctx, selectStmt.TimeWindow.Sliding.Val, targets, visibleCTEs, outerTargetQualifiers,
				); ok {
					return target, true
				}
			}
			if selectStmt.TimeWindow.Fill != nil {
				if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
					ctx, selectStmt.TimeWindow.Fill.Val, targets, visibleCTEs, outerTargetQualifiers,
				); ok {
					return target, true
				}
			}
		}
	case *tree.ParenSelect:
		return findMySQLDMLTargetInSelectWithQueryTargets(
			ctx, selectStmt.Select, targets, visibleCTEs, outerTargetQualifiers, queryCorrelatedTargets,
		)
	case *tree.UnionClause:
		if target, ok := findMySQLDMLTargetInSelectWithQueryTargets(
			ctx, selectStmt.Left, targets, visibleCTEs, outerTargetQualifiers, queryCorrelatedTargets,
		); ok {
			return target, true
		}
		return findMySQLDMLTargetInSelectWithQueryTargets(
			ctx, selectStmt.Right, targets, visibleCTEs, outerTargetQualifiers, queryCorrelatedTargets,
		)
	case *tree.SelectClause:
		correlatedTargets := mysqlMergeDMLTargets(
			queryCorrelatedTargets,
			mysqlCorrelatedUpdateTargets(selectStmt, outerTargetQualifiers),
		)
		if selectStmt.From != nil {
			for _, tableExpr := range selectStmt.From.Tables {
				if target, ok := findMySQLDMLTargetInDirectTableExprExcept(
					ctx, tableExpr, targets, visibleCTEs, correlatedTargets, outerTargetQualifiers,
				); ok {
					return target, true
				}
			}
		}
		for _, selectExpr := range selectStmt.Exprs {
			if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
				ctx, selectExpr.Expr, targets, visibleCTEs, outerTargetQualifiers,
			); ok {
				return target, true
			}
		}
		if selectStmt.Where != nil {
			if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
				ctx, selectStmt.Where.Expr, targets, visibleCTEs, outerTargetQualifiers,
			); ok {
				return target, true
			}
		}
		if selectStmt.GroupBy != nil {
			for _, exprs := range selectStmt.GroupBy.GroupByExprsList {
				for _, expr := range exprs {
					if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
						ctx, expr, targets, visibleCTEs, outerTargetQualifiers,
					); ok {
						return target, true
					}
				}
			}
			for _, expr := range selectStmt.GroupBy.GroupingSet {
				if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
					ctx, expr, targets, visibleCTEs, outerTargetQualifiers,
				); ok {
					return target, true
				}
			}
		}
		if selectStmt.Having != nil {
			return findMySQLDMLTargetInExprWithOuterTargets(
				ctx, selectStmt.Having.Expr, targets, visibleCTEs, outerTargetQualifiers,
			)
		}
	case *tree.ValuesClause:
		for _, row := range selectStmt.Rows {
			for _, expr := range row {
				if target, ok := findMySQLDMLTargetInExprWithOuterTargets(
					ctx, expr, targets, visibleCTEs, outerTargetQualifiers,
				); ok {
					return target, true
				}
			}
		}
	}
	return "", false
}

func mysqlUpdateTargetQualifiers(
	ctx CompilerContext,
	tableExprs tree.TableExprs,
	targets []mysqlDMLTarget,
) map[mysqlDMLTarget]map[string]struct{} {
	result := make(map[mysqlDMLTarget]map[string]struct{}, len(targets))
	var collect func(tree.TableExpr)
	collect = func(expr tree.TableExpr) {
		switch tableExpr := expr.(type) {
		case *tree.TableName:
			if target, ok := mysqlDMLTargetForTableName(ctx, tableExpr, targets); ok {
				mysqlAddTargetQualifier(result, target, string(tableExpr.ObjectName))
			}
		case *tree.AliasedTableExpr:
			if tableExpr.As.Alias != "" {
				if tableName, ok := mysqlUnwrapTableName(tableExpr.Expr); ok {
					if target, matched := mysqlDMLTargetForTableName(ctx, tableName, targets); matched {
						mysqlAddTargetQualifier(result, target, string(tableExpr.As.Alias))
					}
				}
				return
			}
			collect(tableExpr.Expr)
		case *tree.ParenTableExpr:
			collect(tableExpr.Expr)
		case *tree.JoinTableExpr:
			collect(tableExpr.Left)
			collect(tableExpr.Right)
		case *tree.ApplyTableExpr:
			collect(tableExpr.Left)
			collect(tableExpr.Right)
		}
	}
	for _, tableExpr := range tableExprs {
		collect(tableExpr)
	}
	return result
}

func mysqlUnwrapTableName(expr tree.TableExpr) (*tree.TableName, bool) {
	switch tableExpr := expr.(type) {
	case *tree.TableName:
		return tableExpr, true
	case *tree.ParenTableExpr:
		return mysqlUnwrapTableName(tableExpr.Expr)
	default:
		return nil, false
	}
}

func mysqlDMLTargetForTableName(
	ctx CompilerContext,
	tableName *tree.TableName,
	targets []mysqlDMLTarget,
) (mysqlDMLTarget, bool) {
	dbName := string(tableName.SchemaName)
	if dbName == "" {
		dbName = ctx.DefaultDatabase()
	}
	objRef, tableDef, err := ctx.Resolve(dbName, string(tableName.ObjectName), nil)
	if err != nil || tableDef == nil {
		return mysqlDMLTarget{}, false
	}
	for _, target := range targets {
		if target.matches(objRef, tableDef, dbName, string(tableName.ObjectName)) {
			return target, true
		}
	}
	return mysqlDMLTarget{}, false
}

func mysqlAddTargetQualifier(
	targets map[mysqlDMLTarget]map[string]struct{},
	target mysqlDMLTarget,
	qualifier string,
) {
	qualifiers := targets[target]
	if qualifiers == nil {
		qualifiers = make(map[string]struct{})
		targets[target] = qualifiers
	}
	qualifiers[strings.ToLower(qualifier)] = struct{}{}
}

func mysqlCorrelatedUpdateTargets(
	selectStmt *tree.SelectClause,
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
) map[mysqlDMLTarget]struct{} {
	if len(outerTargetQualifiers) == 0 {
		return nil
	}
	result := make(map[mysqlDMLTarget]struct{})
	for target, qualifiers := range outerTargetQualifiers {
		if mysqlSelectClauseReferencesOuterQualifier(selectStmt, qualifiers, nil) {
			result[target] = struct{}{}
		}
	}
	return result
}

func mysqlCorrelatedUpdateTargetsInSelect(
	selectStmt *tree.Select,
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
) map[mysqlDMLTarget]struct{} {
	if len(outerTargetQualifiers) == 0 {
		return nil
	}
	result := make(map[mysqlDMLTarget]struct{})
	for target, qualifiers := range outerTargetQualifiers {
		if mysqlSelectWrapperReferencesOuterQualifier(selectStmt, qualifiers, nil) {
			result[target] = struct{}{}
		}
	}
	return result
}

func mysqlSelectWrapperReferencesOuterQualifier(
	selectStmt *tree.Select,
	qualifiers map[string]struct{},
	shadowed map[string]struct{},
) bool {
	localShadowed := mysqlCloneNames(shadowed)
	mysqlCollectSelectLocalQualifiers(selectStmt.Select, localShadowed)
	for _, order := range selectStmt.OrderBy {
		if mysqlExprReferencesOuterQualifier(order.Expr, qualifiers, localShadowed) {
			return true
		}
	}
	if mysqlLimitReferencesOuterQualifier(selectStmt.Limit, qualifiers, localShadowed) {
		return true
	}
	if selectStmt.TimeWindow == nil {
		return false
	}
	if selectStmt.TimeWindow.Interval != nil && mysqlExprReferencesOuterQualifier(
		selectStmt.TimeWindow.Interval.Val, qualifiers, localShadowed,
	) {
		return true
	}
	if selectStmt.TimeWindow.Sliding != nil && mysqlExprReferencesOuterQualifier(
		selectStmt.TimeWindow.Sliding.Val, qualifiers, localShadowed,
	) {
		return true
	}
	return selectStmt.TimeWindow.Fill != nil && mysqlExprReferencesOuterQualifier(
		selectStmt.TimeWindow.Fill.Val, qualifiers, localShadowed,
	)
}

func mysqlMergeDMLTargets(
	first map[mysqlDMLTarget]struct{},
	second map[mysqlDMLTarget]struct{},
) map[mysqlDMLTarget]struct{} {
	if len(first) == 0 {
		return second
	}
	if len(second) == 0 {
		return first
	}
	result := make(map[mysqlDMLTarget]struct{}, len(first)+len(second))
	for target := range first {
		result[target] = struct{}{}
	}
	for target := range second {
		result[target] = struct{}{}
	}
	return result
}

func mysqlSelectReferencesOuterQualifier(
	stmt tree.SelectStatement,
	qualifiers map[string]struct{},
	shadowed map[string]struct{},
) bool {
	switch selectStmt := stmt.(type) {
	case *tree.Select:
		if mysqlSelectReferencesOuterQualifier(selectStmt.Select, qualifiers, shadowed) {
			return true
		}
		return mysqlSelectWrapperReferencesOuterQualifier(selectStmt, qualifiers, shadowed)
	case *tree.ParenSelect:
		return mysqlSelectReferencesOuterQualifier(selectStmt.Select, qualifiers, shadowed)
	case *tree.UnionClause:
		return mysqlSelectReferencesOuterQualifier(selectStmt.Left, qualifiers, shadowed) ||
			mysqlSelectReferencesOuterQualifier(selectStmt.Right, qualifiers, shadowed)
	case *tree.SelectClause:
		return mysqlSelectClauseReferencesOuterQualifier(selectStmt, qualifiers, shadowed)
	case *tree.ValuesClause:
		for _, row := range selectStmt.Rows {
			for _, expr := range row {
				if mysqlExprReferencesOuterQualifier(expr, qualifiers, shadowed) {
					return true
				}
			}
		}
	}
	return false
}

func mysqlSelectClauseReferencesOuterQualifier(
	selectStmt *tree.SelectClause,
	qualifiers map[string]struct{},
	shadowed map[string]struct{},
) bool {
	localShadowed := mysqlCloneNames(shadowed)
	if selectStmt.From != nil {
		for _, tableExpr := range selectStmt.From.Tables {
			mysqlCollectLocalTableQualifiers(tableExpr, localShadowed)
		}
	}
	for _, selectExpr := range selectStmt.Exprs {
		if mysqlExprReferencesOuterQualifier(selectExpr.Expr, qualifiers, localShadowed) {
			return true
		}
	}
	if selectStmt.Where != nil && mysqlExprReferencesOuterQualifier(selectStmt.Where.Expr, qualifiers, localShadowed) {
		return true
	}
	if selectStmt.GroupBy != nil {
		for _, exprs := range selectStmt.GroupBy.GroupByExprsList {
			for _, expr := range exprs {
				if mysqlExprReferencesOuterQualifier(expr, qualifiers, localShadowed) {
					return true
				}
			}
		}
		for _, expr := range selectStmt.GroupBy.GroupingSet {
			if mysqlExprReferencesOuterQualifier(expr, qualifiers, localShadowed) {
				return true
			}
		}
	}
	if selectStmt.Having != nil && mysqlExprReferencesOuterQualifier(selectStmt.Having.Expr, qualifiers, localShadowed) {
		return true
	}
	if selectStmt.From != nil {
		for _, tableExpr := range selectStmt.From.Tables {
			if mysqlTableExprReferencesOuterQualifier(tableExpr, qualifiers, localShadowed) {
				return true
			}
		}
	}
	return false
}

func mysqlExprReferencesOuterQualifier(
	expr tree.Expr,
	qualifiers map[string]struct{},
	shadowed map[string]struct{},
) bool {
	if expr == nil {
		return false
	}
	found := false
	walkGroupingSetOrderByExpr(expr, func(node tree.Expr) bool {
		if found {
			return false
		}
		switch typedExpr := node.(type) {
		case *tree.UnresolvedName:
			qualifier := strings.ToLower(typedExpr.TblName())
			_, isOuterTarget := qualifiers[qualifier]
			_, isShadowed := shadowed[qualifier]
			found = qualifier != "" && isOuterTarget && !isShadowed
			return !found
		case *tree.Subquery:
			found = mysqlSelectReferencesOuterQualifier(typedExpr.Select, qualifiers, shadowed)
			return false
		}
		return true
	})
	return found
}

func mysqlLimitReferencesOuterQualifier(
	limit *tree.Limit,
	qualifiers map[string]struct{},
	shadowed map[string]struct{},
) bool {
	if limit == nil {
		return false
	}
	return mysqlExprReferencesOuterQualifier(limit.Count, qualifiers, shadowed) ||
		mysqlExprReferencesOuterQualifier(limit.Offset, qualifiers, shadowed)
}

func mysqlTableExprReferencesOuterQualifier(
	expr tree.TableExpr,
	qualifiers map[string]struct{},
	shadowed map[string]struct{},
) bool {
	switch tableExpr := expr.(type) {
	case *tree.AliasedTableExpr:
		return mysqlTableExprReferencesOuterQualifier(tableExpr.Expr, qualifiers, shadowed)
	case *tree.ParenTableExpr:
		return mysqlTableExprReferencesOuterQualifier(tableExpr.Expr, qualifiers, shadowed)
	case *tree.JoinTableExpr:
		if mysqlTableExprReferencesOuterQualifier(tableExpr.Left, qualifiers, shadowed) ||
			mysqlTableExprReferencesOuterQualifier(tableExpr.Right, qualifiers, shadowed) {
			return true
		}
		if condition, ok := tableExpr.Cond.(*tree.OnJoinCond); ok {
			return mysqlExprReferencesOuterQualifier(condition.Expr, qualifiers, shadowed)
		}
	case *tree.ApplyTableExpr:
		return mysqlTableExprReferencesOuterQualifier(tableExpr.Left, qualifiers, shadowed) ||
			mysqlTableExprReferencesOuterQualifier(tableExpr.Right, qualifiers, shadowed)
	}
	return false
}

func mysqlCollectLocalTableQualifiers(expr tree.TableExpr, names map[string]struct{}) {
	switch tableExpr := expr.(type) {
	case *tree.TableName:
		names[strings.ToLower(string(tableExpr.ObjectName))] = struct{}{}
	case *tree.AliasedTableExpr:
		if tableExpr.As.Alias != "" {
			names[strings.ToLower(string(tableExpr.As.Alias))] = struct{}{}
			return
		}
		mysqlCollectLocalTableQualifiers(tableExpr.Expr, names)
	case *tree.ParenTableExpr:
		mysqlCollectLocalTableQualifiers(tableExpr.Expr, names)
	case *tree.JoinTableExpr:
		mysqlCollectLocalTableQualifiers(tableExpr.Left, names)
		mysqlCollectLocalTableQualifiers(tableExpr.Right, names)
	case *tree.ApplyTableExpr:
		mysqlCollectLocalTableQualifiers(tableExpr.Left, names)
		mysqlCollectLocalTableQualifiers(tableExpr.Right, names)
	}
}

func mysqlCollectSelectLocalQualifiers(stmt tree.SelectStatement, names map[string]struct{}) {
	switch selectStmt := stmt.(type) {
	case *tree.Select:
		mysqlCollectSelectLocalQualifiers(selectStmt.Select, names)
	case *tree.ParenSelect:
		mysqlCollectSelectLocalQualifiers(selectStmt.Select, names)
	case *tree.SelectClause:
		if selectStmt.From != nil {
			for _, tableExpr := range selectStmt.From.Tables {
				mysqlCollectLocalTableQualifiers(tableExpr, names)
			}
		}
	}
}

func mysqlCloneNames(names map[string]struct{}) map[string]struct{} {
	result := make(map[string]struct{}, len(names))
	for name := range names {
		result[name] = struct{}{}
	}
	return result
}

func findMySQLDMLTargetInDirectTableExpr(
	ctx CompilerContext,
	expr tree.TableExpr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
) (string, bool) {
	return findMySQLDMLTargetInDirectTableExprExcept(ctx, expr, targets, visibleCTEs, nil, nil)
}

func findMySQLDMLTargetInDirectTableExprExcept(
	ctx CompilerContext,
	expr tree.TableExpr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
	ignoredTargets map[mysqlDMLTarget]struct{},
	outerTargetQualifiers map[mysqlDMLTarget]map[string]struct{},
) (string, bool) {
	switch tableExpr := expr.(type) {
	case *tree.TableName:
		if tableExpr.SchemaName == "" {
			if _, ok := visibleCTEs[strings.ToLower(string(tableExpr.ObjectName))]; ok {
				return "", false
			}
		}
		dbName := string(tableExpr.SchemaName)
		if dbName == "" {
			dbName = ctx.DefaultDatabase()
		}
		objRef, tableDef, err := ctx.Resolve(dbName, string(tableExpr.ObjectName), nil)
		if err != nil || tableDef == nil {
			return "", false
		}
		for _, target := range targets {
			if target.matches(objRef, tableDef, dbName, string(tableExpr.ObjectName)) {
				if _, ignored := ignoredTargets[target]; ignored {
					return "", false
				}
				return target.name, true
			}
		}
	case *tree.AliasedTableExpr:
		return findMySQLDMLTargetInDirectTableExprExcept(
			ctx, tableExpr.Expr, targets, visibleCTEs, ignoredTargets, outerTargetQualifiers,
		)
	case *tree.ParenTableExpr:
		return findMySQLDMLTargetInDirectTableExprExcept(
			ctx, tableExpr.Expr, targets, visibleCTEs, ignoredTargets, outerTargetQualifiers,
		)
	case *tree.JoinTableExpr:
		if target, ok := findMySQLDMLTargetInDirectTableExprExcept(
			ctx, tableExpr.Left, targets, visibleCTEs, ignoredTargets, outerTargetQualifiers,
		); ok {
			return target, true
		}
		if target, ok := findMySQLDMLTargetInDirectTableExprExcept(
			ctx, tableExpr.Right, targets, visibleCTEs, ignoredTargets, outerTargetQualifiers,
		); ok {
			return target, true
		}
		if condition, ok := tableExpr.Cond.(*tree.OnJoinCond); ok {
			return findMySQLDMLTargetInExprWithOuterTargets(
				ctx, condition.Expr, targets, visibleCTEs, outerTargetQualifiers,
			)
		}
	case *tree.ApplyTableExpr:
		if target, ok := findMySQLDMLTargetInDirectTableExprExcept(
			ctx, tableExpr.Left, targets, visibleCTEs, ignoredTargets, outerTargetQualifiers,
		); ok {
			return target, true
		}
		return findMySQLDMLTargetInDirectTableExprExcept(
			ctx, tableExpr.Right, targets, visibleCTEs, ignoredTargets, outerTargetQualifiers,
		)
	case *tree.Subquery, *tree.StatementSource:
		// A FROM-subquery is a derived-table boundary. Its target-table read is
		// allowed because that result can be materialized before the DML write.
		return "", false
	}
	return "", false
}

// Outer UPDATE/DELETE table lists may contain subqueries in JOIN conditions,
// but their ordinary table references are not themselves subqueries. Inspect
// only those condition expressions and preserve legal self-join DML.
func findMySQLDMLTargetInOuterTableExpr(
	ctx CompilerContext,
	expr tree.TableExpr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
) (string, bool) {
	switch tableExpr := expr.(type) {
	case *tree.AliasedTableExpr:
		return findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr.Expr, targets, visibleCTEs)
	case *tree.ParenTableExpr:
		return findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr.Expr, targets, visibleCTEs)
	case *tree.JoinTableExpr:
		if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr.Left, targets, visibleCTEs); ok {
			return target, true
		}
		if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr.Right, targets, visibleCTEs); ok {
			return target, true
		}
		if condition, ok := tableExpr.Cond.(*tree.OnJoinCond); ok {
			return findMySQLDMLTargetInExpr(ctx, condition.Expr, targets, visibleCTEs)
		}
	case *tree.ApplyTableExpr:
		if target, ok := findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr.Left, targets, visibleCTEs); ok {
			return target, true
		}
		return findMySQLDMLTargetInOuterTableExpr(ctx, tableExpr.Right, targets, visibleCTEs)
	case *tree.Subquery, *tree.StatementSource:
		return "", false
	}
	return "", false
}

func (target mysqlDMLTarget) matches(objRef *ObjectRef, tableDef *TableDef, dbName, tableName string) bool {
	if target.objID != 0 && objRef != nil && objRef.Obj != 0 {
		return target.objID == objRef.Obj
	}
	if target.tableID != 0 && tableDef != nil && tableDef.TblId != 0 {
		return target.tableID == tableDef.TblId
	}
	return strings.EqualFold(target.schema, dbName) && strings.EqualFold(target.name, tableName)
}
