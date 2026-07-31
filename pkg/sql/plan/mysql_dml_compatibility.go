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

	for _, updateExpr := range stmt.Exprs {
		if target, ok := findMySQLDMLTargetInExpr(ctx, updateExpr.Expr, targets, visibleCTEs); ok {
			return moerr.NewUpdateTableUsed(ctx.GetContext(), target)
		}
	}
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
	if limit == nil {
		return "", false
	}
	if target, ok := findMySQLDMLTargetInExpr(ctx, limit.Count, targets, visibleCTEs); ok {
		return target, true
	}
	return findMySQLDMLTargetInExpr(ctx, limit.Offset, targets, visibleCTEs)
}

func findMySQLDMLTargetInExpr(
	ctx CompilerContext,
	expr tree.Expr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
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
		found, _ = findMySQLDMLTargetInSelect(ctx, subquery.Select, targets, visibleCTEs)
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
	switch selectStmt := stmt.(type) {
	case *tree.Select:
		visibleCTEs = mysqlCTENames(selectStmt.With, visibleCTEs)
		if target, ok := findMySQLDMLTargetInSelect(ctx, selectStmt.Select, targets, visibleCTEs); ok {
			return target, true
		}
		for _, order := range selectStmt.OrderBy {
			if target, ok := findMySQLDMLTargetInExpr(ctx, order.Expr, targets, visibleCTEs); ok {
				return target, true
			}
		}
		if target, ok := findMySQLDMLTargetInLimit(ctx, selectStmt.Limit, targets, visibleCTEs); ok {
			return target, true
		}
		if selectStmt.TimeWindow != nil {
			if selectStmt.TimeWindow.Interval != nil {
				if target, ok := findMySQLDMLTargetInExpr(ctx, selectStmt.TimeWindow.Interval.Val, targets, visibleCTEs); ok {
					return target, true
				}
			}
			if selectStmt.TimeWindow.Sliding != nil {
				if target, ok := findMySQLDMLTargetInExpr(ctx, selectStmt.TimeWindow.Sliding.Val, targets, visibleCTEs); ok {
					return target, true
				}
			}
			if selectStmt.TimeWindow.Fill != nil {
				if target, ok := findMySQLDMLTargetInExpr(ctx, selectStmt.TimeWindow.Fill.Val, targets, visibleCTEs); ok {
					return target, true
				}
			}
		}
	case *tree.ParenSelect:
		return findMySQLDMLTargetInSelect(ctx, selectStmt.Select, targets, visibleCTEs)
	case *tree.UnionClause:
		if target, ok := findMySQLDMLTargetInSelect(ctx, selectStmt.Left, targets, visibleCTEs); ok {
			return target, true
		}
		return findMySQLDMLTargetInSelect(ctx, selectStmt.Right, targets, visibleCTEs)
	case *tree.SelectClause:
		if selectStmt.From != nil {
			for _, tableExpr := range selectStmt.From.Tables {
				if target, ok := findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr, targets, visibleCTEs); ok {
					return target, true
				}
			}
		}
		for _, selectExpr := range selectStmt.Exprs {
			if target, ok := findMySQLDMLTargetInExpr(ctx, selectExpr.Expr, targets, visibleCTEs); ok {
				return target, true
			}
		}
		if selectStmt.Where != nil {
			if target, ok := findMySQLDMLTargetInExpr(ctx, selectStmt.Where.Expr, targets, visibleCTEs); ok {
				return target, true
			}
		}
		if selectStmt.GroupBy != nil {
			for _, exprs := range selectStmt.GroupBy.GroupByExprsList {
				for _, expr := range exprs {
					if target, ok := findMySQLDMLTargetInExpr(ctx, expr, targets, visibleCTEs); ok {
						return target, true
					}
				}
			}
			for _, expr := range selectStmt.GroupBy.GroupingSet {
				if target, ok := findMySQLDMLTargetInExpr(ctx, expr, targets, visibleCTEs); ok {
					return target, true
				}
			}
		}
		if selectStmt.Having != nil {
			return findMySQLDMLTargetInExpr(ctx, selectStmt.Having.Expr, targets, visibleCTEs)
		}
	case *tree.ValuesClause:
		for _, row := range selectStmt.Rows {
			for _, expr := range row {
				if target, ok := findMySQLDMLTargetInExpr(ctx, expr, targets, visibleCTEs); ok {
					return target, true
				}
			}
		}
	}
	return "", false
}

func findMySQLDMLTargetInDirectTableExpr(
	ctx CompilerContext,
	expr tree.TableExpr,
	targets []mysqlDMLTarget,
	visibleCTEs map[string]struct{},
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
				return target.name, true
			}
		}
	case *tree.AliasedTableExpr:
		return findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr.Expr, targets, visibleCTEs)
	case *tree.ParenTableExpr:
		return findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr.Expr, targets, visibleCTEs)
	case *tree.JoinTableExpr:
		if target, ok := findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr.Left, targets, visibleCTEs); ok {
			return target, true
		}
		if target, ok := findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr.Right, targets, visibleCTEs); ok {
			return target, true
		}
		if condition, ok := tableExpr.Cond.(*tree.OnJoinCond); ok {
			return findMySQLDMLTargetInExpr(ctx, condition.Expr, targets, visibleCTEs)
		}
	case *tree.ApplyTableExpr:
		if target, ok := findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr.Left, targets, visibleCTEs); ok {
			return target, true
		}
		return findMySQLDMLTargetInDirectTableExpr(ctx, tableExpr.Right, targets, visibleCTEs)
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
