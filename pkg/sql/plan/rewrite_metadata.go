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
	"context"
	"strings"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const informationSchemaColumnsTable = "columns"

// applyRewriteMetadataVisibility keeps information_schema.COLUMNS consistent
// with the projection exposed by an active row-column rewrite.  The catalog
// view is expanded before this hook runs, so filtering its final output keeps
// the existing object-visibility predicates and works for every COLUMNS view
// implementation (including UNION and subscription variants).
func (builder *QueryBuilder) applyRewriteMetadataVisibility(
	nodeID int32, ctx *BindContext, schema, table string,
) (int32, error) {
	if ctx == nil || ctx.remapOption == nil ||
		!strings.EqualFold(schema, INFORMATION_SCHEMA) ||
		!strings.EqualFold(table, informationSchemaColumnsTable) ||
		len(ctx.remapOption.Rewrites) == 0 {
		return nodeID, nil
	}

	schemaPos, tablePos, columnPos := -1, -1, -1
	for i, heading := range ctx.headings {
		switch {
		case strings.EqualFold(heading, "TABLE_SCHEMA"):
			schemaPos = i
		case strings.EqualFold(heading, "TABLE_NAME"):
			tablePos = i
		case strings.EqualFold(heading, "COLUMN_NAME"):
			columnPos = i
		}
	}
	if schemaPos < 0 || tablePos < 0 || columnPos < 0 {
		return nodeID, nil
	}

	policies := make(map[string]rewriteMetadataPolicy)
	for key, chain := range ctx.remapOption.Rewrites {
		if len(chain) == 0 {
			continue
		}
		db, tableName := rewriteTargetName(key, chain[len(chain)-1])
		if db == "" || tableName == "" || isSystemMetadataDatabase(db) {
			continue
		}
		policy := rewriteMetadataPolicy{database: db, table: tableName, all: true}
		for _, rewrite := range chain {
			names, all, supported := rewriteVisibleColumns(rewrite.Stmt, db, tableName)
			if !supported {
				policy.all = false
				policy.names = nil
				policy.supported = false
				break
			}
			if all {
				continue
			}
			policy.supported = true
			if policy.all {
				policy.all = false
				policy.names = names
				continue
			}
			policy.names = intersectRewriteColumnNames(policy.names, names)
		}
		// A non-projection or otherwise unprovable rewrite is fail-closed. An
		// empty intersection is also intentionally retained: it hides every
		// column for that target instead of disclosing a base column name.
		if policy.all {
			continue
		}
		canonical := strings.ToLower(db) + "\x00" + strings.ToLower(tableName)
		if old, ok := policies[canonical]; ok {
			old.names = intersectRewriteColumnNames(old.names, policy.names)
			old.all = false
			old.supported = old.supported && policy.supported
			policies[canonical] = old
		} else {
			policies[canonical] = policy
		}
	}

	if len(policies) == 0 {
		return nodeID, nil
	}
	tag := ctx.rootTag()
	schemaExpr := GetColExpr(rewriteMetadataOutputType(ctx, schemaPos), tag, int32(schemaPos))
	tableExpr := GetColExpr(rewriteMetadataOutputType(ctx, tablePos), tag, int32(tablePos))
	columnExpr := GetColExpr(rewriteMetadataOutputType(ctx, columnPos), tag, int32(columnPos))

	filters := make([]*planpb.Expr, 0, len(policies))
	for _, policy := range policies {
		notSchema, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<>", []*planpb.Expr{
			schemaExpr, makePlan2StringConstExprWithType(policy.database),
		})
		if err != nil {
			return nodeID, err
		}
		notTable, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<>", []*planpb.Expr{
			tableExpr, makePlan2StringConstExprWithType(policy.table),
		})
		if err != nil {
			return nodeID, err
		}
		parts := []*planpb.Expr{notSchema, notTable}
		if len(policy.names) > 0 {
			allowed := make([]*planpb.Expr, 0, len(policy.names))
			for name := range policy.names {
				eq, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "=", []*planpb.Expr{
					columnExpr, makePlan2StringConstExprWithType(name),
				})
				if err != nil {
					return nodeID, err
				}
				allowed = append(allowed, eq)
			}
			allowedExpr, err := combineRewriteMetadataExprs(builder.GetContext(), "or", allowed)
			if err != nil {
				return nodeID, err
			}
			parts = append(parts, allowedExpr)
		}
		predicate, err := combineRewriteMetadataExprs(builder.GetContext(), "or", parts)
		if err != nil {
			return nodeID, err
		}
		filters = append(filters, predicate)
	}
	filter, err := combineRewriteMetadataExprs(builder.GetContext(), "and", filters)
	if err != nil {
		return nodeID, err
	}
	return builder.appendNode(&planpb.Node{
		NodeType:     planpb.Node_FILTER,
		Children:     []int32{nodeID},
		FilterList:   []*planpb.Expr{filter},
		NotCacheable: true,
	}, ctx), nil
}

type rewriteMetadataPolicy struct {
	database  string
	table     string
	names     map[string]struct{}
	all       bool
	supported bool
}

func rewriteMetadataOutputType(ctx *BindContext, position int) planpb.Type {
	if position >= 0 && position < len(ctx.results) && ctx.results[position] != nil {
		return ctx.results[position].Typ
	}
	if position >= 0 && position < len(ctx.projects) && ctx.projects[position] != nil {
		return ctx.projects[position].Typ
	}
	return makePlan2StringConstExprWithType("").Typ
}

func combineRewriteMetadataExprs(ctx context.Context, name string, exprs []*planpb.Expr) (*planpb.Expr, error) {
	if len(exprs) == 0 {
		return nil, nil
	}
	result := exprs[0]
	for i := 1; i < len(exprs); i++ {
		var err error
		result, err = BindFuncExprImplByPlanExpr(ctx, name, []*planpb.Expr{result, exprs[i]})
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func rewriteTargetName(key string, rewrite *tree.Rewrite) (string, string) {
	if rewrite != nil && rewrite.DbName != "" && rewrite.TableName != "" {
		return rewrite.DbName, rewrite.TableName
	}
	parts := strings.SplitN(key, ".", 2)
	if len(parts) != 2 || rewrite == nil {
		return "", ""
	}
	return parts[0], parts[1]
}

func isSystemMetadataDatabase(database string) bool {
	switch strings.ToLower(database) {
	case "mo_catalog", INFORMATION_SCHEMA, "mysql", "system", "system_metrics", "mo_task", "mo_debug":
		return true
	default:
		return false
	}
}

func intersectRewriteColumnNames(left, right map[string]struct{}) map[string]struct{} {
	if left == nil || right == nil {
		return map[string]struct{}{}
	}
	result := make(map[string]struct{})
	for name := range left {
		if _, ok := right[name]; ok {
			result[name] = struct{}{}
		}
	}
	return result
}

func rewriteVisibleColumns(stmt tree.Statement, database, table string) (map[string]struct{}, bool, bool) {
	var selectStmt tree.SelectStatement
	switch statement := stmt.(type) {
	case *tree.Select:
		selectStmt = statement.Select
	case *tree.ParenSelect:
		selectStmt = statement.Select
	default:
		return nil, false, false
	}
	for {
		switch statement := selectStmt.(type) {
		case *tree.ParenSelect:
			selectStmt = statement.Select
		case *tree.Select:
			selectStmt = statement.Select
		default:
			goto projection
		}
	}

projection:
	clause, ok := selectStmt.(*tree.SelectClause)
	if !ok || !rewriteMetadataSingleTableSource(clause, database, table) || len(clause.Exprs) == 0 {
		return nil, false, false
	}
	names := make(map[string]struct{}, len(clause.Exprs))
	for _, expr := range clause.Exprs {
		switch value := expr.Expr.(type) {
		case tree.UnqualifiedStar, *tree.UnqualifiedStar:
			return nil, true, true
		case *tree.UnresolvedName:
			if value.Star || value.ColName() == "" {
				return nil, true, true
			}
			if expr.As != nil && !expr.As.Empty() && !strings.EqualFold(expr.As.Origin(), value.ColNameOrigin()) {
				return nil, false, false
			}
			names[strings.ToLower(value.ColName())] = struct{}{}
		default:
			return nil, false, false
		}
	}
	return names, false, true
}

func rewriteMetadataSingleTableSource(clause *tree.SelectClause, database, table string) bool {
	if clause.From == nil || len(clause.From.Tables) != 1 {
		return false
	}
	var source tree.TableExpr = clause.From.Tables[0]
	if joined, ok := source.(*tree.JoinTableExpr); ok {
		if joined.Right != nil {
			return false
		}
		source = joined.Left
	}
	if aliased, ok := source.(*tree.AliasedTableExpr); ok {
		source = aliased.Expr
	}
	tableName, ok := source.(*tree.TableName)
	if !ok || !strings.EqualFold(string(tableName.Name()), table) {
		return false
	}
	sourceDatabase := string(tableName.Schema())
	return sourceDatabase == "" || strings.EqualFold(sourceDatabase, database)
}
