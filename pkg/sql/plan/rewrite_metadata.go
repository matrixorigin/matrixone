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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const informationSchemaColumnsTable = "columns"

func rewriteTableKey(database, table string, lower int64) string {
	return tree.NewCStr(database, lower).Compare() + "." + tree.NewCStr(table, lower).Compare()
}

func rewriteChainForTable(ctx CompilerContext, option *tree.RewriteOption, database, table string) []*tree.Rewrite {
	if option == nil || len(option.Rewrites) == 0 {
		return nil
	}
	return option.Rewrites[rewriteTableKey(database, table, ctx.GetLowerCaseTableNames())]
}

func rewritePrivilegeError(ctx context.Context) error {
	return moerr.NewInternalError(ctx, "do not have privilege to execute the statement")
}

// Physical SHOW output can include expressions and constraints. Require a
// proven all-column chain before exposing it.
func checkShowRewriteMetadata(
	ctx CompilerContext, option *tree.RewriteOption, database, table string,
	def *TableDef, currentLocal bool,
) error {
	chain := rewriteChainForTable(ctx, option, database, table)
	if len(chain) == 0 {
		return nil
	}
	if currentLocal && !def.IsTemporary && def.TableType == catalog.SystemOrdinaryRel {
		lower := ctx.GetLowerCaseTableNames()
		for _, rewrite := range chain {
			if rewrite != nil {
				_, all, supported := rewriteVisibleColumns(rewrite.Stmt, database, table, lower)
				if supported && all {
					continue
				}
			}
			return rewritePrivilegeError(ctx.GetContext())
		}
		return nil
	}
	return rewritePrivilegeError(ctx.GetContext())
}

// Apply the same column visibility to catalog column and index-part rows.
// At the view boundary, ordinary catalog grants and subscription scope have
// already been bound. A functional index part has a NULL column name and
// cannot satisfy a restricted projection.
func (builder *QueryBuilder) applyRewriteMetadataVisibility(
	nodeID int32, ctx *BindContext, schema, table string,
) (int32, error) {
	if ctx == nil || ctx.remapOption == nil || len(ctx.remapOption.Rewrites) == 0 ||
		!strings.EqualFold(schema, INFORMATION_SCHEMA) ||
		(!strings.EqualFold(table, informationSchemaColumnsTable) && !strings.EqualFold(table, informationSchemaStatistics)) {
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
		return nodeID, rewritePrivilegeError(builder.GetContext())
	}

	// Rewrite lookup uses the caller mode, which may differ from the stored
	// information_schema view SQL mode in ctx.lower.
	lower := builder.compCtx.GetLowerCaseTableNames()
	policies := make(map[string]rewriteMetadataPolicy)
	for key, chain := range ctx.remapOption.Rewrites {
		if len(chain) == 0 {
			continue
		}
		db, tableName := rewriteTargetName(key, chain[len(chain)-1])
		if db == "" || tableName == "" || isSystemDatabase(db) {
			continue
		}
		policy := rewriteMetadataPolicy{database: db, table: tableName, all: true}
		for _, rewrite := range chain {
			var stmt tree.Statement
			if rewrite != nil {
				stmt = rewrite.Stmt
			}
			names, all, supported := rewriteVisibleColumns(stmt, db, tableName, lower)
			if !supported {
				policy.all = false
				policy.names = nil
				break
			}
			if all {
				continue
			}
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
		canonical := db + "\x00" + tableName
		if lower != 0 {
			canonical = strings.ToLower(canonical)
		}
		if old, ok := policies[canonical]; ok {
			old.names = intersectRewriteColumnNames(old.names, policy.names)
			old.all = false
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
	columnExpr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "lower", []*planpb.Expr{
		GetColExpr(rewriteMetadataOutputType(ctx, columnPos), tag, int32(columnPos)),
	})
	if err != nil {
		return nodeID, err
	}

	if lower != 0 {
		schemaExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "lower", []*planpb.Expr{schemaExpr})
		if err != nil {
			return nodeID, err
		}
		tableExpr, err = BindFuncExprImplByPlanExpr(builder.GetContext(), "lower", []*planpb.Expr{tableExpr})
		if err != nil {
			return nodeID, err
		}
	}

	filters := make([]*planpb.Expr, 0, len(policies))
	for _, policy := range policies {
		notSchema, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<>", []*planpb.Expr{
			schemaExpr, makePlan2StringConstExprWithType(tree.NewCStr(policy.database, lower).Compare()),
		})
		if err != nil {
			return nodeID, err
		}
		notTable, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "<>", []*planpb.Expr{
			tableExpr, makePlan2StringConstExprWithType(tree.NewCStr(policy.table, lower).Compare()),
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
	builder.qry.HasTableRewrite = true
	return builder.appendNode(&planpb.Node{
		NodeType:     planpb.Node_FILTER,
		Children:     []int32{nodeID},
		FilterList:   []*planpb.Expr{filter},
		NotCacheable: true,
	}, ctx), nil
}

type rewriteMetadataPolicy struct {
	database string
	table    string
	names    map[string]struct{}
	all      bool
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
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
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

func rewriteVisibleColumns(stmt tree.Statement, database, table string, lower int64) (map[string]struct{}, bool, bool) {
	clause := rewriteSelectClause(stmt, false)
	if clause == nil {
		return nil, false, false
	}
	qualifier, aliased, ok := rewriteSingleTableSource(clause, database, table, "", lower)
	if !ok {
		return nil, false, false
	}
	names, all, ok := rewriteProjectionColumns(clause.Exprs, database, qualifier, aliased, lower)
	if !ok || all {
		return nil, all, ok
	}
	visible := make(map[string]struct{}, len(names))
	for _, name := range names {
		visible[name] = struct{}{}
	}
	return visible, false, true
}

// Parentheses preserve a projection. For writable rules also prove that
// every SELECT wrapper and the innermost clause preserve base-row identity.
func rewriteSelectClause(stmt tree.Statement, writable bool) *tree.SelectClause {
	for {
		switch s := stmt.(type) {
		case *tree.ParenSelect:
			if s == nil {
				return nil
			}
			stmt = s.Select
		case *tree.Select:
			if s == nil || s.With != nil || s.RewriteOption != nil || s.TimeWindow != nil || s.RankOption != nil {
				return nil
			}
			if writable && (s.IsPerform || len(s.OrderBy) != 0 || s.Limit != nil ||
				s.Ep != nil || len(s.IntoVars) != 0 || s.SelectLockInfo != nil) {
				return nil
			}
			stmt = s.Select
		case *tree.SelectClause:
			if s == nil || s.GroupBy != nil || s.Having != nil || len(s.Windows) != 0 || len(s.Exprs) == 0 {
				return nil
			}
			if writable && (s.Distinct || s.Option&^tree.QuerySpecOptionAll != 0 || len(s.IntoVars) != 0 || s.IntoExport != nil) {
				return nil
			}
			return s
		default:
			return nil
		}
	}
}

func rewriteProjectionColumns(exprs tree.SelectExprs, database, qualifier string, aliased bool, lower int64) ([]string, bool, bool) {
	matches := func(value, expected string) bool {
		return tree.NewCStr(value, lower).Compare() == tree.NewCStr(expected, lower).Compare()
	}
	var names []string
	all := false
	for _, expr := range exprs {
		switch value := expr.Expr.(type) {
		case tree.UnqualifiedStar, *tree.UnqualifiedStar:
			all = true
		case *tree.UnresolvedName:
			if value.Star {
				// A star has no column part: Parts[0] is the relation, unlike
				// ordinary columns. Restrict it to the binder's relation key.
				if value.NumParts != 1 || !matches(value.CStrParts[0].Origin(), qualifier) {
					return nil, false, false
				}
				all = true
				continue
			}
			if value.NumParts < 1 || value.NumParts > 3 || value.ColName() == "" ||
				(value.NumParts >= 2 && !matches(value.TblNameOrigin(), qualifier)) ||
				(value.NumParts == 3 && (aliased || !matches(value.DbNameOrigin(), database))) {
				return nil, false, false
			}
			if expr.As != nil && !expr.As.Empty() && !strings.EqualFold(expr.As.Origin(), value.ColNameOrigin()) {
				return nil, false, false
			}
			names = append(names, strings.ToLower(value.ColName()))
		default:
			return nil, false, false
		}
	}
	// Validate every projection, even after a star, before granting all columns.
	if all {
		return nil, true, true
	}
	return names, false, true
}

// Return the sole current source's effective relation qualifier. A matching
// spelling is not proof of the current schema for historical/external sources.
func rewriteSingleTableSource(clause *tree.SelectClause, database, table, defaultDatabase string, lower int64) (string, bool, bool) {
	if clause.From == nil || len(clause.From.Tables) != 1 {
		return "", false, false
	}
	var source tree.TableExpr = clause.From.Tables[0]
	if joined, ok := source.(*tree.JoinTableExpr); ok {
		if joined.Right != nil {
			return "", false, false
		}
		source = joined.Left
	}
	qualifier := table
	aliased := false
	if alias, ok := source.(*tree.AliasedTableExpr); ok {
		if len(alias.As.Cols) != 0 {
			return "", false, false
		}
		if alias.As.Alias != "" {
			qualifier = string(alias.As.Alias)
			aliased = true
		}
		source = alias.Expr
	}
	tableName, ok := source.(*tree.TableName)
	if !ok || tableName.AtTsExpr != nil || tableName.IcebergRef != nil || tableName.ExplicitCatalog ||
		tree.NewCStr(string(tableName.Name()), lower).Compare() != tree.NewCStr(table, lower).Compare() {
		return "", false, false
	}
	sourceDatabase := string(tableName.Schema())
	// Only callers that have proven the default database and ruled out CTE
	// shadowing may identify an unqualified source with the write target.
	if !tableName.ExplicitSchema {
		if isPlaceholderTableName(sourceDatabase, string(tableName.Name())) {
			return "", false, false
		}
		sourceDatabase = defaultDatabase
	}
	if sourceDatabase == "" || tree.NewCStr(sourceDatabase, lower).Compare() != tree.NewCStr(database, lower).Compare() {
		return "", false, false
	}
	return qualifier, aliased, true
}
