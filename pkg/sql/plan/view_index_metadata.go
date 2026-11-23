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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
)

// protectHistoricalTableListing enforces the current listing boundary while
// binding a historical system view; it never changes its persisted definition.
func protectHistoricalTableListing(ctx context.Context, schema, table, definition string, snapshot *Snapshot, stmt *tree.Select) error {
	if !IsSnapshotValid(snapshot) || !strings.EqualFold(schema, "information_schema") || !strings.EqualFold(table, "tables") ||
		sysview.InformationSchemaViewDefinitionMatches(definition, sysview.InformationSchemaTablesCurrentDDL) {
		return nil
	}
	if stmt == nil || stmt.With != nil {
		return moerr.NewNotSupported(ctx, "unsupported historical information_schema.TABLES definition")
	}
	clause := selectClauseOf(stmt)
	if clause == nil || clause.From == nil || len(clause.From.Tables) != 1 {
		return moerr.NewNotSupported(ctx, "unsupported historical information_schema.TABLES source")
	}
	source := clause.From.Tables[0]
	if join, ok := source.(*tree.JoinTableExpr); ok {
		if join.Right != nil || join.Cond != nil {
			return moerr.NewNotSupported(ctx, "unsupported historical information_schema.TABLES source")
		}
		source = join.Left
	}
	aliased, ok := source.(*tree.AliasedTableExpr)
	if !ok || aliased == nil || len(aliased.As.Cols) != 0 {
		return moerr.NewNotSupported(ctx, "unsupported historical information_schema.TABLES source")
	}
	relation, ok := aliased.Expr.(*tree.TableName)
	if !ok || relation == nil || !strings.EqualFold(string(relation.SchemaName), catalog.MO_CATALOG) ||
		!strings.EqualFold(string(relation.ObjectName), catalog.MO_TABLES) || relation.AtTsExpr != nil {
		return moerr.NewNotSupported(ctx, "unsupported historical information_schema.TABLES source")
	}
	alias := string(aliased.As.Alias)
	if alias == "" {
		alias = string(relation.ObjectName)
	}
	call := func(name string, args ...tree.Expr) *tree.FuncExpr {
		return &tree.FuncExpr{
			Func:     tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(name)),
			FuncName: tree.NewCStr(name, 1),
			Exprs:    args,
		}
	}
	column := func(name string) tree.Expr {
		return call("coalesce", tree.NewUnresolvedName(tree.NewCStr(alias, 1), tree.NewCStr(name, 1)), tree.NewNumVal("", "", false, tree.P_char))
	}
	filter := tree.NewNotExpr(call(catalog.PhysicalIndexPredicateFunction,
		column(catalog.SystemRelAttr_Name), column(catalog.SystemRelAttr_Kind), column(catalog.SystemRelAttr_ExtraInfo)))
	if clause.Where == nil {
		clause.Where = &tree.Where{Type: tree.AstWhere, Expr: filter}
	} else {
		clause.Where.Expr = tree.NewAndExpr(clause.Where.Expr, filter)
	}
	return nil
}
