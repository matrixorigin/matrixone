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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// deleteSource retains the original source identity until CTE validation and
// distinguishes an explicit alias, which can override a target qualifier.
type deleteSource struct {
	schema, table string
	explicitAlias bool
}

// CollectDeleteSourceAliases maps visible FROM names (explicit aliases or table
// names) to their original sources. Omitted schemas remain empty for CTE checks
// and the consumer's statement default.
// Derived sources have no writable base table identity and are omitted.
func CollectDeleteSourceAliases(tableRefs tree.TableExprs) map[string]deleteSource {
	aliases := make(map[string]deleteSource)
	for _, ref := range tableRefs {
		walkDMLSourceAliases(ref, "", func(alias string, source *tree.TableName, explicit bool) {
			aliases[alias] = deleteSource{schema: string(source.SchemaName), table: string(source.ObjectName), explicitAlias: explicit}
		})
	}
	return aliases
}

func walkDMLSourceAliases(expr tree.TableExpr, alias string, visit func(string, *tree.TableName, bool)) {
	switch t := expr.(type) {
	case *tree.TableName:
		explicit := alias != ""
		if !explicit {
			alias = string(t.ObjectName)
		}
		visit(alias, t, explicit)
	case *tree.AliasedTableExpr:
		if t.As.Alias != "" {
			alias = string(t.As.Alias)
		}
		walkDMLSourceAliases(t.Expr, alias, visit)
	case *tree.ParenTableExpr:
		walkDMLSourceAliases(t.Expr, alias, visit)
	case *tree.ApplyTableExpr:
		if alias == "" {
			walkDMLSourceAliases(t.Left, "", visit)
		}
	case *tree.JoinTableExpr:
		// A group alias does not identify an individual writable table.
		if alias == "" {
			walkDMLSourceAliases(t.Left, "", visit)
			walkDMLSourceAliases(t.Right, "", visit)
		}
	}
}

// ResolveDMLTargetName applies DELETE's source aliases before catalog resolution
// or temporary-table classification. An alias hit replaces even a qualified
// target's schema, while retaining its original object name as the binding alias.
// Other DML callers pass nil aliases and keep their original names unchanged.
func ResolveDMLTargetName(dbName, tableName, alias string, aliases map[string]deleteSource) (string, string, string) {
	if source, ok := aliases[tableName]; ok {
		return source.schema, source.table, tableName
	}
	return dbName, tableName, alias
}

// resolveDeleteTargetAliases validates writable source identity before either
// DELETE planner resolves catalog tables. CTEs name sources, not target aliases.
func resolveDeleteTargetAliases(ctx CompilerContext, stmt *tree.Delete) (map[string]deleteSource, error) {
	aliases := CollectDeleteSourceAliases(stmt.TableRefs)
	var ctes map[string]bool
	if stmt.With != nil {
		ctes = make(map[string]bool, len(stmt.With.CTEs))
		for _, cte := range stmt.With.CTEs {
			ctes[string(cte.Name.Alias)] = true
		}
	}
	for _, target := range stmt.Tables {
		if aliased, ok := target.(*tree.AliasedTableExpr); ok {
			target = aliased.Expr
		}
		name, ok := target.(*tree.TableName)
		if !ok {
			return nil, moerr.NewInvalidInput(ctx.GetContext(), "unsupported DELETE target")
		}
		dbName, tableName := string(name.SchemaName), string(name.ObjectName)
		source := deleteSource{schema: dbName, table: tableName}
		if len(stmt.TableRefs) != 0 {
			var found bool
			source, found = aliases[tableName]
			if !found {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "missing FROM-clause entry for table '%s'", tableName)
			}
		}
		if source.schema == "" && ctes[source.table] {
			return nil, moerr.NewInvalidInputf(ctx.GetContext(), "cannot delete from CTE %s", source.table)
		}
		// Explicit aliases retain the existing qualified-target precedence. A plain
		// table name, however, cannot redirect a qualified target to another schema.
		if dbName != "" && !source.explicitAlias {
			sourceDB := source.schema
			if sourceDB == "" {
				sourceDB = ctx.DefaultDatabase()
			}
			if dbName != sourceDB || tableName != source.table {
				return nil, moerr.NewInvalidInputf(ctx.GetContext(), "DELETE target %s.%s does not match its source", dbName, tableName)
			}
		}
	}
	return aliases, nil
}
